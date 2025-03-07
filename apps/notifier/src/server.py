"""Server that sends notifications to users."""

import asyncio
import contextlib
import datetime
import logging
import pickle
from contextlib import asynccontextmanager
from pathlib import Path

import fastapi
import prometheus_client
from async_utils import schedule_task
from custom_types import BlueskyDid, BlueskyRKey, BlueskyUri, bluesky_uri
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from . import firestore
from .bluesky import get_post
from .notify import process_post
from .prometheus import PROCESSED_POST_SIZE, RECEIVED_MESSAGES

logger = logging.getLogger(__name__)

MAX_NOTIFICATION_TIME = datetime.timedelta(hours=1)

# Settings for all users in bluenotify
USER_SETTINGS = firestore.AllFollowSettings()

# All posts that have been processed, with timestamps, so we can avoid duplicates
PROCESSED_POSTS: dict[BlueskyUri, datetime.datetime] = {}

PROCESSED_POST_FILE = Path("processed_posts.pickle")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Run the lifespan of the app."""
    load_processed_posts()
    USER_SETTINGS.listen_to_changes()
    schedule_task(clean_processed_posts())
    yield
    save_processed_posts()


def load_processed_posts():
    """Load the processed posts from disk."""
    if PROCESSED_POST_FILE.exists():
        data = pickle.load(PROCESSED_POST_FILE.open("rb"))
        PROCESSED_POSTS.update(data)


def save_processed_posts():
    """Save the processed posts to disk."""
    pickle.dump(PROCESSED_POSTS, PROCESSED_POST_FILE.open("wb"))


app = FastAPI(lifespan=lifespan)


@app.get("/")
def read_root():
    """Check if the server is online."""
    return "Server online."


@app.get("/users_to_watch")
async def get_users():
    """Get the users that have notifications enabled for them."""
    return USER_SETTINGS.users_to_watch


@app.websocket("/users_to_watch")
async def stream_users(websocket: WebSocket):
    """Stream events for when users to watch are added or removed."""
    with contextlib.suppress(WebSocketDisconnect):
        await websocket.accept()
        last_users_set = USER_SETTINGS.users_to_watch.copy()

        for user in last_users_set:
            await websocket.send_json({"action": "add", "user": user})
        while True:
            await USER_SETTINGS.changed.wait()

            new_set = USER_SETTINGS.users_to_watch.copy()
            for user in new_set - last_users_set:
                await websocket.send_json({"action": "add", "user": user})
            for user in last_users_set - new_set:
                await websocket.send_json({"action": "remove", "user": user})
            last_users_set = new_set


@app.post("/post/{did}/{rkey}")
async def post(did: BlueskyDid, rkey: BlueskyRKey):
    """Post a message to a user."""
    uri = bluesky_uri(did, rkey)
    if uri in PROCESSED_POSTS:
        logger.info(f"ignoring processed post: {uri}")
        return

    RECEIVED_MESSAGES.inc()

    PROCESSED_POSTS[uri] = datetime.datetime.now(datetime.UTC)
    save_processed_posts()

    post = await get_post(did, rkey)

    schedule_task(process_post(post, USER_SETTINGS))


@app.post("/repost/{did}/{rkey}")
async def repost(did: BlueskyDid, rkey: BlueskyRKey):
    """Post a message to a user."""
    uri = bluesky_uri(did, rkey)
    if uri in PROCESSED_POSTS:
        logger.info(f"ignoring processed post: {uri}")
        return

    RECEIVED_MESSAGES.inc()

    PROCESSED_POSTS[uri] = datetime.datetime.now(datetime.UTC)
    save_processed_posts()

    post = await get_post(did, rkey, repost=True)

    schedule_task(process_post(post, USER_SETTINGS))


@app.get("/metrics")
async def metrics():
    """Get the metrics for the server."""
    return fastapi.responses.PlainTextResponse(prometheus_client.generate_latest())


async def clean_processed_posts():
    """Remove old posts from the processed posts."""
    while True:
        await asyncio.sleep(60)
        now = datetime.datetime.now(datetime.UTC)
        for uri, time in list(PROCESSED_POSTS.items()):
            if now - time > MAX_NOTIFICATION_TIME:
                PROCESSED_POSTS.pop(uri, None)

        PROCESSED_POST_SIZE.set(len(PROCESSED_POSTS))
