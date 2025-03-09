use again::RetryPolicy;
use async_nats::jetstream::kv::Store;
use async_nats::jetstream::stream::Stream;
use async_nats::jetstream::Message;
use axum::{routing::get, Router};
use bluesky_utils::{bluesky_browseable_url, parse_created_at, parse_uri};
use cached::proc_macro::cached;
use futures::TryStreamExt;
use lazy_static::lazy_static;
use prometheus::{
    self, register_histogram, register_int_counter, register_int_gauge, Encoder, Histogram,
    IntCounter, IntGauge, TextEncoder,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tokio::time::timeout;
use tracing::{debug, error, info, span, warn, Instrument, Level};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;
use user_settings::{AllUserSettings, UserSettingsMap};
mod fcm;
use crate::fcm::FcmClient;
use url::Url;

const OLDEST_POST_AGE: chrono::Duration = chrono::Duration::hours(2);
lazy_static! {
    static ref RECEIVED_MESSAGES_COUNTER: IntCounter = register_int_counter!(
        "received_messages",
        "The number of messages received by the server"
    )
    .unwrap();
    static ref NOTIFICATIONS_SENT_COUNTER: IntCounter = register_int_counter!(
        "notifications_sent",
        "The number of notifications sent by the server"
    )
    .unwrap();
    static ref POST_HANDLE_TIME: Histogram = register_histogram!(
        "post_handle_time_seconds",
        "Time spent handling a post",
        vec![
            0.5, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 12.5, 15.0, 17.5, 20.0, 30.0,
            40.0, 50.0, 60.0, 120.0, 180.0, 240.0, 300.0, 360.0
        ]
    )
    .unwrap();
    static ref TOKIO_ALIVE_TASKS: IntGauge = register_int_gauge!(
        "tokio_alive_tasks",
        "The number of living tasks in the tokio runtime"
    )
    .unwrap();
}

type SharedUserSettings = Arc<RwLock<AllUserSettings>>;

#[derive(Debug, Clone, Deserialize, Serialize)]
struct JetstreamPost {
    did: String,
    time_us: u64, // microseconds timestamp
    commit: Commit,
}

impl JetstreamPost {
    fn post_datetime(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        let record = &self.commit.record;
        let created_at = match record {
            Record::Post(post) => &post.created_at,
            Record::Repost(repost) => &repost.created_at,
        };
        let result = parse_created_at(created_at);
        if result.is_err() {
            return None;
        }
        Some(result.unwrap())
    }

    fn event_datetime(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        chrono::DateTime::from_timestamp_micros(self.time_us as i64)
    }

    fn post_id(&self) -> String {
        let rkey = &self.commit.rkey;
        let did = &self.did;
        format!("{}:{}", did, rkey)
    }

    fn parse_raw_json(
        json_string: &str,
    ) -> Result<JetstreamPost, Box<dyn std::error::Error + Send + Sync>> {
        let json: Value = serde_json::from_str(json_string)?;
        Self::parse_value(json)
    }

    fn parse_value(json: Value) -> Result<JetstreamPost, Box<dyn std::error::Error + Send + Sync>> {
        let did = json["did"].as_str();
        if did.is_none() {
            return Err("did is missing".into());
        }
        let did = did.unwrap().to_string();
        let time_us = json["time_us"].as_u64();
        if time_us.is_none() {
            return Err("time_us is missing".into());
        }
        let time_us = time_us.unwrap();
        let rkey = json["commit"]["rkey"].as_str();
        if rkey.is_none() {
            return Err("rkey is missing".into());
        }
        let rkey = rkey.unwrap().to_string();
        let raw_record = json["commit"]["record"].clone();

        let record = if raw_record["$type"] == "app.bsky.feed.post" {
            Record::Post(serde_json::from_value(raw_record)?)
        } else if raw_record["$type"] == "app.bsky.feed.repost" {
            Record::Repost(serde_json::from_value(raw_record)?)
        } else {
            return Err("unknown record type".into());
        };
        Ok(JetstreamPost {
            did,
            time_us,
            commit: Commit { rkey, record },
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
enum PostType {
    Post,
    Repost,
    Reply,
    Quote,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Commit {
    // rev: String,
    // operation: String,
    // collection: String,
    rkey: String,
    // cid: String,
    record: Record,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
enum Record {
    Post(PostRecord),
    Repost(RepostRecord),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct PostRecord {
    #[serde(alias = "$type")]
    record_type: String,
    #[serde(alias = "createdAt")]
    created_at: String,
    text: String,
    embed: Option<Embed>,
    reply: Option<ReplyData>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct RepostRecord {
    #[serde(alias = "$type")]
    record_type: String,
    #[serde(alias = "createdAt")]
    created_at: String,
    subject: RecordReference,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct RecordReference {
    cid: String,
    uri: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Embed {
    #[serde(alias = "$type")]
    embed_type: String,
    record: Option<Value>,
    media: Option<Media>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Media {
    #[serde(alias = "$type")]
    media_type: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ReplyData {
    parent: RecordReference,
}

async fn get_bluesky_display_name_and_handle(
    did: &str,
    client: Option<reqwest::Client>,
    cache: Option<Store>,
) -> Result<(String, String), Box<dyn std::error::Error + Send + Sync>> {
    let cache_key = format!("display_name.{}", did.replace(":", "_"));

    if let Some(cache) = cache.clone() {
        debug!(
            "Checking cache for handle for {:?}: Key: {:?}",
            did, cache_key
        );
        let cached = cache.get(&cache_key).await;
        if let Ok(cached) = cached {
            if let Some(cached) = cached {
                let cached = String::from_utf8(cached.into());
                if cached.is_err() {
                    let msg: String = cached.unwrap_err().to_string();
                    error!("Error getting handle, corrupt cache: {}", msg);
                } else {
                    debug!("Using cached handle for {:?}", did);
                    let (display, handle): (String, String) =
                        serde_json::from_str(&cached.unwrap()).unwrap();
                    return Ok((display, handle));
                }
            } else {
                debug!("No cached handle for {:?}", did);
            }
        } else {
            let msg: String = cached.unwrap_err().to_string();
            error!("Error getting handle from cache: {}", msg);
        }
    }

    debug!("Getting handle for {:?}", did);
    let start = chrono::Utc::now();
    let url = format!(
        "https://public.api.bsky.app/xrpc/app.bsky.actor.getProfile?actor={}",
        did
    );
    let client = client.unwrap_or_else(|| reqwest::Client::new());
    let response = client.get(&url).send().await;
    if response.is_err() {
        let msg: String = response.unwrap_err().to_string();
        error!("Error getting handle: {}", msg);
        return Err(msg.into());
    }
    let response = response.unwrap();
    if !response.status().is_success() {
        let msg: String = response.error_for_status().unwrap_err().to_string();
        error!("Error getting handle: {}", msg);
        return Err(msg.into());
    }
    let json: Value = response.json().await.unwrap();
    let handle = json["handle"].as_str().unwrap_or("[No handle]");
    let mut display = json["displayName"].as_str().unwrap_or("");

    if display.is_empty() {
        display = handle;
    }

    let end = chrono::Utc::now();
    let duration = end - start;
    debug!("Got handle for {:?} in {:?}", did, duration);

    let result = (display.to_string(), handle.to_string());

    if let Some(cache) = cache {
        _ = cache
            .put(&cache_key, serde_json::to_string(&result).unwrap().into())
            .await;
    }

    Ok(result)
}

async fn get_following(
    did: &str,
    client: Option<reqwest::Client>,
    cache: Option<Store>,
) -> Result<HashSet<String>, Box<dyn std::error::Error + Send + Sync>> {
    let cache_key = format!("following.{}", did.replace(":", "_"));

    if let Some(cache) = cache.clone() {
        debug!(
            "Checking cache for following for {:?}: Key: {:?}",
            did, cache_key
        );
        let cached = cache.get(&cache_key).await;
        if let Ok(cached) = cached {
            if let Some(cached) = cached {
                let cached = String::from_utf8(cached.into());
                if cached.is_err() {
                    let msg: String = cached.unwrap_err().to_string();
                    error!("Error getting following, corrupt cache: {}", msg);
                } else {
                    debug!("Using cached following for {:?}", did);
                    let following: HashSet<String> =
                        serde_json::from_str(&cached.unwrap()).unwrap();
                    return Ok(following);
                }
            } else {
                debug!("No cached following for {:?}", did);
            }
        } else {
            let msg: String = cached.unwrap_err().to_string();
            error!("Error getting following from cache: {}", msg);
        }
    }

    debug!("Getting following for {:?}", did);
    let start = chrono::Utc::now();
    let client = client.unwrap_or_else(|| reqwest::Client::new());

    // First check and make sure they have less than 10,000 following, otherwise ignore
    // since this will choke the post in our system
    let profile_url = format!(
        "https://public.api.bsky.app/xrpc/app.bsky.actor.getProfile?actor={}",
        did
    );

    let response = client.get(&profile_url).send().await;
    if response.is_err() {
        let msg: String = response.unwrap_err().to_string();
        error!("Error getting profile: {}", msg);
        return Err(msg.into());
    }
    let response = response.unwrap();
    if !response.status().is_success() {
        let msg: String = response.error_for_status().unwrap_err().to_string();
        error!("Error getting profile: {}", msg);
        return Err(msg.into());
    }

    let json: Result<Value, _> = response.json().await;
    let mut following_count = 0;
    if let Ok(json) = json {
        following_count = json["followsCount"].as_u64().unwrap_or(0);
    }
    if following_count > 10_000 {
        return Ok(HashSet::new());
    }

    let mut cursor: Option<String> = None;
    let mut following = HashSet::new();
    loop {
        let url = format!(
            "https://public.api.bsky.app/xrpc/app.bsky.graph.getFollows?actor={}&limit=100&cursor={}",
            did,
            cursor.clone().unwrap_or_default()
        );
        debug!("Getting following from {:?}", url);
        let response = client.get(&url).send().await;
        if response.is_err() {
            let msg: String = response.unwrap_err().to_string();
            error!("Error getting following: {}", msg);
            return Err(msg.into());
        }
        let response = response.unwrap();
        if !response.status().is_success() {
            let msg: String = response.error_for_status().unwrap_err().to_string();
            error!("Error getting following: {}", msg);
            return Err(msg.into());
        }
        let json: Value = response.json().await.unwrap();
        for follower in json["follows"].as_array().unwrap() {
            following.insert(follower["did"].as_str().unwrap().to_string());
        }
        let cursor_value = json["cursor"].as_str();
        if cursor_value.is_none() {
            break;
        }
        cursor = Some(cursor_value.unwrap().to_string());
    }

    let end = chrono::Utc::now();
    let duration = end - start;
    debug!("Got following for {:?} in {:?}", did, duration);

    if let Some(cache) = cache {
        _ = cache
            .put(
                &cache_key,
                serde_json::to_string(&following).unwrap().into(),
            )
            .await;
    }

    Ok(following)
}

async fn load_bluesky_post(
    uri: &str,
    client: Option<reqwest::Client>,
) -> Result<JetstreamPost, Box<dyn std::error::Error + Send + Sync>> {
    let url = format!(
        "https://public.api.bsky.app/xrpc/app.bsky.feed.getPosts?uris={}",
        uri
    );
    let client = client.unwrap_or_else(|| reqwest::Client::new());
    let response = client.get(&url).send().await;
    if response.is_err() {
        let msg: String = response.unwrap_err().to_string();
        error!("Error getting post: {}", msg);
        return Err(msg.into());
    }
    let response = response.unwrap();
    if !response.status().is_success() {
        let msg: String = response.error_for_status().unwrap_err().to_string();
        error!("Error getting post: {}", msg);
        return Err(msg.into());
    }
    let raw_json_response: Value = response.json().await.unwrap();
    let record: Result<PostRecord, _> =
        serde_json::from_value(raw_json_response["posts"][0]["record"].clone());
    if record.is_err() {
        error!("Error deserializing post: {:?}", record);
        return Err(format!("Error deserializing post: {:?}", record).into());
    }
    let (did, rkey) = parse_uri(uri).unwrap_or(("None".to_string(), "None".to_string()));

    let post = JetstreamPost {
        did,
        time_us: 0,
        commit: Commit {
            rkey,
            record: Record::Post(record.unwrap()),
        },
    };

    Ok(post)
}

async fn check_possible_recipient(
    possible_recipient: String,
    poster_did: String,
    user_settings: SharedUserSettings,
    post_type: PostType,
    post: &JetstreamPost,
    retry: RetryPolicy,
    client: Option<reqwest::Client>,
    cache: Store,
) -> (String, bool) {
    let user_settings = {
        let all_settings = user_settings.read().await;
        if !all_settings.settings.contains_key(&possible_recipient) {
            return (possible_recipient, false);
        }
        all_settings
            .settings
            .get(&possible_recipient)
            .unwrap()
            .clone()
    };

    let wanted_post_types = &user_settings.settings.get(&poster_did).unwrap().post_types;

    match post_type {
        PostType::Post | PostType::Quote => {
            if !wanted_post_types.contains(&user_settings::PostType::Post) {
                return (possible_recipient, false);
            }
        }
        PostType::Repost => {
            if !wanted_post_types.contains(&user_settings::PostType::Repost) {
                return (possible_recipient, false);
            }
        }
        PostType::Reply => {
            if wanted_post_types.contains(&user_settings::PostType::Reply) {
                return (possible_recipient, true);
            } else if wanted_post_types.contains(&user_settings::PostType::ReplyToFriend) {
                let parent_poster_did = match &post.commit.record {
                    Record::Post(record) => {
                        let uri = record.reply.as_ref().unwrap().parent.uri.clone();
                        parse_uri(&uri).unwrap().0
                    }
                    _ => return (possible_recipient, false),
                };
                let mut following = HashSet::new();
                debug!(
                    "Checking if {:?} follows {:?}",
                    possible_recipient, parent_poster_did
                );
                for bluesky_account_did in user_settings.accounts.iter() {
                    let new_following = retry
                        .retry(|| {
                            timeout(
                                Duration::from_secs(60),
                                get_following(
                                    bluesky_account_did,
                                    client.clone(),
                                    Some(cache.clone()),
                                ),
                            )
                        })
                        .await;
                    if new_following.is_err() {
                        error!("Timed out getting following for {:?}!", bluesky_account_did);
                        continue;
                    }
                    let new_following = new_following.unwrap().unwrap_or_default();
                    following.extend(new_following);
                }
                if !following.contains(&parent_poster_did) {
                    return (possible_recipient, false);
                }
            } else {
                return (possible_recipient, false);
            }
        }
    };
    (possible_recipient, true)
}

async fn process_post(
    post: JetstreamPost,
    nats_message: Option<Message>,
    user_settings: SharedUserSettings,
    fcm_client: Option<Arc<FcmClient>>,
    cache: Store,
) {
    info!("Processing post: {:?}", post.post_id());
    let poster_did = post.did.clone();

    let client = reqwest::Client::new();

    let post_type = match &post.commit.record {
        Record::Post(record) => {
            if record.reply.is_some() {
                PostType::Reply
            } else if record.embed.is_some()
                && record
                    .embed
                    .as_ref()
                    .unwrap()
                    .embed_type
                    .contains("app.bsky.embed.record")
            {
                PostType::Quote
            } else {
                PostType::Post
            }
        }
        Record::Repost(_) => PostType::Repost,
    };

    let retry = RetryPolicy::fixed(Duration::from_millis(5000))
        .with_max_retries(10)
        .with_jitter(false);

    info!("Post type: {:?}", post_type);

    // check if this post has any interested listeners BEFORE downloading anything else
    let fcm_recipients: HashSet<String> = {
        let mut fcm_recipients = {
            user_settings
                .read()
                .await
                .fcm_tokens_by_watched
                .get(&poster_did)
                .cloned()
                .unwrap_or_default()
        };

        let mut task_group = tokio::task::JoinSet::new();
        for possible_recipient in fcm_recipients.iter() {
            let user_settings = user_settings.clone();
            let poster_did = poster_did.clone();
            let post_type = post_type.clone();
            let post = post.clone();
            let possible_recipient = possible_recipient.clone();
            let retry = retry.clone();
            let client = client.clone();
            let cache = cache.clone();
            task_group.spawn(async move {
                check_possible_recipient(
                    possible_recipient,
                    poster_did,
                    user_settings,
                    post_type,
                    &post,
                    retry.clone(),
                    Some(client.clone()),
                    cache.clone(),
                )
                .await
            });
        }

        while let Some(result) = task_group.join_next().await {
            let (recipient, should_notify) = result.unwrap();
            if !should_notify {
                fcm_recipients.remove(&recipient);
            }
        }

        fcm_recipients
    };

    if fcm_recipients.is_empty() {
        info!("No interested listeners for this post");
        if nats_message.is_some() {
            let ack = nats_message.unwrap().ack().await;
            if ack.is_err() {
                error!("Error acknowledging message: {:?}", ack);
                return;
            }
        }
        return;
    }
    info!("Interested listeners found: {:?}", fcm_recipients);

    let user_name = retry
        .retry(|| {
            timeout(
                Duration::from_secs(60 * 1),
                get_bluesky_display_name_and_handle(
                    &poster_did,
                    Some(client.clone()),
                    Some(cache.clone()),
                ),
            )
        })
        .await
        .unwrap_or(Ok(("[error loading user]".to_string(), "".to_string())))
        .unwrap_or(("[error loading user]".to_string(), "".to_string()))
        .0;

    let mut notification_title;
    let mut source_post = post.clone();

    match post_type {
        PostType::Post => {
            notification_title = format!("{} posted:", user_name);
        }
        PostType::Repost => {
            notification_title = format!("{} reposted:", user_name);
            match &source_post.commit.record {
                Record::Repost(record) => {
                    source_post = match retry
                        .retry(|| {
                            timeout(
                                Duration::from_secs(60 * 1),
                                load_bluesky_post(&record.subject.uri, Some(client.clone())),
                            )
                        })
                        .await
                    {
                        Ok(post) => match post {
                            Ok(post) => post,
                            Err(e) => {
                                error!("Error loading Repost: {:?}", e);
                                return;
                            }
                        },
                        Err(e) => {
                            error!("Loading Repost timed out: {:?}", e);
                            return;
                        }
                    };
                }
                Record::Post(_) => {
                    return;
                }
            };
        }
        PostType::Quote => {
            notification_title = format!("{} quote posted:", user_name);
        }
        PostType::Reply => {
            // fallback title
            notification_title = format!("{} replied:", user_name);
            match &source_post.commit.record {
                Record::Post(record) => {
                    let reply_data = record.reply.as_ref().unwrap();
                    let uri = &reply_data.parent.uri;
                    let source_did = parse_uri(uri).unwrap().0;
                    let other_username = retry
                        .retry(|| {
                            timeout(
                                Duration::from_secs(60 * 1),
                                get_bluesky_display_name_and_handle(
                                    &source_did,
                                    Some(client.clone()),
                                    Some(cache.clone()),
                                ),
                            )
                        })
                        .await
                        .unwrap_or(Ok(("[error loading user]".to_string(), "".to_string())))
                        .unwrap_or(("[error loading user]".to_string(), "".to_string()))
                        .0;
                    notification_title = format!("{} replied to {}:", user_name, other_username);
                }
                _ => {}
            }
        }
    }

    let source_post_record = match &source_post.commit.record {
        Record::Post(record) => record,
        Record::Repost(_) => return, // Shouldn't happen, can't repost a repost, and we should have loaded the source
    };

    let mut notification_body = source_post_record.text.clone();

    if notification_body.is_empty() && source_post_record.embed.is_some() {
        let mut media_type = source_post_record
            .embed
            .as_ref()
            .unwrap()
            .embed_type
            .clone();
        // if the user is quoting a post, embed is nested
        if source_post_record.embed.as_ref().unwrap().media.is_some() {
            media_type = source_post_record
                .embed
                .as_ref()
                .unwrap()
                .media
                .as_ref()
                .unwrap()
                .media_type
                .clone();
        }
        if media_type.contains("image") {
            notification_body = "[image]".to_string();
        } else if media_type.contains("video") {
            notification_body = "[video]".to_string();
        } else if media_type.contains("external") {
            notification_body = "[link]".to_string();
        }
    }

    let post_owner_handle = retry
        .retry(|| {
            timeout(
                Duration::from_secs(60 * 1),
                get_bluesky_display_name_and_handle(
                    &source_post.did,
                    Some(client.clone()),
                    Some(cache.clone()),
                ),
            )
        })
        .await
        .unwrap_or(Ok(("".to_string(), (&source_post.did.clone()).to_owned())))
        .unwrap_or(("".to_string(), (&source_post.did.clone()).to_owned()))
        .1;

    let rkey = source_post.commit.rkey.clone();

    let url = bluesky_browseable_url(&post_owner_handle, &rkey);

    if nats_message.is_some() {
        let ack = nats_message.unwrap().ack().await;
        if ack.is_err() {
            error!("Error acknowledging message: {:?}", ack);
            return;
        }
    }
    NOTIFICATIONS_SENT_COUNTER.inc_by(fcm_recipients.len() as u64);

    let post_time = post.event_datetime();
    if let Some(post_time) = post_time {
        let handle_time = (chrono::Utc::now() - post_time).num_milliseconds() as f64 / 1000.0;
        info!("Post handle time: {:?}", handle_time);
        POST_HANDLE_TIME.observe(handle_time);
    }

    send_notification(
        fcm_client,
        fcm_recipients,
        notification_title,
        notification_body,
        url,
    )
    .await;
}

async fn send_notification(
    fcm_client: Option<Arc<FcmClient>>,
    fcm_recipients: HashSet<String>,
    title: String,
    body: String,
    url: String,
) {
    info!("Sending notification to {} users", fcm_recipients.len());
    info!("Title: {}", title);
    info!("Body: {}", body);
    info!("URL: {}", url);
    let mock = std::env::var("MOCK")
        .unwrap_or("false".to_string())
        .to_ascii_lowercase()
        == "true";
    if mock || fcm_client.is_none() {
        warn!("Mock mode, will not send notification!");
        return;
    }
    let fcm_client = fcm_client.unwrap();
    let mut message = serde_json::json!({
        "validate_only": false,
        "message": {
            "notification": {
                "title": title,
                "body": body
            },
            "data": {
                "url": url,
                "type": "post",
                "text": body,
                "post_user_did": "n/a",
                "post_user_handle": "n/a",
                "post_id": "n/a",
            },
            "android": {
                "priority": "high",
                "notification": {
                    "proxy": "DENY",
                }
            },
            "apns": {
                "payload": {
                    "aps": {
                        "alert": {
                            "title": title,
                            "body": body
                        },
                        "sound": "default"
                    }
                }
            }
        }
    });
    for fcm_token in fcm_recipients {
        message["message"]["token"] = Value::String(fcm_token);
        let response = fcm_client.send(message.clone()).await;
        if response.is_err() {
            error!("Error sending notification: {:?}", response);
        }
    }
}

async fn listen_to_posts(
    posts_stream: Stream,
    user_settings: SharedUserSettings,
    fcm_client: Arc<FcmClient>,
    cache: Store,
) {
    let consumer = posts_stream
        .get_or_create_consumer(
            "watched_posts",
            async_nats::jetstream::consumer::pull::Config {
                durable_name: Some("notifier_post_listener".to_string()),
                deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::New,
                ack_wait: Duration::from_secs(60 * 5),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let metrics = Handle::current().metrics();

    loop {
        let mut messages = consumer.messages().await.unwrap();
        while let Ok(Some(message)) = messages.try_next().await {
            let json_string = String::from_utf8_lossy(message.payload.as_ref());
            let post: Result<JetstreamPost, _> = JetstreamPost::parse_raw_json(&json_string);
            if post.is_err() {
                error!(
                    "Failed to deserialize post: {:?}\nData: {}",
                    post, json_string
                );
                _ = message.ack().await;
                continue;
            }
            info!("Received post: {:?}", post);
            RECEIVED_MESSAGES_COUNTER.inc();

            let post = post.unwrap();
            let post_datetime = post.post_datetime();
            if post_datetime.is_none() {
                error!("Error parsing post datetime");
                _ = message.ack().await;
                continue;
            }
            let now = chrono::Utc::now();
            if now - post_datetime.unwrap() > OLDEST_POST_AGE {
                // this post is too old, ignore it
                info!("Post is too old, ignoring");
                _ = message.ack().await;
                continue;
            }
            let post_id = post.post_id();
            tokio::spawn({
                let user_settings = user_settings.clone();
                let fcm_client = fcm_client.clone();
                let cache = cache.clone();
                debug!("Spawning process_post");
                async move {
                    let result = timeout(
                        Duration::from_secs(60 * 5),
                        process_post(post, Some(message), user_settings, Some(fcm_client), cache)
                            .instrument(span!(Level::INFO, "process", post_id = post_id.as_str())),
                    )
                    .await;
                    if result.is_err() {
                        error!("Processing timed out after 5 minutes: {:?}", result);
                    }
                }
            });

            TOKIO_ALIVE_TASKS.set(metrics.num_alive_tasks() as i64);
        }
    }
}

async fn initial_load_user_settings(kv_store: &Store, current_settings: SharedUserSettings) {
    let value = kv_store.get("user_settings").await.unwrap();
    if value.is_none() {
        return;
    }
    let value = value.unwrap();
    let new_settings: Result<UserSettingsMap, _> = serde_json::from_slice(value.as_ref());
    if new_settings.is_err() {
        error!("Error deserializing user settings: {:?}", new_settings);
        return;
    }
    let mut current_settings = current_settings.write().await;
    current_settings.settings.clear();
    current_settings.settings.extend(new_settings.unwrap());
    current_settings.rebuild_cache();
    info!(
        "Loaded settings. Length: {:?}",
        current_settings.settings.len()
    );
}

async fn listen_to_user_settings(kv_store: Store, current_settings: SharedUserSettings) {
    loop {
        let messages = kv_store.watch("user_settings").await;
        if messages.is_err() {
            error!("Error watching watched_users");
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }
        let mut messages = messages.unwrap();
        while let Ok(Some(message)) = messages.try_next().await {
            let new_settings: Result<UserSettingsMap, _> =
                serde_json::from_slice(message.value.as_ref());
            if new_settings.is_err() {
                error!("Error deserializing user settings: {:?}", new_settings);
                continue;
            }
            info!("Received new settings!");
            let mut current_settings = current_settings.write().await;
            current_settings.settings.clear();
            current_settings.settings.extend(new_settings.unwrap());
            current_settings.rebuild_cache();
            info!(
                "Updated settings. Length: {:?}",
                current_settings.settings.len()
            );
        }
    }
}

async fn metrics() -> String {
    let mut buffer = vec![];
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

async fn _main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let loki_url = std::env::var("LOKI_URL");
    if let Ok(loki_url) = loki_url {
        let environment = std::env::var("ENVIRONMENT").unwrap_or("dev".to_string());
        let (layer, task) = tracing_loki::builder()
            .label("environment", environment)?
            .label("service_name", "notifier")?
            .extra_field("pid", format!("{}", std::process::id()))?
            .build_url(Url::parse(&loki_url).unwrap())?;

        tracing_subscriber::registry()
            .with(layer.with_filter(tracing_subscriber::filter::EnvFilter::from_default_env()))
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::stdout)
                    .with_filter(tracing_subscriber::filter::EnvFilter::from_default_env()),
            )
            .init();

        tokio::spawn(task);
        tracing::info!("Notifier starting, loki tracing enabled.");
    } else {
        error!("LOKI_URL not set, will not send logs to Loki");
        tracing_subscriber::fmt::init();
    }

    TOKIO_ALIVE_TASKS.set(0);
    RECEIVED_MESSAGES_COUNTER.reset();
    NOTIFICATIONS_SENT_COUNTER.reset();

    let mut nats_host = std::env::var("NATS_HOST").unwrap_or("localhost".to_string());
    if !nats_host.contains(':') {
        nats_host.push_str(":4222");
    }

    let service_account_path = "./cert.json";
    // Create a new FCM client
    let fcm_client = Arc::new(FcmClient::new(service_account_path).await?);

    let axum_url = std::env::var("BIND_NOTIFIER").unwrap_or("0.0.0.0:8003".to_string());

    info!("Connecting to NATS at {}", nats_host);
    let nats_client = async_nats::connect(nats_host).await?;
    let nats_js = async_nats::jetstream::new(nats_client);

    // Get key/value store
    _ = nats_js
        .create_key_value(async_nats::jetstream::kv::Config {
            bucket: "bluenotify_kv_store".to_string(),
            history: 1,
            ..Default::default()
        })
        .await;
    let kv_store = nats_js.get_key_value("bluenotify_kv_store").await.unwrap();

    // Get cache
    _ = nats_js
        .create_key_value(async_nats::jetstream::kv::Config {
            bucket: "bluenotify_cache".to_string(),
            history: 1,
            max_age: Duration::from_secs(60 * 60 * 1),
            ..Default::default()
        })
        .await;
    let cache = nats_js.get_key_value("bluenotify_cache").await.unwrap();

    let user_settings: SharedUserSettings =
        Arc::new(RwLock::new(AllUserSettings::new(HashMap::new())));

    initial_load_user_settings(&kv_store, user_settings.clone()).await;

    let posts_stream = nats_js.get_stream("watched_posts").await?;

    let axum_app = Router::new()
        .route("/", get(|| async { "Notifier server online." }))
        .route("/metrics", get(metrics));
    info!("Listening for HTTP requests on {}", axum_url);
    let axum_listener = tokio::net::TcpListener::bind(axum_url).await.unwrap();

    let mut tasks: JoinSet<_> = JoinSet::new();
    tasks.spawn(async move {
        axum::serve(axum_listener, axum_app).await.unwrap();
    });
    let shared_settings = user_settings.clone();
    tasks.spawn(async move {
        listen_to_posts(posts_stream, shared_settings, fcm_client.clone(), cache).await;
    });
    let shared_settings = user_settings.clone();
    tasks.spawn(async move {
        listen_to_user_settings(kv_store, shared_settings).await;
    });

    tasks.join_all().await;
    Ok(())
}

fn main() {
    let sentry_dsn = std::env::var("SENTRY_DSN");
    if sentry_dsn.is_ok() && !sentry_dsn.as_ref().unwrap().is_empty() {
        let _guard = sentry::init((
            sentry_dsn.ok(),
            sentry::ClientOptions {
                release: sentry::release_name!(),
                ..Default::default()
            },
        ));
    }

    _ = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(_main());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_username() {
        let did = "did:plc:jpkjnmydclkafjyicv3s6hcx";
        let handle = get_bluesky_display_name_and_handle(did, None, None).await;
        assert_eq!(handle.unwrap().1, "austinwitherspoon.com");
    }
    #[tokio::test]
    async fn test_follows() {
        let did = "did:plc:jpkjnmydclkafjyicv3s6hcx";
        let follows = get_following(did, None, None).await;
        println!("{:?}", follows);
        assert!(follows.unwrap().len() > 60);

        // Test somebody with way too many following, ignore
        let did = "did:plc:w3xevyycvef7y4tqsojptrf5";
        let follows = get_following(did, None, None).await;
        println!("{:?}", follows);
        assert!(follows.unwrap().len() == 0);
    }
    #[tokio::test]
    async fn test_get_post() {
        let uri = "at://did:plc:jpkjnmydclkafjyicv3s6hcx/app.bsky.feed.post/3lhl4k52fek22";
        let post = load_bluesky_post(uri, None).await;
        println!("{:?}", post);
        assert!(post.is_ok());
    }

    #[test]
    fn test_models() {
        let inputs = vec![
            r#"{"did":"did:plc:vhwscbpufmtoekc5hyz73vpa","time_us":1739074582960891,"kind":"commit","commit":{"rev":"3lhprhfyklu24","operation":"create","collection":"app.bsky.feed.post","rkey":"3lhprhfeijk27","record":{"$type":"app.bsky.feed.post","createdAt":"2025-02-09T04:16:22.163Z","embed":{"$type":"app.bsky.embed.recordWithMedia","media":{"$type":"app.bsky.embed.images","images":[{"alt":"","aspectRatio":{"height":670,"width":739},"image":{"$type":"blob","ref":{"$link":"bafkreifyetr44wuxhi7gjzrcnvjpligg2awcvycml37tktvptfh6ylua4q"},"mimeType":"image/jpeg","size":392305}}]},"record":{"$type":"app.bsky.embed.record","record":{"cid":"bafyreibbg2leljyfcurxfnscsozon7h7ta4hetheopctcejdsmpnfooy4q","uri":"at://did:plc:jpkjnmydclkafjyicv3s6hcx/app.bsky.feed.post/3lacsvjm7o62x"}}},"langs":["en"],"text":""},"cid":"bafyreigxjzf7hiygzsh4cnpndpfjoy5sbai3m5dsejnil2zq6ebqexlioy"}}"#,
            r#"{"did":"did:plc:vhwscbpufmtoekc5hyz73vpa","time_us":1739075366266743,"kind":"commit","commit":{"rev":"3lhps6qxmsu2e","operation":"create","collection":"app.bsky.feed.post","rkey":"3lhps6qvn3k27","record":{"$type":"app.bsky.feed.post","createdAt":"2025-02-09T04:29:26.015Z","embed":{"$type":"app.bsky.embed.record","record":{"cid":"bafyreib72cfeelnufyrp6ok373i5mcdgypq3ye4qu625p5q6m6grlzioyu","uri":"at://did:plc:jpkjnmydclkafjyicv3s6hcx/app.bsky.feed.post/3lacy7ydylc22"}},"langs":["en"],"reply":{"parent":{"cid":"bafyreibbg2leljyfcurxfnscsozon7h7ta4hetheopctcejdsmpnfooy4q","uri":"at://did:plc:jpkjnmydclkafjyicv3s6hcx/app.bsky.feed.post/3lacsvjm7o62x"},"root":{"cid":"bafyreibbg2leljyfcurxfnscsozon7h7ta4hetheopctcejdsmpnfooy4q","uri":"at://did:plc:jpkjnmydclkafjyicv3s6hcx/app.bsky.feed.post/3lacsvjm7o62x"}},"text":""},"cid":"bafyreihbp6dtgxyg4q77tor3lyvgxy57c2s3zozuvbg3semjfr45hdbkji"}}"#,
            r#"{"did":"did:plc:vhwscbpufmtoekc5hyz73vpa","time_us":1739070925289753,"kind":"commit","commit":{"rev":"3lhpo2fqaxw2q","operation":"create","collection":"app.bsky.feed.post","rkey":"3lhpo2fqeik2q","record":{"$type":"app.bsky.feed.post","createdAt":"2025-02-09T03:15:25.122Z","embed":{"$type":"app.bsky.embed.record","record":{"cid":"bafyreibbg2leljyfcurxfnscsozon7h7ta4hetheopctcejdsmpnfooy4q","uri":"at://did:plc:jpkjnmydclkafjyicv3s6hcx/app.bsky.feed.post/3lacsvjm7o62x"}},"langs":["en"],"text":"Quote Post"},"cid":"bafyreibw7kojbp3hwxvnzueihuz4draatigiius2axg5vhablylifstlbu"}}"#,
            r#"{"did":"did:plc:vhwscbpufmtoekc5hyz73vpa","time_us":1739120673659627,"kind":"commit","commit":{"rev":"3lhr4ezinfv2q","operation":"create","collection":"app.bsky.feed.post","rkey":"3lhr4ezad322p","record":{"$type":"app.bsky.feed.post","createdAt":"2025-02-09T17:04:33.236Z","langs":["en"],"text":"test"},"cid":"bafyreih5742ytclvqqmxtspocjnb7xrp3afxp6x4pwlm72fp6nb4wgw3te"}}"#,
            r#"{"did":"did:plc:vhwscbpufmtoekc5hyz73vpa","time_us":1739074582960891,"kind":"commit","commit":{"rev":"3lhprhfyklu24","operation":"create","collection":"app.bsky.feed.post","rkey":"3lhprhfeijk27","record":{"$type":"app.bsky.feed.post","createdAt":"2025-02-09T04:16:22.163Z","embed":{"$type":"app.bsky.embed.recordWithMedia","media":{"$type":"app.bsky.embed.images","images":[{"alt":"","aspectRatio":{"height":670,"width":739},"image":{"$type":"blob","ref":{"$link":"bafkreifyetr44wuxhi7gjzrcnvjpligg2awcvycml37tktvptfh6ylua4q"},"mimeType":"image/jpeg","size":392305}}]},"record":{"$type":"app.bsky.embed.record","record":{"cid":"bafyreibbg2leljyfcurxfnscsozon7h7ta4hetheopctcejdsmpnfooy4q","uri":"at://did:plc:jpkjnmydclkafjyicv3s6hcx/app.bsky.feed.post/3lacsvjm7o62x"}}},"langs":["en"],"text":""},"cid":"bafyreigxjzf7hiygzsh4cnpndpfjoy5sbai3m5dsejnil2zq6ebqexlioy"}}"#,
            r#"{"did":"did:plc:vhwscbpufmtoekc5hyz73vpa","time_us":1739070925289753,"kind":"commit","commit":{"rev":"3lhpo2fqaxw2q","operation":"create","collection":"app.bsky.feed.post","rkey":"3lhpo2fqeik2q","record":{"$type":"app.bsky.feed.post","createdAt":"2025-02-09T03:15:25.122Z","embed":{"$type":"app.bsky.embed.record","record":{"cid":"bafyreibbg2leljyfcurxfnscsozon7h7ta4hetheopctcejdsmpnfooy4q","uri":"at://did:plc:jpkjnmydclkafjyicv3s6hcx/app.bsky.feed.post/3lacsvjm7o62x"}},"langs":["en"],"text":"Quote Post"},"cid":"bafyreibw7kojbp3hwxvnzueihuz4draatigiius2axg5vhablylifstlbu"}}"#,
            r#"{"did":"did:plc:vhwscbpufmtoekc5hyz73vpa","time_us":1739074582960891,"kind":"commit","commit":{"rev":"3lhprhfyklu24","operation":"create","collection":"app.bsky.feed.post","rkey":"3lhprhfeijk27","record":{"$type":"app.bsky.feed.post","createdAt":"2025-02-09T04:16:22.163Z","embed":{"$type":"app.bsky.embed.recordWithMedia","media":{"$type":"app.bsky.embed.images","images":[{"alt":"","aspectRatio":{"height":670,"width":739},"image":{"$type":"blob","ref":{"$link":"bafkreifyetr44wuxhi7gjzrcnvjpligg2awcvycml37tktvptfh6ylua4q"},"mimeType":"image/jpeg","size":392305}}]},"record":{"$type":"app.bsky.embed.record","record":{"cid":"bafyreibbg2leljyfcurxfnscsozon7h7ta4hetheopctcejdsmpnfooy4q","uri":"at://did:plc:jpkjnmydclkafjyicv3s6hcx/app.bsky.feed.post/3lacsvjm7o62x"}}},"langs":["en"],"text":""},"cid":"bafyreigxjzf7hiygzsh4cnpndpfjoy5sbai3m5dsejnil2zq6ebqexlioy"}}"#,
            r#"{"did":"did:plc:vhwscbpufmtoekc5hyz73vpa","time_us":1739070946025127,"kind":"commit","commit":{"rev":"3lhpo2zhudg2n","operation":"create","collection":"app.bsky.feed.post","rkey":"3lhpo2zhmos2q","record":{"$type":"app.bsky.feed.post","createdAt":"2025-02-09T03:15:45.808Z","langs":["en"],"reply":{"parent":{"cid":"bafyreibbg2leljyfcurxfnscsozon7h7ta4hetheopctcejdsmpnfooy4q","uri":"at://did:plc:jpkjnmydclkafjyicv3s6hcx/app.bsky.feed.post/3lacsvjm7o62x"},"root":{"cid":"bafyreibbg2leljyfcurxfnscsozon7h7ta4hetheopctcejdsmpnfooy4q","uri":"at://did:plc:jpkjnmydclkafjyicv3s6hcx/app.bsky.feed.post/3lacsvjm7o62x"}},"text":"test reply"},"cid":"bafyreib6l7rltqha76gbrcyezukpskh7yf5gtqpdxru72xcjuqabe5ckte"}}"#,
            r#"{"did":"did:plc:vhwscbpufmtoekc5hyz73vpa","time_us":1739074686768901,"kind":"commit","commit":{"rev":"3lhprkiymb72t","operation":"create","collection":"app.bsky.feed.post","rkey":"3lhprkidbgs27","record":{"$type":"app.bsky.feed.post","createdAt":"2025-02-09T04:18:05.931Z","embed":{"$type":"app.bsky.embed.images","images":[{"alt":"","aspectRatio":{"height":670,"width":739},"image":{"$type":"blob","ref":{"$link":"bafkreifyetr44wuxhi7gjzrcnvjpligg2awcvycml37tktvptfh6ylua4q"},"mimeType":"image/jpeg","size":392305}}]},"langs":["en"],"reply":{"parent":{"cid":"bafyreibbg2leljyfcurxfnscsozon7h7ta4hetheopctcejdsmpnfooy4q","uri":"at://did:plc:jpkjnmydclkafjyicv3s6hcx/app.bsky.feed.post/3lacsvjm7o62x"},"root":{"cid":"bafyreibbg2leljyfcurxfnscsozon7h7ta4hetheopctcejdsmpnfooy4q","uri":"at://did:plc:jpkjnmydclkafjyicv3s6hcx/app.bsky.feed.post/3lacsvjm7o62x"}},"text":""},"cid":"bafyreihlqnfjcmph2w3prstvc35lbrmte6yu4kd5b2war5s6qu6efxplq4"}}"#,
            r#"{"did":"did:plc:vhwscbpufmtoekc5hyz73vpa","time_us":1739075366266743,"kind":"commit","commit":{"rev":"3lhps6qxmsu2e","operation":"create","collection":"app.bsky.feed.post","rkey":"3lhps6qvn3k27","record":{"$type":"app.bsky.feed.post","createdAt":"2025-02-09T04:29:26.015Z","embed":{"$type":"app.bsky.embed.record","record":{"cid":"bafyreib72cfeelnufyrp6ok373i5mcdgypq3ye4qu625p5q6m6grlzioyu","uri":"at://did:plc:jpkjnmydclkafjyicv3s6hcx/app.bsky.feed.post/3lacy7ydylc22"}},"langs":["en"],"reply":{"parent":{"cid":"bafyreibbg2leljyfcurxfnscsozon7h7ta4hetheopctcejdsmpnfooy4q","uri":"at://did:plc:jpkjnmydclkafjyicv3s6hcx/app.bsky.feed.post/3lacsvjm7o62x"},"root":{"cid":"bafyreibbg2leljyfcurxfnscsozon7h7ta4hetheopctcejdsmpnfooy4q","uri":"at://did:plc:jpkjnmydclkafjyicv3s6hcx/app.bsky.feed.post/3lacsvjm7o62x"}},"text":""},"cid":"bafyreihbp6dtgxyg4q77tor3lyvgxy57c2s3zozuvbg3semjfr45hdbkji"}}"#,
            r#"{"did":"did:plc:vhwscbpufmtoekc5hyz73vpa","time_us":1739075494566375,"kind":"commit","commit":{"rev":"3lhpsckuobb2q","operation":"create","collection":"app.bsky.feed.post","rkey":"3lhpscjtnvs27","record":{"$type":"app.bsky.feed.post","createdAt":"2025-02-09T04:31:32.828Z","embed":{"$type":"app.bsky.embed.external","external":{"description":"ALT: a baby in a striped shirt is covering his face with his hand and says `` dear lort '' .","thumb":{"$type":"blob","ref":{"$link":"bafkreifl5e5bn4gxxnmwuhx6vuhjvppyyi6mg4vryrhtpxxdptkjvyzyx4"},"mimeType":"image/jpeg","size":403665},"title":"a baby in a striped shirt is covering his face with his hand and says `` dear lort '' .","uri":"https://media.tenor.com/rjjtn8dd0tgAAAAC/baby-facepalm.gif?hh=498&ww=498"}},"langs":["en"],"reply":{"parent":{"cid":"bafyreibbg2leljyfcurxfnscsozon7h7ta4hetheopctcejdsmpnfooy4q","uri":"at://did:plc:jpkjnmydclkafjyicv3s6hcx/app.bsky.feed.post/3lacsvjm7o62x"},"root":{"cid":"bafyreibbg2leljyfcurxfnscsozon7h7ta4hetheopctcejdsmpnfooy4q","uri":"at://did:plc:jpkjnmydclkafjyicv3s6hcx/app.bsky.feed.post/3lacsvjm7o62x"}},"text":""},"cid":"bafyreig4ckxykhrryffns2am7zbe7rzo4dxt6j427cucxy4ig4iczzrk4y"}}"#,
        ];

        for input in inputs {
            println!("Json Input: {}", input);
            let post: JetstreamPost = JetstreamPost::parse_raw_json(input).unwrap();
            println!("{:?}\n\n", post);
        }
    }
}
