use axum::body::Bytes;
use axum::extract::{MatchedPath, Path, State};
use axum::http::{HeaderMap, Request, StatusCode};
use axum::response::Response;
use axum::routing::{delete, get, post, Router};
use axum::Json;
use axum_prometheus::metrics_exporter_prometheus::PrometheusHandle;
use axum_prometheus::PrometheusMetricLayer;
use database_schema::diesel_async::pooled_connection::deadpool::Object;
use database_schema::timestamp::SerializableTimestamp;
use database_schema::{
    accounts, notification_settings, notifications, run_migrations, users, NewUser, Notification,
    User, UserAccount, UserSetting,
};
use diesel::prelude::*;
use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use lazy_static::lazy_static;
use prometheus::{self, register_int_gauge, Encoder, IntGauge, TextEncoder};
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tower_governor::key_extractor;
use tower_governor::{governor::GovernorConfigBuilder, GovernorLayer};
use tower_http::{classify::ServerErrorsFailureClass, trace::TraceLayer};
use tracing::{debug, error, info, info_span, warn, Span};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;
use url::Url;

mod json;
use json::UserSettings;

lazy_static! {
    static ref SUBSCRIBED_USERS_COUNTER: IntGauge = register_int_gauge!(
        "subscribed_users",
        "The number of users subscribed to bluenotify notifications"
    )
    .unwrap();
    static ref WATCHED_USERS: IntGauge = register_int_gauge!(
        "watched_users",
        "The number of bluesky users watched by the server for updates"
    )
    .unwrap();
}

#[derive(Clone)]
struct AxumState {
    pool: Pool<AsyncPgConnection>,
    kv_store: async_nats::jetstream::kv::Store,
    metric_handle: PrometheusHandle,
}

async fn get_or_create_user(
    mut conn: &mut Object<AsyncPgConnection>,
    fcm_token: String,
    device_uuid: Option<String>,
) -> Result<User, StatusCode> {
    info!(
        "Creating user if not exists: {} (device_uuid: {:?})",
        fcm_token, device_uuid
    );
    let now = chrono::Utc::now().naive_utc();
    let new_user = NewUser {
        fcm_token: fcm_token.clone(),
        created_at: now.into(),
        updated_at: now.into(),
        device_uuid: device_uuid.clone(),
    };

    // Try lookup by device_uuid if provided
    if let Some(ref uuid) = device_uuid {
        let user_by_uuid = users::table
            .filter(users::device_uuid.eq(uuid))
            .first::<User>(&mut conn)
            .await;
        match user_by_uuid {
            Ok(user) => return Ok(user),
            Err(diesel::result::Error::NotFound) => {
                // fallback to fcm_token below
            }
            Err(error) => {
                error!(
                    "Error checking for existing user by device_uuid {}: {:?}",
                    uuid, error
                );
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    }

    // Fallback to lookup by fcm_token
    let existing_user = users::table
        .filter(users::fcm_token.eq(&fcm_token))
        .first::<User>(&mut conn)
        .await;
    match existing_user {
        Ok(user) => Ok(user),
        Err(diesel::result::Error::NotFound) => {
            let user = diesel::insert_into(users::table)
                .values(new_user)
                .get_result::<User>(&mut conn)
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            info!("Created user: {}", fcm_token);
            Ok(user)
        }
        Err(error) => {
            error!(
                "Error checking for existing user {}: {:?}",
                fcm_token, error
            );
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

#[axum::debug_handler]
async fn get_notifications(
    State(AxumState { pool, .. }): State<AxumState>,
    Path(fcm_token): Path<String>,
) -> Result<String, StatusCode> {
    let mut conn = pool
        .get()
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

    let results = notifications::table
        .filter(notifications::user_id.eq(&fcm_token))
        .select(Notification::as_select())
        .load(&mut conn)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(serde_json::to_string(&results).unwrap())
}

#[axum::debug_handler]
async fn clear_notifications(
    State(AxumState { pool, .. }): State<AxumState>,
    Path(fcm_token): Path<String>,
) -> Result<String, StatusCode> {
    info!("Clearing notifications for user: {}", fcm_token);
    let mut conn = pool
        .get()
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

    diesel::delete(notifications::table.filter(notifications::user_id.eq(&fcm_token)))
        .execute(&mut conn)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok("Success".to_string())
}

#[axum::debug_handler]
async fn delete_notification(
    State(AxumState { pool, .. }): State<AxumState>,
    Path((fcm_token, notification_id)): Path<(String, i32)>,
) -> Result<String, StatusCode> {
    info!(
        "deleting notification {} for user: {}",
        notification_id, fcm_token
    );

    let mut conn = pool
        .get()
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

    diesel::delete(notifications::dsl::notifications.find(notification_id))
        .execute(&mut conn)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok("Success".to_string())
}

#[axum::debug_handler]
/// Update the entire settings bundle for a user in one go
async fn update_settings(
    State(AxumState { pool, kv_store, .. }): State<AxumState>,
    Path(fcm_token): Path<String>,
    Json(payload): Json<UserSettings>,
) -> Result<String, StatusCode> {
    info!("Updating settings for user: {}", fcm_token);

    let mut payload = payload.clone();

    let mut conn = pool
        .get()
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

    // Use device_uuid if provided, otherwise None
    let device_uuid = payload.device_uuid.clone();

    let user = get_or_create_user(&mut conn, fcm_token.clone(), device_uuid.clone()).await?;
    let now: SerializableTimestamp = chrono::Utc::now().naive_utc().into();

    diesel::update(users::table.filter(users::id.eq(&user.id)))
        .set((
            users::updated_at.eq(now),
            users::deleted_at.eq::<Option<SerializableTimestamp>>(None),
            users::device_uuid.eq(device_uuid),
            users::fcm_token.eq(fcm_token.clone()),
            // If fcm_token changed, update last_token_refresh
            users::last_token_refresh.eq(if user.fcm_token != fcm_token {
                Some(now)
            } else {
                user.last_token_refresh
            }),
        ))
        .execute(&mut conn)
        .await
        .map_err(|e| {
            error!("Error updating user: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // add any missing accounts from specific settings
    for setting in (&payload.notification_settings).clone() {
        if !&payload
            .accounts
            .iter()
            .any(|account| account.account_did == setting.user_account_did)
        {
            payload.accounts.push(json::Account {
                account_did: setting.user_account_did.clone(),
            });
        }
    }

    for account in &payload.accounts {
        let existing_account = UserAccount::belonging_to(&user)
            .filter(accounts::account_did.eq(&account.account_did))
            .first::<UserAccount>(&mut conn)
            .await;

        match existing_account {
            Ok(_existing_account) => {}
            Err(_) => {
                let new_account = UserAccount {
                    user_id: user.id,
                    account_did: account.account_did.clone(),
                    created_at: now,
                    too_many_follows: false,
                };
                diesel::insert_into(accounts::table)
                    .values(new_account)
                    .execute(&mut conn)
                    .await
                    .map_err(|e| {
                        error!("Error inserting account {}: {:?}", account.account_did, e);
                        StatusCode::INTERNAL_SERVER_ERROR
                    })?;
            }
        }
    }

    diesel::delete(
        accounts::table
            .filter(accounts::user_id.eq(&user.id))
            .filter(
                accounts::account_did.ne_all(
                    payload
                        .accounts
                        .iter()
                        .map(|account| account.account_did.clone())
                        .collect::<Vec<String>>(),
                ),
            ),
    )
    .execute(&mut conn)
    .await
    .map_err(|e| {
        error!("Error deleting accounts: {:?}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    diesel::delete(
        notification_settings::table.filter(notification_settings::user_id.eq(&user.id)),
    )
    .execute(&mut conn)
    .await
    .map_err(|e| {
        error!("Error deleting notification settings: {:?}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    for setting in payload.notification_settings {
        let now = chrono::Utc::now().naive_utc();
        let new_setting = UserSetting {
            user_id: user.id,
            user_account_did: setting.user_account_did,
            following_did: setting.following_did,
            post_type: setting.post_type.clone(),
            word_allow_list: setting.word_allow_list.clone(),
            word_block_list: setting.word_block_list.clone(),
            created_at: now.into(),
        };
        debug!("Inserting notification setting: {:?}", new_setting);
        diesel::insert_into(notification_settings::table)
            .values(new_setting)
            .on_conflict((
                notification_settings::user_id,
                notification_settings::user_account_did,
                notification_settings::following_did,
            ))
            .do_update()
            .set((
                notification_settings::post_type.eq(setting.post_type),
                notification_settings::word_allow_list.eq(setting.word_allow_list),
                notification_settings::word_block_list.eq(setting.word_block_list),
            ))
            .execute(&mut conn)
            .await
            .map_err(|e| {
                error!("Error inserting notification setting: {:?}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
    }

    info!("Updated settings for user: {}", fcm_token);

    if let Err(e) = update_watched_users(&mut conn, &kv_store).await {
        error!("Error updating watched users: {:?}", e);
    }

    Ok("Success".to_string())
}

#[axum::debug_handler]
async fn delete_settings(
    State(AxumState { pool, kv_store, .. }): State<AxumState>,
    Path(fcm_token): Path<String>,
) -> Result<String, StatusCode> {
    info!("Deleting settings for user: {}", fcm_token);
    let mut conn = pool
        .get()
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

    let user = get_or_create_user(&mut conn, fcm_token.clone(), None).await?;

    diesel::delete(
        notification_settings::table.filter(notification_settings::user_id.eq(&user.id)),
    )
    .execute(&mut conn)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if let Err(e) = update_watched_users(&mut conn, &kv_store).await {
        error!("Error updating watched users: {:?}", e);
    }

    Ok("Success".to_string())
}

#[derive(Deserialize)]
struct DeleteAccountRequest {
    fcm_token: String,
    device_uuid: Option<String>,
}

#[axum::debug_handler]
async fn delete_account(
    State(AxumState { pool, kv_store, .. }): State<AxumState>,
    Json(payload): Json<DeleteAccountRequest>,
) -> Result<String, StatusCode> {
    let mut conn = pool
        .get()
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

    // Try lookup by device_uuid if provided, else by fcm_token
    let user: User = if let Some(ref uuid) = payload.device_uuid {
        let user = users::table
            .filter(users::device_uuid.eq(uuid))
            .first::<User>(&mut conn)
            .await;
        match user {
            Ok(user) => user,
            Err(diesel::result::Error::NotFound) => {
                // fallback to fcm_token below
                users::table
                    .filter(users::fcm_token.eq(&payload.fcm_token))
                    .first::<User>(&mut conn)
                    .await
                    .map_err(|_| StatusCode::NOT_FOUND)?
            }
            Err(error) => {
                error!(
                    "Error checking for existing user by device_uuid {}: {:?}",
                    uuid, error
                );
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    } else {
        users::table
            .filter(users::fcm_token.eq(&payload.fcm_token))
            .first::<User>(&mut conn)
            .await
            .map_err(|_| StatusCode::NOT_FOUND)?
    };

    let now: SerializableTimestamp = chrono::Utc::now().naive_utc().into();
    // Delete notification_settings, accounts, and user (CASCADE should handle all if set up)
    diesel::update(users::table.filter(users::id.eq(user.id)))
        .set((users::deleted_at.eq(Some(now)),))
        .execute(&mut conn)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if let Err(e) = update_watched_users(&mut conn, &kv_store).await {
        error!("Error updating watched users: {:?}", e);
    }

    Ok("Success".to_string())
}

async fn update_watched_users(
    mut conn: &mut Object<AsyncPgConnection>,
    kv_store: &async_nats::jetstream::kv::Store,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let users_count = database_schema::schema::users::table
        .filter(database_schema::schema::users::dsl::deleted_at.is_null())
        .count()
        .get_result::<i64>(&mut conn)
        .await?;
    SUBSCRIBED_USERS_COUNTER.set(users_count);

    let watched_users = database_schema::schema::notification_settings::table
        .select(database_schema::schema::notification_settings::dsl::following_did)
        .distinct()
        .load::<String>(&mut conn)
        .await?;

    WATCHED_USERS.set(watched_users.len() as i64);

    // Store the watched users in the kv store
    let watched_users_json = serde_json::to_string(&watched_users).unwrap();
    let result = kv_store
        .put("watched_users", watched_users_json.into())
        .await;
    if let Err(e) = result {
        error!("Error storing watched users in kv store: {e:?}");
    } else {
        info!("Watched users stored in kv store successfully.");
    }
    Ok(())
}

#[axum::debug_handler]
async fn notification_opened(
    State(AxumState { pool, .. }): State<AxumState>,
    Path(fcm_token): Path<String>,
) -> Result<String, StatusCode> {
    let mut conn = pool
        .get()
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

    let user = users::table
        .filter(users::fcm_token.eq(&fcm_token))
        .first::<User>(&mut conn)
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;

    let now: SerializableTimestamp = chrono::Utc::now().naive_utc().into();

    diesel::update(users::table.filter(users::id.eq(user.id)))
        .set(users::last_interaction.eq(Some(now)))
        .execute(&mut conn)
        .await
        .map_err(|e| {
            error!("Error updating last_interaction: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok("Success".to_string())
}

#[axum::debug_handler]
async fn proxy_post_image(Path(url): Path<String>) -> Result<Response, StatusCode> {
    info!("Proxying image: {}", url);

    // verify that it's a bluesky url
    if !url.starts_with("https://cdn.bsky.app") {
        return Err(StatusCode::BAD_REQUEST);
    }

    let client = reqwest::Client::new();
    let response = client
        .get(&url)
        .header("User-Agent", "BlueNotify Server")
        .send()
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    if response.status() != StatusCode::OK {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut headers = HeaderMap::new();
    for (key, value) in response.headers() {
        headers.insert(key.clone(), value.clone());
    }
    let bytes = response
        .bytes()
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(
            "Content-Type",
            headers
                .get("Content-Type")
                .unwrap_or(&"image/jpeg".parse().unwrap()),
        )
        .body(bytes.into())
        .unwrap())
}

async fn metrics(State(AxumState { metric_handle, .. }): State<AxumState>) -> String {
    let mut buffer = vec![];
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    let mut metrics = String::from_utf8(buffer).unwrap() + "\n";

    // add axum metrics
    metrics += &metric_handle.render();

    metrics
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
            .with(sentry_tracing::layer())
            .init();

        tokio::spawn(task);
        info!("Web Server starting, loki tracing enabled.");
    } else {
        error!("LOKI_URL not set, will not send logs to Loki");
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(sentry_tracing::layer())
            .init();
    }

    info!("Getting DB");
    let pg_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pg_config = AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(&pg_url);
    let pg_pool = Pool::builder(pg_config).build()?;
    info!("Got DB");

    run_migrations()?;

    let mut nats_host = std::env::var("NATS_HOST").unwrap_or("localhost".to_string());
    if !nats_host.contains(':') {
        nats_host.push_str(":4222");
    }
    info!("Connecting to NATS at {}", nats_host);
    let nats_client = async_nats::connect(nats_host).await?;
    let jetstream = async_nats::jetstream::new(nats_client);

    _ = jetstream
        .create_key_value(async_nats::jetstream::kv::Config {
            bucket: "bluenotify_kv_store".to_string(),
            history: 1,
            ..Default::default()
        })
        .await;

    let kv_store = jetstream
        .get_key_value("bluenotify_kv_store")
        .await
        .unwrap();

    let governor_conf = Arc::new(
        GovernorConfigBuilder::default()
            .key_extractor(key_extractor::SmartIpKeyExtractor {})
            .period(Duration::from_millis(200))
            .burst_size(10)
            .finish()
            .unwrap(),
    );
    let governor_limiter = governor_conf.limiter().clone();
    let cleanup_interval = Duration::from_secs(60);
    // a separate background task to clean up
    std::thread::spawn(move || loop {
        std::thread::sleep(cleanup_interval);
        governor_limiter.retain_recent();
    });
    let axum_url = std::env::var("BIND_WEB").unwrap_or("0.0.0.0:8004".to_string());

    let (prometheus_layer, metric_handle) = PrometheusMetricLayer::pair();

    let axum_app = Router::new()
        .route("/", get(|| async { "Web server online." }))
        .route("/metrics", get(metrics))
        .route("/notifications/{fcm_token}", get(get_notifications))
        .route(
            "/notifications/{fcm_token}/clear",
            delete(clear_notifications),
        )
        .route(
            "/notifications/{fcm_token}/{notification_id}",
            delete(delete_notification),
        )
        .route("/settings/{fcm_token}", post(update_settings))
        .route("/settings/{fcm_token}", delete(delete_settings))
        .route("/image/{url}", get(proxy_post_image))
        .route(
            "/notifications/{fcm_token}/opened",
            post(notification_opened),
        )
        .route("/account", delete(delete_account))
        .with_state(AxumState {
            pool: pg_pool.clone(),
            kv_store: kv_store.clone(),
            metric_handle: metric_handle.clone(),
        })
        .layer(GovernorLayer {
            config: governor_conf,
        })
        .layer(prometheus_layer)
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &Request<_>| {
                    // Log the matched route's path (with placeholders not filled in).
                    // Use request.uri() or OriginalUri if you want the real path.
                    let route = request
                        .extensions()
                        .get::<MatchedPath>()
                        .map(MatchedPath::as_str);

                    // extract fcm_token from the request path
                    // how many slashes until fcm_token?
                    let path = request.uri().path();
                    let fcm_token = match route {
                        Some(route) => {
                            let mut path_parts = path.split('/');
                            let route = route.to_string();
                            let route_parts = route.split('/').collect::<Vec<&str>>();
                            match route_parts.iter().position(|x| *x == "{fcm_token}") {
                                Some(index) => path_parts.nth(index).unwrap_or("").to_string(),
                                None => "".to_string(),
                            }
                        }
                        None => "".to_string(),
                    };

                    info_span!(
                        "http_request",
                        method = ?request.method(),
                        route,
                        token = fcm_token,
                    )
                })
                .on_request(|_request: &Request<_>, _span: &Span| {})
                .on_response(|_response: &Response, _latency: Duration, _span: &Span| {
                    // check if the status was not 2xx
                    if _response.status() != StatusCode::OK {
                        _span.record("error", "true");
                        warn!("Error: {:?}", _response);
                    }
                })
                .on_failure(
                    |_error: ServerErrorsFailureClass, _latency: Duration, _span: &Span| {
                        _span.record("error", "true");
                        warn!("Error status: {:?}", _error);
                    },
                ),
        );

    let addr = axum_url.parse::<SocketAddr>().unwrap();
    info!("listening on {}", addr);
    let listener = TcpListener::bind(addr).await.unwrap();

    let mut tasks: JoinSet<_> = JoinSet::new();
    tasks.spawn(async move {
        axum::serve(
            listener,
            axum_app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
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
        let result = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(_main());

        if let Err(e) = result {
            error!("Error: {:?}", e);
        }
    } else {
        let result = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(_main());

        if let Err(e) = result {
            eprintln!("Error: {:?}", e);
        }
    }
}
