use std::{borrow::Cow, net::SocketAddr, sync::Arc, time::Duration};

use axum::{
    body::{BoxBody, Bytes},
    error_handling::HandleErrorLayer,
    extract::{ContentLengthLimit, Extension},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, get_service, post},
    Router,
};
use sentry::integrations::tower::{NewSentryLayer, SentryHttpLayer};
use sentry::integrations::tracing as sentry_tracing;
use tokio::runtime::Runtime;
use tower::{BoxError, ServiceBuilder};
use tower_http::{services::ServeDir, trace::TraceLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use camino::Utf8PathBuf as PathBuf;
use clap::Parser;
use color_eyre::eyre::Result;
use sourmash::index::revindex::{prepare_query, RevIndex, RevIndexOps};
use sourmash::prelude::*;
use sourmash::selection::Selection;
use sourmash::signature::{Signature, SigsTrait};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    /// Path to rocksdb index dir
    // index: PathBuf,

    #[clap(value_name = "INDEX_PATH", num_args = 1..)]
    index: Vec<PathBuf>,

    /// Location of the data for signatures.
    /// Either a zip file or a path to a directory containing signatures.
    #[clap(short = 'l', long = "location")]
    location: Option<PathBuf>,

    /// Path to static assets
    #[clap(short = 'a', long = "assets", default_value = "assets/")]
    assets: PathBuf,

    /// ksize
    #[clap(short = 'k', long = "ksize", default_value = "21")]
    ksize: u8,

    /// scaled
    #[clap(short = 's', long = "scaled", default_value = "1000")]
    scaled: usize,

    /// port
    #[clap(short = 'p', long = "port", default_value = "3059")]
    port: u16,

    /// threshold_bp
    #[clap(short = 't', long = "threshold_bp", default_value = "50000")]
    threshold_bp: usize,
}

fn main() -> Result<()> {
    let _guard = sentry::init((
        std::env::var("SENTRY_DSN").unwrap_or_else(|_| "".to_string()),
        sentry::ClientOptions {
            release: sentry::release_name!(),
            traces_sample_rate: 1.0,
            enable_profiling: true,
            profiles_sample_rate: 1.0,
            environment: Some(
                std::env::var("BRANCHWATER_ENVIRONMENT")
                    .unwrap_or("development".into())
                    .into(),
            ),
            ..Default::default()
        },
    ));

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "branchwater=debug,tower_http=debug".into()),
        ))
        .with(tracing_subscriber::fmt::layer().json())
        .with(sentry_tracing::layer())
        .init();

    let opts = Cli::parse();

    let selection = Selection::builder()
        .ksize(opts.ksize.into())
        .scaled(opts.scaled as u32)
        .build();

    let threshold = opts.threshold_bp / opts.scaled;

    let location = opts.location.map(|path| {
        if path.ends_with(".zip") {
            format!("zip://{}", path)
        } else {
            format!("fs://{}", path)
        }
    });

    // let state = Arc::new(State {
    //     db: Arc::new(RevIndex::open(opts.index, true, location.as_deref())?),
    //     selection: Arc::new(selection),
    //     threshold,
    // });

    let dbs: Vec<Arc<RevIndex>> = opts
        .index
        .into_iter()
        .map(|p| RevIndex::open(p, true, location.as_deref()))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(Arc::new)
        .collect();

    let state = Arc::new(State {
        dbs: Arc::new(dbs),
        selection: Arc::new(selection),
        threshold,
    });

    // Build our application by composing routes
    let app = Router::new()
        .route("/search", post(search))
        .route("/health", get(health))
        //.route("/gather", post(gather))
        .fallback(get_service(ServeDir::new(opts.assets)).handle_error(handle_static_serve_error))
        // Add middleware to all routes
        .layer(
            ServiceBuilder::new()
                .layer(NewSentryLayer::new_from_top())
                .layer(SentryHttpLayer::with_transaction())
                // Handle errors from middleware
                .layer(HandleErrorLayer::new(handle_error))
                .load_shed()
                .concurrency_limit(200)
                .timeout(Duration::from_secs(3600))
                .layer(TraceLayer::new_for_http())
                .layer(Extension(state))
                .into_inner(),
        );

    // Create the runtime
    let rt = Runtime::new()?;

    let addr = SocketAddr::from(([0, 0, 0, 0], opts.port));
    tracing::debug!("listening on {}", addr);

    // Spawn the root task
    rt.block_on(async {
        // Run our app with hyper
        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
            .unwrap();
    });

    Ok(())
}

type SharedState = Arc<State>;

struct State {
    dbs: Arc<Vec<Arc<RevIndex>>>,
    // db: Arc<RevIndex>,
    selection: Arc<Selection>,
    threshold: usize,
}

impl State {
    async fn search(&self, query: Signature) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        // After
        use std::collections::HashMap;

        let dbs = self.dbs.clone();
        let threshold = self.threshold;
        let selection = self.selection.clone();

        let Ok((merged_matches, query_size)) = tokio::task::spawn_blocking(move || {
            if let Some(mh) = prepare_query(query, &selection) {
                let mut agg: HashMap<String, usize> = HashMap::new();

                for db in dbs.iter() {
                    let counter = db.counter_for_query(&mh);
                    let matches = db.matches_from_counter(counter, threshold);
                    for (path, size) in matches {
                        let key = path.to_string();
                        *agg.entry(key).or_insert(0) += size; // sum sizes across DBs
                    }
                }

                // Sort by descending intersection size (optional but useful)
                let mut merged: Vec<(String, usize)> = agg.into_iter().collect();
                merged.sort_unstable_by(|a, b| b.1.cmp(&a.1));

                Ok((merged, mh.size() as f64))
            } else {
                Err("Could not extract compatible sketch to compare")
            }
        })
        .await?
        else {
            return Err("Could not extract compatible sketch to compare".into());
        };

        let mut csv = vec!["SRA accession,containment".into()];
        csv.extend(merged_matches.into_iter().map(|(path, size)| {
            let containment = size as f64 / query_size;
            format!(
                "{},{}",
                path.split('/').last().unwrap().split('.').next().unwrap(),
                containment
            )
        }));
        Ok(csv)
        // let db = self.db.clone();
        // let threshold = self.threshold;
        // let selection = self.selection.clone();
        //
        // let Ok((matches, query_size)) = tokio::task::spawn_blocking(move || {
        //     if let Some(mh) = prepare_query(query, &selection) {
        //         let counter = db.counter_for_query(&mh);
        //         let matches = db.matches_from_counter(counter, threshold);
        //         Ok((matches, mh.size() as f64))
        //     } else {
        //         Err("Could not extract compatible sketch to compare")
        //     }
        // })
        // .await?
        // else {
        //     return Err("Could not extract compatible sketch to compare".into());
        // };
        //
        // let mut csv = vec!["SRA accession,containment".into()];
        // csv.extend(matches.into_iter().map(|(path, size)| {
        //     let containment = size as f64 / query_size;
        //     format!(
        //         "{},{}",
        //         path.split('/').last().unwrap().split('.').next().unwrap(),
        //         containment
        //     )
        // }));
        // Ok(csv)
    }

    fn parse_sig(&self, raw_data: &[u8]) -> Result<Signature, BoxError> {
        Ok(Signature::from_reader(raw_data)?
            .swap_remove(0)
            .select(&self.selection)?)
    }
}

async fn search(
    ContentLengthLimit(bytes): ContentLengthLimit<Bytes, { 1024 * 5_000 }>, // ~5mb
    Extension(state): Extension<SharedState>,
    //) -> Result<Json<serde_json::Value>, StatusCode> {
) -> Response<BoxBody> {
    let sig = match state.parse_sig(&bytes) {
        Ok(sig) => sig,
        Err(e) => {
            return {
                (
                    StatusCode::BAD_REQUEST,
                    format!("Error parsing signature: {e}"),
                )
                    .into_response()
            }
        }
    };

    match state.search(sig).await {
        Ok(matches) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            matches.join("\n"),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Something went wrong: {e}"),
        )
            .into_response(),
    }
}

#[derive(serde::Serialize)]
struct Health {
    status: &'static str,
    db_count: usize,
}

async fn health(Extension(state): Extension<SharedState>) -> impl IntoResponse {
    let body = serde_json::to_string(&Health {
        status: "ok",
        db_count: state.dbs.len(),
    }).unwrap();

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        body,
    )
}

// async fn health() -> Response<BoxBody> {
//     (StatusCode::OK, "I'm doing science and I'm still alive").into_response()
// }

async fn handle_static_serve_error(error: std::io::Error) -> impl IntoResponse {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Cow::from(format!("Unhandled static serve error: {}", error)),
    )
}

async fn handle_error(error: BoxError) -> impl IntoResponse {
    if error.is::<tower::timeout::error::Elapsed>() {
        return (StatusCode::REQUEST_TIMEOUT, Cow::from("request timed out"));
    }

    if error.is::<tower::load_shed::error::Overloaded>() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Cow::from("service is overloaded, try again later"),
        );
    }

    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Cow::from(format!("Unhandled internal error: {}", error)),
    )
}
