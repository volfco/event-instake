mod models;
mod worker;

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{debug, info, instrument, trace};
use actix_web::{get, post, web, App, HttpServer, Responder, HttpResponse, Result, HttpRequest};
use actix_web::http::header::{HeaderValue, ToStrError};
use actix_web::http::StatusCode;
use actix_web::middleware::Logger;
use actix_web::web::Data;
use clickhouse_rs::Pool;
use tokio::sync::mpsc::error::SendError;
use crate::models::{Application, RawMessage};

use futures_util::stream::{self, StreamExt};
use futures_util::task::SpawnExt;
use tokio::sync::RwLock;

#[instrument]
#[allow(clippy::async_yields_async)]
#[post("/intake/{index}.json")]
async fn intake_json(req: HttpRequest, state: web::Data<models::Application>, body: web::Bytes, index: web::Path<String>) -> impl Responder {
    let current_index = index.into_inner();
    let valid = match req.headers().get("authorization") {
        None => false,
        Some(val) => match val.to_str() {
            Ok(value) => {
                let handle = state.api_keys.read().await;
                let header_parts = value.split(' ').collect::<Vec<&str>>();
                if header_parts.len() != 2 || header_parts[0] != "Token" {
                    debug!("Malformed authorization header");
                    false
                } else if let Some(indexes) = handle.get(header_parts[1]) {
                    debug!("api key found. index match: {:?}", indexes.contains(&current_index));
                    indexes.contains(&current_index)
                } else {
                    false
                }
            }
            Err(_) => false
        }
    };
    if !valid {
        return HttpResponse::Forbidden();
    }

    let message = RawMessage{
        accepted: chrono::Utc::now(),
        index: current_index,
        body: body.to_vec()
    };
    let pool = state.clickhouse.clone();
    tokio::task::spawn(async move {
        worker::process_json(pool, message).await
    });

    HttpResponse::Accepted()
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();

    let mut api_keys = HashMap::new();
    api_keys.insert("default".to_string(), vec!["dockerAgentEvents".to_string()]);

    let app = Application {
        clickhouse: Pool::new("tcp://127.0.0.1:9000"),
        api_keys: Arc::new(RwLock::new(api_keys))
    };

    HttpServer::new(move || {
        App::new()
            .app_data(Data::new(app.clone()))
            .wrap(Logger::default())
            .route("/", web::get().to(|| async { "hello." }))
            .service(intake_json)
    })
        .bind(("127.0.0.1", 8080))?
        .run()
        .await
}