use std::{sync::Arc, collections::HashMap};
use tokio::sync::{RwLock};
use chrono::{DateTime, Utc};
use clickhouse_rs::Pool as cPool;


#[derive(Debug, Clone)]
pub struct Application {
    pub clickhouse: cPool,
    /// API Key HashMap. <api-key, Vec<indexes>>
    pub api_keys: Arc<RwLock<HashMap<String, Vec<String>>>>
}

#[derive(Debug, Clone)]
pub struct RawMessage {
    pub accepted: DateTime<Utc>,
    pub index: String,
    pub body: Vec<u8>,
}