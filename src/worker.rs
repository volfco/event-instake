use std::collections::HashMap;
use chrono::{DateTime, TimeZone, Utc};
use chrono_tz::Tz;
use clickhouse_rs::{Block, Pool};
use serde_json::Value;
use crate::RawMessage;
use tracing::{instrument, trace, debug, info, warn, error};


#[instrument]
pub async fn process_json(pool: Pool, msg: RawMessage) {

    let parse = serde_json::from_slice(msg.body.as_slice());
    if parse.is_err() {
        warn!("unable to parse message. {:?}", parse.err().unwrap());
        return
    }
    let msg_body: Value = parse.unwrap();

    let messages = if msg_body.is_array() {
        msg_body.as_array().unwrap().to_vec()
    } else {
        vec![msg_body]
    };

    // transform from a row based dataset to a column one
    let mut columns: HashMap<String, Vec<Value>> = HashMap::new();

    for message in messages {
        let obj = message.as_object().unwrap();
        // TODO maybe do a preflight check of all the data to make sure it matches
        for (field, value) in obj.iter() {
            if !columns.contains_key(field) {
                columns.insert(field.clone(), vec![]);
            }
            columns.get_mut(field).unwrap().push(value.clone());
        }
    }

    if columns.len() == 0 {
        warn!("empty input. nothing to do");
        return
    }

    let mut block = Block::new();

    for (key, value) in columns {
        if value[0].is_i64() && key == "timestamp" { // We can safely assume that any i64 column named Timestamp is a Timestamp
            let values: Vec<DateTime<Tz>> = value.iter().map(|i| if i.is_i64() { Utc.timestamp(i.as_i64().unwrap(), 0).with_timezone(&Tz::UTC) } else { panic!("type changed unexpectedly ") }).collect();
            block = block.column(key.as_str(), values);
        } else if value[0].is_i64() {
            let values: Vec<i64> = value.iter().map(|i| if i.is_i64() { i.as_i64().unwrap() } else { panic!("type changed unexpectedly ") }).collect();
            block = block.column(key.as_str(), values);
        } else if value[0].is_string() {
            let values: Vec<String> = value.iter().map(|i| if i.is_string() { i.as_str().unwrap().to_string() } else { panic!("type changed unexpectedly ") }).collect();
            block = block.column(key.as_str(), values);
        } else if value[0].is_f64() {
            let values: Vec<f64> = value.iter().map(|i| if i.is_f64() { i.as_f64().unwrap() } else { panic!("type changed unexpectedly ") }).collect();
            block = block.column(key.as_str(), values);
        } else {
            warn!("unexpected data type");
        }
    }

    let mut client = match pool.get_handle().await {
        Ok(client) => client,
        Err(err) => {
            error!("{:?}", err);
            return
        }
    };
    match client.insert(msg.index, block).await {
        Ok(_) => {}
        Err(err) =>
            error!("{:?}", err)
    }

}
