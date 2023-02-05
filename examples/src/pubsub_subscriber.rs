mod structs;

use std::{sync::Arc, collections::HashMap};

use anyhow::Result;
use futures::future::BoxFuture;
use rabbitmq_rpc::subscriber;
use structs::*;

async fn on_info(request: Vec<u8>) -> Result<()> {
  let request: InfoMessage = structs::bytes_to_struct(request);
  log::info!("on_info: request = {:?}", request);
  Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
  // logger
  tracing_subscriber::fmt::init();
  // connect
  let host = "localhost";
  let port = 5672;
  let username = "guest";
  let password = "guest";
  let exchange_name = String::from("amq.topic");
  let queue_name = String::from("q.pubsub");
  let subscriber_consumer_tag = String::from("subscriber_consumer_tag");
  let mut message_handlers: HashMap<String, subscriber::OnMessageCallback> = HashMap::new();
  message_handlers.insert(
    String::from("info"),
    Arc::new(move |a| Box::pin(on_info(a)) as BoxFuture<'static, Result<()>>),
  );
  let subscriber = subscriber::QueueSubscriber::new(
    host.to_string(),
    port,
    username.to_string(),
    password.to_string(),
    exchange_name,
    queue_name,
    subscriber_consumer_tag,
    message_handlers
  );
  let (_connection, channel) = subscriber.connect().await.unwrap();
  // start consuming replies
  subscriber.start_consuming(&channel).await.unwrap();
  // prevent process from closing
  loop {
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
  }
}
