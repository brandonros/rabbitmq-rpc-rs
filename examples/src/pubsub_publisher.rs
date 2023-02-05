mod structs;

use anyhow::Result;
use rabbitmq_rpc::publisher;
use structs::*;

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
  let publisher = publisher::QueuePublisher::new(host.to_string(), port, username.to_string(), password.to_string(), exchange_name, queue_name);
  let (_connection, channel) = publisher.connect().await.unwrap();
  // start publishing messages
  loop {
    let message_type = "info";
    let message = InfoMessage {
      value: String::from("Hello, world!"),
    };
    log::info!("publishing {:?}", message);
    let message_bytes = struct_to_bytes(&message);
    publisher.publish(&channel, message_type, message_bytes).await?;
    // sleep
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
  }
}
