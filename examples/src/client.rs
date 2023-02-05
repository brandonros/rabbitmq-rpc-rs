mod structs;

use anyhow::Result;
use rabbitmq_rpc::rabbitmq_requester;
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
  let request_queue_name = String::from("q.req");
  let reply_queue_name = String::from("q.reply");
  let reply_consumer_tag = String::from("reply_consumer");
  let reply_consumer = rabbitmq_requester::QueueReplyConsumer::new(
    host.to_string(),
    port,
    username.to_string(),
    password.to_string(),
    exchange_name,
    request_queue_name,
    reply_queue_name,
    reply_consumer_tag,
  );
  let (_connection, channel) = reply_consumer.connect().await.unwrap();
  // start consuming replies
  reply_consumer.start_consuming(&channel).await.unwrap();
  // make add request
  {
    let request = AddRequest { a: 1, b: 1 };
    log::info!("making request {:?}", request);
    let request_bytes = structs::struct_to_bytes(&request);
    let timeout_ms = 2000;
    let message_type = "add";
    let response_bytes = reply_consumer.request(&channel, message_type, request_bytes, timeout_ms).await?;
    let response: AddResponse = structs::bytes_to_struct(response_bytes);
    log::info!("got response {:?}", response);
  }
  // make subtract request
  {
    let request = SubtractRequest { a: 1, b: 1 };
    log::info!("making request {:?}", request);
    let request_bytes = structs::struct_to_bytes(&request);
    let timeout_ms = 2000;
    let message_type = "subtract";
    let response_bytes = reply_consumer.request(&channel, message_type, request_bytes, timeout_ms).await?;
    let response: SubtractResponse = structs::bytes_to_struct(response_bytes);
    log::info!("got response {:?}", response);
  }
  // exit
  Ok(())
}
