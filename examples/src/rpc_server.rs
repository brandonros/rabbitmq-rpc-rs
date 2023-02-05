mod structs;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use futures::future::BoxFuture;
use rabbitmq_rpc::replier;
use structs::*;

async fn on_add(deliver: amqprs::Deliver, basic_properties: amqprs::BasicProperties, request: Arc<Vec<u8>>) -> Result<Vec<u8>> {
  let request: AddRequest = structs::bytes_to_struct(&request);
  log::info!("on_add: deliver = {:?} basic_properties = {:?} request = {:?}", deliver, basic_properties, request);
  // implement logic
  let response = AddResponse { value: request.a + request.b };
  // return response
  let response_bytes = structs::struct_to_bytes(&response);
  Ok(response_bytes)
}

async fn on_subtract(deliver: amqprs::Deliver, basic_properties: amqprs::BasicProperties, request: Arc<Vec<u8>>) -> Result<Vec<u8>> {
  let request: SubtractRequest = structs::bytes_to_struct(&request);
  log::info!("on_subtract: deliver = {:?} basic_properties = {:?} request = {:?}", deliver, basic_properties, request);
  // implement logic
  let response = SubtractResponse { value: request.a - request.b };
  // return response
  let response_bytes = structs::struct_to_bytes(&response);
  Ok(response_bytes)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
  // logger
  tracing_subscriber::fmt::init();
  // init rabbitmq
  let host = "localhost";
  let port = 5672;
  let username = "guest";
  let password = "guest";
  let exchange_name = String::from("amq.topic");
  let request_queue_name = String::from("q.req");
  let reply_queue_name = String::from("q.reply");
  let request_consumer_tag = String::from("request_consumer_tag");
  let mut request_handlers: HashMap<String, replier::OnRequestCallback> = HashMap::new();
  request_handlers.insert(
    String::from("add"),
    Arc::new(move |deliver, basic_properties, request| Box::pin(on_add(deliver, basic_properties, request)) as BoxFuture<'static, Result<Vec<u8>>>),
  );
  request_handlers.insert(
    String::from("subtract"),
    Arc::new(move |deliver, basic_properties, request| Box::pin(on_subtract(deliver, basic_properties, request)) as BoxFuture<'static, Result<Vec<u8>>>),
  );
  let request_consumer = replier::QueueRequestConsumer::new(
    host.to_string(),
    port,
    username.to_string(),
    password.to_string(),
    exchange_name,
    request_queue_name,
    reply_queue_name,
    request_consumer_tag,
    request_handlers,
  );
  // connect
  let (_connection, channel) = request_consumer.connect().await?;
  // start consuming
  request_consumer.start_consuming(&channel).await?;
  // prevent process from closing
  loop {
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
  }
}
