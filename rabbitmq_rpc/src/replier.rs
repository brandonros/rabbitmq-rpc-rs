use futures::future::BoxFuture;
use std::{collections::HashMap, sync::Arc};

use amqprs::{channel::Channel, connection::Connection};
use anyhow::Result;
use async_trait::async_trait;

pub type OnRequestCallback = Arc<dyn Fn(Arc<Vec<u8>>) -> BoxFuture<'static, Result<Vec<u8>>> + Send + Sync>;

pub struct QueueRequestConsumer {
  pub host: String,
  pub port: u16,
  pub username: String,
  pub password: String,
  pub exchange_name: String,
  pub request_queue_name: String,
  pub reply_queue_name: String,
  pub request_consumer_tag: String,
  pub request_handlers: HashMap<String, OnRequestCallback>,
}

struct MyAsyncRequestConsumer {
  pub exchange_name: String,
  pub request_queue_name: String,
  pub reply_queue_name: String,
  pub request_handlers: HashMap<String, OnRequestCallback>,
}

#[async_trait]
impl amqprs::consumer::AsyncConsumer for MyAsyncRequestConsumer {
  async fn consume(&mut self, channel: &amqprs::channel::Channel, deliver: amqprs::Deliver, basic_properties: amqprs::BasicProperties, content: Vec<u8>) {
    //println!("request\n\tdeliver {:?} basic_properties {:?} content {:?}", deliver, basic_properties, content);
    let exchange_name = deliver.exchange();
    if *exchange_name != self.exchange_name {
      panic!("received message for unknown exchange");
    }
    let routing_key = deliver.routing_key();
    if *routing_key != self.request_queue_name {
      panic!("received message for unknown queue");
    }
    // calculate reply
    // TODO: does arc::new() prevent content.clone() here?
    let message_type = basic_properties.message_type().unwrap();
    let message_type_handler = self.request_handlers.get(message_type).unwrap();
    let content = Arc::new(content);
    let response_content_bytes = (message_type_handler)(content).await.unwrap();
    // write reply
    let correlation_id = basic_properties.correlation_id().unwrap();
    let args = amqprs::channel::BasicPublishArguments::default()
      .exchange(self.exchange_name.clone())
      .routing_key(self.reply_queue_name.clone())
      .finish();
    let properties = amqprs::BasicProperties::default()
      .with_reply_to(&self.reply_queue_name)
      .with_correlation_id(correlation_id)
      .finish();
    let result = channel.basic_publish(properties, response_content_bytes, args).await;
    if result.is_err() {
      println!("{:?}", result.err());
    }
  }
}

impl QueueRequestConsumer {
  pub fn new(
    host: String,
    port: u16,
    username: String,
    password: String,
    exchange_name: String,
    request_queue_name: String,
    reply_queue_name: String,
    request_consumer_tag: String,
    request_handlers: HashMap<String, OnRequestCallback>,
  ) -> QueueRequestConsumer {
    return QueueRequestConsumer {
      host,
      port,
      username,
      password,
      exchange_name,
      request_queue_name,
      reply_queue_name,
      request_consumer_tag,
      request_handlers,
    };
  }

  pub async fn connect(&self) -> Result<(Connection, Channel)> {
    let connection_arguments = amqprs::connection::OpenConnectionArguments::new(&self.host, self.port, &self.username, &self.password);
    let connection = amqprs::connection::Connection::open(&connection_arguments).await?;
    connection.register_callback(amqprs::callbacks::DefaultConnectionCallback).await?;
    // open a channel on the connection
    let channel = connection.open_channel(None).await?;
    channel.register_callback(amqprs::callbacks::DefaultChannelCallback).await?;
    // declare queues
    let request_queue_declare_args = amqprs::channel::QueueDeclareArguments::default()
      .queue(self.request_queue_name.clone())
      .finish();
    let reply_queue_declare_args = amqprs::channel::QueueDeclareArguments::default().queue(self.reply_queue_name.clone()).finish();
    channel.queue_declare(request_queue_declare_args).await?;
    channel.queue_declare(reply_queue_declare_args).await?;
    // bind the queue to exchange
    let request_queue_bind_args = amqprs::channel::QueueBindArguments::default()
      .queue(self.request_queue_name.to_string())
      .routing_key(self.request_queue_name.to_string())
      .exchange(self.exchange_name.to_string())
      .finish();
    let reply_queue_bind_args = amqprs::channel::QueueBindArguments::default()
      .queue(self.reply_queue_name.to_string())
      .routing_key(self.reply_queue_name.to_string())
      .exchange(self.exchange_name.to_string())
      .finish();
    channel.queue_bind(request_queue_bind_args).await?;
    channel.queue_bind(reply_queue_bind_args).await?;
    Ok((connection, channel))
  }

  pub async fn start_consuming(&self, channel: &Channel) -> Result<()> {
    let request_consumer_args = amqprs::channel::BasicConsumeArguments::new(&self.request_queue_name, &self.request_consumer_tag)
      .no_ack(true)
      .finish();
    let inner = MyAsyncRequestConsumer {
      // weird workaround due to move that happens in next call
      exchange_name: self.exchange_name.clone(),
      request_queue_name: self.request_queue_name.clone(),
      reply_queue_name: self.reply_queue_name.clone(),
      request_handlers: self.request_handlers.clone(),
    };
    channel.basic_consume(inner, request_consumer_args).await?;
    Ok(())
  }
}
