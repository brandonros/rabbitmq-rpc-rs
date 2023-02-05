use futures::future::BoxFuture;
use std::{collections::HashMap, sync::Arc};

use amqprs::{channel::Channel, connection::Connection};
use anyhow::Result;
use async_trait::async_trait;

pub type OnMessageCallback = Arc<dyn Fn(Vec<u8>) -> BoxFuture<'static, Result<()>> + Send + Sync>;

pub struct QueueSubscriber {
  pub host: String,
  pub port: u16,
  pub username: String,
  pub password: String,
  pub exchange_name: String,
  pub queue_name: String,
  pub subscriber_consumer_tag: String,
  pub message_handlers: HashMap<String, OnMessageCallback>,
}

struct MyAsyncQueueSubscriber {
  pub exchange_name: String,
  pub queue_name: String,
  pub message_handlers: HashMap<String, OnMessageCallback>,
}

#[async_trait]
impl amqprs::consumer::AsyncConsumer for MyAsyncQueueSubscriber {
  async fn consume(&mut self, channel: &amqprs::channel::Channel, deliver: amqprs::Deliver, basic_properties: amqprs::BasicProperties, content: Vec<u8>) {
    //println!("request\n\tdeliver {:?} basic_properties {:?} content {:?}", deliver, basic_properties, content);
    let exchange_name = deliver.exchange();
    if *exchange_name != self.exchange_name {
      panic!("received message for unknown exchange");
    }
    let routing_key = deliver.routing_key();
    if *routing_key != self.queue_name {
      panic!("received message for unknown queue");
    }
    // fire handler
    // TODO: figure out how to not need clone here
    let message_type = basic_properties.message_type().unwrap();
    let message_type_handler = self.message_handlers.get(message_type).unwrap();
    (message_type_handler)(content.clone()).await.unwrap();
  }
}

impl QueueSubscriber {
  pub fn new(
    host: String,
    port: u16,
    username: String,
    password: String,
    exchange_name: String,
    queue_name: String,
    subscriber_consumer_tag: String,
    message_handlers: HashMap<String, OnMessageCallback>,
  ) -> QueueSubscriber {
    return QueueSubscriber {
      host,
      port,
      username,
      password,
      exchange_name,
      queue_name,
      subscriber_consumer_tag,
      message_handlers,
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
    let queue_declare_args = amqprs::channel::QueueDeclareArguments::default().queue(self.queue_name.clone()).finish();
    channel.queue_declare(queue_declare_args).await?;
    // bind the queue to exchange
    let queue_bind_args = amqprs::channel::QueueBindArguments::default()
      .queue(self.queue_name.clone())
      .routing_key(self.queue_name.clone())
      .exchange(self.exchange_name.clone())
      .finish();
    channel.queue_bind(queue_bind_args).await?;
    Ok((connection, channel))
  }

  pub async fn start_consuming(&self, channel: &Channel) -> Result<()> {
    let request_consumer_args = amqprs::channel::BasicConsumeArguments::new(&self.queue_name, &self.subscriber_consumer_tag)
      .no_ack(true)
      .finish();
    let inner = MyAsyncQueueSubscriber {
      // weird workaround due to move that happens in next call
      exchange_name: self.exchange_name.clone(),
      queue_name: self.queue_name.clone(),
      message_handlers: self.message_handlers.clone(),
    };
    channel.basic_consume(inner, request_consumer_args).await?;
    Ok(())
  }
}
