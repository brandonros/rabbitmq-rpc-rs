use amqprs::{channel::Channel, connection::Connection};
use anyhow::Result;

pub struct QueuePublisher {
  pub host: String,
  pub port: u16,
  pub username: String,
  pub password: String,
  pub exchange_name: String,
  pub queue_name: String,
}

impl QueuePublisher {
  pub fn new(
    host: String,
    port: u16,
    username: String,
    password: String,
    exchange_name: String,
    queue_name: String,
  ) -> QueuePublisher {
    return QueuePublisher {
      host,
      port,
      username,
      password,
      exchange_name,
      queue_name,
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
    let queue_declare_args = amqprs::channel::QueueDeclareArguments::default()
      .queue(self.queue_name.clone())
      .finish();
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

  pub async fn publish(&self, channel: &Channel, message_type: &str, message_bytes: Vec<u8>) -> Result<()> {
    // generate correlation id
    let correlation_id = format!("{}", uuid::Uuid::new_v4());
    // do rabbitmq request
    let args = amqprs::channel::BasicPublishArguments::default()
      .exchange(self.exchange_name.clone())
      .routing_key(self.queue_name.clone())
      .finish();
    let properties = amqprs::BasicProperties::default()
      .with_correlation_id(&correlation_id)
      .with_message_type(message_type)
      .finish();
    channel.basic_publish(properties, message_bytes, args).await?;
    Ok(())
  }
}
