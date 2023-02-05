use std::{collections::HashMap, sync::Arc};

use amqprs::{channel::Channel, connection::Connection};
use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::{oneshot::Sender, Mutex};

type SenderCorrelationIdMap = Arc<Mutex<HashMap<String, Sender<Vec<u8>>>>>;

lazy_static::lazy_static! {
    static ref SENDER_CORRELATION_ID_MAP: SenderCorrelationIdMap = Arc::new(Mutex::new(HashMap::new()));
}

struct MyAsyncReplyConsumer {
    pub reply_queue_name: String,
}

#[async_trait]
impl amqprs::consumer::AsyncConsumer for MyAsyncReplyConsumer {
    async fn consume(
        &mut self, // use `&mut self` to make trait object to be `Sync`
        _channel: &amqprs::channel::Channel,
        deliver: amqprs::Deliver,
        basic_properties: amqprs::BasicProperties,
        content: Vec<u8>,
    ) {
        //println!("reply\n\tdeliver {:?} basic_properties {:?} content {:?}", deliver, basic_properties, content);
        let routing_key = deliver.routing_key();
        if *routing_key != self.reply_queue_name {
            panic!("received message from unexpected queue");
        }
        // mark response received
        let correlation_id = basic_properties.correlation_id().unwrap();
        let mut sender_correlation_id_map = SENDER_CORRELATION_ID_MAP.lock().await;
        if sender_correlation_id_map.contains_key(correlation_id) == false {
            println!("no sender for {}?", correlation_id);
            return;
        }
        //println!("marking response received {}", correlation_id);
        let sender = sender_correlation_id_map.remove(correlation_id).unwrap();
        sender.send(content).unwrap();
    }
}

pub struct QueueReplyConsumer {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub exchange_name: String,
    pub request_queue_name: String,
    pub reply_queue_name: String,
    pub reply_consumer_tag: String,
}

impl QueueReplyConsumer {
    pub fn new(
        host: String,
        port: u16,
        username: String,
        password: String,
        exchange_name: String,
        request_queue_name: String,
        reply_queue_name: String,
        reply_consumer_tag: String,
    ) -> QueueReplyConsumer {
        return QueueReplyConsumer {
            host,
            port,
            username,
            password,
            exchange_name,
            request_queue_name,
            reply_queue_name,
            reply_consumer_tag,
        };
    }

    pub async fn connect(&self) -> Result<(Connection, Channel)> {
        let connection_arguments = amqprs::connection::OpenConnectionArguments::new(
            &self.host,
            self.port,
            &self.username,
            &self.password,
        );
        let connection = amqprs::connection::Connection::open(&connection_arguments).await?;
        connection
            .register_callback(amqprs::callbacks::DefaultConnectionCallback)
            .await?;
        // open a channel on the connection
        let channel = connection.open_channel(None).await?;
        channel
            .register_callback(amqprs::callbacks::DefaultChannelCallback)
            .await?;
        // declare queues
        let request_queue_declare_args = amqprs::channel::QueueDeclareArguments::default()
            .queue(self.request_queue_name.clone())
            .finish();
        let reply_queue_declare_args = amqprs::channel::QueueDeclareArguments::default()
            .queue(self.reply_queue_name.clone())
            .finish();
        channel.queue_declare(request_queue_declare_args).await?;
        channel.queue_declare(reply_queue_declare_args).await?;
        // bind the queue to exchange
        let request_queue_bind_args = amqprs::channel::QueueBindArguments::default()
            .queue(self.request_queue_name.clone())
            .routing_key(self.request_queue_name.clone())
            .exchange(self.exchange_name.clone())
            .finish();
        let reply_queue_bind_args = amqprs::channel::QueueBindArguments::default()
            .queue(self.reply_queue_name.clone())
            .routing_key(self.reply_queue_name.clone())
            .exchange(self.exchange_name.clone())
            .finish();
        channel.queue_bind(request_queue_bind_args).await?;
        channel.queue_bind(reply_queue_bind_args).await?;
        Ok((connection, channel))
    }

    pub async fn request(
        &self,
        channel: &Channel,
        message_type: &str,
        request_bytes: Vec<u8>,
        timeout_ms: u64,
    ) -> Result<Vec<u8>> {
        // generate correlation id
        let correlation_id = format!("{}", uuid::Uuid::new_v4());
        // generate sender + receiver
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut sender_correlation_id_map = SENDER_CORRELATION_ID_MAP.lock().await;
        sender_correlation_id_map.insert(correlation_id.clone(), tx);
        drop(sender_correlation_id_map);
        // do rabbitmq request
        let args = amqprs::channel::BasicPublishArguments::default()
            .exchange(self.exchange_name.clone())
            .routing_key(self.request_queue_name.clone())
            .finish();
        let properties = amqprs::BasicProperties::default()
            .with_reply_to(&self.reply_queue_name)
            .with_correlation_id(&correlation_id)
            .with_message_type(message_type)
            .finish();
        channel
            .basic_publish(properties, request_bytes, args)
            .await?;
        // wait for response with timeout
        let response =
            tokio::time::timeout(std::time::Duration::from_millis(timeout_ms), rx).await??;
        Ok(response)
    }

    pub async fn start_consuming(&self, channel: &Channel) -> Result<()> {
        let reply_consumer_args = amqprs::channel::BasicConsumeArguments::new(
            &self.reply_queue_name,
            &self.reply_consumer_tag,
        )
        .no_ack(true)
        .finish();
        let inner = MyAsyncReplyConsumer {
            // weird workaround due to move that happens in next call
            reply_queue_name: self.reply_queue_name.clone(),
        };
        channel.basic_consume(inner, reply_consumer_args).await?;
        Ok(())
    }
}
