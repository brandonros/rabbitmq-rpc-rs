# rabbitmq-rpc-rs
Rust request/reply over RabbitMQ library

## How to test

```shell
docker run -it --rm --hostname rabbitmq --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
cargo run --example rpc_server
cargo run --example rpc_client
cargo run --example pubsub_subscriber
cargo run --example pubsub_publisher
```