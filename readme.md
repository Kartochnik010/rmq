# Learn RabbitMQ 

### Current state
- RPC

![rpc](image.png)

## Quick Start

    docker run --rm -d --name rabbitmq -p 15672:15672 -p 5672:5672 rabbitmq:3-management

You can visit `http://localhost:15672` to see the RabbitMQ management console. The default username and password are `guest`.

## Consepts

- **Producer**: A program that sends messages.
- **Consumer**: A program that receives messages.
- **Queue**: A buffer that stores messages.
- **Exchange**: Receives messages from producers and pushes them to queues.
- **Binding**: A set of rules that defines which messages should go to which queues.
- **Routing Key**: A key that the exchange uses to route messages to queues.
- **Connection**: A TCP connection between your application and the RabbitMQ broker.
- **Channel**: A virtual connection inside a connection.
- **Message**: A piece of data that is sent by a producer and consumed by a consumer.
- **Acknowledgement**: A signal sent by a consumer to tell RabbitMQ that a message has been received and processed.
- **Publisher Confirms**: A feature that allows a producer to know that a message has been received by RabbitMQ.
- **Consumer Acknowledgements**: A feature that allows a consumer to tell RabbitMQ that a message has been received and processed.
- **Dead Letter Exchange**: An exchange that receives messages that were not consumed after a certain period of time.
- **Dead Letter Queue**: A queue that receives messages that were not consumed after a certain period of time.
- **Dead Letter Routing Key**: A key that the exchange uses to route messages to the dead letter queue.
- **Message TTL**: A time-to-live value that defines how long a message can live in a queue before it is discarded.
- **Queue TTL**: A time-to-live value that defines how long a queue can live before it is discarded.
- **Alternate Exchange**: An exchange that receives messages that were not routed to any queue.
- **Shovel**: A feature that allows you to move messages between two RabbitMQ brokers.
- **Federation**: A feature that allows you to move messages between two RabbitMQ brokers in a more efficient way.
- **Cluster**: A group of RabbitMQ brokers that are connected to each other.
- **High Availability**: A feature that allows you to replicate queues across multiple RabbitMQ brokers.
- **Sharding**: A feature that allows you to split a queue into multiple partitions.
- **Partition**: A subset of a queue that stores a portion of the messages.
- **Prefetch Count**: A value that defines how many messages a consumer can receive at once.
- **Consumer Priority**: A value that defines the priority of a consumer.
- **Flow Control**: A feature that allows a consumer to tell RabbitMQ to stop sending messages.
- **Connection Recovery**: A feature that allows a connection to recover from network failures.
- **Channel Recovery**: A feature that allows a channel to recover from network failures.


## Setup Users, Permissions, Tags, and Virtual Hosts
Inside container you can access the rabbitmqctl command to manage users, permissions, tags, and virtual hosts.     
    
    RMQ_USERNAME=admin
    RMQ_PASSWORD=admin
    RMQ_VHOST=test
    # delete the default user
    rabbitmqctl delete_user guest
    
    # create a user
    rabbitmqctl add_user $RMQ_USERNAME $RMQ_PASSWORD 
    
    # set user tags 
    rabbitmqctl set_user_tags $RMQ_USERNAME administrator 
    
    # create a virtual host
    rabbitmqctl add_vhost $RMQ_VHOST 
    
    # set permissions
    rabbitmqctl set_permissions -p $RMQ_VHOST $RMQ_USERNAME ".*" ".*" ".*"


Now an exchange should be created. Choose the type of exchange.

    # Create an Exchange
    rabbitmqadmin declare exchange --vhost=test name=test_events type=topic -u admin -p admin durable=true
    rabbitmqctl set_topic_permissions -p test admin test_events "^test.*" "^test.*"

    # change exchange type to fanout
    rabbitmqadmin declare exchange --vhost=test name=test_callbacks type=direct -u admin -p admin durable=true
    rabbitmqctl set_topic_permissions -p test admin test_callbacks ".*" ".*"


Connection string will be `amqp://$RMQ_USERNAME:$RMQ_PASSWORD@localhost/$RMQ_VHOST`

## Best practices
- **Use a single connection per application**: Creating a connection is an expensive operation, so you should reuse the same connection for all your channels.
- **Use a single channel per thread**: Creating a channel is a cheap operation, so you should create a new channel for each thread in your application.
- **Use a single consumer per channel**: Creating a consumer is an expensive operation, so you should create a new consumer for each channel.
- **Use publisher confirms**: Publisher confirms allow you to know that a message has been received by RabbitMQ.
- **Use consumer acknowledgements**: Consumer acknowledgements allow you to tell RabbitMQ that a message has been received and processed.
- **Use dead letter exchanges**: Dead letter exchanges allow you to move messages that were not consumed to a separate queue.
- **Use message TTL**: Message TTL allows you to discard messages that were not consumed after a certain period of time.
- **Use queue TTL**: Queue TTL allows you to discard queues that were not used after a certain period of time.
- **Use alternate exchanges**: Alternate exchanges allow you to move messages that were not routed to any queue to a separate exchange.
- **Use shovels**: Shovels allow you to move messages between two RabbitMQ brokers.
- **Use federation**: Federation allows you to move messages between two RabbitMQ brokers in a more efficient way.
- **Use clusters**: Clusters allow you to replicate queues across multiple RabbitMQ brokers.
- **Use high availability**: High availability allows you to replicate queues across multiple RabbitMQ brokers in a more efficient way.
- **Use sharding**: Sharding allows you to split a queue into multiple partitions.
- **Use prefetch count**: Prefetch count allows you to control how many messages a consumer can receive at once.
- **Use consumer priority**: Consumer priority allows you to control the priority of a consumer.
- **Use flow control**: Flow control allows you to tell RabbitMQ to stop sending messages to a consumer.
- **Use connection recovery**: Connection recovery allows a connection to recover from network failures.
- **Use channel recovery**: Channel recovery allows a channel to recover from network failures.
