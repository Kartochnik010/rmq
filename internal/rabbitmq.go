package internal

import (
	"context"
	"fmt"
	"log"

	ampq "github.com/rabbitmq/amqp091-go"
)

type RMQClient struct {
	conn *ampq.Connection
	ch   *ampq.Channel
}

func ConnectRMQ(username, password, host, vhost string) (*ampq.Connection, error) {
	return ampq.Dial(fmt.Sprintf("amqp://%s:%s@%s/%s", username, password, host, vhost))
}

func NewRMQClient(conn *ampq.Connection) (*RMQClient, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	if err := ch.Confirm(false); err != nil {
		return nil, err
	}

	return &RMQClient{conn, ch}, nil
}

func (r *RMQClient) Close() error {
	return r.ch.Close()
}

// if queueName is empty, a random queue name will be generated by rabbitmq
func (r *RMQClient) CreateQueue(queueName string, durable, autodelete bool) (*ampq.Queue, error) {
	// name string - the name of the queue
	// durable bool - durable queues remain active when a rabbitmq restarts
	// autoDelete bool - autoDelete queues are deleted when the last consumer unsubscribes
	// exclusive bool - exclusive queues can only be used by the connection that created them
	// noWait bool - noWait will not wait for the response
	// args ampq.Table - arguments for declaring the queue
	q, err := r.ch.QueueDeclare(queueName, durable, autodelete, false, false, nil)
	if err != nil {
		return nil, err
	}
	return &q, nil
}

func (r *RMQClient) CreateBinding(name, binding, exchange string) error {
	// destination string - the name of the queue
	// key string - the routing key
	// source string - the name of the exchange
	// noWait bool - noWait will not wait for the response
	// args ampq.Table - arguments for declaring the binding
	return r.ch.QueueBind(name, binding, exchange, false, nil)
}

func (r *RMQClient) Send(ctx context.Context, exchange, routingKey string, options ampq.Publishing) error {
	// exchange string - the name of the exchange
	// key string - the routing key
	// mandatory bool - mandatory will return an error if the message cannot be routed
	// immediate bool - immediate will return an error if the message cannot be routed or stored
	// msg ampq.Publishing - the message to send
	confirm, err := r.ch.PublishWithDeferredConfirmWithContext(ctx, exchange, routingKey, true, false, options)
	if err != nil {
		return err
	}
	log.Println(confirm.Wait())
	return nil
}

func (r *RMQClient) Consume(queue, consumer string, autoAck bool) (<-chan ampq.Delivery, error) {
	// queue string - the name of the queue
	// consumer string - the name of the consumer
	// autoAck bool - autoAck will automatically acknowledge messages
	// exclusive bool - exclusive queues can only be used by the connection that created them
	// noLocal bool - noLocal will not send messages that were published by the same connection
	// noWait bool - noWait will not wait for the response
	// args ampq.Table - arguments for consuming the queue
	return r.ch.Consume(queue, consumer, autoAck, false, false, false, nil)

}

// ApplyQoS applies the Quality of Service settings to the channel
func (r *RMQClient) ApplyQoS(count, size int, global bool) error {
	// prefetchCount int - the number of messages to fetch
	// prefetchSize int - the size of the messages to fetch
	// global bool - global will apply the settings to the entire connection
	return r.ch.Qos(count, size, global)
}
