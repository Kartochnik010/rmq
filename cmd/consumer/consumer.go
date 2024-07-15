package main

import (
	"context"
	"fmt"
	"log"
	"rmq/internal"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"
)

const (
	username = "admin"
	password = "admin"
	host     = "157.230.119.33:5672"
	vhost    = "test"
)

func main() {
	conn, err := internal.ConnectRMQ(username, password, host, vhost)
	if err != nil {
		log.Printf("Failed to connect to RMQ: %v", err)
		return
	}
	defer conn.Close()

	publishConn, err := internal.ConnectRMQ(username, password, host, vhost)
	if err != nil {
		log.Printf("Failed to connect to RMQ: %v", err)
		return
	}
	defer publishConn.Close()

	publishRMQClient, err := internal.NewRMQClient(publishConn)
	if err != nil {
		log.Printf("Failed to create RMQ client: %v", err)
		return
	}
	defer publishRMQClient.Close()

	defer conn.Close()

	rmqClient, err := internal.NewRMQClient(conn)
	if err != nil {
		log.Printf("Failed to create RMQ client: %v", err)
		return
	}
	defer rmqClient.Close()

	queue, err := rmqClient.CreateQueue("", true, true)
	if err != nil {
		log.Printf("Failed to create queue: %v", err)
		return
	}

	if err := rmqClient.CreateBinding(queue.Name, "", "test_events"); err != nil {
		log.Printf("Failed to create binding: %v", err)
		return
	}

	messageBus, err := rmqClient.Consume(queue.Name, "email-service", false)
	if err != nil {
		log.Printf("Failed to consume message: %v", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	if err := rmqClient.ApplyQoS(10, 0, true); err != nil {
		log.Printf("Failed to apply QoS: %v", err)
		return
	}

	// allows to run multiple goroutines concurrently
	g.SetLimit(10)

	go func() {
		for message := range messageBus {
			g.Go(func() error {
				log.Printf("Received message: %s", message.Body)
				time.Sleep(5 * time.Second)
				if err := message.Ack(false); err != nil {
					return fmt.Errorf("failed to ack message: %v", err)
				}

				if err := publishRMQClient.Send(ctx, "test_callbacks", message.ReplyTo, amqp091.Publishing{
					ContentType:   "text/plain",
					CorrelationId: message.CorrelationId,
					Body:          []byte("Message processed"),
					DeliveryMode:  amqp091.Persistent,
				}); err != nil {
					return fmt.Errorf("failed to send message: %v", err)
				}
				fmt.Printf("Message acked: %v\n", message.DeliveryTag)
				return nil
			})

		}
	}()
	fmt.Println("Press enter to exit...")
	fmt.Scanln()

}
