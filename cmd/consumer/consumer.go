package main

import (
	"context"
	"fmt"
	"log"
	"rmq/internal"
	"time"

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
		log.Fatalf("Failed to connect to RMQ: %v", err)
	}
	defer conn.Close()

	rmqClient, err := internal.NewRMQClient(conn)
	if err != nil {
		log.Fatalf("Failed to create RMQ client: %v", err)
	}
	defer rmqClient.Close()

	queue, err := rmqClient.CreateQueue("", true, true)
	if err != nil {
		log.Fatalf("Failed to create queue: %v", err)
	}

	if err := rmqClient.CreateBinding(queue.Name, "", "test_events"); err != nil {
		log.Fatalf("Failed to create binding: %v", err)
	}

	messageBus, err := rmqClient.Consume(queue.Name, "email-service", false)
	if err != nil {
		log.Fatalf("Failed to consume message: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	g, _ := errgroup.WithContext(ctx)

	// allows to run multiple goroutines concurrently
	g.SetLimit(10)

	go func() {
		for message := range messageBus {
			g.Go(func() error {
				log.Printf("Received message: %s", message.Body)
				time.Sleep(5 * time.Second)
				if err := message.Ack(false); err != nil {
					return fmt.Errorf("Failed to ack message: %v", err)
				}
				fmt.Printf("Message acked: %v\n", message.DeliveryTag)
				return nil
			})

		}
	}()
	fmt.Println("Press enter to exit...")
	fmt.Scanln()

}
