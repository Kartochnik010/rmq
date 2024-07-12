package main

import (
	"context"
	"fmt"
	"log"
	"rmq/internal"

	"time"

	"github.com/brianvoe/gofakeit"
	"github.com/rabbitmq/amqp091-go"
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

	fmt.Println("Press enter to send message...")
	for {
		fmt.Scanln()
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)

		if err := rmqClient.Send(ctx, "test_events", "test.created.us", amqp091.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: amqp091.Persistent,
			Body:         []byte("Durable " + gofakeit.HackerPhrase()),
		}); err != nil {
			log.Fatalf("Failed to send message: %v", err)
		}
		fmt.Println("Message sent")
	}

}
