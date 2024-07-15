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
		log.Printf("Failed to connect to RMQ: %v", err)
		return
	}
	defer conn.Close()

	consumeConn, err := internal.ConnectRMQ(username, password, host, vhost)
	if err != nil {
		log.Printf("Failed to connect to RMQ: %v", err)
		return
	}
	defer consumeConn.Close()

	consumerRMQClient, err := internal.NewRMQClient(consumeConn)
	if err != nil {
		log.Printf("Failed to create RMQ client: %v", err)
		return
	}
	defer consumerRMQClient.Close()

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
	if err != rmqClient.CreateBinding(queue.Name, queue.Name, "test_callbacks") {
		log.Printf("Failed to create binding: %v", err)
		return
	}
	messageBus, err := consumerRMQClient.Consume(queue.Name, "email-service", true)
	if err != nil {
		log.Printf("Failed to consume message: %v", err)
		return
	}

	go func() {
		for message := range messageBus {
			log.Printf("Message Callback %s\n", message.CorrelationId)
		}
	}()

	fmt.Println("Press enter to send message...")
	for {
		fmt.Scanln()
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)

		if err := rmqClient.Send(ctx, "test_events", "test.created.us", amqp091.Publishing{
			ContentType:   "text/plain",
			DeliveryMode:  amqp091.Persistent,
			ReplyTo:       queue.Name,
			CorrelationId: fmt.Sprint(gofakeit.Int16()),
			Body:          []byte("Durable " + gofakeit.HackerPhrase()),
		}); err != nil {
			log.Fatalf("Failed to send message: %v", err)
		}
		fmt.Println("Message sent")
	}

}
