package main

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"os"
	"os/signal"
	"log"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	
	fmt.Println("Connection to amqp successfull")
	defer conn.Close()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()
}

func failOnError(err error, msg string) {
  if err != nil {
    log.Panicf("%s: %s", msg, err)
  }
}

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error
