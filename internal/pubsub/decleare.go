package pubsub

import (
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueDuration string

const (
	Durable   QueueDuration = "durable"
	Transient QueueDuration = "transient"
)

func DeclareAndBind(conn *amqp.Connection, exchange, queueName, key string, queueType QueueDuration) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	durable := queueType == Durable
	autoDelete := queueType == Transient
	exclusive := autoDelete
	noWait := false

	queue, err := ch.QueueDeclare(queueName, durable, autoDelete, exclusive, noWait, nil)
	if err != nil {
		return ch, amqp.Queue{}, err
	}
	ch.QueueBind(queue.Name, key, exchange, noWait, nil)
	return ch, queue, nil
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}
