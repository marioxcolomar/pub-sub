package pubsub

import (
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType string

const (
	Ack         AckType = "ack"
	NackRequeue AckType = "requeue"
	NackDiscard AckType = "discard"
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType QueueDuration,
	handler func(T) AckType,
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		log.Fatal(err)
	}

	messages, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}

	unmarshaller := func(data []byte) (T, error) {
		var target T
		err := json.Unmarshal(data, &target)
		return target, err
	}
	go func() {
		defer ch.Close()
		for message := range messages {
			target, err := unmarshaller(message.Body)
			if err != nil {
				fmt.Printf("Could not unmarshal message: %v\n", err)
				continue
			}
			switch handler(target) {
			case Ack:
				fmt.Println("Ack")
				message.Ack(false)

			case NackRequeue:
				fmt.Println("Nack requeue")
				message.Nack(false, true)

			case NackDiscard:
				fmt.Println("Nack discard")
				message.Nack(false, false)

			}
		}
	}()

	return nil
}

type QueueDuration string

const (
	Durable   QueueDuration = "durable"
	Transient QueueDuration = "transient"
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType QueueDuration,
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("Could not create channel: %v", err)
	}

	durable := queueType == Durable
	autoDelete := queueType == Transient
	exclusive := autoDelete
	noWait := false

	queue, err := ch.QueueDeclare(
		queueName,  // name
		durable,    // durable
		autoDelete, // delete when unused
		exclusive,  // exclusive
		noWait,     // no-wait
		amqp.Table{
			"x-dead-letter-exchange": "peril_dlx",
		}, // args
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("Could not declare queue: %v", err)
	}
	err = ch.QueueBind(queue.Name, key, exchange, noWait, nil)
	if err != nil {

		return nil, amqp.Queue{}, fmt.Errorf("Could not bind queue: %v", err)
	}

	return ch, queue, nil
}
