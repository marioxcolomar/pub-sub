package pubsub

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"

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
	return subscribe[T](
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		func(data []byte) (T, error) {
			var target T
			err := json.Unmarshal(data, &target)
			return target, err
		},
	)
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType QueueDuration,
	handler func(T) AckType,
) error {
	return subscribe[T](
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		func(data []byte) (T, error) {
			buffer := bytes.NewBuffer(data)
			decoder := gob.NewDecoder(buffer)
			var target T
			err := decoder.Decode(&target)
			return target, err
		},
	)
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType QueueDuration,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("Could not declare and bind queue: %v", err)
	}

	messages, err := ch.Consume(
		queue.Name, // queue
		"",         // consumer
		false,      // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		return fmt.Errorf("Could not consume messages: %v", err)
	}

	go func() {
		defer ch.Close()

		for message := range messages {
			target, err := unmarshaller(message.Body)
			if err != nil {
				fmt.Printf("Could not decode the message: %s", err)
				continue
			}

			switch handler(target) {
			case Ack:
				message.Ack(false)
			case NackDiscard:
				message.Nack(false, false)
			case NackRequeue:
				message.Nack(false, true)
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
