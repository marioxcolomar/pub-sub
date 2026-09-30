package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")

	fmt.Println("Connection to amqp successfull")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	username, err := gamelogic.ClientWelcome()
	failOnError(err, "Client failed in welcome message")

	_, queue, err := pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilDirect,
		routing.PauseKey+"."+username,
		routing.PauseKey,
		pubsub.Transient,
	)
	if err != nil {
		log.Fatalf("Could not subscrive to pause: %v", err)
	}
	fmt.Printf("Queue %v declared and bound! \n", queue.Name)

	gameState := gamelogic.NewGameState(username)

	for {
		input := gamelogic.GetInput()
		if input == nil {
			continue
		}
		switch input[0] {
		case "spawn":
			err := gameState.CommandSpawn(input)
			if err != nil {
				fmt.Println(err)
				continue
			}
		case "move":
			move, err := gameState.CommandMove(input)
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Printf("Your move was successful: %#v", move)
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return

		default:
			fmt.Println("Unknown command")
		}
	}

}

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}
