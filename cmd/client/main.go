package main

import (
	"fmt"
	"log"
	"strconv"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Connection to amqp successfull!")

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Could not create channel: %v", err)
	}

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Client failed to get username: %v", err)
	}

	gameState := gamelogic.NewGameState(username)

	// Subscribe to army moves
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix+"."+gameState.GetUsername(),
		routing.ArmyMovesPrefix+".*",
		pubsub.Transient,
		handlerMove(gameState, ch),
	)
	if err != nil {
		log.Fatalf("Could not subscribe to army moves: %v", err)
	}

	// Subscribe to recognition of war
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		routing.WarRecognitionsPrefix+".*",
		pubsub.Durable,
		handlerWar(gameState, ch),
	)
	if err != nil {
		log.Fatalf("Could not subscribe to recognition of war: %v", err)
	}

	// Subscribe to pause
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		routing.PauseKey+"."+gameState.GetUsername(),
		routing.PauseKey,
		pubsub.Transient,
		handlerPause(gameState),
	)
	if err != nil {
		log.Fatalf("Could not subscribe to pause: %v", err)
	}

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		switch input[0] {
		case "spawn":
			err = gameState.CommandSpawn(input)
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
			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				routing.ArmyMovesPrefix+"."+move.Player.Username,
				move,
			)
			if err != nil {
				fmt.Printf("Errors: %s\n", err)
				continue
			}
			fmt.Printf("You moved %v units to %s\n", len(move.Units), move.ToLocation)
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			if len(input) == 1 {
				fmt.Println("Spamming command missing")
			}
			c := input[1]
			t, err := strconv.Atoi(c)
			if err != nil {
				fmt.Println("Unable to read spam option")
			}
			for i := range t {
				fmt.Printf("Handling malicious log %v for username %v\n", i, username)
				lg := gamelogic.GetMaliciousLog()
				pubsub.PublishJSON(
					ch,
					routing.ExchangePerilTopic,
					routing.GameLogSlug+"."+username,
					lg,
				)
			}
		case "quit":
			gamelogic.PrintQuit()
			return

		default:
			fmt.Println("Unknown command")
		}
	}
}
