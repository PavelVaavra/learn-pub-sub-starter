package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	url := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(url)
	if err != nil {
		log.Fatalf("Error connecting to RabbitMQ: %v", err)
		return
	}
	defer conn.Close()
	fmt.Println("Connected to RabbitMQ")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Error during client welcome: %v", err)
		return
	}

	ch, _, err := pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilDirect,
		strings.Join([]string{routing.PauseKey, username}, "."),
		routing.PauseKey,
		pubsub.Transient,
	)
	if err != nil {
		log.Fatalf("Error declaring and binding queue: %v", err)
		return
	}
	defer ch.Close()

	gameState := gamelogic.NewGameState(username)

	for true {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		cmd := words[0]
		switch cmd {
		case "spawn":
			err := gameState.CommandSpawn(words)
			if err != nil {
				log.Fatalf("Error executing spawn command: %v", err)
			}
		case "move":
			_, err := gameState.CommandMove(words)
			if err != nil {
				log.Fatalf("Error executing move command: %v", err)
			}
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
			fmt.Printf("Unknown command: %s\n", cmd)
		}
	}
}
