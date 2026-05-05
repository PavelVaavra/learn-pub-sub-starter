package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerGameLog() func(routing.GameLog) pubsub.Acktype {
	return func(gl routing.GameLog) pubsub.Acktype {
		defer fmt.Print("> ")
		err := gamelogic.WriteLog(gl)
		if err != nil {
			fmt.Printf("Error writing log: %v\n", err)
			return pubsub.NackRequeue
		}
		return pubsub.Ack
	}
}

func main() {
	fmt.Println("Starting Peril server...")
	url := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(url)
	if err != nil {
		log.Fatalf("Error connecting to RabbitMQ: %v", err)
		return
	}
	defer conn.Close()
	fmt.Println("Connected to RabbitMQ")

	// ch, queue, err := pubsub.DeclareAndBind(
	// 	conn,
	// 	routing.ExchangePerilTopic,
	// 	routing.GameLogSlug,
	// 	"game_logs.*",
	// 	pubsub.Durable,
	// )
	// if err != nil {
	// 	log.Fatalf("Error declaring and binding queue: %v", err)
	// 	return
	// }
	// fmt.Printf("Queue %v declared and bound!\n", queue.Name)

	err = pubsub.SubscribeGob(
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GameLogSlug+".*",
		pubsub.Durable,
		handlerGameLog(),
	)
	if err != nil {
		log.Fatalf("Error subscribing to game logs: %v", err)
		return
	}

	gamelogic.PrintServerHelp()

	for true {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		cmd := words[0]
		switch cmd {
		case "pause":
			pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: true,
			})
			fmt.Println("Pause message sent")
		case "resume":
			pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: false,
			})
			fmt.Println("Resume message sent")
		case "quit":
			fmt.Println("Quitting Peril server...")
			return
		default:
			fmt.Printf("Unknown command: %s\n", cmd)
		}
	}
}
