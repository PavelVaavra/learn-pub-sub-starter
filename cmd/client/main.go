package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.Acktype {
	return func(state routing.PlayingState) pubsub.Acktype {
		defer fmt.Print("> ")
		gs.HandlePause(state)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.Acktype {
	return func(move gamelogic.ArmyMove) pubsub.Acktype {
		defer fmt.Print("> ")
		moveOutcome := gs.HandleMove(move)
		switch moveOutcome {
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		case gamelogic.MoveOutcomeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				routing.WarRecognitionsPrefix+"."+gs.GetUsername(),
				gamelogic.RecognitionOfWar{
					Attacker: move.Player,
					Defender: gs.GetPlayerSnap(),
				},
			)
			if err != nil {
				fmt.Printf("error: %s\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		}
		fmt.Println("error: unknown move outcome")
		return pubsub.NackDiscard
	}
}

func handlerWar(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.Acktype {
	return func(recognition gamelogic.RecognitionOfWar) pubsub.Acktype {
		defer fmt.Print("> ")
		warOutcome, winner, loser := gs.HandleWar(recognition)
		switch warOutcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			message := winner + " won a war against " + loser
			err := PublishGameLog(ch, routing.ExchangePerilTopic, routing.GameLogSlug+"."+gs.GetUsername(), gs.GetUsername(), message)
			if err != nil {
				fmt.Printf("error: %s\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeYouWon:
			message := winner + " won a war against " + loser
			err := PublishGameLog(ch, routing.ExchangePerilTopic, routing.GameLogSlug+"."+gs.GetUsername(), gs.GetUsername(), message)
			if err != nil {
				fmt.Printf("error: %s\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			message := "A war between " + winner + " and " + loser + " resulted in a draw"
			err := PublishGameLog(ch, routing.ExchangePerilTopic, routing.GameLogSlug+"."+gs.GetUsername(), gs.GetUsername(), message)
			if err != nil {
				fmt.Printf("error: %s\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		}
		fmt.Println("error: unknown war outcome")
		return pubsub.NackDiscard
	}
}

func PublishGameLog(ch *amqp.Channel, exchange, key, username, message string) error {
	err := pubsub.PublishGob(ch, exchange, key, routing.GameLog{
		CurrentTime: time.Now(),
		Message:     message,
		Username:    username,
	})
	if err != nil {
		return err
	}
	return nil
}

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

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("could not create channel: %v", err)
	}

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Error during client welcome: %v", err)
		return
	}

	gameState := gamelogic.NewGameState(username)

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		routing.PauseKey+"."+gameState.GetUsername(),
		routing.PauseKey,
		pubsub.Transient,
		handlerPause(gameState),
	)
	if err != nil {
		log.Fatalf("Error subscribing to pause messages: %v", err)
		return
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix+"."+gameState.GetUsername(),
		routing.ArmyMovesPrefix+".*",
		pubsub.Transient,
		handlerMove(gameState, ch),
	)
	if err != nil {
		log.Fatalf("Error subscribing to army move messages: %v", err)
		return
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		routing.WarRecognitionsPrefix+".*",
		pubsub.Durable,
		handlerWar(gameState, ch),
	)
	if err != nil {
		log.Fatalf("Error subscribing to war messages: %v", err)
		return
	}

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
				fmt.Println(err)
				continue
			}
		case "move":
			move, err := gameState.CommandMove(words)
			if err != nil {
				fmt.Println(err)
				continue
			}
			err = pubsub.PublishJSON(ch, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+move.Player.Username, move)
			if err != nil {
				fmt.Printf("error: %s\n", err)
				continue
			}
			fmt.Printf("Moved %v units to %s\n", len(move.Units), move.ToLocation)
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			if len(words) != 2 {
				fmt.Printf("usage: spam <n times>")
				continue
			}
			n, err := strconv.Atoi(words[1])
			if err != nil {
				fmt.Printf("error: %s\n", err)
				continue
			}
			for i := 0; i < n; i++ {
				log := gamelogic.GetMaliciousLog()
				err := pubsub.PublishGob(ch, routing.ExchangePerilTopic, routing.GameLogSlug+"."+gameState.GetUsername(), routing.GameLog{
					CurrentTime: time.Now(),
					Message:     log,
					Username:    gameState.GetUsername(),
				})
				if err != nil {
					fmt.Printf("error: %s\n", err)
				}
			}
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Printf("Unknown command: %s\n", cmd)
		}
	}
}
