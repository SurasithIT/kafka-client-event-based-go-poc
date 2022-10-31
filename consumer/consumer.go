package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const bootstrapServers = "localhost:9093"
const groupId = "info-group"
const topic = "info-topic"

func main() {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          groupId,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	consumer.Subscribe(topic, nil)

	// Read message
	go func() {
		for {
			if msg, err := consumer.ReadMessage(-1); err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("Message: %v\n", msg)
			}
		}
	}()

	// Poll message
	go func() {
		run := true
		for run {
			ev := consumer.Poll(100)
			switch e := ev.(type) {
			case *kafka.Message:
				// application-specific processing
				fmt.Printf("Message: %v\n", e)
			case kafka.PartitionEOF:
				fmt.Printf("PartitionEOF: %v\n", e)
			case kafka.Error:
				fmt.Printf("Error: %v\n", e)
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}()
}
