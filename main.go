package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const bootstrapServer = "localhost:9093"
const alarmTopic = "alarm-topic"
const alarmGroupId = "alarm-group"
const infoTopic = "info-topic"
const infoGroupId = "info-group"

func main() {
	alarmConsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServer,
		"group.id":          alarmGroupId,
		"auto.offset.reset": "latest",
	})
	if err != nil {
		panic(err)
	}
	defer alarmConsumer.Close()

	alarmConsumer.Subscribe(alarmTopic, nil)

	defer alarmConsumer.Unsubscribe()

	infoConsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":    bootstrapServer,
		"group.id":             infoGroupId,
		"auto.offset.reset":    "earliest",
		"enable.auto.commit":   false,
		"enable.partition.eof": true,
	})

	if err != nil {
		panic(err)
	}
	defer infoConsumer.Close()

	for {
		if alarmMsg, err := alarmConsumer.ReadMessage(-1); err != nil {
			fmt.Printf("Consumer error: %v (%v)\n", err, alarmMsg)
		} else {
			// Read onlyone  latest message
			alarmMsg.TopicPartition.Offset = kafka.OffsetTail(1)

			fmt.Println("===================")
			fmt.Printf("Message on %s: %s\n", alarmMsg.TopicPartition, string(alarmMsg.Value))
			endTime := alarmMsg.Timestamp
			run := true
			infoConsumer.Subscribe(infoTopic, nil)

			for run {
				infoEvent := infoConsumer.Poll(100)
				switch infoMsg := infoEvent.(type) {
				case *kafka.Message:
					if infoMsg.Timestamp.After(endTime) {
						fmt.Printf("%% timestamp is over Message offset %s ,\ntime stamp : %s ,\nendTime: %s\n", infoMsg.TopicPartition.Offset, infoMsg.Timestamp, endTime)
						run = false
						break
					}

					processMessage(infoMsg)
					_, err := infoConsumer.CommitMessage(infoMsg)
					if err != nil {
						fmt.Println(err)
						run = false
						break
					}
				case kafka.PartitionEOF:
					fmt.Printf("PartitionEOF: %v\n", infoMsg)
					run = false
				case kafka.OffsetsCommitted:
					fmt.Printf("OffsetsCommitted: %v\n", infoMsg)
				case kafka.Error:
					fmt.Printf("Error: %v\n", infoMsg)
				default:
					fmt.Printf("Ignored %v\n", infoMsg)
				}
			}
			infoConsumer.Unsubscribe()
		}
	}
}

func processMessage(message *kafka.Message) {
	fmt.Printf("%% Message offset %s => %s\n", message.TopicPartition.Offset, message.Value)

}
