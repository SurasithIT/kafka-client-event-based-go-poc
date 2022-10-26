package main

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	bootstrapServer := "localhost:9093"
	alarmTopic := "alarm-topic"
	infoTopic := "info-topic"

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServer,
	})

	if err != nil {
		fmt.Printf("Error create producer %s", err)
	}
	defer producer.Close()

	// Produce messages to topic (Synchronously)
	go func() {
		for {
			produceMessageSync(producer, alarmTopic, nil, []byte("test send alarm"))
			time.Sleep(time.Second * time.Duration(20))
		}
	}()

	// Produce messages to topic (asynchronously)
	go func() {
		for {
			produceMessageAsync(producer, infoTopic, nil, []byte("test send info"))
			time.Sleep(time.Second * time.Duration(3))
		}
	}()

	time.Sleep(time.Minute * time.Duration(5))
}

func produceMessageAsync(producer *kafka.Producer, topic string, key []byte, message []byte) {
	// Delivery report handler for produced messages
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
					continue
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
					continue
				}
			}
		}
	}()

	producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
		Value:          message,
	}, nil)

	// Wait for message deliveries before shutting down
	// producer.Flush(15 * 1000)
}

func produceMessageSync(producer *kafka.Producer, topic string, key []byte, message []byte) {
	delivery_chan := make(chan kafka.Event, 10000)
	err := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
		Value:          message},
		delivery_chan,
	)
	if err != nil {
		fmt.Printf("Produce failed: %v\n", err)
	}

	e := <-delivery_chan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
	} else {
		fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}
	close(delivery_chan)
}
