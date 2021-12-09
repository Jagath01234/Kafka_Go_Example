package main

import (
	"github.com/Shopify/sarama"
	"log"
	"os"
	"os/signal"
	"sync"
)

var (
	enqueued     int
	kafkaBrokers = []string{"localhost:9092"}
	KafkaTopic   = "sarama_topic1"
)

const loginEventAvroSchema = `{"type": "record", "name": "Location", "fields": [{"name": "Latitude", "type": "float"},{"name": "Longitude", "type": "float"}]}`

func main() {

	go callProducer()
	go callConsumer()

	var wg sync.WaitGroup
	wg.Add(1)
	wg.Wait()
}

func callProducer() {
	producer, err := SetupProducer()
	if err != nil {
		panic(err)
	} else {
		log.Println("Kafka AsyncProducer up and running!")
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	ProduceMessages(producer, signals)

	log.Printf("Kafka AsyncProducer finished with %d messages produced.", enqueued)
}

func callConsumer() {
	config := sarama.NewConfig()

	consumer, ex := sarama.NewConsumer(kafkaBrokers, config)
	if ex != nil {
		panic(ex)
	} else {
		log.Println("Consumer is up and running ...!")
	}
	subscribe(KafkaTopic, consumer)
}
