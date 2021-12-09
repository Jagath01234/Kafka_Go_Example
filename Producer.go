package main

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/linkedin/goavro"
	"math/rand"
	"os"
	"time"
	"trainingProject/main/model"
)

func SetupProducer() (sarama.AsyncProducer, error) {
	config := sarama.NewConfig()
	return sarama.NewAsyncProducer(kafkaBrokers, config)
}

func ProduceMessages(producer sarama.AsyncProducer, signals chan os.Signal) {
	for {
		time.Sleep(time.Second)

		//	location :=model.Location{7.3245,5.6345}
		message := &sarama.ProducerMessage{Topic: KafkaTopic, Value: sarama.StringEncoder(encode())}
		select {
		case producer.Input() <- message:
			enqueued++
		case <-signals:
			producer.AsyncClose()
			return
		}
	}
}

func encode() []byte {
	codec, err := goavro.NewCodec(loginEventAvroSchema)
	if err != nil {
		panic(err)
	}
	location := model.Location{Latitude: rand.Float64(), Longitude: rand.Float64()}

	json, err := json.Marshal(location)

	native, _, err := codec.NativeFromTextual(json)
	if err != nil {
		fmt.Println(err)
	}

	binary, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		fmt.Println(err)
	}
	return binary
}
