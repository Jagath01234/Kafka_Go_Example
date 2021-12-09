package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/linkedin/goavro"
	"time"
)

func subscribe(topic string, consumer sarama.Consumer) {

	initialOffset := sarama.OffsetNewest

	pc, _ := consumer.ConsumePartition(topic, 0, initialOffset)

	for {
		time.Sleep(time.Second)
		for message := range pc.Messages() {
			messageReceived(message)
		}
	}
}

func messageReceived(message *sarama.ConsumerMessage) {
	codec, err := goavro.NewCodec(loginEventAvroSchema)
	if err != nil {
		panic(err)
	}

	native, _, err := codec.NativeFromBinary(message.Value)
	if err != nil {
		fmt.Println(err)
	}
	text, _ := codec.TextualFromNative(nil, native)

	fmt.Println(string(text))
}
