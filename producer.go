package main

import (
	//"fmt"
	"gopkg.in/Shopify/sarama.v1"
)

type KafkaPublisher struct {
	asyncProducer sarama.AsyncProducer
}

func (producer *KafkaPublisher) Init() error {
	config := sarama.NewConfig()
	//config.Producer.Flush.MaxMessages = 20000
	//config.Producer.Compression = sarama.CompressionSnappy
	//config.Producer.Return.Successes = true
	asyncProducer, err := sarama.NewAsyncProducer([]string{"ip:9092"}, config)
	producer.asyncProducer = asyncProducer
	return err

}

func (producer *KafkaPublisher) Send(bridgeMsg *BridgeMsg) {
	msg := bridgeMsg.toJsonString()
	//fmt.Println("send msg", msg)
	message := &sarama.ProducerMessage{Topic: "vd.info", Value: sarama.StringEncoder(msg)}
	producer.asyncProducer.Input() <- message
}
