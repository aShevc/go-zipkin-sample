package kafka

import (
	"github.com/elodina/siesta-producer"
	"github.com/yanzay/log"
	"time"
	"github.com/elodina/siesta"
)

var (
	Producer *producer.KafkaProducer
)

func NewProducer(brokerList []string) (*producer.KafkaProducer, error) {
	producerConfig := producer.NewProducerConfig()

	log.Infof("Creating producer with broker list: %v", brokerList)
	producerConfig.BatchSize = 1
	producerConfig.ClientID = "sample-producer"
	producerConfig.MaxRequests = 10
	producerConfig.SendRoutines = 10
	producerConfig.ReceiveRoutines = 10
	producerConfig.ReadTimeout = 5 * time.Second
	producerConfig.WriteTimeout = 5 * time.Second
	producerConfig.RequiredAcks = 1
	producerConfig.AckTimeoutMs = 2000
	producerConfig.Linger = 1 * time.Second
	producerConfig.Retries = 5
	producerConfig.RetryBackoff = 100 * time.Millisecond
	kafkaConnectorConfig := siesta.NewConnectorConfig()
	kafkaConnectorConfig.BrokerList = brokerList
	connector, err := siesta.NewDefaultConnector(kafkaConnectorConfig); if err != nil {
		return nil, err
	}
	return producer.NewKafkaProducer(producerConfig, producer.ByteSerializer, producer.ByteSerializer, connector), nil
}
