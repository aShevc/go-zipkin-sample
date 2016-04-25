package main

import (
	kafkaClient "gopkg.in/Shopify/sarama.v1"
	"os"
	"os/signal"
	"fmt"
	"github.com/elodina/go-avro"
	"github.com/yanzay/log"
	"github.com/aShevc/go-zipkin-sample/kafka"
	"github.com/aShevc/go-zipkin-sample/tracer"
	"github.com/aShevc/go-zipkin-sample"
	"errors"
)

func main() {
	if len(os.Args) < 2 {
		panic(errors.New("At least one argument, representing Kafka broker address should be specified"))
	}
	producerBrokerList := []string{os.Args[1]}

	//Initializing producer
	newProducer, err := kafka.NewProducer(producerBrokerList); if err != nil {
		log.Error("Failed to create a producer")
		panic(err)
	}

	kafka.Producer = newProducer

	//Initializing tracer
	tracer.Tracer = tracer.NewDefaultTracer("sample-consumer", 1, "zipkin")

	//Creating consumers
	consumer, err := kafkaClient.NewConsumer([]string{os.Args[1]}, nil); if err != nil {
		panic(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			panic(err)
		}
	}()

	partitionConsumer, err := consumer.ConsumePartition("sample-topic", 0, kafkaClient.OffsetNewest); if err != nil {
		panic(err)
	}
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			panic(err)
		}
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	consumed := 0
	ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			fmt.Printf("Consumed message w/ offset %d\n", msg.Offset)
			decodedRecord, err := decodeAvro(msg.Value); if err != nil {
				log.Error("Unexpected error while deserializing Avro message")
				panic(err)
			}

			// Having Avro record decoded, we need to extract the field into which we encoded the trace info
			// and feed it to NewSpanfromAvro method of go-zipkin library, this will create a new span
			kafkaMsgSpan := tracer.Tracer.NewSpanFromAvro("kafka_msg", getTraceInfoFromRecord(decodedRecord))
			kafkaMsgSpan.ClientReceiveAndCollect()
			consumed++
		case <-signals:
			break ConsumerLoop
		}
	}

	fmt.Printf("Consumed: %d\n", consumed)
}

func decodeAvro(msgValue []byte) (*avro.GenericRecord, error) {
	reader := avro.NewGenericDatumReader()
	reader.SetSchema(sample.NewLogLinedMessage().Schema())
	decoder := avro.NewBinaryDecoder(msgValue)
	decodedRecord := avro.NewGenericRecord(sample.NewLogLinedMessage().Schema())
	err := reader.Read(decodedRecord, decoder)
	if err != nil {
		panic(err)
	}

	return decodedRecord, nil
}

func getTraceInfoFromRecord(record *avro.GenericRecord) interface{} {
	logLine := record.Get("logline").(*avro.GenericRecord)
	return logLine.Get("traceInfo")
}