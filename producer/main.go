package main

import (
	"time"
	"github.com/elodina/go-avro"
	"bytes"
	"github.com/yanzay/log"
	producer "github.com/elodina/siesta-producer"
	"github.com/aShevc/go-zipkin-sample/kafka"
	"github.com/aShevc/go-zipkin-sample/tracer"
	"github.com/aShevc/go-zipkin-sample"
	"os"
	"errors"
	"github.com/elodina/go-zipkin"
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
	tracer.Tracer = tracer.NewDefaultTracer("sample-producer", 1, "zipkin")

	topic := "sample-topic"

	for {
		kafkaMsgSpan := tracer.Tracer.NewSpan("kafka_msg")

		kafkaMsg, err := composeAvro("ping", kafkaMsgSpan); if err != nil {
			log.Error("Unexpected error while composing Avro message")
			panic(err)
		}

		kafka.Producer.Send(&producer.ProducerRecord{Topic: topic, Value: kafkaMsg.Bytes(), Partition: 0})

		kafkaMsgSpan.ClientSendAndCollect()
		time.Sleep(1000 * time.Millisecond)
	}
}

func composeAvro(payloadMsg string, zipkinSpan *zipkin.Span) (*bytes.Buffer, error) {
	record := avro.NewGenericRecord(sample.NewLogLinedMessage().Schema())
	logline := avro.NewGenericRecord(sample.NewLogLine().Schema())
	payload := avro.NewGenericRecord(sample.NewMessage().Schema())

	payload.Set("time", time.Now().UnixNano() / 1000000)
	payload.Set("msg", payloadMsg)
	record.Set("payload", payload)
	// We need to get Avro-encoded Zipkin trace info
	logline.Set("traceInfo", zipkinSpan.GetAvroTraceInfo())
	record.Set("logline", logline)

	writer := avro.NewGenericDatumWriter()
	writer.SetSchema(sample.NewLogLinedMessage().Schema())
	buffer := new(bytes.Buffer)
	encoder := avro.NewBinaryEncoder(buffer)
	err := writer.Write(record, encoder)
	if err != nil {
		return nil, err
	}

	return buffer, nil
}


