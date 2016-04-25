package tracer

import (
	"github.com/elodina/go-zipkin"
	"github.com/aShevc/go-zipkin-sample/kafka"
)

var (
	Tracer *zipkin.Tracer
)

func NewDefaultTracer(serviceName string, sampleRate int, kafkaTopic string) *zipkin.Tracer {

	return zipkin.NewTracer(serviceName, sampleRate, kafka.Producer, zipkin.LocalNetworkIP(), zipkin.DefaultPort(), kafkaTopic)
}