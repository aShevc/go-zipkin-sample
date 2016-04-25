go-zipkin Sample
================

# Overview
This repo contains example on low-level using of Zipkin-based traces in a distributed application. In this example Kafka
is being used as a traces transport as well as the transport between distributed application components. Avro is being 
used for serialization. The application components are Kafka producer and consumer. Producer writes new messages to 
Kafka every second, Zipkin trace info is sent along this message encoded into the Avro serialized object. Every message 
in this message stream is traced.

# Usage
In order to launch this example, clone this repo and launch from this directory:

```
godep restore
go install ./...
```

This will build the consumer and producer execution binaries. You may launch them like this:

```
cd $GOPATH/bin
./producer <kafka_broker_address>
./consumer <kafka_broker_address>
```
