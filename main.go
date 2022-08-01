package main

import (
	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde/protobuf"
	"github.com/golang/protobuf/ptypes/timestamp"
)

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig("http://localhost:9081"))
	checkErr(err)

	serializerConfig := protobuf.NewSerializerConfig()
	serializer, err := protobuf.NewSerializer(srClient, serde.ValueSerde, serializerConfig)
	checkErr(err)

	validTopic := "valid.topic"
	validMsg := &Valid{
		Test: "test",
	}
	_, err = serializer.Serialize(validTopic, validMsg)

	invalidTopic := "invalid.topic"
	invalidMsg := &Invalid{
		Timestamp: &timestamp.Timestamp{},
	}

	_, err = serializer.Serialize(invalidTopic, invalidMsg)

	checkErr(err)
}
