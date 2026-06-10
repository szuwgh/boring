package core

import (
	"github.com/szuwgh/boring/core/consumer"
	"github.com/szuwgh/boring/core/producer"
)

var RegisterInstance *Register

func init() {
	RegisterInstance = NewRegister()
}

// Register is a struct that holds the producers and consumers
// It allows for dynamic registration and retrieval of producers and consumers

type Register struct {
	Producers map[string]producer.ProducerBuilder
	Consumers map[string]consumer.ConsumerBuilder
}

func NewRegister() *Register {
	return &Register{
		Producers: make(map[string]producer.ProducerBuilder),
		Consumers: make(map[string]consumer.ConsumerBuilder),
	}
}

func (r *Register) RegisterProducer(name string, builder producer.ProducerBuilder) {
	r.Producers[name] = builder
}

func (r *Register) RegisterConsumer(name string, builder consumer.ConsumerBuilder) {
	r.Consumers[name] = builder
}

func (r *Register) GetProducer(name string) (producer.ProducerBuilder, bool) {
	builder, exists := r.Producers[name]
	return builder, exists
}

func (r *Register) GetConsumer(name string) (consumer.ConsumerBuilder, bool) {
	builder, exists := r.Consumers[name]
	return builder, exists
}
