package core

import (
	"github.com/szuwgh/boring/core/queue"
	"github.com/szuwgh/boring/core/stream"
)

var RegisterInstance *Register

func init() {
	RegisterInstance = NewRegister()
}

// Register is a struct that holds the producers and consumers
// It allows for dynamic registration and retrieval of producers and consumers

type Register struct {
	Producers       map[string]queue.ProducerBuilder
	Consumers       map[string]queue.ConsumerBuilder
	StreamProducers map[string]stream.ProducerBuilder
	StreamConsumers map[string]stream.ConsumerBuilder
}

func NewRegister() *Register {
	return &Register{
		Producers:       make(map[string]queue.ProducerBuilder),
		Consumers:       make(map[string]queue.ConsumerBuilder),
		StreamProducers: make(map[string]stream.ProducerBuilder),
		StreamConsumers: make(map[string]stream.ConsumerBuilder),
	}
}

func (r *Register) RegisterProducer(name string, builder queue.ProducerBuilder) {
	r.Producers[name] = builder
}

func (r *Register) RegisterConsumer(name string, builder queue.ConsumerBuilder) {
	r.Consumers[name] = builder
}

func (r *Register) RegisterStreamProducer(name string, builder stream.ProducerBuilder) {
	r.StreamProducers[name] = builder
}

func (r *Register) RegisterStreamConsumer(name string, builder stream.ConsumerBuilder) {
	r.StreamConsumers[name] = builder
}

func (r *Register) GetProducer(name string) (queue.ProducerBuilder, bool) {
	builder, exists := r.Producers[name]
	return builder, exists
}

func (r *Register) GetConsumer(name string) (queue.ConsumerBuilder, bool) {
	builder, exists := r.Consumers[name]
	return builder, exists
}

func (r *Register) GetStreamProducer(name string) (stream.ProducerBuilder, bool) {
	builder, exists := r.StreamProducers[name]
	return builder, exists
}

func (r *Register) GetStreamConsumer(name string) (stream.ConsumerBuilder, bool) {
	builder, exists := r.StreamConsumers[name]
	return builder, exists
}
