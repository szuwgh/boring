package core

import (
	"fmt"
	"log"

	"github.com/szuwgh/boring/core/consumer"
	"github.com/szuwgh/boring/core/producer"
	"github.com/szuwgh/boring/core/queue"
)

var EngineInstance *Engine

type Engine struct {
	boring map[string]*Boring
}

func (e *Engine) GetBoring(name string) (*Boring, bool) {
	b, ok := e.boring[name]
	return b, ok
}

func (e *Engine) Run() {
	for _, b := range e.boring {
		b.Run()
	}
}

type Boring struct {
	producer producer.Producer
	consumer []consumer.Consumer
	queue    queue.Queue
	done     chan struct{}
}

func NewEngineFromConfig(cfg *Config, reg *Register) (*Engine, error) {
	engine := &Engine{
		boring: make(map[string]*Boring),
	}

	for _, bc := range cfg.Boring {
		b := &Boring{
			queue: queue.NewMemoryQueue(1024),
		}

		// Build producer
		if bc.Producer != nil {
			typeName, _ := bc.Producer["type"].(string)
			if typeName == "" {
				return nil, fmt.Errorf("pipeline %q: producer missing type", bc.Name)
			}
			builder, ok := reg.GetProducer(typeName)
			if !ok {
				return nil, fmt.Errorf("pipeline %q: unknown producer type %q", bc.Name, typeName)
			}
			b.producer = builder(bc.Producer)
		}

		// Build consumers
		for i, cc := range bc.Consumers {
			typeName, _ := cc["type"].(string)
			if typeName == "" {
				return nil, fmt.Errorf("pipeline %q: consumer[%d] missing type", bc.Name, i)
			}
			builder, ok := reg.GetConsumer(typeName)
			if !ok {
				return nil, fmt.Errorf("pipeline %q: unknown consumer type %q", bc.Name, typeName)
			}
			b.consumer = append(b.consumer, builder(cc))
		}

		engine.boring[bc.Name] = b
	}

	return engine, nil
}

func (b *Boring) Consumers() []consumer.Consumer {
	return b.consumer
}

func (b *Boring) Run() {
	b.done = make(chan struct{})

	// If the producer supports replies, wire it to consumers that need it.
	// This enables bidirectional flow: WS producer ← im_reply consumer.
	if replier, ok := b.producer.(producer.Replier); ok {
		for _, c := range b.consumer {
			if ra, ok := c.(consumer.ReplyAware); ok {
				ra.SetReplyFunc(replier.Reply)
			}
		}
	}

	// Producer goroutine: produce messages and enqueue them
	go func() {
		defer b.queue.Close()
		for {
			select {
			case <-b.done:
				return
			default:
			}
			data, err := b.producer.Produce()
			if err != nil {
				return
			}
			b.queue.Enqueue(data)
		}
	}()

	// Consumer goroutine: dequeue messages and fan out to all consumers
	go func() {
		for {
			data, err := b.queue.Dequeue()
			if err != nil {
				return
			}
			for _, c := range b.consumer {
				err := c.Consume(data)
				if err != nil {
					log.Printf("[boring] consumer error: %v", err)
				}
			}
		}
	}()

	// Start producer
	err := b.producer.Start()
	if err != nil {
		log.Printf("[boring] producer start failed: %v", err)
		return
	}
}

func (b *Boring) Stop() {
	close(b.done)
}
