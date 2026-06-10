package core

import (
	"fmt"
	"log"

	"github.com/szuwgh/boring/core/config"
	"github.com/szuwgh/boring/core/queue"
	"github.com/szuwgh/boring/core/stream"
)

var EngineInstance *Engine

type Engine struct {
	queue  map[string]*queue.Queuing
	stream map[string]*stream.Pipeline
}

func (e *Engine) GetBoring(name string) (*queue.Queuing, bool) {
	b, ok := e.queue[name]
	return b, ok
}

func (e *Engine) Run() {
	for _, b := range e.queue {
		b.Run()
	}
	for _, s := range e.stream {
		s.Run()
	}
}

func (e *Engine) Stop() {
	for _, b := range e.queue {
		b.Stop()
	}
	for _, s := range e.stream {
		if err := s.Stop(); err != nil {
			log.Printf("[boring] stream stop error: %v", err)
		}
	}
}

func NewEngineFromConfig(cfg *config.Config, reg *Register) (*Engine, error) {
	engine := &Engine{
		queue:  make(map[string]*queue.Queuing),
		stream: make(map[string]*stream.Pipeline),
	}

	for _, bc := range cfg.Boring {
		mode := bc.Mode
		if mode == "" {
			mode = "message"
		}
		switch mode {
		case "message":
			b, err := queue.NewMessagePipelineFromConfig(bc, reg)
			if err != nil {
				return nil, err
			}
			engine.queue[bc.Name] = b
		case "stream":
			s, err := stream.NewStreamPipelineFromConfig(bc, reg)
			if err != nil {
				return nil, err
			}
			engine.stream[bc.Name] = s
		default:
			return nil, fmt.Errorf("pipeline %q: unknown mode %q", bc.Name, bc.Mode)
		}
	}

	return engine, nil
}
