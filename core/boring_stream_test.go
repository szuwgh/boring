package core

import (
	"testing"

	"github.com/szuwgh/boring/core/config"
	"github.com/szuwgh/boring/core/stream"
)

func TestNewEngineFromConfigRejectsUnknownMode(t *testing.T) {
	cfg := &config.Config{Boring: []config.BoringConfig{{Name: "bad", Mode: "bogus"}}}
	_, err := NewEngineFromConfig(cfg, NewRegister())
	if err == nil {
		t.Fatal("expected unknown mode error")
	}
}

func TestNewEngineFromConfigRejectsStreamWithMultipleConsumers(t *testing.T) {
	reg := NewRegister()
	reg.RegisterStreamProducer("fake", func(map[string]any) stream.Producer { return nil })
	reg.RegisterStreamConsumer("fake", func(map[string]any) stream.Consumer { return nil })

	cfg := &config.Config{Boring: []config.BoringConfig{{
		Name:     "bad-stream",
		Mode:     "stream",
		Producer: map[string]any{"type": "fake"},
		Consumers: []map[string]any{
			{"type": "fake"},
			{"type": "fake"},
		},
	}}}
	_, err := NewEngineFromConfig(cfg, reg)
	if err == nil {
		t.Fatal("expected multiple consumers error")
	}
}
