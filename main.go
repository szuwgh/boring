package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/szuwgh/boring/core"
	"github.com/szuwgh/boring/core/consumer"
	"github.com/szuwgh/boring/core/consumer/claude"
	"github.com/szuwgh/boring/core/consumer/terminal"
	"github.com/szuwgh/boring/core/producer"
	"github.com/szuwgh/boring/core/producer/http"
	"github.com/szuwgh/boring/core/producer/websocket"
)

type stdinProducer struct {
	scanner *bufio.Scanner
}

func (s *stdinProducer) Produce() ([]byte, error) {
	if s.scanner.Scan() {
		line := s.scanner.Bytes()
		cp := make([]byte, len(line))
		copy(cp, line)
		return cp, nil
	}
	if err := s.scanner.Err(); err != nil {
		return nil, err
	}
	return nil, io.EOF
}

func (s *stdinProducer) Start() error {
	return nil
}

type stdinConfig struct{}

func stdinBuilder(config *stdinConfig) producer.Producer {
	return &stdinProducer{scanner: bufio.NewScanner(os.Stdin)}
}

func main() {
	// Register plugin builders
	core.RegisterInstance.RegisterConsumer("terminal",
		consumer.MakeConsumerBuilder(terminal.TerminalBuilder))
	core.RegisterInstance.RegisterConsumer("claude",
		consumer.MakeConsumerBuilder(claude.ClaudeBuilder))
	core.RegisterInstance.RegisterProducer("stdin",
		producer.MakeProducerBuilder(stdinBuilder))
	core.RegisterInstance.RegisterProducer("http",
		producer.MakeProducerBuilder(http.NewHttpProducer))
	core.RegisterInstance.RegisterProducer("websocket",
		producer.MakeProducerBuilder(websocket.WebSocketProducerBuilder))

	// Load config
	cfg, err := core.LoadConfig("boring.toml")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// Build engine from config
	engine, err := core.NewEngineFromConfig(cfg, core.RegisterInstance)
	if err != nil {
		log.Fatalf("failed to create engine: %v", err)
	}

	fmt.Println("boring started, type messages to stdin (Ctrl+C to quit)")
	engine.Run()

	// Wait for interrupt signal
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	fmt.Println("\nshutting down...")
}
