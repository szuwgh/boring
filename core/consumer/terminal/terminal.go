package terminal

import (
	"fmt"

	"github.com/szuwgh/boring/core/consumer"
)

type TerminalConsumer struct {
	config *TerminalConsumerConfig
}

func TerminalBuilder(config *TerminalConsumerConfig) consumer.Consumer {
	return &TerminalConsumer{config: config}
}

func (t *TerminalConsumer) Consume(data []byte) error {
	if t.config != nil && t.config.Prefix != "" {
		fmt.Printf("%s %s\n", t.config.Prefix, string(data))
		return nil
	}
	fmt.Println(string(data))
	return nil
}
