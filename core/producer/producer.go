package producer

import (
	toml "github.com/pelletier/go-toml/v2"
)

type Producer interface {
	Produce() ([]byte, error)
	Start() error
}

// Replier is implemented by producers that support sending replies back.
// Used for bidirectional producers like WebSocket.
type Replier interface {
	Reply(data []byte) error
}

type ProducerBuilder func(config map[string]any) Producer

func MakeProducerBuilder[T any](fn func(config *T) Producer) ProducerBuilder {
	return func(config map[string]any) Producer {
		var cfg T
		b, err := toml.Marshal(config)
		if err == nil {
			toml.Unmarshal(b, &cfg)
		}
		return fn(&cfg)
	}
}
