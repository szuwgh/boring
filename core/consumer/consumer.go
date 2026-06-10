package consumer

import (
	toml "github.com/pelletier/go-toml/v2"
)

type Consumer interface {
	Consume(data []byte) error
}

// ReplyFunc sends data back through the producer's reply channel.
type ReplyFunc func(data []byte) error

// ReplyAware is implemented by consumers that need to send replies
// back through the producer (e.g., im_reply consumer → WebSocket producer).
type ReplyAware interface {
	SetReplyFunc(fn ReplyFunc)
}

type ConsumerBuilder func(config map[string]any) Consumer

func MakeConsumerBuilder[T any](fn func(config *T) Consumer) ConsumerBuilder {
	return func(config map[string]any) Consumer {
		var cfg T
		b, err := toml.Marshal(config)
		if err == nil {
			toml.Unmarshal(b, &cfg)
		}
		return fn(&cfg)
	}
}
