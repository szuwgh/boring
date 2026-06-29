package stream

import (
	"net"

	toml "github.com/pelletier/go-toml/v2"
)

type Producer interface {
	Start() error
	Accept() (net.Conn, error)
	Stop() error
}

type Consumer interface {
	ConsumeConn(conn net.Conn) error
}

type AddrProvider interface {
	Addr() net.Addr
}

type RouteDescriber interface {
	RouteDescription() string
}

type ProducerBuilder func(config map[string]any) Producer

type ConsumerBuilder func(config map[string]any) Consumer

func MakeProducerBuilder[T any](fn func(config *T) Producer) ProducerBuilder {
	return func(config map[string]any) Producer {
		var cfg T
		b, err := toml.Marshal(config)
		if err == nil {
			_ = toml.Unmarshal(b, &cfg)
		}
		return fn(&cfg)
	}
}

func MakeConsumerBuilder[T any](fn func(config *T) Consumer) ConsumerBuilder {
	return func(config map[string]any) Consumer {
		var cfg T
		b, err := toml.Marshal(config)
		if err == nil {
			_ = toml.Unmarshal(b, &cfg)
		}
		return fn(&cfg)
	}
}
