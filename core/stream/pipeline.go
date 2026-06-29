package stream

import (
	"fmt"
	"log"

	"github.com/szuwgh/boring/core/config"
)

type Pipeline struct {
	Name     string
	Producer Producer
	Consumer Consumer
}

func (p *Pipeline) Run() {
	if err := p.Producer.Start(); err != nil {
		log.Printf("[stream:%s] producer start failed: %v", p.Name, err)
		return
	}
	log.Print(p.routeLogLine())

	go func() {
		for {
			conn, err := p.Producer.Accept()
			if err != nil {
				return
			}
			go func() {
				if err := p.Consumer.ConsumeConn(conn); err != nil {
					log.Printf("[stream:%s] consumer error: %v", p.Name, err)
				}
			}()
		}
	}()
}

func (p *Pipeline) routeLogLine() string {
	listenAddr := "unknown"
	if provider, ok := p.Producer.(AddrProvider); ok {
		if addr := provider.Addr(); addr != nil {
			listenAddr = addr.String()
		}
	}

	targetAddr := "unknown"
	if describer, ok := p.Consumer.(RouteDescriber); ok {
		if route := describer.RouteDescription(); route != "" {
			targetAddr = route
		}
	}

	return fmt.Sprintf("[stream:%s] listening %s -> %s", p.Name, listenAddr, targetAddr)
}

func (p *Pipeline) Stop() error {
	if p.Producer == nil {
		return nil
	}
	return p.Producer.Stop()
}

type Registry interface {
	GetStreamProducer(name string) (ProducerBuilder, bool)
	GetStreamConsumer(name string) (ConsumerBuilder, bool)
}

func NewStreamPipelineFromConfig(bc config.BoringConfig, reg Registry) (*Pipeline, error) {
	if bc.Producer == nil {
		return nil, fmt.Errorf("pipeline %q: stream producer missing", bc.Name)
	}
	typeName, _ := bc.Producer["type"].(string)
	if typeName == "" {
		return nil, fmt.Errorf("pipeline %q: stream producer missing type", bc.Name)
	}
	producerBuilder, ok := reg.GetStreamProducer(typeName)
	if !ok {
		return nil, fmt.Errorf("pipeline %q: unknown stream producer type %q", bc.Name, typeName)
	}

	if len(bc.Consumers) != 1 {
		return nil, fmt.Errorf("pipeline %q: stream mode requires exactly one consumer", bc.Name)
	}
	cc := bc.Consumers[0]
	consumerType, _ := cc["type"].(string)
	if consumerType == "" {
		return nil, fmt.Errorf("pipeline %q: stream consumer missing type", bc.Name)
	}
	consumerBuilder, ok := reg.GetStreamConsumer(consumerType)
	if !ok {
		return nil, fmt.Errorf("pipeline %q: unknown stream consumer type %q", bc.Name, consumerType)
	}

	return &Pipeline{
		Name:     bc.Name,
		Producer: producerBuilder(bc.Producer),
		Consumer: consumerBuilder(cc),
	}, nil
}
