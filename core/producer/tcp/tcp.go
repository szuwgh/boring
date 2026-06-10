package tcp

import (
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/szuwgh/boring/core/stream"
)

type Listener struct {
	config   *ListenerConfig
	listener net.Listener
	mu       sync.Mutex
}

var _ stream.Producer = (*Listener)(nil)

func ListenerBuilder(config *ListenerConfig) stream.Producer {
	return &Listener{config: config}
}

func (l *Listener) Start() error {
	if l.config == nil || l.config.Listen == "" {
		return fmt.Errorf("tcp_listener: listen is required")
	}
	ln, err := net.Listen("tcp", l.config.Listen)
	if err != nil {
		return fmt.Errorf("tcp_listener: listen %s: %w", l.config.Listen, err)
	}
	l.mu.Lock()
	l.listener = ln
	l.mu.Unlock()
	log.Printf("[tcp_listener:%s] listening on %s", l.config.Name, ln.Addr())
	return nil
}

func (l *Listener) Accept() (net.Conn, error) {
	l.mu.Lock()
	ln := l.listener
	l.mu.Unlock()
	if ln == nil {
		return nil, fmt.Errorf("tcp_listener: not started")
	}
	return ln.Accept()
}

func (l *Listener) Stop() error {
	l.mu.Lock()
	ln := l.listener
	l.listener = nil
	l.mu.Unlock()
	if ln == nil {
		return nil
	}
	return ln.Close()
}

func (l *Listener) Addr() net.Addr {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.listener == nil {
		return nil
	}
	return l.listener.Addr()
}
