package http

import (
	"fmt"
	"io"
	"net"
	nethttp "net/http"

	"github.com/szuwgh/boring/core/queue"
)

type HttpProducer struct {
	config *HttpProducerConfig
	dataCh chan []byte
}

func NewHttpProducer(cfg *HttpProducerConfig) queue.Producer {
	if cfg.Host == "" {
		cfg.Host = "0.0.0.0"
	}
	if cfg.Port == 0 {
		cfg.Port = 8080
	}
	return &HttpProducer{
		config: cfg,
		dataCh: make(chan []byte, 256),
	}
}

func (h *HttpProducer) Start() error {
	addr := fmt.Sprintf("%s:%d", h.config.Host, h.config.Port)

	mux := nethttp.NewServeMux()
	mux.HandleFunc(h.config.Path, h.handleIngest)

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	//fmt.Printf("http producer %s listening on %s\n", h.config.Name, addr)
	go nethttp.Serve(listener, mux)

	return nil
}

func (h *HttpProducer) handleIngest(w nethttp.ResponseWriter, r *nethttp.Request) {
	if r.Method != nethttp.MethodPost {
		w.WriteHeader(nethttp.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		w.WriteHeader(nethttp.StatusBadRequest)
		return
	}

	select {
	case h.dataCh <- body:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(nethttp.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	default:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(nethttp.StatusServiceUnavailable)
		w.Write([]byte(`{"status":"queue full"}`))
	}
}

func (h *HttpProducer) Produce() ([]byte, error) {
	data, ok := <-h.dataCh
	if !ok {
		return nil, fmt.Errorf("http producer closed")
	}
	return data, nil
}
