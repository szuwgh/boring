package websocket

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/szuwgh/boring/core/producer"
)

// registerResponse is the JSON response from POST /api/v1/agents/register
type registerResponse struct {
	Agent struct {
		ID     int64  `json:"id"`
		UserID int64  `json:"user_id"`
		Name   string `json:"name"`
	} `json:"agent"`
	Token   string `json:"token"`
	Message string `json:"message"`
}

type WebSocketProducer struct {
	config *WebSocketProducerConfig

	conn   *websocket.Conn
	dataCh chan []byte // incoming messages from IM Server → Produce()
	sendCh chan []byte // outgoing messages from Reply() → writeLoop
	done   chan struct{}

	agentToken string // resolved token (from config or registration)
	agentID    int64
}

// Compile-time check: WebSocketProducer implements both Producer and Replier.
var _ producer.Producer = (*WebSocketProducer)(nil)
var _ producer.Replier = (*WebSocketProducer)(nil)

func WebSocketProducerBuilder(config *WebSocketProducerConfig) producer.Producer {
	if config.ReconnectSec <= 0 {
		config.ReconnectSec = 5
	}
	if config.HeartbeatSec <= 0 {
		config.HeartbeatSec = 30
	}
	if config.BufferSize <= 0 {
		config.BufferSize = 256
	}
	return &WebSocketProducer{
		config: config,
		dataCh: make(chan []byte, config.BufferSize),
		sendCh: make(chan []byte, config.BufferSize),
		done:   make(chan struct{}),
	}
}

// Produce blocks until an incoming message arrives from the IM Server.
// Called by the boring pipeline's producer goroutine.
func (p *WebSocketProducer) Produce() ([]byte, error) {
	select {
	case data, ok := <-p.dataCh:
		if !ok {
			return nil, fmt.Errorf("websocket producer closed")
		}
		return data, nil
	case <-p.done:
		return nil, fmt.Errorf("websocket producer stopped")
	}
}

// Start initiates the agent registration (if needed) and WebSocket connection.
// It launches background goroutines and returns immediately.
func (p *WebSocketProducer) Start() error {
	// Resolve agent token
	token, err := p.resolveToken()
	if err != nil {
		return fmt.Errorf("resolve agent token: %w", err)
	}
	p.agentToken = token
	log.Printf("[ws] agent token resolved, connecting to %s", p.config.ServerURL)

	// Start connection loop in background
	go p.connectLoop()
	return nil
}

// Reply sends data back to the IM Server via the WebSocket connection.
// Called by consumers (e.g., im_reply) that need to send responses.
func (p *WebSocketProducer) Reply(data []byte) error {
	select {
	case p.sendCh <- data:
		return nil
	case <-p.done:
		return fmt.Errorf("websocket producer stopped")
	}
}

// resolveToken returns an agent token using a three-level lookup:
//  1. config agent_token (explicit)
//  2. cached token file from previous registration
//  3. register with registration_key → save token to file
func (p *WebSocketProducer) resolveToken() (string, error) {
	// 1. If token is provided directly in config, use it
	if p.config.AgentToken != "" {
		return p.config.AgentToken, nil
	}

	if p.config.RegistrationKey == "" {
		return "", fmt.Errorf("either agent_token or registration_key must be provided")
	}

	// 2. Try loading cached token from file
	if cached := p.loadTokenFile(); cached != "" {
		log.Printf("[ws] loaded cached token from %s", p.tokenFilePath())
		return cached, nil
	}

	// 3. Register with key, then save token to file
	token, err := p.registerAgent()
	if err != nil {
		return "", err
	}
	p.saveTokenFile(token)
	return token, nil
}

// tokenFilePath returns the path for the token cache file.
func (p *WebSocketProducer) tokenFilePath() string {
	if p.config.TokenFile != "" {
		return p.config.TokenFile
	}
	return ".agent_token"
}

// loadTokenFile reads the cached token from disk. Returns "" if not found or unreadable.
func (p *WebSocketProducer) loadTokenFile() string {
	data, err := os.ReadFile(p.tokenFilePath())
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

// saveTokenFile writes the token to the cache file.
func (p *WebSocketProducer) saveTokenFile(token string) {
	path := p.tokenFilePath()
	if err := os.WriteFile(path, []byte(token+"\n"), 0600); err != nil {
		log.Printf("[ws] warning: failed to save token to %s: %v", path, err)
	} else {
		log.Printf("[ws] token saved to %s", path)
	}
}

// registerAgent calls POST /api/v1/agents/register to create a new agent.
func (p *WebSocketProducer) registerAgent() (string, error) {
	name := p.config.AgentName
	if name == "" {
		name = "boring-agent"
	}
	desc := p.config.AgentDesc
	if desc == "" {
		desc = "AI Agent powered by boring"
	}

	payload, _ := json.Marshal(map[string]string{
		"key":         p.config.RegistrationKey,
		"name":        name,
		"description": desc,
	})

	apiURL := p.config.ServerURL + "/api/v1/agents/register"
	resp, err := http.Post(apiURL, "application/json", bytes.NewReader(payload))
	if err != nil {
		return "", fmt.Errorf("POST %s: %w", apiURL, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("registration failed (HTTP %d): %s", resp.StatusCode, string(body))
	}

	var result registerResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("parse response: %w", err)
	}

	p.agentID = result.Agent.ID
	log.Printf("[ws] agent registered: id=%d, name=%s", result.Agent.ID, result.Agent.Name)
	log.Printf("[ws] agent token: %s", result.Token)
	return result.Token, nil
}

// connectLoop keeps trying to connect to the IM Server.
// On disconnect, it waits and retries.
func (p *WebSocketProducer) connectLoop() {
	for {
		select {
		case <-p.done:
			return
		default:
		}

		conn, err := p.dial()
		if err != nil {
			log.Printf("[ws] connect failed: %v, retry in %ds", err, p.config.ReconnectSec)
			select {
			case <-time.After(time.Duration(p.config.ReconnectSec) * time.Second):
				continue
			case <-p.done:
				return
			}
		}

		p.conn = conn
		log.Println("[ws] connected to IM Server")
		p.runConnection()
		log.Printf("[ws] disconnected, reconnecting in %ds...", p.config.ReconnectSec)

		select {
		case <-time.After(time.Duration(p.config.ReconnectSec) * time.Second):
		case <-p.done:
			return
		}
	}
}

// dial creates a new WebSocket connection to the IM Server.
func (p *WebSocketProducer) dial() (*websocket.Conn, error) {
	u, err := url.Parse(p.config.ServerURL)
	if err != nil {
		return nil, fmt.Errorf("parse server URL: %w", err)
	}

	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	case "ws", "wss":
		// already correct
	default:
		u.Scheme = "ws"
	}
	u.Path = "/ws"
	q := u.Query()
	q.Set("token", p.agentToken)
	u.RawQuery = q.Encode()

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", u.Host, err)
	}
	return conn, nil
}

// runConnection manages one WebSocket connection's lifecycle.
// Returns when the connection is lost.
func (p *WebSocketProducer) runConnection() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// When either goroutine exits and cancels ctx, close the conn
	// to unblock the other goroutine.
	go func() {
		<-ctx.Done()
		if p.conn != nil {
			p.conn.Close()
		}
	}()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()
		p.readLoop()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()
		p.writeLoop(ctx)
	}()

	wg.Wait()
}

// readLoop reads messages from the WebSocket connection and pushes them to dataCh.
// It runs until the connection is closed or an error occurs.
func (p *WebSocketProducer) readLoop() {
	for {
		_, data, err := p.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				log.Printf("[ws] read error: %v", err)
			}
			return
		}

		// Filter out heartbeat responses — don't push them to the pipeline
		var peek struct {
			Action string `json:"action"`
		}
		if json.Unmarshal(data, &peek) == nil && peek.Action == "heartbeat" {
			continue
		}

		select {
		case p.dataCh <- data:
		default:
			log.Println("[ws] incoming buffer full, message dropped")
		}
	}
}

// writeLoop sends outgoing messages and heartbeats.
// Only this goroutine writes to the WebSocket connection (gorilla/websocket requirement).
func (p *WebSocketProducer) writeLoop(ctx context.Context) {
	heartbeat := time.NewTicker(time.Duration(p.config.HeartbeatSec) * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.done:
			return
		case data := <-p.sendCh:
			if err := p.conn.WriteMessage(websocket.TextMessage, data); err != nil {
				log.Printf("[ws] write error: %v", err)
				return
			}
		case <-heartbeat.C:
			hb := []byte(`{"action":"heartbeat"}`)
			if err := p.conn.WriteMessage(websocket.TextMessage, hb); err != nil {
				log.Printf("[ws] heartbeat send error: %v", err)
				return
			}
		}
	}
}
