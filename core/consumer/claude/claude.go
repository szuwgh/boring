package claude

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"sync"

	"github.com/szuwgh/boring/core/consumer"
)

// wsMessage mirrors the IM Server's WSMessage format.
type wsMessage struct {
	Action     string `json:"action"`
	MsgType    int16  `json:"msg_type,omitempty"`
	SenderID   int64  `json:"sender_id,omitempty"`
	SenderName string `json:"sender_name,omitempty"`
	ReceiverID int64  `json:"receiver_id,omitempty"`
	GroupID    int64  `json:"group_id,omitempty"`
	Content    string `json:"content,omitempty"`
	Timestamp  int64  `json:"timestamp,omitempty"`
}

type claudeResponse struct {
	Result    string `json:"result"`
	SessionID string `json:"session_id"`
	IsError   bool   `json:"is_error"`
}

type ClaudeConsumer struct {
	config   *ClaudeConsumerConfig
	sessions map[int64]string // per-user session (IM mode)
	mu       sync.Mutex
	replyFn  consumer.ReplyFunc // non-nil when wired to a Replier producer
}

// Compile-time checks
var _ consumer.Consumer = (*ClaudeConsumer)(nil)
var _ consumer.ReplyAware = (*ClaudeConsumer)(nil)

func ClaudeBuilder(config *ClaudeConsumerConfig) consumer.Consumer {
	return &ClaudeConsumer{
		config:   config,
		sessions: make(map[int64]string),
	}
}

func (c *ClaudeConsumer) SetReplyFunc(fn consumer.ReplyFunc) {
	c.replyFn = fn
}

func (c *ClaudeConsumer) Consume(data []byte) error {
	// If replyFn is set, we're in IM reply mode (WebSocket pipeline).
	// Otherwise, fall back to the original stdout mode.
	if c.replyFn != nil {
		return c.consumeIM(data)
	}
	return c.consumeRaw(data)
}

// consumeRaw is the original behavior: send raw data to Claude, print result.
func (c *ClaudeConsumer) consumeRaw(data []byte) error {
	result, err := c.callClaude(0, string(data))
	if err != nil {
		return err
	}
	fmt.Println(result)
	return nil
}

// consumeIM parses an IM WSMessage, calls Claude, and sends the reply back.
func (c *ClaudeConsumer) consumeIM(data []byte) error {
	var msg wsMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return fmt.Errorf("claude: unmarshal IM message: %w", err)
	}

	if msg.Action != "chat_recv" || msg.Content == "" {
		return nil
	}

	log.Printf("[claude] from=%s(%d) content=%q", msg.SenderName, msg.SenderID, msg.Content)

	reply, err := c.callClaude(msg.SenderID, msg.Content)
	if err != nil {
		log.Printf("[claude] error: %v", err)
		reply = fmt.Sprintf("抱歉，处理消息时出错: %v", err)
	}

	// Build reply in IM Server WSMessage format
	replyMsg := wsMessage{
		Action:  "chat",
		MsgType: msg.MsgType,
		Content: reply,
	}
	if msg.MsgType == 2 {
		replyMsg.GroupID = msg.GroupID
	} else {
		replyMsg.ReceiverID = msg.SenderID
	}

	replyData, err := json.Marshal(replyMsg)
	if err != nil {
		return fmt.Errorf("claude: marshal reply: %w", err)
	}

	if err := c.replyFn(replyData); err != nil {
		return fmt.Errorf("claude: send reply: %w", err)
	}

	log.Printf("[claude] replied to %d: %q", msg.SenderID, reply)
	return nil
}

// callClaude invokes the Claude CLI and returns the response text.
// senderID=0 means single-session mode (raw/stdout); >0 means per-user session (IM).
func (c *ClaudeConsumer) callClaude(senderID int64, content string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	args := []string{"-p", content, "--output-format", "json"}

	// Resume session
	if senderID > 0 {
		if sid, ok := c.sessions[senderID]; ok {
			args = append(args, "--resume", sid)
		}
	} else {
		if sid, ok := c.sessions[0]; ok {
			args = append(args, "--resume", sid)
		}
	}

	if c.config != nil && c.config.Model != "" {
		args = append(args, "--model", c.config.Model)
	}
	if c.config != nil && c.config.MaxTurns > 0 {
		args = append(args, "--max-turns", strconv.Itoa(c.config.MaxTurns))
	}

	cmd := exec.Command("claude", args...)
	cmd.Env = os.Environ()
	if c.config != nil && c.config.APIKey != "" {
		cmd.Env = append(cmd.Env, "ANTHROPIC_API_KEY="+c.config.APIKey)
	}
	if c.config != nil && c.config.BaseURL != "" {
		cmd.Env = append(cmd.Env, "ANTHROPIC_BASE_URL="+c.config.BaseURL)
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("claude exec: %w\n%s", err, string(output))
	}

	var resp claudeResponse
	if err := json.Unmarshal(output, &resp); err != nil {
		return string(output), nil
	}

	if resp.SessionID != "" {
		c.sessions[senderID] = resp.SessionID
	}

	if resp.IsError {
		return "", fmt.Errorf("claude error: %s", resp.Result)
	}

	return resp.Result, nil
}
