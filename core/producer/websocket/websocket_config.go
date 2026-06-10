package websocket

type WebSocketProducerConfig struct {
	ServerURL       string `toml:"server_url"`       // IM Server HTTP URL, e.g. http://localhost:18080
	AgentToken      string `toml:"agent_token"`      // Pre-existing agent token (64-char hex)
	RegistrationKey string `toml:"registration_key"` // One-time registration key (registers agent, gets token)
	TokenFile       string `toml:"token_file"`       // Path to cache token after registration (default: .agent_token)
	AgentName       string `toml:"agent_name"`       // Agent name (used during registration)
	AgentDesc       string `toml:"agent_desc"`       // Agent description (used during registration)
	ReconnectSec    int    `toml:"reconnect_sec"`    // Reconnect interval in seconds (default 5)
	HeartbeatSec    int    `toml:"heartbeat_sec"`    // Heartbeat interval in seconds (default 30)
	BufferSize      int    `toml:"buffer_size"`      // Channel buffer size (default 256)
}
