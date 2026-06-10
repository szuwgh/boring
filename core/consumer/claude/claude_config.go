package claude

type ClaudeConsumerConfig struct {
	Name     string `toml:"name"`
	Model    string `toml:"model"`
	MaxTurns int    `toml:"max_turns"`
	BaseURL  string `toml:"base_url"`
	APIKey   string `toml:"api_key"`
}
