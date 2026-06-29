package connectforward

type Config struct {
	Name         string `toml:"name"`
	Host         string `toml:"host"`
	Port         int    `toml:"port"`
	Target       string `toml:"target"`
	AllowAnyPort bool   `toml:"allow_any_port"`
}
