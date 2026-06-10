package tcp

type ListenerConfig struct {
	Name   string `toml:"name"`
	Listen string `toml:"listen"`
}
