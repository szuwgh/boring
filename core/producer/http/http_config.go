package http

type HttpProducerConfig struct {
	Name string `toml:"name"`
	Port int    `toml:"port"`
	Host string `toml:"host"`
	Path string `toml:"path"`
}
