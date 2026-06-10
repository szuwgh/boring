package sshforward

type Config struct {
	Name     string `toml:"name"`
	SSHAddr  string `toml:"ssh_addr"`
	User     string `toml:"user"`
	Password string `toml:"password"`
	Target   string `toml:"target"`
}
