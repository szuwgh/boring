package httpproxy

import (
	"fmt"
	"github.com/szuwgh/boring/core/stream"
	"net"
	"net/http"
	"net/netip"
	"time"
)

type Config struct {
	AllowClients []string `toml:"allow_clients"`
	AllowedPorts []int    `toml:"allowed_ports"`
	AllowAnyPort bool     `toml:"allow_any_port"`
}
type Consumer struct {
	cfg       Config
	clients   []netip.Prefix
	ports     map[int]bool
	err       error
	transport *http.Transport
}

func Builder(cfg *Config) stream.Consumer {
	c := &Consumer{ports: make(map[int]bool)}
	if cfg == nil {
		c.err = fmt.Errorf("http_proxy: config required")
		return c
	}
	c.cfg = *cfg
	if len(cfg.AllowClients) == 0 {
		c.err = fmt.Errorf("http_proxy: allow_clients required")
	}
	for _, s := range cfg.AllowClients {
		p, e := netip.ParsePrefix(s)
		if e != nil {
			c.err = e
			continue
		}
		if p.Addr().Is4In6() && p.Bits() >= 96 {
			p = netip.PrefixFrom(p.Addr().Unmap(), p.Bits()-96)
		}
		c.clients = append(c.clients, p)
	}
	if !cfg.AllowAnyPort {
		ports := cfg.AllowedPorts
		if ports == nil {
			ports = []int{80, 443}
		}
		if len(ports) == 0 {
			c.err = fmt.Errorf("http_proxy: allowed_ports empty")
		}
		for _, p := range ports {
			if p < 1 || p > 65535 {
				c.err = fmt.Errorf("http_proxy: invalid port %d", p)
			}
			c.ports[p] = true
		}
	}
	c.transport = &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 10 * time.Second}).DialContext, DisableCompression: true, MaxIdleConns: 100, MaxIdleConnsPerHost: 8, IdleConnTimeout: 30 * time.Second, ResponseHeaderTimeout: 60 * time.Second}
	return c
}
