package httpproxy

import "testing"

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *Config
		invalid bool
	}{
		{"nil", nil, true},
		{"empty", &Config{}, true},
		{"invalid CIDR", &Config{AllowClients: []string{"127.0.0.1"}}, true},
		{"default ports", &Config{AllowClients: []string{"127.0.0.1/32"}}, false},
		{"empty ports", &Config{AllowClients: []string{"127.0.0.1/32"}, AllowedPorts: []int{}}, true},
		{"invalid port", &Config{AllowClients: []string{"127.0.0.1/32"}, AllowedPorts: []int{65536}}, true},
		{"any ignores ports", &Config{AllowClients: []string{"127.0.0.1/32"}, AllowAnyPort: true, AllowedPorts: []int{-1}}, false},
		{"IPv6", &Config{AllowClients: []string{"::1/128"}}, false},
		{"mapped IPv4", &Config{AllowClients: []string{"::ffff:127.0.0.1/128"}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Builder(tt.cfg).(*Consumer)
			if (c.err != nil) != tt.invalid {
				t.Fatalf("error=%v want invalid=%v", c.err, tt.invalid)
			}
			if !tt.invalid && !tt.cfg.AllowAnyPort && tt.cfg.AllowedPorts == nil {
				if !c.ports[80] || !c.ports[443] || c.ports[8080] {
					t.Fatal("incorrect default ports")
				}
			}
		})
	}
}
