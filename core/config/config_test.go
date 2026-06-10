package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigReadsStreamBoringPipeline(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "boring.toml")
	content := []byte(`
[[boring]]
name = "dev-ssh"
mode = "stream"

[boring.producer]
type = "tcp_listener"
listen = "127.0.0.1:2223"

[[boring.consumer]]
type = "ssh_forward"
ssh_addr = "192.168.4.134:22"
user = "unvdb"
password = "secret"
target = "192.168.4.134:22"
`)
	if err := os.WriteFile(path, content, 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig returned error: %v", err)
	}
	if len(cfg.Boring) != 1 {
		t.Fatalf("expected 1 boring pipeline, got %d", len(cfg.Boring))
	}
	bc := cfg.Boring[0]
	if bc.Name != "dev-ssh" || bc.Mode != "stream" {
		t.Fatalf("unexpected boring metadata: %#v", bc)
	}
	if bc.Producer["type"] != "tcp_listener" || bc.Producer["listen"] != "127.0.0.1:2223" {
		t.Fatalf("unexpected producer config: %#v", bc.Producer)
	}
	if len(bc.Consumers) != 1 {
		t.Fatalf("expected 1 consumer, got %d", len(bc.Consumers))
	}
	cc := bc.Consumers[0]
	if cc["type"] != "ssh_forward" || cc["ssh_addr"] != "192.168.4.134:22" || cc["user"] != "unvdb" || cc["password"] != "secret" || cc["target"] != "192.168.4.134:22" {
		t.Fatalf("unexpected consumer config: %#v", cc)
	}
}
