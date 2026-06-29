package connectforward

import (
	"bufio"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func TestConsumeConnForwardsAllowedConnectTarget(t *testing.T) {
	upstream := newTestUpstream(t)
	consumer := Builder(&Config{Host: "git.unvdb.com", Port: 443, Target: upstream.addr})

	client, local := net.Pipe()
	defer client.Close()
	setDeadline(t, client)
	setDeadline(t, local)

	errCh := make(chan error, 1)
	go func() { errCh <- consumer.ConsumeConn(local) }()

	if _, err := io.WriteString(client, "CONNECT git.unvdb.com:443 HTTP/1.1\r\nHost: git.unvdb.com:443\r\n\r\n"); err != nil {
		t.Fatalf("write connect request: %v", err)
	}

	reader := bufio.NewReader(client)
	status := readStatusLine(t, reader)
	if status != "HTTP/1.1 200 Connection Established" {
		t.Fatalf("unexpected status: %q", status)
	}
	readHeaders(t, reader)

	if _, err := io.WriteString(client, "ping"); err != nil {
		t.Fatalf("write tunneled bytes: %v", err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(client, buf); err != nil {
		t.Fatalf("read tunneled bytes: %v", err)
	}
	if string(buf) != "pong" {
		t.Fatalf("unexpected tunneled response: %q", string(buf))
	}

	_ = client.Close()
	if err := <-errCh; err != nil {
		t.Fatalf("ConsumeConn returned error: %v", err)
	}
}

func TestConsumeConnRejectsNonConnectMethod(t *testing.T) {
	status := runRejectedRequest(t, &Config{Host: "git.unvdb.com", Port: 443, Target: "127.0.0.1:1"}, "GET / HTTP/1.1\r\nHost: git.unvdb.com\r\n\r\n")
	if status != "HTTP/1.1 405 Method Not Allowed" {
		t.Fatalf("unexpected status: %q", status)
	}
}

func TestConsumeConnRejectsWrongHost(t *testing.T) {
	status := runRejectedRequest(t, &Config{Host: "git.unvdb.com", Port: 443, Target: "127.0.0.1:1"}, "CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n")
	if status != "HTTP/1.1 403 Forbidden" {
		t.Fatalf("unexpected status: %q", status)
	}
}

func TestConsumeConnRejectsMalformedConnectTarget(t *testing.T) {
	status := runRejectedRequest(t, &Config{Host: "git.unvdb.com", Port: 443, Target: "127.0.0.1:1"}, "CONNECT badtarget HTTP/1.1\r\nHost: badtarget\r\n\r\n")
	if status != "HTTP/1.1 400 Bad Request" {
		t.Fatalf("unexpected status: %q", status)
	}
}

func TestConsumeConnRequiresConfig(t *testing.T) {
	client, local := net.Pipe()
	defer client.Close()
	setDeadline(t, client)
	setDeadline(t, local)

	err := (&Consumer{}).ConsumeConn(local)
	if err == nil || !strings.Contains(err.Error(), "config is required") {
		t.Fatalf("expected config error, got %v", err)
	}
}

func TestRouteDescriptionReturnsTarget(t *testing.T) {
	consumer := Builder(&Config{Target: "git.unvdb.com:443"})

	describer, ok := consumer.(interface{ RouteDescription() string })
	if !ok {
		t.Fatalf("connect forward consumer does not describe its route")
	}
	if got := describer.RouteDescription(); got != "git.unvdb.com:443" {
		t.Fatalf("unexpected route description: %q", got)
	}
}

func runRejectedRequest(t *testing.T, cfg *Config, request string) string {
	t.Helper()
	consumer := Builder(cfg)
	client, local := net.Pipe()
	defer client.Close()
	setDeadline(t, client)
	setDeadline(t, local)

	errCh := make(chan error, 1)
	go func() { errCh <- consumer.ConsumeConn(local) }()

	if _, err := io.WriteString(client, request); err != nil {
		t.Fatalf("write request: %v", err)
	}
	reader := bufio.NewReader(client)
	status := readStatusLine(t, reader)
	readHeaders(t, reader)
	if err := <-errCh; err == nil {
		t.Fatalf("expected ConsumeConn to return an error")
	}
	return status
}

func readStatusLine(t *testing.T, reader *bufio.Reader) string {
	t.Helper()
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read status line: %v", err)
	}
	return strings.TrimRight(line, "\r\n")
}

func readHeaders(t *testing.T, reader *bufio.Reader) {
	t.Helper()
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read header: %v", err)
		}
		if line == "\r\n" {
			return
		}
	}
}

func setDeadline(t *testing.T, conn net.Conn) {
	t.Helper()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
}

type testUpstream struct {
	addr string
}

func newTestUpstream(t *testing.T) testUpstream {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen upstream: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		buf := make([]byte, 4)
		if _, err := io.ReadFull(conn, buf); err != nil {
			return
		}
		if string(buf) == "ping" {
			_, _ = conn.Write([]byte("pong"))
		}
	}()

	return testUpstream{addr: ln.Addr().String()}
}
