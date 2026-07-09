package connectforward

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"strconv"
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

func TestConsumeConnKeepsReadingAfterClientHalfClosesWrite(t *testing.T) {
	payload := strings.Repeat("pack-data-", 4096)
	upstream := newHalfCloseUpstream(t, payload)
	consumer := Builder(&Config{Host: "git.unvdb.com", Port: 443, Target: upstream.addr})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen local: %v", err)
	}
	defer ln.Close()

	errCh := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}
		errCh <- consumer.ConsumeConn(conn)
	}()

	clientConn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial local: %v", err)
	}
	defer clientConn.Close()
	setDeadline(t, clientConn)
	client := clientConn.(*net.TCPConn)

	if _, err := io.WriteString(client, "CONNECT git.unvdb.com:443 HTTP/1.1\r\nHost: git.unvdb.com:443\r\n\r\n"); err != nil {
		t.Fatalf("write connect request: %v", err)
	}

	reader := bufio.NewReader(client)
	status := readStatusLine(t, reader)
	if status != "HTTP/1.1 200 Connection Established" {
		t.Fatalf("unexpected status: %q", status)
	}
	readHeaders(t, reader)

	if _, err := io.WriteString(client, "want-pack"); err != nil {
		t.Fatalf("write tunneled request: %v", err)
	}
	if err := client.CloseWrite(); err != nil {
		t.Fatalf("close client write side: %v", err)
	}

	got := make([]byte, len(payload))
	if _, err := io.ReadFull(reader, got); err != nil {
		t.Fatalf("read tunneled response: %v", err)
	}
	if string(got) != payload {
		t.Fatalf("unexpected tunneled response length: got %d want %d", len(got), len(payload))
	}

	if err := <-errCh; err != nil {
		t.Fatalf("ConsumeConn returned error: %v", err)
	}
}

func TestConsumeConnForwardsHTTPProxyRequest(t *testing.T) {
	upstream := newHTTPProxyUpstream(t, "git-http-ok")
	consumer := Builder(&Config{Host: "git.unvdb.com", Port: 80, Target: upstream.addr})

	client, local := net.Pipe()
	defer client.Close()
	setDeadline(t, client)
	setDeadline(t, local)

	errCh := make(chan error, 1)
	go func() { errCh <- consumer.ConsumeConn(local) }()

	if _, err := io.WriteString(client, "GET http://git.unvdb.com/udb-tx_da_grp/udb-da.git/info/refs?service=git-upload-pack HTTP/1.1\r\nHost: git.unvdb.com\r\n\r\n"); err != nil {
		t.Fatalf("write proxy request: %v", err)
	}

	resp, err := http.ReadResponse(bufio.NewReader(client), nil)
	if err != nil {
		t.Fatalf("read proxy response: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read proxy response body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %s", resp.Status)
	}
	if string(body) != "git-http-ok" {
		t.Fatalf("unexpected body: %q", string(body))
	}

	req := <-upstream.requests
	if req.Method != http.MethodGet {
		t.Fatalf("unexpected upstream method: %q", req.Method)
	}
	if req.RequestURI != "/udb-tx_da_grp/udb-da.git/info/refs?service=git-upload-pack" {
		t.Fatalf("unexpected upstream request URI: %q", req.RequestURI)
	}
	if req.Host != "git.unvdb.com" {
		t.Fatalf("unexpected upstream host: %q", req.Host)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("ConsumeConn returned error: %v", err)
	}
}

func TestConsumeConnRejectsNonConnectMethod(t *testing.T) {
	status := runRejectedRequest(t, &Config{Host: "git.unvdb.com", Port: 443, Target: "127.0.0.1:1"}, "GET http://example.com/ HTTP/1.1\r\nHost: example.com\r\n\r\n")
	if status != "HTTP/1.1 403 Forbidden" {
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

func newHalfCloseUpstream(t *testing.T, response string) testUpstream {
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
		if _, err := io.Copy(io.Discard, conn); err != nil {
			return
		}
		_, _ = io.WriteString(conn, response)
	}()

	return testUpstream{addr: ln.Addr().String()}
}

type httpProxyUpstream struct {
	addr     string
	requests chan *http.Request
}

func newHTTPProxyUpstream(t *testing.T, responseBody string) httpProxyUpstream {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen upstream: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	upstream := httpProxyUpstream{
		addr:     ln.Addr().String(),
		requests: make(chan *http.Request, 1),
	}

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

		req, err := http.ReadRequest(bufio.NewReader(conn))
		if err != nil {
			return
		}
		upstream.requests <- req
		_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: "+strconv.Itoa(len(responseBody))+"\r\nConnection: close\r\n\r\n"+responseBody)
	}()

	return upstream
}
