package connectforward

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/szuwgh/boring/core/stream"
)

type Consumer struct {
	config *Config
}

var _ stream.Consumer = (*Consumer)(nil)

func Builder(config *Config) stream.Consumer {
	return &Consumer{config: config}
}

func (c *Consumer) RouteDescription() string {
	if c == nil || c.config == nil {
		return ""
	}
	return c.config.Target
}

func (c *Consumer) ConsumeConn(local net.Conn) error {
	defer local.Close()
	if err := c.validate(); err != nil {
		return err
	}

	reader := bufio.NewReader(local)
	req, err := http.ReadRequest(reader)
	if err != nil {
		_ = writeResponse(local, http.StatusBadRequest, "Bad Request")
		return fmt.Errorf("connect_forward: read request: %w", err)
	}

	if req.Method == http.MethodConnect {
		return c.handleConnect(local, remoteTarget{host: req.Host}, reader)
	}

	return c.handleHTTPProxyRequest(local, req)
}

type remoteTarget struct {
	host string
}

func (c *Consumer) handleConnect(local net.Conn, target remoteTarget, reader *bufio.Reader) error {
	if err := c.validateTarget(target.host); err != nil {
		status := http.StatusForbidden
		reason := "Forbidden"
		if strings.Contains(err.Error(), "malformed") {
			status = http.StatusBadRequest
			reason = "Bad Request"
		}
		_ = writeResponse(local, status, reason)
		return err
	}

	remote, err := net.Dial("tcp", c.config.Target)
	if err != nil {
		_ = writeResponse(local, http.StatusBadGateway, "Bad Gateway")
		return fmt.Errorf("connect_forward: dial target %s: %w", c.config.Target, err)
	}
	defer remote.Close()

	if err := writeResponse(local, http.StatusOK, "Connection Established"); err != nil {
		return fmt.Errorf("connect_forward: write connect response: %w", err)
	}

	log.Printf("connect_forward: established %s -> %s via %s", local.RemoteAddr(), target.host, c.config.Target)
	pipe(local, remote, reader)
	return nil
}

func (c *Consumer) handleHTTPProxyRequest(local net.Conn, req *http.Request) error {
	if req.Body != nil {
		defer req.Body.Close()
	}

	target, err := c.httpProxyTarget(req)
	if err != nil {
		status := http.StatusForbidden
		reason := "Forbidden"
		if strings.Contains(err.Error(), "malformed") {
			status = http.StatusBadRequest
			reason = "Bad Request"
		}
		_ = writeResponse(local, status, reason)
		return err
	}

	remote, err := net.Dial("tcp", c.config.Target)
	if err != nil {
		_ = writeResponse(local, http.StatusBadGateway, "Bad Gateway")
		return fmt.Errorf("connect_forward: dial target %s: %w", c.config.Target, err)
	}
	defer remote.Close()

	req.RequestURI = ""
	req.URL.Scheme = ""
	req.URL.Host = ""
	req.Host = c.config.Host
	req.Close = true
	req.Header.Del("Proxy-Connection")
	req.Header.Set("Connection", "close")

	if err := req.Write(remote); err != nil {
		_ = writeResponse(local, http.StatusBadGateway, "Bad Gateway")
		return fmt.Errorf("connect_forward: write http request to %s: %w", c.config.Target, err)
	}
	closeWrite(remote)

	n, err := io.Copy(local, remote)
	log.Printf("connect_forward: http %s %s via %s finished bytes=%d err=%v", req.Method, target, c.config.Target, n, err)
	closeWrite(local)
	return nil
}

func (c *Consumer) validate() error {
	if c.config == nil {
		return fmt.Errorf("connect_forward: config is required")
	}
	if c.config.Host == "" {
		return fmt.Errorf("connect_forward: host is required")
	}
	if c.config.Port <= 0 && !c.config.AllowAnyPort {
		return fmt.Errorf("connect_forward: port is required")
	}
	if c.config.Target == "" {
		return fmt.Errorf("connect_forward: target is required")
	}
	return nil
}

func (c *Consumer) validateTarget(target string) error {
	host, portText, err := net.SplitHostPort(target)
	if err != nil {
		return fmt.Errorf("connect_forward: malformed connect target %q: %w", target, err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return fmt.Errorf("connect_forward: malformed connect target %q: %w", target, err)
	}
	if !strings.EqualFold(host, c.config.Host) {
		return fmt.Errorf("connect_forward: target host %q is not allowed", host)
	}
	if !c.config.AllowAnyPort && port != c.config.Port {
		return fmt.Errorf("connect_forward: target port %d is not allowed", port)
	}
	return nil
}

func (c *Consumer) httpProxyTarget(req *http.Request) (string, error) {
	if req.URL == nil {
		return "", fmt.Errorf("connect_forward: malformed http target: missing URL")
	}
	if req.URL.Scheme != "" && !strings.EqualFold(req.URL.Scheme, "http") {
		return "", fmt.Errorf("connect_forward: target scheme %q is not allowed", req.URL.Scheme)
	}

	target := req.URL.Host
	if target == "" {
		target = req.Host
	}
	if target == "" {
		return "", fmt.Errorf("connect_forward: malformed http target: missing host")
	}

	host, port, err := splitHostPortDefault(target, 80)
	if err != nil {
		return "", fmt.Errorf("connect_forward: malformed http target %q: %w", target, err)
	}
	if !strings.EqualFold(host, c.config.Host) {
		return "", fmt.Errorf("connect_forward: target host %q is not allowed", host)
	}
	if !c.config.AllowAnyPort && port != c.config.Port {
		return "", fmt.Errorf("connect_forward: target port %d is not allowed", port)
	}
	return net.JoinHostPort(host, strconv.Itoa(port)), nil
}

func splitHostPortDefault(target string, defaultPort int) (string, int, error) {
	host, portText, err := net.SplitHostPort(target)
	if err != nil {
		if strings.Contains(err.Error(), "missing port in address") {
			return target, defaultPort, nil
		}
		return "", 0, err
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return "", 0, err
	}
	return host, port, nil
}

func writeResponse(conn net.Conn, status int, reason string) error {
	_, err := fmt.Fprintf(conn, "HTTP/1.1 %d %s\r\n\r\n", status, reason)
	return err
}

func pipe(local, remote net.Conn, localReader *bufio.Reader) {
	done := make(chan struct{}, 2)
	go func() {
		n, err := io.Copy(local, remote)
		if err != nil {
			log.Printf("connect_forward: remote->client finished bytes=%d err=%v", n, err)
		}
		closeWrite(local)
		done <- struct{}{}
	}()
	go func() {
		n, err := io.Copy(remote, localReader)
		if err != nil {
			log.Printf("connect_forward: client->remote finished bytes=%d err=%v", n, err)
		}
		closeWrite(remote)
		done <- struct{}{}
	}()
	<-done
	<-done
	_ = local.Close()
	_ = remote.Close()
}

type closeWriter interface {
	CloseWrite() error
}

func closeWrite(conn net.Conn) {
	if cw, ok := conn.(closeWriter); ok {
		_ = cw.CloseWrite()
		return
	}
	_ = conn.Close()
}
