package connectforward

import (
	"bufio"
	"fmt"
	"io"
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
	if req.Body != nil {
		_ = req.Body.Close()
	}

	if req.Method != http.MethodConnect {
		_ = writeResponse(local, http.StatusMethodNotAllowed, "Method Not Allowed")
		return fmt.Errorf("connect_forward: unsupported method %q", req.Method)
	}

	if err := c.validateConnectTarget(req.Host); err != nil {
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

	pipe(local, remote, reader)
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

func (c *Consumer) validateConnectTarget(target string) error {
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

func writeResponse(conn net.Conn, status int, reason string) error {
	_, err := fmt.Fprintf(conn, "HTTP/1.1 %d %s\r\n\r\n", status, reason)
	return err
}

func pipe(local, remote net.Conn, localReader *bufio.Reader) {
	done := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(local, remote)
		done <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(remote, localReader)
		done <- struct{}{}
	}()
	<-done
	_ = local.Close()
	_ = remote.Close()
}
