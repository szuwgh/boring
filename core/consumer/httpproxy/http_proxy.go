package httpproxy

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
)

func (c *Consumer) RouteDescription() string { return "dynamic HTTP/HTTPS proxy" }
func (c *Consumer) allowedPort(p int) bool {
	return p > 0 && p <= 65535 && (c.cfg.AllowAnyPort || c.ports[p])
}
func (c *Consumer) ConsumeConn(conn net.Conn) error {
	defer conn.Close()
	if c.err != nil {
		return c.err
	}
	host, _, err := net.SplitHostPort(conn.RemoteAddr().String())
	if err != nil {
		return err
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return err
	}
	ip = ip.Unmap()
	allowed := false
	for _, p := range c.clients {
		if p.Contains(ip) {
			allowed = true
			break
		}
	}
	if !allowed {
		return fmt.Errorf("http_proxy: client denied")
	}
	reader := bufio.NewReader(conn)
	for {
		req, e := http.ReadRequest(reader)
		if e != nil {
			if e == io.EOF {
				return nil
			}
			reply(conn, 400)
			return e
		}
		if req.Method == http.MethodConnect {
			return c.connect(conn, reader, req)
		}
		closeClient := req.Close || req.ProtoMajor < 1 || (req.ProtoMajor == 1 && req.ProtoMinor == 0)
		if e = c.forward(conn, req); e != nil {
			return e
		}
		if closeClient {
			return nil
		}
	}
}
func reply(w io.Writer, status int) {
	fmt.Fprintf(w, "HTTP/1.1 %d %s\r\nContent-Length: 0\r\nConnection: close\r\n\r\n", status, http.StatusText(status))
}
