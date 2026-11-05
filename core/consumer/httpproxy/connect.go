package httpproxy

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func (c *Consumer) connect(local net.Conn, r *bufio.Reader, req *http.Request) error {
	if req.ContentLength > 0 || len(req.TransferEncoding) > 0 {
		reply(local, 400)
		local.Close()
		if req.Body != nil {
			req.Body.Close()
		}
		return fmt.Errorf("CONNECT request body is not supported")
	}
	authority := req.RequestURI
	if authority == "" || authority != req.Host {
		reply(local, 400)
		return fmt.Errorf("invalid CONNECT authority")
	}
	host, p, e := net.SplitHostPort(authority)
	port, pe := strconv.Atoi(p)
	if e != nil || pe != nil || host == "" || strings.ContainsAny(host, "/@?# \\ \t\r\n") || port < 1 || port > 65535 {
		reply(local, 400)
		return fmt.Errorf("invalid CONNECT authority")
	}
	if !c.allowedPort(port) {
		reply(local, 403)
		return fmt.Errorf("port denied")
	}
	log.Printf("[http_proxy] client=%q method=%q target=%q", local.RemoteAddr().String(), req.Method, authority)
	remote, err := (&net.Dialer{Timeout: 10 * time.Second}).DialContext(context.Background(), "tcp", net.JoinHostPort(host, p))
	if err != nil {
		status := 502
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			status = 504
		}
		reply(local, status)
		return err
	}
	defer remote.Close()
	if _, err = io.WriteString(local, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		return err
	}
	done := make(chan error, 2)
	copySide := func(dst net.Conn, src io.Reader) {
		_, e := io.Copy(dst, src)
		if e != nil {
			local.Close()
			remote.Close()
		} else if cw, ok := dst.(interface{ CloseWrite() error }); ok {
			cw.CloseWrite()
		} else {
			dst.Close()
		}
		done <- e
	}
	go copySide(local, remote)
	go copySide(remote, r)
	a := <-done
	b := <-done
	if a != nil {
		return a
	}
	return b
}
