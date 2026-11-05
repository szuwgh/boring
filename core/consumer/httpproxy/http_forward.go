package httpproxy

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
)

func strip(h http.Header) {
	for _, v := range h.Values("Connection") {
		for _, s := range strings.Split(v, ",") {
			h.Del(strings.TrimSpace(s))
		}
	}
	for _, s := range []string{"Connection", "Proxy-Connection", "Proxy-Authenticate", "Proxy-Authorization", "Keep-Alive", "TE", "Trailer", "Transfer-Encoding", "Upgrade"} {
		h.Del(s)
	}
}
func (c *Consumer) forward(conn net.Conn, req *http.Request) error {
	completed := false
	defer func() {
		if !completed {
			conn.Close()
		}
		req.Body.Close()
	}()
	if req.URL.Scheme != "http" || req.URL.Host == "" || req.URL.User != nil || req.URL.Hostname() == "" {
		reply(conn, 400)
		return fmt.Errorf("invalid proxy URL")
	}
	port := 80
	if s := req.URL.Port(); s != "" {
		p, e := strconv.Atoi(s)
		if e != nil || p < 1 || p > 65535 {
			reply(conn, 400)
			return fmt.Errorf("invalid port")
		}
		port = p
	}
	if !c.allowedPort(port) {
		reply(conn, 403)
		return fmt.Errorf("port denied")
	}
	closeClient := req.Close || req.ProtoMajor == 1 && req.ProtoMinor == 0
	req.RequestURI = ""
	req.Host = req.URL.Host
	req.Close = false
	strip(req.Header)
	req.Trailer = nil
	log.Printf("[http_proxy] client=%q method=%q url=%q", conn.RemoteAddr().String(), req.Method, req.URL.String())
	resp, err := c.transport.RoundTrip(req)
	if err != nil {
		status := 502
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			status = 504
		}
		reply(conn, status)
		return err
	}
	defer resp.Body.Close()
	strip(resp.Header)
	resp.ProtoMajor = 1
	resp.ProtoMinor = 1
	resp.Proto = "HTTP/1.1"
	resp.Close = closeClient
	resp.Trailer = nil
	if resp.ContentLength < 0 && req.Method != http.MethodHead && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotModified {
		resp.TransferEncoding = []string{"chunked"}
	}
	err = resp.Write(conn)
	completed = err == nil
	return err
}
