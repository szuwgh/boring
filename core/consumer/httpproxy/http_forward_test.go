package httpproxy

import (
	"bufio"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func testProxy(t *testing.T, cfg *Config) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	consumer := Builder(cfg)
	t.Cleanup(consumer.(*Consumer).transport.CloseIdleConnections)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go consumer.ConsumeConn(conn)
		}
	}()
	return "http://" + ln.Addr().String()
}
func proxyClient(t *testing.T, proxy string) *http.Client {
	t.Helper()
	u, err := url.Parse(proxy)
	if err != nil {
		t.Fatal(err)
	}
	tr := &http.Transport{Proxy: http.ProxyURL(u), TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	t.Cleanup(tr.CloseIdleConnections)
	return &http.Client{Transport: tr, Timeout: 3 * time.Second}
}
func TestHTTPDynamicForwarding(t *testing.T) {
	for _, secure := range []bool{false, true} {
		t.Run(map[bool]string{false: "HTTP", true: "HTTPS"}[secure], func(t *testing.T) {
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case <-r.Context().Done():
					t.Error("request context canceled prematurely")
				case <-time.After(30 * time.Millisecond):
				}
				body, _ := io.ReadAll(r.Body)
				w.Header().Set("X-Upstream", "yes")
				w.WriteHeader(201)
				io.WriteString(w, r.Method+" "+r.URL.RequestURI()+" "+string(body))
			})
			var server *httptest.Server
			if secure {
				server = httptest.NewTLSServer(handler)
			} else {
				server = httptest.NewServer(handler)
			}
			defer server.Close()
			p := testProxy(t, &Config{AllowClients: []string{"127.0.0.1/32"}, AllowAnyPort: true})
			client := proxyClient(t, p)
			for i := 0; i < 2; i++ {
				resp, err := client.Post(server.URL+"/hello?q=1", "text/plain", strings.NewReader("payload"))
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil {
					t.Fatal(err)
				}
				if resp.StatusCode != 201 || resp.Header.Get("X-Upstream") != "yes" || string(body) != "POST /hello?q=1 payload" {
					t.Fatalf("unexpected response %d %q", resp.StatusCode, body)
				}
			}
		})
	}
}
func TestHTTPPortPolicy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok") }))
	defer server.Close()
	for _, any := range []bool{false, true} {
		p := testProxy(t, &Config{AllowClients: []string{"127.0.0.1/32"}, AllowedPorts: []int{80}, AllowAnyPort: any})
		resp, err := proxyClient(t, p).Get(server.URL)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		want := 403
		if any {
			want = 200
		}
		if resp.StatusCode != want {
			t.Fatalf("any=%v got %d", any, resp.StatusCode)
		}
	}
}
func TestClientDenied(t *testing.T) {
	p := testProxy(t, &Config{AllowClients: []string{"192.0.2.1/32"}, AllowAnyPort: true})
	resp, err := proxyClient(t, p).Get("http://127.0.0.1:9/")
	if err == nil {
		resp.Body.Close()
		if resp.StatusCode < 400 {
			t.Fatal("unauthorized client allowed")
		}
	}
}
func TestConnectBufferedBytesAndHalfClose(t *testing.T) {
	upstream, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer upstream.Close()
	go func() {
		c, e := upstream.Accept()
		if e != nil {
			return
		}
		defer c.Close()
		data, _ := io.ReadAll(c)
		c.Write(append([]byte("echo:"), data...))
	}()
	p := testProxy(t, &Config{AllowClients: []string{"127.0.0.1/32"}, AllowAnyPort: true})
	u, _ := url.Parse(p)
	conn, err := net.Dial("tcp", u.Host)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(3 * time.Second))
	io.WriteString(conn, "CONNECT "+upstream.Addr().String()+" HTTP/1.1\r\nHost: "+upstream.Addr().String()+"\r\n\r\npayload")
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatal(resp.Status)
	}
	conn.(*net.TCPConn).CloseWrite()
	body, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "echo:payload" {
		t.Fatalf("got %q", body)
	}
}
