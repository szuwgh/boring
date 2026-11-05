package httpproxy

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestHTTPReusesUpstreamWithoutAddingCompression(t *testing.T) {
	var connections atomic.Int32
	upstream := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Accept-Encoding"); got != "" {
			t.Errorf("proxy added Accept-Encoding: %q", got)
		}
		io.WriteString(w, "ok")
	}))
	upstream.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	upstream.Start()
	defer upstream.Close()
	client := proxyClient(t, testProxy(t, &Config{AllowClients: []string{"127.0.0.1/32"}, AllowAnyPort: true}))
	client.Transport.(*http.Transport).DisableCompression = true
	for i := 0; i < 3; i++ {
		resp, err := client.Get(upstream.URL)
		if err != nil {
			t.Fatal(err)
		}
		_, err = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := connections.Load(); got != 1 {
		t.Fatalf("upstream connections=%d, want 1", got)
	}
}

func TestHTTPPreservesExplicitCompressedBytes(t *testing.T) {
	payload := []byte{0x1f, 0x8b, 0x08, 0, 0, 0, 0, 0, 0, 0xff}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept-Encoding") != "gzip" {
			t.Error("explicit compression header lost")
		}
		w.Header().Set("Content-Encoding", "gzip")
		w.Write(payload)
	}))
	defer upstream.Close()
	client := proxyClient(t, testProxy(t, &Config{AllowClients: []string{"127.0.0.1/32"}, AllowAnyPort: true}))
	req, _ := http.NewRequest("GET", upstream.URL, nil)
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) || resp.Header.Get("Content-Encoding") != "gzip" {
		t.Fatalf("compressed payload modified: %x", got)
	}
}
