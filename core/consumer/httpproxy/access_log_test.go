package httpproxy

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestProxyLogsEachHTTPAndConnectTarget(t *testing.T) {
	var logs bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(previous)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok") }))
	defer server.Close()
	proxy := testProxy(t, &Config{AllowClients: []string{"127.0.0.1/32"}, AllowAnyPort: true})
	client := proxyClient(t, proxy)
	for i := 0; i < 2; i++ {
		resp, err := client.Get(server.URL + "/path?q=1")
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	conn, err := net.Dial("tcp", strings.TrimPrefix(proxy, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(3 * time.Second))
	target := strings.TrimPrefix(server.URL, "http://")
	io.WriteString(conn, "CONNECT "+target+" HTTP/1.1\r\nHost: "+target+"\r\n\r\n")
	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil || !strings.Contains(line, "200") {
		t.Fatalf("CONNECT: %q %v", line, err)
	}
	output := logs.String()
	if count := strings.Count(output, `method="GET" url="`+server.URL+`/path?q=1"`); count != 2 {
		t.Fatalf("want two HTTP log entries, got %d: %s", count, output)
	}
	if !strings.Contains(output, `method="CONNECT" target="`+target+`"`) || !strings.Contains(output, `client="127.0.0.1:`) {
		t.Fatalf("missing CONNECT/client log: %s", output)
	}
}
