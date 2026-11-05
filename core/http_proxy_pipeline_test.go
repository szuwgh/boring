package core

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/pelletier/go-toml/v2"
	"github.com/szuwgh/boring/core/config"
	"github.com/szuwgh/boring/core/consumer/httpproxy"
	"github.com/szuwgh/boring/core/producer/tcp"
	"github.com/szuwgh/boring/core/stream"
)

func TestHTTPProxyPipelineFromTOML(t *testing.T) {
	var cfg config.Config
	err := toml.Unmarshal([]byte(`[[boring]]
name="proxy"
mode="stream"
[boring.producer]
type="tcp_listener"
listen="127.0.0.1:0"
[[boring.consumer]]
type="http_proxy"
allow_clients=["127.0.0.1/32"]
allow_any_port=true
`), &cfg)
	if err != nil {
		t.Fatal(err)
	}
	reg := NewRegister()
	reg.RegisterStreamProducer("tcp_listener", stream.MakeProducerBuilder(tcp.ListenerBuilder))
	reg.RegisterStreamConsumer("http_proxy", stream.MakeConsumerBuilder(httpproxy.Builder))
	engine, err := NewEngineFromConfig(&cfg, reg)
	if err != nil {
		t.Fatal(err)
	}
	engine.Run()
	defer engine.Stop()
	addr := engine.stream["proxy"].Producer.(stream.AddrProvider).Addr()
	proxyURL, _ := url.Parse("http://" + addr.String())
	tr := &http.Transport{Proxy: http.ProxyURL(proxyURL)}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: 3 * time.Second}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "pipeline-ok") }))
	defer upstream.Close()
	resp, err := client.Get(upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "pipeline-ok" {
		t.Fatalf("got %q", body)
	}
}
