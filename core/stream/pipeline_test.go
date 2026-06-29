package stream

import (
	"net"
	"testing"
)

func TestPipelineRouteLogLineIncludesListenAndTarget(t *testing.T) {
	p := &Pipeline{
		Name:     "git-proxy",
		Producer: fakeRouteProducer{addr: fakeAddr("[::]:18080")},
		Consumer: fakeRouteConsumer{route: "git.unvdb.com:443"},
	}

	got := p.routeLogLine()
	want := "[stream:git-proxy] listening [::]:18080 -> git.unvdb.com:443"
	if got != want {
		t.Fatalf("route log line mismatch:\nwant %q\n got %q", want, got)
	}
}

func TestPipelineRouteLogLineFallsBackToUnknown(t *testing.T) {
	p := &Pipeline{
		Name:     "git-proxy",
		Producer: fakeRouteProducer{},
		Consumer: fakeRouteConsumer{},
	}

	got := p.routeLogLine()
	want := "[stream:git-proxy] listening unknown -> unknown"
	if got != want {
		t.Fatalf("route log line mismatch:\nwant %q\n got %q", want, got)
	}
}

type fakeRouteProducer struct {
	addr net.Addr
}

func (f fakeRouteProducer) Start() error              { return nil }
func (f fakeRouteProducer) Accept() (net.Conn, error) { return nil, net.ErrClosed }
func (f fakeRouteProducer) Stop() error               { return nil }
func (f fakeRouteProducer) Addr() net.Addr            { return f.addr }

type fakeRouteConsumer struct {
	route string
}

func (f fakeRouteConsumer) ConsumeConn(net.Conn) error { return nil }
func (f fakeRouteConsumer) RouteDescription() string   { return f.route }

type fakeAddr string

func (f fakeAddr) Network() string { return "tcp" }
func (f fakeAddr) String() string  { return string(f) }
