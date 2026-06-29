package sshforward

import "testing"

func TestRouteDescriptionReturnsTarget(t *testing.T) {
	consumer := Builder(&Config{Target: "git.unvdb.com:443"})

	describer, ok := consumer.(interface{ RouteDescription() string })
	if !ok {
		t.Fatalf("ssh forward consumer does not describe its route")
	}
	if got := describer.RouteDescription(); got != "git.unvdb.com:443" {
		t.Fatalf("unexpected route description: %q", got)
	}
}
