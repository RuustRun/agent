package docker

import (
	"context"
	"net"
	"testing"
)

// TestProbeTCP covers the agent-side health probe: a listening port is healthy,
// and a refused connection (the localhost-bind case the docs describe) is not.
func TestProbeTCP(t *testing.T) {
	ctx := context.Background()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	// Accept and drop connections so dials succeed.
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()

	if !probeTCP(ctx, ln.Addr().String()) {
		t.Errorf("probeTCP(%s) = false, want true (something is listening)", ln.Addr())
	}
	// Port 1 has nothing listening: a bound-to-loopback app looks like this from
	// the bridge address, so the probe must report unhealthy.
	if probeTCP(ctx, "127.0.0.1:1") {
		t.Errorf("probeTCP(127.0.0.1:1) = true, want false (connection refused)")
	}
}
