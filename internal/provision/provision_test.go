package provision

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/RuustRun/agent/internal/contract"
)

// fetchManifest should hit the right path, present the host token, decode the manifest
// and reject one with no generation.
func TestFetchManifest(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Path; got != "/api/v1/hosts/host-1/provisioning" {
			t.Errorf("unexpected path %q", got)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer tok-1" {
			t.Errorf("unexpected auth header %q", got)
		}
		_, _ = w.Write([]byte(`{"generation":"abc123","firewall":{"blockedOutboundPorts":[25,465]},"packages":["iproute2"],"agentNetworkCaps":true}`))
	}))
	defer srv.Close()

	o := Options{ControlPlaneURL: srv.URL, HostID: "host-1", Token: "tok-1", HTTP: srv.Client()}
	m, err := fetchManifest(context.Background(), o)
	if err != nil {
		t.Fatalf("fetchManifest: %v", err)
	}
	if m.Generation != "abc123" {
		t.Errorf("generation = %q, want abc123", m.Generation)
	}
	if m.Firewall == nil || len(m.Firewall.BlockedOutboundPorts) != 2 || m.Firewall.BlockedOutboundPorts[1] != 465 {
		t.Errorf("firewall ports not decoded: %+v", m.Firewall)
	}
	if !m.AgentNetworkCaps {
		t.Errorf("agentNetworkCaps should be true")
	}
}

func TestFetchManifestRejectsNoGeneration(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"firewall":{"blockedOutboundPorts":[25]}}`))
	}))
	defer srv.Close()

	o := Options{ControlPlaneURL: srv.URL, HostID: "host-1", Token: "tok-1", HTTP: srv.Client()}
	if _, err := fetchManifest(context.Background(), o); err == nil {
		t.Fatal("expected an error for a manifest with no generation")
	}
}

// A nil policy turns every switch off, so the script removes what it installed (the BYO
// case, and a managed host after the operator turns hardening off).
func TestNetHardeningEnv(t *testing.T) {
	off := "RUUST_DROP_TCP_IP_OPTIONS=0\nRUUST_DISABLE_MPTCP=0\nRUUST_BLOCK_SCTP=0\n"
	if got := netHardeningEnv(nil); got != off {
		t.Errorf("nil policy = %q, want %q", got, off)
	}
	on := &contract.NetHardening{DropTCPIPOptions: true, DisableMPTCP: true, BlockSCTP: true}
	want := "RUUST_DROP_TCP_IP_OPTIONS=1\nRUUST_DISABLE_MPTCP=1\nRUUST_BLOCK_SCTP=1\n"
	if got := netHardeningEnv(on); got != want {
		t.Errorf("full policy = %q, want %q", got, want)
	}
	partial := &contract.NetHardening{DropTCPIPOptions: true}
	if got := netHardeningEnv(partial); got != "RUUST_DROP_TCP_IP_OPTIONS=1\nRUUST_DISABLE_MPTCP=0\nRUUST_BLOCK_SCTP=0\n" {
		t.Errorf("partial policy = %q", got)
	}
}

// The control plane's camelCase keys must decode onto the policy.
func TestFetchManifestNetHardening(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"generation":"g1","netHardening":{"dropTcpIpOptions":true,"disableMptcp":true,"blockSctp":true}}`))
	}))
	defer srv.Close()

	o := Options{ControlPlaneURL: srv.URL, HostID: "host-1", Token: "tok-1", HTTP: srv.Client()}
	m, err := fetchManifest(context.Background(), o)
	if err != nil {
		t.Fatalf("fetchManifest: %v", err)
	}
	nh := m.NetHardening
	if nh == nil || !nh.DropTCPIPOptions || !nh.DisableMPTCP || !nh.BlockSCTP {
		t.Errorf("netHardening not decoded: %+v", nh)
	}
}
