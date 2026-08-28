package docker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
)

// TestProbeHTTP checks the HTTP health probe: a 2xx or 3xx is healthy, a 4xx/5xx or
// a refused connection is not. No Docker required.
func TestProbeHTTP(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/healthz":
			w.WriteHeader(http.StatusOK)
		case "/redir":
			http.Redirect(w, r, "/healthz", http.StatusFound)
		case "/boom":
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	u, _ := url.Parse(srv.URL)
	host := u.Hostname()
	port, _ := strconv.Atoi(u.Port())
	ctx := context.Background()

	cases := []struct {
		path string
		want bool
	}{
		{"/healthz", true},  // 200
		{"/redir", true},    // 302, the server answered
		{"healthz", true},   // a missing leading slash is tolerated
		{"/boom", false},    // 500
		{"/missing", false}, // 404
	}
	for _, c := range cases {
		if got := probeHTTP(ctx, host, port, c.path); got != c.want {
			t.Errorf("probeHTTP(%q) = %v, want %v", c.path, got, c.want)
		}
	}
	if probeHTTP(ctx, "127.0.0.1", 1, "/") {
		t.Error("a probe to a closed port should be unhealthy")
	}
}
