package main

import "testing"

func TestRequireSecureURL(t *testing.T) {
	cases := []struct {
		url string
		ok  bool
	}{
		{"https://cp.ruust.run", true},
		{"https://cp.ruust.run/x", true},
		{"http://cp.ruust.run", false},
		{"http://192.0.2.10:3939", false},
		{"http://localhost:3939", true},
		{"http://127.0.0.1:3939", true},
	}
	for _, c := range cases {
		err := requireSecureURL(c.url)
		if (err == nil) != c.ok {
			t.Errorf("requireSecureURL(%q): ok=%v err=%v", c.url, c.ok, err)
		}
	}
	// Explicit opt-out allows a remote http URL.
	t.Setenv("RUUST_ALLOW_INSECURE_CP", "1")
	if err := requireSecureURL("http://cp.internal"); err != nil {
		t.Errorf("opt-out should allow http: %v", err)
	}
}
