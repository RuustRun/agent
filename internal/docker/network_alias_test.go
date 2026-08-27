package docker

import "testing"

func TestNetworkAliases(t *testing.T) {
	got := networkAliases("db-egg")
	want := []string{"db-egg", "db-egg.coop.internal"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("networkAliases(\"db-egg\") = %v, want %v", got, want)
	}
	if networkAliases("") != nil {
		t.Errorf("networkAliases(\"\") = %v, want nil", networkAliases(""))
	}
}
