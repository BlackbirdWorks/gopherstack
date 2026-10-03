package regionpeers_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/regionpeers"
)

type fake struct {
	region string
	state  string
}

func newSet() *regionpeers.Set[fake] {
	return regionpeers.New("us-east-1", func(r string) *fake { return &fake{region: r} })
}

func TestSet_Get(t *testing.T) {
	t.Parallel()

	tests := []struct {
		set    *regionpeers.Set[fake]
		name   string
		region string
		wantNk bool
	}{
		{name: "home", set: newSet(), region: "us-east-1", wantNk: true},
		{name: "empty", set: newSet(), region: "", wantNk: true},
		{name: "other", set: newSet(), region: "eu-west-1"},
		{name: "nil-set", set: nil, region: "eu-west-1", wantNk: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := tc.set.Get(tc.region)
			if tc.wantNk {
				assert.Nil(t, got)

				return
			}

			require.NotNil(t, got)
			assert.Equal(t, tc.region, got.region)
			assert.Same(t, got, tc.set.Get(tc.region))
		})
	}
}

func TestSet_SnapshotRestore(t *testing.T) {
	t.Parallel()

	snap := func(p *fake) []byte { return []byte(`"` + p.state + `"`) }
	restore := func(p *fake, d []byte) error { return json.Unmarshal(d, &p.state) }
	closeFn := func(*fake) {}

	tests := []struct {
		regions  map[string]string
		name     string
		base     string
		wantSame bool
	}{
		{name: "no-peers-byte-identical", base: `{"version":2}`, wantSame: true},
		{name: "one-peer", base: `{"version":2}`, regions: map[string]string{"eu-west-1": "a"}},
		{name: "two-peers", base: `{"version":2}`, regions: map[string]string{"eu-west-1": "a", "ap-south-1": "b"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			src := newSet()
			for r, st := range tc.regions {
				src.Get(r).state = st
			}

			out := src.Snapshot([]byte(tc.base), snap)
			if tc.wantSame {
				assert.JSONEq(t, tc.base, string(out))
				assert.Equal(t, tc.base, string(out))
			}

			dst := newSet()
			dst.Get("stale-region")
			require.NoError(t, dst.Restore(out, restore, closeFn))

			for r, st := range tc.regions {
				assert.Equal(t, st, dst.Get(r).state)
			}

			assert.Empty(t, dst.Get("stale-region").state)
		})
	}
}
