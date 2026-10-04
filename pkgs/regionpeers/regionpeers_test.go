package regionpeers_test

import (
	"encoding/json"
	"fmt"
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
			dst.Get("xx-stale-1")
			require.NoError(t, dst.Restore(out, restore, closeFn))

			for r, st := range tc.regions {
				assert.Equal(t, st, dst.Get(r).state)
			}

			assert.Empty(t, dst.Get("xx-stale-1").state)
		})
	}
}

func TestSet_All(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		regions []string
		want    []string
	}{
		{name: "none", want: []string{}},
		{name: "home-ignored", regions: []string{"us-east-1"}, want: []string{}},
		{
			name:    "sorted",
			regions: []string{"eu-west-1", "ap-south-1", "eu-west-1"},
			want:    []string{"ap-south-1", "eu-west-1"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newSet()
			for _, r := range tc.regions {
				s.Get(r)
			}

			all := s.All()
			got := make([]string, 0, len(all))

			for _, p := range all {
				got = append(got, p.region)
			}

			assert.Equal(t, tc.want, got)
			assert.Len(t, s.All(), len(tc.want))
		})
	}

	var nilSet *regionpeers.Set[fake]
	assert.Empty(t, nilSet.All())
}

func TestFirst(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
		in   []string
	}{
		{name: "first-non-empty", in: []string{"", "eu-west-1", "us-west-2"}, want: "eu-west-1"},
		{name: "all-empty", in: []string{"", ""}},
		{name: "none"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, regionpeers.First(tc.in...))
		})
	}
}

func TestBackend(t *testing.T) {
	t.Parallel()

	s := newSet()
	home := &fake{region: "us-east-1"}
	backendFor := func(region string) any {
		if p := s.Get(region); p != nil {
			return p
		}

		return home
	}

	tests := []struct {
		name   string
		region string
		want   string
	}{
		{name: "home", region: "us-east-1", want: "us-east-1"},
		{name: "empty-is-home", region: "", want: "us-east-1"},
		{name: "sibling", region: "eu-west-1", want: "eu-west-1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, ok := regionpeers.Backend[*fake](backendFor, tc.region)
			require.True(t, ok)
			assert.Equal(t, tc.want, got.region)
		})
	}

	_, ok := regionpeers.Backend[*string](backendFor, "eu-west-1")
	assert.False(t, ok)
}

func TestValidRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		region string
		want   bool
	}{
		{"us-east-1", true},
		{"ap-southeast-5", true},
		{"us-gov-west-1", true},
		{"us-isob-east-1", true},
		{"eusc-de-east-1", true},
		{"il-central-1", true},
		{"", false},
		{"nowhere", false},
		{"US-EAST-1", false},
		{"us-east-1/../x", false},
		{"us-east-1\n", false},
		{"aaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbbbbbbbbbb-1", false},
	}

	for _, tc := range tests {
		t.Run(tc.region, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, regionpeers.ValidRegion(tc.region))
		})
	}
}

func TestSet_GetBounded(t *testing.T) {
	t.Parallel()

	built := 0
	s := regionpeers.New("us-east-1", func(r string) *fake {
		built++

		return &fake{region: r}
	})

	assert.Nil(t, s.Get("garbage"))
	assert.Nil(t, s.Get("../../etc"))
	assert.Zero(t, built)

	for i := range 3 * regionpeers.MaxPeers {
		s.Get(fmt.Sprintf("aa-bb-%d", i))
	}

	assert.Equal(t, regionpeers.MaxPeers, built)
	assert.Len(t, s.All(), regionpeers.MaxPeers)
	assert.NotNil(t, s.Get("aa-bb-0"))
	assert.Nil(t, s.Get("zz-yy-99"))
}
