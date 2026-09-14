package main

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type wantClaim struct {
	literal string
	kind    string
}

// TestAnalyzeDir_DelegationShapes drives analyzeDir (the full parse ->
// chase -> extract pipeline) against small synthetic packages under
// testdata/, one per delegation shape gopherstack-h3p1 added chasing for:
// predicate-function calls, map/route-table keys, a multi-hop method-call
// route table, a local slice-of-consts, a local const used only in a
// HasSuffix comparison (the false positive that shape produced before the
// declRanges exclusion), and the chase depth limit itself.
func TestAnalyzeDir_DelegationShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		dir  string
		want []wantClaim
	}{
		{
			dir: "predicate",
			want: []wantClaim{
				{"/widgets", "exact"},
				{"/widgets", "prefix"},
				{"/widgets/extra", "prefix"},
			},
		},
		{
			dir: "mapkeys",
			want: []wantClaim{
				{"/bar", "exact"},
				{"/foo", "exact"},
			},
		},
		{
			// "/things/search" is both a map key (scanMapKeyClaims: exact)
			// and, since it's also a plain quoted string literal sitting in
			// routesB's body text, picked up a second time by the ordinary
			// text scan (defaults to prefix with no "=="/HasPrefix context
			// nearby) -- the same double-reading every real map-literal
			// claim in this repo gets (e.g. accessanalyzer's "/finding"),
			// not something specific to the chase.
			dir: "routetable",
			want: []wantClaim{
				{"/things/search", "exact"},
				{"/things/search", "prefix"},
			},
		},
		{
			dir: "localslice",
			want: []wantClaim{
				{"/alpha", "exact"},
				{"/alpha/", "prefix"},
				{"/beta", "exact"},
				{"/beta/", "prefix"},
			},
		},
		{
			dir: "localconst",
			want: []wantClaim{
				{"/resources/", "prefix"},
			},
		},
		{
			dir: "depthlimit",
			want: []wantClaim{
				{"/depth-four", "exact"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.dir, func(t *testing.T) {
			t.Parallel()

			infos, err := analyzeDir(filepath.Join("testdata", tt.dir), tt.dir, map[string]int{})
			require.NoError(t, err)
			require.Len(t, infos, 1)

			got := make([]wantClaim, 0, len(infos[0].Claims))
			for _, c := range infos[0].Claims {
				got = append(got, wantClaim{c.Literal, c.KindStr})
			}

			assert.ElementsMatch(t, tt.want, got)
		})
	}
}
