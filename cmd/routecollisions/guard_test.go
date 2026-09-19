package main

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIsGuarded_GuardShapes drives analyzeDir against small synthetic
// packages under testdata/, one guarded/unguarded pair per false-positive
// shape gopherstack-op3e's 2026-09-19 audit found (see guard.go's doc
// comments): an ARN service-segment check, a sibling-specific named guard
// (ecr's isRegistryPath), a path-boundary concatenation living inside a
// named helper, exact-match-vs-prefix semantics, a closed route/op-table
// whitelist (both the "!= opUnknown" and comma-ok shapes), and a
// User-Agent marker check.
func TestIsGuarded_GuardShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		dir  string
		want bool
	}{
		{"guardarnguarded", true},
		{"guardarnunguarded", false},
		{"guardisnameguarded", true},
		{"guardisnameunguarded", false},
		{"guardboundaryguarded", true},
		{"guardboundaryunguarded", false},
		{"guardexactguarded", true},
		{"guardexactunguarded", false},
		{"guardclosedtableguarded", true},
		{"guardclosedtableunguarded", false},
		{"guardclosedtablecommaok", true},
		{"guarduseragentguarded", true},
		{"guarduseragentunguarded", false},
	}

	for _, tt := range tests {
		t.Run(tt.dir, func(t *testing.T) {
			t.Parallel()

			infos, err := analyzeDir(filepath.Join("testdata", tt.dir), tt.dir, map[string]int{})
			require.NoError(t, err)
			require.Len(t, infos, 1, "fixture must produce exactly one svcInfo (at least one path claim)")

			assert.Equal(t, tt.want, infos[0].Guarded)

			if tt.want {
				assert.NotEmpty(t, infos[0].GuardEvidence, "guarded fixture must record why")

				for _, ev := range infos[0].GuardEvidence {
					assert.NotEmpty(t, ev.File)
					assert.Positive(t, ev.Line)
					assert.NotEmpty(t, ev.Construct)
				}
			} else {
				assert.Empty(t, infos[0].GuardEvidence)
			}
		})
	}
}

// TestIsGuarded_RealServices spot-checks isGuarded against a handful of the
// real services from gopherstack-op3e's 2026-09-19 audit -- the twelve
// *_routing_cross_service_test.go fixtures proved these are guarded, and
// the old text-regex detector (ExtractServiceFromRequest|is\w+Request\()
// missed all of them (amplify/eks/mediapackage/... never call an
// isXxxRequest helper; dlm/vpclattice/ecr/resourcegroups/codeartifact use
// isXxxPath/isXxxARN; polly uses a closed-table sentinel; appsync uses
// MatchesUserAgentMarker).
func TestIsGuarded_RealServices(t *testing.T) {
	t.Parallel()

	root, err := filepath.Abs("../..")
	require.NoError(t, err)

	tests := []string{
		"amplify", "eks", "accessanalyzer", "dlm", "macie2", "vpclattice",
		"mediapackage", "mediatailor", "ecr", "appsync", "polly", "codeartifact",
		"resourcegroups",
	}

	for _, dir := range tests {
		t.Run(dir, func(t *testing.T) {
			t.Parallel()

			infos, analyzeErr := analyzeDir(filepath.Join(root, "services", dir), dir, map[string]int{})
			require.NoError(t, analyzeErr)
			require.NotEmpty(t, infos)

			guarded := false

			for _, info := range infos {
				if info.Guarded {
					guarded = true
				}
			}

			assert.True(t, guarded, "%s: expected at least one guarded RouteMatcher", dir)
		})
	}
}
