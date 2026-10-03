package main

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func scanFixture(t *testing.T, name string) map[string]finding {
	t.Helper()

	root, err := filepath.Abs("testdata")
	require.NoError(t, err)

	found, err := scanServiceDir(
		filepath.Join(root, "svc", name), root, filepath.Join(root, "cache"),
		map[string]string{"fakesvc": "v1.0.0"},
	)
	require.NoError(t, err)

	out := map[string]finding{}
	for _, f := range found {
		out[f.Code] = f
	}

	return out
}

func TestScanFixtures_FalsePositiveClasses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		confident []string
		review    []string
		absent    []string
	}{
		{
			name:      "alias",
			confident: []string{"InventedAliasException"},
			absent:    []string{"InvalidParameterInput", "LimitExceeded"},
		},
		{
			name:      "member",
			confident: []string{"EnvelopeInvented", "TopLevelInvented"},
			review:    []string{"PerItemFailure", "NestedItemFailure"},
		},
		{
			name:      "constuse",
			confident: []string{"InventedConstCode"},
			review:    []string{"STATUSDONE", "NEVERUSED"},
		},
		{
			name:      "dead",
			confident: []string{"LiveThingException"},
			review:    []string{"DeadThingException"},
		},
		{
			name:      "fallback",
			confident: []string{"InventedCaseException"},
			review: []string{
				"RouterFallbackCode", "GenericCategoryCode", "DefaultFallbackCode",
				"MissingTargetCode", "InitialDefaultCode", "KnownThing",
			},
		},
		{
			name:      "generic",
			confident: []string{"BogusGenericLookalike"},
			absent: []string{
				"ValidationException", "ThrottlingException", "InternalFailure",
				"ServiceUnavailable", "AccessDeniedException", "Sender",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := scanFixture(t, tt.name)

			for _, code := range tt.confident {
				require.Contains(t, got, code)
				assert.Truef(t, got[code].Confident, "%s should be confident: %+v", code, got[code])
			}

			for _, code := range tt.review {
				require.Contains(t, got, code)
				assert.Falsef(t, got[code].Confident, "%s should be demoted: %+v", code, got[code])
				assert.NotEmpty(t, got[code].Reason)
			}

			for _, code := range tt.absent {
				assert.NotContains(t, got, code)
			}
		})
	}
}

func TestParseSchemaQueryAliases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want []string
	}{
		{"present", "testdata/cache/github.com/aws/aws-sdk-go-v2/service/fakesvc@v1.0.0/schemas/schemas.go",
			[]string{"InvalidParameterInput", "LimitExceeded"}},
		{"missing file", "testdata/nope/schemas.go", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseSchemaQueryAliases(tt.path)
			require.NoError(t, err)
			assert.Len(t, got, len(tt.want))

			for _, code := range tt.want {
				assert.True(t, got[code])
			}
		})
	}
}
