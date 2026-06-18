package inspector2_test

// Tests for seedable Inspector2 findings (§I): the backend can be seeded with
// realistic findings that ListFindings returns and filters via filterCriteria,
// and ListFindingAggregations reports the real severity breakdown. This exceeds
// LocalStack, whose ListFindings is hardwired empty.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

func TestSeedFinding_Defaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		in           inspector2.Finding
		wantSeverity string
		wantStatus   string
		wantType     string
		wantErr      bool
	}{
		{
			name:         "all_defaults",
			in:           inspector2.Finding{},
			wantSeverity: "MEDIUM",
			wantStatus:   "ACTIVE",
			wantType:     "PACKAGE_VULNERABILITY",
		},
		{
			name:         "explicit_values",
			in:           inspector2.Finding{Severity: inspector2.FindingSeverity{Label: "CRITICAL"}, Status: "SUPPRESSED", Type: "CODE_VULNERABILITY"},
			wantSeverity: "CRITICAL",
			wantStatus:   "SUPPRESSED",
			wantType:     "CODE_VULNERABILITY",
		},
		{
			name:    "invalid_severity",
			in:      inspector2.Finding{Severity: inspector2.FindingSeverity{Label: "BOGUS"}},
			wantErr: true,
		},
		{
			name:    "invalid_status",
			in:      inspector2.Finding{Status: "DELETED"},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := inspector2.NewInMemoryBackend("123456789012", "us-east-1")
			f, err := b.SeedFinding(tc.in)

			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.wantSeverity, f.Severity.Label)
			assert.Equal(t, tc.wantStatus, f.Status)
			assert.Equal(t, tc.wantType, f.Type)
			assert.NotEmpty(t, f.FindingArn)
			assert.Equal(t, "123456789012", f.AccountID)
			assert.False(t, f.FirstObservedAt.IsZero())
		})
	}
}

func TestListFindings_FilterCriteria(t *testing.T) {
	t.Parallel()

	seed := func(t *testing.T) *inspector2.InMemoryBackend {
		t.Helper()

		b := inspector2.NewInMemoryBackend("123456789012", "us-east-1")
		_, err := b.SeedFinding(
			inspector2.Finding{Severity: inspector2.FindingSeverity{Label: "CRITICAL"}, Type: "PACKAGE_VULNERABILITY", Status: "ACTIVE"},
		)
		require.NoError(t, err)
		_, err = b.SeedFinding(inspector2.Finding{Severity: inspector2.FindingSeverity{Label: "LOW"}, Type: "PACKAGE_VULNERABILITY", Status: "ACTIVE"})
		require.NoError(t, err)
		_, err = b.SeedFinding(inspector2.Finding{Severity: inspector2.FindingSeverity{Label: "HIGH"}, Type: "CODE_VULNERABILITY", Status: "SUPPRESSED"})
		require.NoError(t, err)

		return b
	}

	tests := []struct {
		criteria  map[string]any
		name      string
		wantCount int
	}{
		{
			name:      "no_criteria_returns_all",
			criteria:  nil,
			wantCount: 3,
		},
		{
			name: "severity_equals",
			criteria: map[string]any{
				"severity": []any{map[string]any{"comparison": "EQUALS", "value": "CRITICAL"}},
			},
			wantCount: 1,
		},
		{
			name: "severity_or",
			criteria: map[string]any{
				"severity": []any{
					map[string]any{"comparison": "EQUALS", "value": "CRITICAL"},
					map[string]any{"comparison": "EQUALS", "value": "LOW"},
				},
			},
			wantCount: 2,
		},
		{
			name: "status_suppressed",
			criteria: map[string]any{
				"findingStatus": []any{map[string]any{"comparison": "EQUALS", "value": "SUPPRESSED"}},
			},
			wantCount: 1,
		},
		{
			name: "type_and_status",
			criteria: map[string]any{
				"findingType":   []any{map[string]any{"comparison": "EQUALS", "value": "PACKAGE_VULNERABILITY"}},
				"findingStatus": []any{map[string]any{"comparison": "EQUALS", "value": "ACTIVE"}},
			},
			wantCount: 2,
		},
		{
			name: "not_equals",
			criteria: map[string]any{
				"severity": []any{map[string]any{"comparison": "NOT_EQUALS", "value": "CRITICAL"}},
			},
			wantCount: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := seed(t)
			got, _, err := b.ListFindings(0, "", tc.criteria)
			require.NoError(t, err)
			assert.Len(t, got, tc.wantCount)
		})
	}
}

func TestListFindings_Pagination(t *testing.T) {
	t.Parallel()

	b := inspector2.NewInMemoryBackend("123456789012", "us-east-1")
	for range 5 {
		_, err := b.SeedFinding(inspector2.Finding{Severity: inspector2.FindingSeverity{Label: "MEDIUM"}})
		require.NoError(t, err)
	}

	page1, next, err := b.ListFindings(2, "", nil)
	require.NoError(t, err)
	assert.Len(t, page1, 2)
	require.NotEmpty(t, next)

	page2, next2, err := b.ListFindings(2, next, nil)
	require.NoError(t, err)
	assert.Len(t, page2, 2)
	require.NotEmpty(t, next2)

	page3, next3, err := b.ListFindings(2, next2, nil)
	require.NoError(t, err)
	assert.Len(t, page3, 1)
	assert.Empty(t, next3)

	// No ARN appears twice across pages.
	seen := map[string]bool{}
	for _, p := range [][]*inspector2.Finding{page1, page2, page3} {
		for _, f := range p {
			assert.False(t, seen[f.FindingArn], "duplicate ARN across pages: %s", f.FindingArn)
			seen[f.FindingArn] = true
		}
	}

	assert.Len(t, seen, 5)
}

func TestListFindingAggregations_SeededCounts(t *testing.T) {
	t.Parallel()

	b := inspector2.NewInMemoryBackend("123456789012", "us-east-1")

	empty, err := b.ListFindingAggregations("ACCOUNT", nil)
	require.NoError(t, err)
	assert.Empty(t, empty["responses"])

	for _, sev := range []string{"CRITICAL", "CRITICAL", "HIGH", "LOW"} {
		_, seedErr := b.SeedFinding(inspector2.Finding{Severity: inspector2.FindingSeverity{Label: sev}})
		require.NoError(t, seedErr)
	}

	got, err := b.ListFindingAggregations("ACCOUNT", nil)
	require.NoError(t, err)

	responses, ok := got["responses"].([]map[string]any)
	require.True(t, ok)
	require.Len(t, responses, 1)

	acct, ok := responses[0]["accountAggregation"].(map[string]any)
	require.True(t, ok)
	counts, ok := acct["severityCounts"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, int64(4), counts["all"])
	assert.Equal(t, int64(2), counts["critical"])
	assert.Equal(t, int64(1), counts["high"])
	assert.Equal(t, int64(1), counts["low"])
}
