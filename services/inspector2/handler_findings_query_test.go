package inspector2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- pagination ---

func TestListFindingsPagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		totalFindings int
		pageSize      int
		wantPages     int
		wantTotal     int
	}{
		{
			name:          "single_page_exact",
			totalFindings: 5,
			pageSize:      5,
			wantPages:     1,
			wantTotal:     5,
		},
		{
			name:          "two_pages_even",
			totalFindings: 4,
			pageSize:      2,
			wantPages:     2,
			wantTotal:     4,
		},
		{
			name:          "two_pages_odd",
			totalFindings: 5,
			pageSize:      3,
			wantPages:     2,
			wantTotal:     5,
		},
		{
			name:          "page_larger_than_total",
			totalFindings: 3,
			pageSize:      10,
			wantPages:     1,
			wantTotal:     3,
		},
		{
			name:          "many_pages",
			totalFindings: 10,
			pageSize:      3,
			wantPages:     4,
			wantTotal:     10,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, b := newParityHandlerAndBackend(t)

			for i := range tc.totalFindings {
				paritySeedFinding(t, b, "PACKAGE_VULNERABILITY", "HIGH", "ACTIVE",
					"paging-finding-"+string(rune('A'+i)))
			}

			var allFindings []any
			var nextToken string
			pageCount := 0

			for {
				body := map[string]any{"maxResults": tc.pageSize}
				if nextToken != "" {
					body["nextToken"] = nextToken
				}

				page, nt := parityListFindingsWithToken(t, h, body)
				allFindings = append(allFindings, page...)
				pageCount++
				nextToken = nt

				if nt == "" {
					break
				}
			}

			assert.Equal(t, tc.wantPages, pageCount)
			assert.Len(t, allFindings, tc.wantTotal)
		})
	}
}

// --- no next token on last page ---

func TestListFindingsNoNextTokenOnLastPage(t *testing.T) {
	t.Parallel()

	h, b := newParityHandlerAndBackend(t)

	for range 3 {
		paritySeedFinding(t, b, "PACKAGE_VULNERABILITY", "HIGH", "ACTIVE", "last-page-finding")
	}

	_, nextToken := parityListFindingsWithToken(t, h, map[string]any{"maxResults": 10})
	assert.Empty(t, nextToken)
}

// --- filter by severity ---

func TestListFindingsFilterBySeverity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		filterSeverity string
		wantCount      int
	}{
		{name: "critical", filterSeverity: "CRITICAL", wantCount: 2},
		{name: "high", filterSeverity: "HIGH", wantCount: 1},
		{name: "medium", filterSeverity: "MEDIUM", wantCount: 1},
		{name: "low", filterSeverity: "LOW", wantCount: 0},
		{name: "informational", filterSeverity: "INFORMATIONAL", wantCount: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, b := newParityHandlerAndBackend(t)

			paritySeedFinding(t, b, "PACKAGE_VULNERABILITY", "CRITICAL", "ACTIVE", "critical-1")
			paritySeedFinding(t, b, "PACKAGE_VULNERABILITY", "CRITICAL", "ACTIVE", "critical-2")
			paritySeedFinding(t, b, "NETWORK_REACHABILITY", "HIGH", "ACTIVE", "high-1")
			paritySeedFinding(t, b, "CODE_VULNERABILITY", "MEDIUM", "ACTIVE", "medium-1")

			findings := parityListFindings(t, h, map[string]any{
				"filterCriteria": map[string]any{
					"severity": []any{map[string]any{"value": tc.filterSeverity}},
				},
			})

			assert.Len(t, findings, tc.wantCount)

			for _, f := range findings {
				m := f.(map[string]any)
				sev := m["severity"].(map[string]any)
				assert.Equal(t, tc.filterSeverity, sev["label"])
			}
		})
	}
}

// --- filter by status ---

func TestListFindingsFilterByStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		filterStatus string
		wantCount    int
	}{
		{name: "active", filterStatus: "ACTIVE", wantCount: 3},
		{name: "suppressed", filterStatus: "SUPPRESSED", wantCount: 2},
		{name: "closed", filterStatus: "CLOSED", wantCount: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, b := newParityHandlerAndBackend(t)

			paritySeedFinding(t, b, "PACKAGE_VULNERABILITY", "CRITICAL", "ACTIVE", "active-1")
			paritySeedFinding(t, b, "PACKAGE_VULNERABILITY", "HIGH", "ACTIVE", "active-2")
			paritySeedFinding(t, b, "PACKAGE_VULNERABILITY", "MEDIUM", "ACTIVE", "active-3")
			paritySeedFinding(t, b, "NETWORK_REACHABILITY", "HIGH", "SUPPRESSED", "suppressed-1")
			paritySeedFinding(t, b, "CODE_VULNERABILITY", "LOW", "SUPPRESSED", "suppressed-2")
			paritySeedFinding(t, b, "PACKAGE_VULNERABILITY", "CRITICAL", "CLOSED", "closed-1")

			findings := parityListFindings(t, h, map[string]any{
				"filterCriteria": map[string]any{
					"findingStatus": []any{map[string]any{"value": tc.filterStatus}},
				},
			})

			assert.Len(t, findings, tc.wantCount)

			for _, f := range findings {
				m := f.(map[string]any)
				assert.Equal(t, tc.filterStatus, m["status"])
			}
		})
	}
}

// --- combined filter (severity AND status) ---

func TestListFindingsCombinedFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		severity  string
		status    string
		wantCount int
	}{
		{name: "critical_active", severity: "CRITICAL", status: "ACTIVE", wantCount: 2},
		{name: "high_suppressed", severity: "HIGH", status: "SUPPRESSED", wantCount: 1},
		{name: "medium_closed", severity: "MEDIUM", status: "CLOSED", wantCount: 0},
		{name: "critical_closed", severity: "CRITICAL", status: "CLOSED", wantCount: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, b := newParityHandlerAndBackend(t)

			paritySeedFinding(t, b, "PACKAGE_VULNERABILITY", "CRITICAL", "ACTIVE", "c-a-1")
			paritySeedFinding(t, b, "PACKAGE_VULNERABILITY", "CRITICAL", "ACTIVE", "c-a-2")
			paritySeedFinding(t, b, "PACKAGE_VULNERABILITY", "CRITICAL", "CLOSED", "c-c-1")
			paritySeedFinding(t, b, "PACKAGE_VULNERABILITY", "HIGH", "ACTIVE", "h-a-1")
			paritySeedFinding(t, b, "NETWORK_REACHABILITY", "HIGH", "SUPPRESSED", "h-s-1")
			paritySeedFinding(t, b, "CODE_VULNERABILITY", "MEDIUM", "ACTIVE", "m-a-1")

			findings := parityListFindings(t, h, map[string]any{
				"filterCriteria": map[string]any{
					"severity":      []any{map[string]any{"value": tc.severity}},
					"findingStatus": []any{map[string]any{"value": tc.status}},
				},
			})

			assert.Len(t, findings, tc.wantCount)

			for _, f := range findings {
				m := f.(map[string]any)
				sev := m["severity"].(map[string]any)
				assert.Equal(t, tc.severity, sev["label"])
				assert.Equal(t, tc.status, m["status"])
			}
		})
	}
}

// --- filter no match ---

func TestListFindingsFilterNoMatch(t *testing.T) {
	t.Parallel()

	h, b := newParityHandlerAndBackend(t)
	paritySeedFinding(t, b, "PACKAGE_VULNERABILITY", "HIGH", "ACTIVE", "f1")
	paritySeedFinding(t, b, "NETWORK_REACHABILITY", "MEDIUM", "ACTIVE", "f2")

	findings := parityListFindings(t, h, map[string]any{
		"filterCriteria": map[string]any{
			"severity": []any{map[string]any{"value": "CRITICAL"}},
		},
	})

	assert.Empty(t, findings)
}

// --- stable sort order ---

func TestListFindingsStableSortOrder(t *testing.T) {
	t.Parallel()

	h, b := newParityHandlerAndBackend(t)

	for i := range 5 {
		paritySeedFinding(t, b, "PACKAGE_VULNERABILITY", "HIGH", "ACTIVE",
			"sort-finding-"+string(rune('A'+i)))
	}

	first := parityListFindings(t, h, map[string]any{})
	second := parityListFindings(t, h, map[string]any{})

	require.Len(t, first, 5)
	require.Len(t, second, 5)

	for i := range first {
		f1 := first[i].(map[string]any)
		f2 := second[i].(map[string]any)
		assert.Equal(t, f1["findingArn"], f2["findingArn"])
	}
}

// --- pagination with filters ---

func TestListFindingsPaginationWithFilter(t *testing.T) {
	t.Parallel()

	h, b := newParityHandlerAndBackend(t)

	for i := range 5 {
		paritySeedFinding(t, b, "PACKAGE_VULNERABILITY", "CRITICAL", "ACTIVE",
			"crit-"+string(rune('A'+i)))
	}

	for i := range 3 {
		paritySeedFinding(t, b, "NETWORK_REACHABILITY", "HIGH", "ACTIVE",
			"high-"+string(rune('A'+i)))
	}

	var allCritical []any
	var nextToken string

	for {
		body := map[string]any{
			"maxResults": 2,
			"filterCriteria": map[string]any{
				"severity": []any{map[string]any{"value": "CRITICAL"}},
			},
		}
		if nextToken != "" {
			body["nextToken"] = nextToken
		}

		page, nt := parityListFindingsWithToken(t, h, body)
		allCritical = append(allCritical, page...)
		nextToken = nt

		if nt == "" {
			break
		}
	}

	assert.Len(t, allCritical, 5)

	for _, f := range allCritical {
		m := f.(map[string]any)
		sev := m["severity"].(map[string]any)
		assert.Equal(t, "CRITICAL", sev["label"])
	}
}

// --- default max results applied when not specified ---

func TestListFindingsDefaultMaxResults(t *testing.T) {
	t.Parallel()

	h, b := newParityHandlerAndBackend(t)

	for range 5 {
		paritySeedFinding(t, b, "PACKAGE_VULNERABILITY", "HIGH", "ACTIVE", "default-max-f")
	}

	findings, nextToken := parityListFindingsWithToken(t, h, map[string]any{})
	assert.Len(t, findings, 5)
	assert.Empty(t, nextToken)
}

// --- empty token on fresh start ---

func TestListFindingsEmptyTokenMeansStart(t *testing.T) {
	t.Parallel()

	h, b := newParityHandlerAndBackend(t)

	for i := range 3 {
		paritySeedFinding(t, b, "PACKAGE_VULNERABILITY", "HIGH", "ACTIVE",
			"token-start-"+string(rune('A'+i)))
	}

	f1, _ := parityListFindingsWithToken(t, h, map[string]any{"maxResults": 3})
	f2, _ := parityListFindingsWithToken(t, h, map[string]any{"maxResults": 3, "nextToken": ""})

	require.Len(t, f1, 3)
	require.Len(t, f2, 3)

	for i := range f1 {
		m1 := f1[i].(map[string]any)
		m2 := f2[i].(map[string]any)
		assert.Equal(t, m1["findingArn"], m2["findingArn"])
	}
}
