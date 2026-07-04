package inspector2_test

// Parity §I: ListFindings real stateful implementation.
//
// These tests verify that ListFindings:
//   - Returns an empty slice when no findings exist
//   - Returns seeded findings correctly (round-trip)
//   - Supports pagination via maxResults and nextToken
//   - Filters by severityLabel
//   - Filters by findingStatus
//   - Combines filters (AND semantics)
//   - Returns correct finding fields (findingArn, awsAccountId, type, severity, status, etc.)
//   - Handles invalid JSON body gracefully
//   - Produces stable sort order (by ARN) across calls
//   - Persists findings through Snapshot/Restore
//   - Clears findings on Reset

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

// --- helpers ---

func newParityBackend() *inspector2.InMemoryBackend {
	return inspector2.NewInMemoryBackend("000000000000", "us-east-1")
}

func newParityHandlerAndBackend(t *testing.T) (*inspector2.Handler, *inspector2.InMemoryBackend) {
	t.Helper()

	b := newParityBackend()

	return inspector2.NewHandler(b), b
}

func parityDo(t *testing.T, h *inspector2.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func parityDoRaw(t *testing.T, h *inspector2.Handler, method, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func parityListFindings(t *testing.T, h *inspector2.Handler, body map[string]any) []any {
	t.Helper()

	rec := parityDo(t, h, http.MethodPost, "/findings/list", body)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	findings, ok := resp["findings"].([]any)
	if !ok {
		return []any{}
	}

	return findings
}

func parityListFindingsWithToken(t *testing.T, h *inspector2.Handler, body map[string]any) ([]any, string) {
	t.Helper()

	rec := parityDo(t, h, http.MethodPost, "/findings/list", body)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	findings, _ := resp["findings"].([]any)
	nextToken, _ := resp["nextToken"].(string)

	return findings, nextToken
}

func paritySeedFinding(
	t *testing.T,
	b *inspector2.InMemoryBackend,
	findingType, severity, status, title string,
) string {
	t.Helper()

	return inspector2.SeedFinding(
		b,
		findingType,
		severity,
		status,
		title,
		"Description for "+title,
		[]inspector2.FindingResource{{Type: "AWS_EC2_INSTANCE", ID: "i-12345678"}},
	)
}

// --- empty state tests ---

func TestParityFindings_Empty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "no_body",
			body: nil,
		},
		{
			name: "empty_body",
			body: map[string]any{},
		},
		{
			name: "with_max_results",
			body: map[string]any{"maxResults": 100},
		},
		{
			name: "with_severity_filter",
			body: map[string]any{
				"filterCriteria": map[string]any{
					"severity": []any{map[string]any{"value": "CRITICAL"}},
				},
			},
		},
		{
			name: "with_status_filter",
			body: map[string]any{
				"filterCriteria": map[string]any{
					"findingStatus": []any{map[string]any{"value": "ACTIVE"}},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newParityHandlerAndBackend(t)
			findings := parityListFindings(t, h, tc.body)
			assert.Empty(t, findings)
		})
	}
}

// --- single finding round-trip ---

func TestParityFindings_SingleRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		findingType string
		severity    string
		status      string
		title       string
	}{
		{
			name:        "critical_active",
			findingType: "PACKAGE_VULNERABILITY",
			severity:    "CRITICAL",
			status:      "ACTIVE",
			title:       "Critical CVE in openssl",
		},
		{
			name:        "high_active",
			findingType: "NETWORK_REACHABILITY",
			severity:    "HIGH",
			status:      "ACTIVE",
			title:       "Port 22 reachable from internet",
		},
		{
			name:        "medium_suppressed",
			findingType: "PACKAGE_VULNERABILITY",
			severity:    "MEDIUM",
			status:      "SUPPRESSED",
			title:       "Suppressed medium finding",
		},
		{
			name:        "low_closed",
			findingType: "CODE_VULNERABILITY",
			severity:    "LOW",
			status:      "CLOSED",
			title:       "Low severity code issue",
		},
		{
			name:        "informational_active",
			findingType: "PACKAGE_VULNERABILITY",
			severity:    "INFORMATIONAL",
			status:      "ACTIVE",
			title:       "Informational finding",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, b := newParityHandlerAndBackend(t)
			findingARN := paritySeedFinding(t, b, tc.findingType, tc.severity, tc.status, tc.title)

			findings := parityListFindings(t, h, map[string]any{})

			require.Len(t, findings, 1)

			f := findings[0].(map[string]any)
			assert.Equal(t, findingARN, f["findingArn"])
			assert.Equal(t, "000000000000", f["awsAccountId"])
			assert.Equal(t, tc.findingType, f["type"])
			assert.Equal(t, tc.status, f["status"])

			sev, ok := f["severity"].(map[string]any)
			require.True(t, ok, "severity should be a map")
			assert.Equal(t, tc.severity, sev["label"])
			assert.NotEmpty(t, f["title"])
			assert.NotEmpty(t, f["description"])
			assert.NotEmpty(t, f["firstObservedAt"])
			assert.NotEmpty(t, f["lastObservedAt"])
			assert.NotEmpty(t, f["updatedAt"])
		})
	}
}

// --- multiple findings ---

func TestParityFindings_Multiple(t *testing.T) {
	t.Parallel()

	h, b := newParityHandlerAndBackend(t)

	arn1 := paritySeedFinding(t, b, "PACKAGE_VULNERABILITY", "CRITICAL", "ACTIVE", "Critical finding")
	arn2 := paritySeedFinding(t, b, "NETWORK_REACHABILITY", "HIGH", "ACTIVE", "High finding")
	arn3 := paritySeedFinding(t, b, "CODE_VULNERABILITY", "MEDIUM", "SUPPRESSED", "Medium finding")

	findings := parityListFindings(t, h, map[string]any{})
	require.Len(t, findings, 3)

	arns := make([]string, 0, 3)
	for _, f := range findings {
		m := f.(map[string]any)
		arns = append(arns, m["findingArn"].(string))
	}

	assert.Contains(t, arns, arn1)
	assert.Contains(t, arns, arn2)
	assert.Contains(t, arns, arn3)
}

// --- count ---

func TestParityFindings_Count(t *testing.T) {
	t.Parallel()

	_, b := newParityHandlerAndBackend(t)

	assert.Equal(t, 0, inspector2.FindingCount(b))

	paritySeedFinding(t, b, "PACKAGE_VULNERABILITY", "HIGH", "ACTIVE", "f1")
	assert.Equal(t, 1, inspector2.FindingCount(b))

	paritySeedFinding(t, b, "NETWORK_REACHABILITY", "CRITICAL", "ACTIVE", "f2")
	assert.Equal(t, 2, inspector2.FindingCount(b))

	paritySeedFinding(t, b, "CODE_VULNERABILITY", "LOW", "CLOSED", "f3")
	assert.Equal(t, 3, inspector2.FindingCount(b))
}

// --- pagination ---

func TestParityFindings_Pagination(t *testing.T) {
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

func TestParityFindings_NoNextTokenOnLastPage(t *testing.T) {
	t.Parallel()

	h, b := newParityHandlerAndBackend(t)

	for range 3 {
		paritySeedFinding(t, b, "PACKAGE_VULNERABILITY", "HIGH", "ACTIVE", "last-page-finding")
	}

	_, nextToken := parityListFindingsWithToken(t, h, map[string]any{"maxResults": 10})
	assert.Empty(t, nextToken)
}

// --- filter by severity ---

func TestParityFindings_FilterBySeverity(t *testing.T) {
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

func TestParityFindings_FilterByStatus(t *testing.T) {
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

func TestParityFindings_CombinedFilter(t *testing.T) {
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

func TestParityFindings_FilterNoMatch(t *testing.T) {
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

func TestParityFindings_StableSortOrder(t *testing.T) {
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

// --- finding fields ---

func TestParityFindings_Fields(t *testing.T) {
	t.Parallel()

	h, b := newParityHandlerAndBackend(t)
	findingARN := inspector2.SeedFinding(
		b,
		"PACKAGE_VULNERABILITY",
		"HIGH",
		"ACTIVE",
		"Field test finding",
		"Testing all fields are present",
		[]inspector2.FindingResource{
			{Type: "AWS_EC2_INSTANCE", ID: "i-abc12345"},
			{Type: "AWS_ECR_CONTAINER_IMAGE", ID: "sha256:abc"},
		},
	)

	findings := parityListFindings(t, h, map[string]any{})
	require.Len(t, findings, 1)

	f := findings[0].(map[string]any)
	assert.Equal(t, findingARN, f["findingArn"])
	assert.Equal(t, "000000000000", f["awsAccountId"])
	assert.Equal(t, "PACKAGE_VULNERABILITY", f["type"])
	assert.Equal(t, "ACTIVE", f["status"])
	assert.Equal(t, "Field test finding", f["title"])
	assert.Equal(t, "Testing all fields are present", f["description"])

	sev, ok := f["severity"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "HIGH", sev["label"])
	assert.Greater(t, sev["score"].(float64), 0.0)

	resources, ok := f["resources"].([]any)
	require.True(t, ok)
	require.Len(t, resources, 2)
	r0 := resources[0].(map[string]any)
	assert.Equal(t, "AWS_EC2_INSTANCE", r0["type"])
	assert.Equal(t, "i-abc12345", r0["id"])

	assert.Contains(t, f, "firstObservedAt")
	assert.Contains(t, f, "lastObservedAt")
	assert.Contains(t, f, "updatedAt")
}

// --- severity scores ---

func TestParityFindings_SeverityScores(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		severity  string
		wantScore float64
	}{
		{name: "critical", severity: "CRITICAL", wantScore: 9.0},
		{name: "high", severity: "HIGH", wantScore: 7.0},
		{name: "medium", severity: "MEDIUM", wantScore: 5.0},
		{name: "low", severity: "LOW", wantScore: 3.0},
		{name: "informational", severity: "INFORMATIONAL", wantScore: 0.0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, b := newParityHandlerAndBackend(t)
			paritySeedFinding(t, b, "PACKAGE_VULNERABILITY", tc.severity, "ACTIVE", "score-test")

			findings := parityListFindings(t, h, map[string]any{})
			require.Len(t, findings, 1)
			f := findings[0].(map[string]any)
			sev := f["severity"].(map[string]any)

			score, _ := sev["score"].(float64)
			assert.InDelta(t, tc.wantScore, score, 1e-9)
		})
	}
}

// --- ARN format ---

func TestParityFindings_ARNFormat(t *testing.T) {
	t.Parallel()

	h, b := newParityHandlerAndBackend(t)
	arn := paritySeedFinding(t, b, "PACKAGE_VULNERABILITY", "HIGH", "ACTIVE", "arn-test")

	assert.Contains(t, arn, "arn:aws:inspector2:us-east-1:000000000000:finding/")

	findings := parityListFindings(t, h, map[string]any{})
	require.Len(t, findings, 1)
	f := findings[0].(map[string]any)
	assert.Equal(t, arn, f["findingArn"])
}

// --- snapshot and restore preserve findings ---

func TestParityFindings_SnapshotRestore(t *testing.T) {
	t.Parallel()

	b := newParityBackend()
	arn1 := paritySeedFinding(t, b, "PACKAGE_VULNERABILITY", "HIGH", "ACTIVE", "snap-f1")
	arn2 := paritySeedFinding(t, b, "NETWORK_REACHABILITY", "CRITICAL", "ACTIVE", "snap-f2")

	snap := b.Snapshot(t.Context())

	b2 := newParityBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	h2 := inspector2.NewHandler(b2)
	findings := parityListFindings(t, h2, map[string]any{})

	require.Len(t, findings, 2)

	arns := make([]string, 0, 2)
	for _, f := range findings {
		m := f.(map[string]any)
		arns = append(arns, m["findingArn"].(string))
	}
	assert.Contains(t, arns, arn1)
	assert.Contains(t, arns, arn2)
}

// --- reset clears findings ---

func TestParityFindings_Reset(t *testing.T) {
	t.Parallel()

	b := newParityBackend()
	h := inspector2.NewHandler(b)

	paritySeedFinding(t, b, "PACKAGE_VULNERABILITY", "HIGH", "ACTIVE", "reset-f1")
	assert.Equal(t, 1, inspector2.FindingCount(b))

	b.Reset()

	assert.Equal(t, 0, inspector2.FindingCount(b))
	findings := parityListFindings(t, h, map[string]any{})
	assert.Empty(t, findings)
}

// --- pagination with filters ---

func TestParityFindings_PaginationWithFilter(t *testing.T) {
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

// --- invalid JSON body ---

func TestParityFindings_InvalidBody(t *testing.T) {
	t.Parallel()

	h, _ := newParityHandlerAndBackend(t)
	rec := parityDoRaw(t, h, http.MethodPost, "/findings/list", []byte("not-json"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- default max results applied when not specified ---

func TestParityFindings_DefaultMaxResults(t *testing.T) {
	t.Parallel()

	h, b := newParityHandlerAndBackend(t)

	for range 5 {
		paritySeedFinding(t, b, "PACKAGE_VULNERABILITY", "HIGH", "ACTIVE", "default-max-f")
	}

	findings, nextToken := parityListFindingsWithToken(t, h, map[string]any{})
	assert.Len(t, findings, 5)
	assert.Empty(t, nextToken)
}

// --- response structure ---

func TestParityFindings_ResponseStructure(t *testing.T) {
	t.Parallel()

	h, b := newParityHandlerAndBackend(t)
	paritySeedFinding(t, b, "PACKAGE_VULNERABILITY", "HIGH", "ACTIVE", "resp-structure-f")

	rec := parityDo(t, h, http.MethodPost, "/findings/list", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.Contains(t, resp, "findings", "response must have findings key")
}

// --- empty token on fresh start ---

func TestParityFindings_EmptyTokenMeansStart(t *testing.T) {
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

// --- finding types ---

func TestParityFindings_FindingTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		findingType string
	}{
		{name: "package_vulnerability", findingType: "PACKAGE_VULNERABILITY"},
		{name: "network_reachability", findingType: "NETWORK_REACHABILITY"},
		{name: "code_vulnerability", findingType: "CODE_VULNERABILITY"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, b := newParityHandlerAndBackend(t)
			paritySeedFinding(t, b, tc.findingType, "HIGH", "ACTIVE", "type-test")

			findings := parityListFindings(t, h, map[string]any{})
			require.Len(t, findings, 1)

			f := findings[0].(map[string]any)
			assert.Equal(t, tc.findingType, f["type"])
		})
	}
}
