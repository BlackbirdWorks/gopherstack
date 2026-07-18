package inspector2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

func TestListFindingsBasic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *inspector2.Handler)
		name string
	}{
		{
			name: "empty_list",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				rec := auditDo(t, h, http.MethodPost, "/findings/list", map[string]any{})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				findings, ok := resp["findings"]
				require.True(t, ok)
				require.NotNil(t, findings)
			},
		},
		{
			name: "with_max_results",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				rec := auditDo(t, h, http.MethodPost, "/findings/list", map[string]any{
					"maxResults": 10,
				})
				assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newAuditHandler(t))
		})
	}
}

// --- empty state tests ---

func TestListFindingsEmpty(t *testing.T) {
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

func TestListFindingsSingleRoundTrip(t *testing.T) {
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

func TestListFindingsMultiple(t *testing.T) {
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

func TestListFindingsCount(t *testing.T) {
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

// --- finding fields ---

func TestListFindingsFields(t *testing.T) {
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

func TestListFindingsSeverityScores(t *testing.T) {
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

func TestListFindingsARNFormat(t *testing.T) {
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

func TestListFindingsSnapshotRestore(t *testing.T) {
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

func TestListFindingsReset(t *testing.T) {
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

// --- response structure ---

func TestListFindingsResponseStructure(t *testing.T) {
	t.Parallel()

	h, b := newParityHandlerAndBackend(t)
	paritySeedFinding(t, b, "PACKAGE_VULNERABILITY", "HIGH", "ACTIVE", "resp-structure-f")

	rec := parityDo(t, h, http.MethodPost, "/findings/list", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.Contains(t, resp, "findings", "response must have findings key")
}

// --- invalid JSON body ---

func TestListFindingsInvalidBody(t *testing.T) {
	t.Parallel()

	h, _ := newParityHandlerAndBackend(t)
	rec := parityDoRaw(t, h, http.MethodPost, "/findings/list", []byte("not-json"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- finding types ---

func TestListFindingsFindingTypes(t *testing.T) {
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
