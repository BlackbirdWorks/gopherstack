package guardduty_test

// Audit batch-1: AWS-accuracy coverage for GuardDuty (go-15xt).
//
// Covers:
//   - Detector lifecycle: CreateDetector → GetDetector → UpdateDetector → DeleteDetector → ListDetectors
//   - One-detector-per-account enforcement
//   - Filter CRUD: CreateFilter, GetFilter, UpdateFilter, DeleteFilter, ListFilters
//   - Finding operations: CreateSampleFindings, ListFindings, GetFindings, ArchiveFindings,
//     UnarchiveFindings, UpdateFindingsFeedback, GetFindingsStatistics
//   - IPSet CRUD: CreateIPSet, GetIPSet, UpdateIPSet, DeleteIPSet, ListIPSets
//   - ThreatIntelSet CRUD: CreateThreatIntelSet, GetThreatIntelSet, UpdateThreatIntelSet,
//     DeleteThreatIntelSet, ListThreatIntelSets
//   - Tags: TagResource, ListTagsForResource, UntagResource
//   - Error paths: duplicate detector, not-found detector/filter/ipset/threatintelset

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/guardduty"
)

// --- helpers ---

func newAuditBackend() *guardduty.InMemoryBackend {
	return guardduty.NewInMemoryBackend("123456789012", "us-east-1")
}

func newAuditHandler(t *testing.T) *guardduty.Handler {
	t.Helper()

	return guardduty.NewHandler(newAuditBackend())
}

func auditDo(t *testing.T, h *guardduty.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var b []byte

	if body != nil {
		var err error
		b, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)
	require.NoError(t, h.Handler()(c))

	return rec
}

func auditCreateDetector(t *testing.T, h *guardduty.Handler) string {
	t.Helper()

	rec := auditDo(t, h, http.MethodPost, "/detector", map[string]any{
		"enable": true,
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	id, ok := resp["detectorId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, id)

	return id
}

// --- detector tests ---

func TestAudit1_Detector_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *guardduty.Handler)
		name string
	}{
		{
			name: "create_enabled",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()
				id := auditCreateDetector(t, h)
				require.NotEmpty(t, id)
			},
		},
		{
			name: "create_disabled",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()
				rec := auditDo(t, h, http.MethodPost, "/detector", map[string]any{
					"enable": false,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				require.NotEmpty(t, resp["detectorId"])
			},
		},
		{
			name: "get_detector",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()
				id := auditCreateDetector(t, h)

				rec := auditDo(t, h, http.MethodGet, "/detector/"+id, nil)
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "ENABLED", resp["status"])
				assert.NotEmpty(t, resp["serviceRole"])
				assert.Equal(t, "SIX_HOURS", resp["findingPublishingFrequency"])
			},
		},
		{
			name: "update_detector_frequency",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()
				id := auditCreateDetector(t, h)

				rec := auditDo(t, h, http.MethodPost, "/detector/"+id, map[string]any{
					"findingPublishingFrequency": "FIFTEEN_MINUTES",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				rec = auditDo(t, h, http.MethodGet, "/detector/"+id, nil)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "FIFTEEN_MINUTES", resp["findingPublishingFrequency"])
			},
		},
		{
			name: "update_detector_disable",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()
				id := auditCreateDetector(t, h)
				enable := false
				rec := auditDo(t, h, http.MethodPost, "/detector/"+id, map[string]any{
					"enable": enable,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				rec = auditDo(t, h, http.MethodGet, "/detector/"+id, nil)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "DISABLED", resp["status"])
			},
		},
		{
			name: "delete_detector",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()
				id := auditCreateDetector(t, h)

				rec := auditDo(t, h, http.MethodDelete, "/detector/"+id, nil)
				require.Equal(t, http.StatusOK, rec.Code)

				rec = auditDo(t, h, http.MethodGet, "/detector/"+id, nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "list_detectors",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()
				id := auditCreateDetector(t, h)

				rec := auditDo(t, h, http.MethodGet, "/detector", nil)
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				ids, ok := resp["detectorIds"].([]any)
				require.True(t, ok)
				require.Len(t, ids, 1)
				assert.Equal(t, id, ids[0])
			},
		},
		{
			name: "duplicate_detector_rejected",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()
				auditCreateDetector(t, h)

				rec := auditDo(t, h, http.MethodPost, "/detector", map[string]any{
					"enable": true,
				})
				assert.Equal(t, http.StatusConflict, rec.Code)
			},
		},
		{
			name: "get_nonexistent_detector",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()
				rec := auditDo(t, h, http.MethodGet, "/detector/nonexistent", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "create_with_tags",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()
				rec := auditDo(t, h, http.MethodPost, "/detector", map[string]any{
					"enable": true,
					"tags":   map[string]string{"env": "test", "team": "security"},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				id := resp["detectorId"].(string)

				rec = auditDo(t, h, http.MethodGet, "/detector/"+id, nil)
				var det map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &det))
				tags, ok := det["tags"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "test", tags["env"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newAuditHandler(t)
			tt.fn(t, h)
		})
	}
}

// --- filter tests ---

func TestAudit1_Filter_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *guardduty.Handler, detectorID string)
		name string
	}{
		{
			name: "create_and_get",
			fn: func(t *testing.T, h *guardduty.Handler, detectorID string) {
				t.Helper()
				rec := auditDo(t, h, http.MethodPost, "/detector/"+detectorID+"/filter", map[string]any{
					"name":        "my-filter",
					"action":      "ARCHIVE",
					"rank":        1,
					"description": "test filter",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var cr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
				assert.Equal(t, "my-filter", cr["name"])

				rec = auditDo(t, h, http.MethodGet, "/detector/"+detectorID+"/filter/my-filter", nil)
				require.Equal(t, http.StatusOK, rec.Code)

				var gr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &gr))
				assert.Equal(t, "ARCHIVE", gr["action"])
				assert.InDelta(t, 1.0, gr["rank"], 0.01)
				assert.Equal(t, "test filter", gr["description"])
			},
		},
		{
			name: "update_filter",
			fn: func(t *testing.T, h *guardduty.Handler, detectorID string) {
				t.Helper()
				auditDo(t, h, http.MethodPost, "/detector/"+detectorID+"/filter", map[string]any{
					"name":   "upd-filter",
					"action": "NOOP",
					"rank":   1,
				})

				rec := auditDo(t, h, http.MethodPost, "/detector/"+detectorID+"/filter/upd-filter", map[string]any{
					"action": "ARCHIVE",
					"rank":   2,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				rec = auditDo(t, h, http.MethodGet, "/detector/"+detectorID+"/filter/upd-filter", nil)
				var gr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &gr))
				assert.Equal(t, "ARCHIVE", gr["action"])
				assert.InDelta(t, 2.0, gr["rank"], 0.01)
			},
		},
		{
			name: "delete_filter",
			fn: func(t *testing.T, h *guardduty.Handler, detectorID string) {
				t.Helper()
				auditDo(t, h, http.MethodPost, "/detector/"+detectorID+"/filter", map[string]any{
					"name": "del-filter", "action": "NOOP", "rank": 1,
				})

				rec := auditDo(t, h, http.MethodDelete, "/detector/"+detectorID+"/filter/del-filter", nil)
				require.Equal(t, http.StatusOK, rec.Code)

				rec = auditDo(t, h, http.MethodGet, "/detector/"+detectorID+"/filter/del-filter", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "list_filters",
			fn: func(t *testing.T, h *guardduty.Handler, detectorID string) {
				t.Helper()
				for _, name := range []string{"filter-a", "filter-b", "filter-c"} {
					auditDo(t, h, http.MethodPost, "/detector/"+detectorID+"/filter", map[string]any{
						"name": name, "action": "NOOP", "rank": 1,
					})
				}

				rec := auditDo(t, h, http.MethodGet, "/detector/"+detectorID+"/filter", nil)
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				names, ok := resp["filterNames"].([]any)
				require.True(t, ok)
				assert.Len(t, names, 3)
			},
		},
		{
			name: "filter_not_found",
			fn: func(t *testing.T, h *guardduty.Handler, detectorID string) {
				t.Helper()
				rec := auditDo(t, h, http.MethodGet, "/detector/"+detectorID+"/filter/nonexistent", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newAuditHandler(t)
			detectorID := auditCreateDetector(t, h)
			tt.fn(t, h, detectorID)
		})
	}
}

// --- finding tests ---

func TestAudit1_Findings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *guardduty.Handler, detectorID string)
		name string
	}{
		{
			name: "create_sample_and_list",
			fn: func(t *testing.T, h *guardduty.Handler, detectorID string) {
				t.Helper()
				rec := auditDo(t, h, http.MethodPost, "/detector/"+detectorID+"/findings/create", map[string]any{
					"findingTypes": []string{"UnauthorizedAccess:IAMUser/ConsoleLoginSuccess.B"},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				rec = auditDo(t, h, http.MethodPost, "/detector/"+detectorID+"/findings", nil)
				require.Equal(t, http.StatusOK, rec.Code)

				var lr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lr))
				ids, ok := lr["findingIds"].([]any)
				require.True(t, ok)
				assert.Len(t, ids, 1)
			},
		},
		{
			name: "get_findings_by_id",
			fn: func(t *testing.T, h *guardduty.Handler, detectorID string) {
				t.Helper()
				auditDo(t, h, http.MethodPost, "/detector/"+detectorID+"/findings/create", map[string]any{
					"findingTypes": []string{"UnauthorizedAccess:IAMUser/ConsoleLoginSuccess.B"},
				})

				// List to get IDs
				rec := auditDo(t, h, http.MethodPost, "/detector/"+detectorID+"/findings", nil)
				var lr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lr))
				ids := lr["findingIds"].([]any)

				// Get by ID
				rec = auditDo(t, h, http.MethodPost, "/detector/"+detectorID+"/findings/get", map[string]any{
					"findingIds": []string{ids[0].(string)},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var gr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &gr))
				findings, ok := gr["findings"].([]any)
				require.True(t, ok)
				require.Len(t, findings, 1)
			},
		},
		{
			name: "archive_and_unarchive",
			fn: func(t *testing.T, h *guardduty.Handler, detectorID string) {
				t.Helper()
				auditDo(t, h, http.MethodPost, "/detector/"+detectorID+"/findings/create", map[string]any{
					"findingTypes": []string{"UnauthorizedAccess:IAMUser/ConsoleLoginSuccess.B"},
				})

				rec := auditDo(t, h, http.MethodPost, "/detector/"+detectorID+"/findings", nil)
				var lr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lr))
				ids := lr["findingIds"].([]any)
				fid := ids[0].(string)

				// Archive
				rec = auditDo(t, h, http.MethodPost, "/detector/"+detectorID+"/findings/archive", map[string]any{
					"findingIds": []string{fid},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				// Verify archived
				rec = auditDo(t, h, http.MethodPost, "/detector/"+detectorID+"/findings/get", map[string]any{
					"findingIds": []string{fid},
				})
				var gr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &gr))
				findings := gr["findings"].([]any)
				f := findings[0].(map[string]any)
				svc := f["service"].(map[string]any)
				assert.Equal(t, true, svc["archived"])

				// Unarchive
				rec = auditDo(t, h, http.MethodPost, "/detector/"+detectorID+"/findings/unarchive", map[string]any{
					"findingIds": []string{fid},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				rec = auditDo(t, h, http.MethodPost, "/detector/"+detectorID+"/findings/get", map[string]any{
					"findingIds": []string{fid},
				})
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &gr))
				findings = gr["findings"].([]any)
				f = findings[0].(map[string]any)
				svc = f["service"].(map[string]any)
				assert.Equal(t, false, svc["archived"])
			},
		},
		{
			name: "get_findings_statistics",
			fn: func(t *testing.T, h *guardduty.Handler, detectorID string) {
				t.Helper()
				auditDo(t, h, http.MethodPost, "/detector/"+detectorID+"/findings/create", map[string]any{
					"findingTypes": []string{
						"UnauthorizedAccess:IAMUser/ConsoleLoginSuccess.B",
						"Backdoor:EC2/DenialOfService.Tcp",
					},
				})

				rec := auditDo(t, h, http.MethodPost, "/detector/"+detectorID+"/findings/statistics", nil)
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotNil(t, resp["findingStatistics"])
			},
		},
		{
			name: "update_findings_feedback",
			fn: func(t *testing.T, h *guardduty.Handler, detectorID string) {
				t.Helper()
				auditDo(t, h, http.MethodPost, "/detector/"+detectorID+"/findings/create", map[string]any{
					"findingTypes": []string{"UnauthorizedAccess:IAMUser/ConsoleLoginSuccess.B"},
				})

				rec := auditDo(t, h, http.MethodPost, "/detector/"+detectorID+"/findings", nil)
				var lr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lr))
				ids := lr["findingIds"].([]any)

				rec = auditDo(t, h, http.MethodPost, "/detector/"+detectorID+"/findings/feedback", map[string]any{
					"findingIds": []string{ids[0].(string)},
					"feedback":   "USEFUL",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newAuditHandler(t)
			detectorID := auditCreateDetector(t, h)
			tt.fn(t, h, detectorID)
		})
	}
}

// --- IPSet tests ---

func TestAudit1_IPSet_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *guardduty.Handler, detectorID string)
		name string
	}{
		{
			name: "create_and_get",
			fn: func(t *testing.T, h *guardduty.Handler, detectorID string) {
				t.Helper()
				rec := auditDo(t, h, http.MethodPost, "/detector/"+detectorID+"/ipset", map[string]any{
					"name":     "my-ipset",
					"format":   "TXT",
					"location": "s3://bucket/ipset.txt",
					"activate": true,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var cr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
				ipSetID := cr["ipSetId"].(string)
				require.NotEmpty(t, ipSetID)

				rec = auditDo(t, h, http.MethodGet, "/detector/"+detectorID+"/ipset/"+ipSetID, nil)
				require.Equal(t, http.StatusOK, rec.Code)

				var gr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &gr))
				assert.Equal(t, "my-ipset", gr["name"])
				assert.Equal(t, "TXT", gr["format"])
				assert.Equal(t, "ACTIVE", gr["status"])
			},
		},
		{
			name: "update_ipset",
			fn: func(t *testing.T, h *guardduty.Handler, detectorID string) {
				t.Helper()
				rec := auditDo(t, h, http.MethodPost, "/detector/"+detectorID+"/ipset", map[string]any{
					"name": "upd-ipset", "format": "TXT", "location": "s3://b/f.txt",
				})
				var cr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
				ipSetID := cr["ipSetId"].(string)

				rec = auditDo(t, h, http.MethodPost, "/detector/"+detectorID+"/ipset/"+ipSetID, map[string]any{
					"name":     "updated-ipset",
					"activate": true,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				rec = auditDo(t, h, http.MethodGet, "/detector/"+detectorID+"/ipset/"+ipSetID, nil)
				var gr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &gr))
				assert.Equal(t, "updated-ipset", gr["name"])
				assert.Equal(t, "ACTIVE", gr["status"])
			},
		},
		{
			name: "delete_ipset",
			fn: func(t *testing.T, h *guardduty.Handler, detectorID string) {
				t.Helper()
				rec := auditDo(t, h, http.MethodPost, "/detector/"+detectorID+"/ipset", map[string]any{
					"name": "del-ipset", "format": "TXT", "location": "s3://b/f.txt",
				})
				var cr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
				ipSetID := cr["ipSetId"].(string)

				rec = auditDo(t, h, http.MethodDelete, "/detector/"+detectorID+"/ipset/"+ipSetID, nil)
				require.Equal(t, http.StatusOK, rec.Code)

				rec = auditDo(t, h, http.MethodGet, "/detector/"+detectorID+"/ipset/"+ipSetID, nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "list_ipsets",
			fn: func(t *testing.T, h *guardduty.Handler, detectorID string) {
				t.Helper()
				for i := range 3 {
					auditDo(t, h, http.MethodPost, "/detector/"+detectorID+"/ipset", map[string]any{
						"name": "ipset-" + string(rune('a'+i)), "format": "TXT", "location": "s3://b/f.txt",
					})
				}

				rec := auditDo(t, h, http.MethodGet, "/detector/"+detectorID+"/ipset", nil)
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				ids, ok := resp["ipSetIds"].([]any)
				require.True(t, ok)
				assert.Len(t, ids, 3)
			},
		},
		{
			name: "create_inactive_by_default",
			fn: func(t *testing.T, h *guardduty.Handler, detectorID string) {
				t.Helper()
				rec := auditDo(t, h, http.MethodPost, "/detector/"+detectorID+"/ipset", map[string]any{
					"name": "inactive-ipset", "format": "TXT", "location": "s3://b/f.txt",
				})
				var cr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
				ipSetID := cr["ipSetId"].(string)

				rec = auditDo(t, h, http.MethodGet, "/detector/"+detectorID+"/ipset/"+ipSetID, nil)
				var gr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &gr))
				assert.Equal(t, "INACTIVE", gr["status"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newAuditHandler(t)
			detectorID := auditCreateDetector(t, h)
			tt.fn(t, h, detectorID)
		})
	}
}

// --- ThreatIntelSet tests ---

func TestAudit1_ThreatIntelSet_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *guardduty.Handler, detectorID string)
		name string
	}{
		{
			name: "create_and_get",
			fn: func(t *testing.T, h *guardduty.Handler, detectorID string) {
				t.Helper()
				rec := auditDo(t, h, http.MethodPost, "/detector/"+detectorID+"/threatintelset", map[string]any{
					"name":     "my-threatset",
					"format":   "TXT",
					"location": "s3://bucket/threats.txt",
					"activate": true,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var cr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
				setID := cr["threatIntelSetId"].(string)
				require.NotEmpty(t, setID)

				rec = auditDo(t, h, http.MethodGet, "/detector/"+detectorID+"/threatintelset/"+setID, nil)
				require.Equal(t, http.StatusOK, rec.Code)

				var gr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &gr))
				assert.Equal(t, "my-threatset", gr["name"])
				assert.Equal(t, "ACTIVE", gr["status"])
			},
		},
		{
			name: "update_and_deactivate",
			fn: func(t *testing.T, h *guardduty.Handler, detectorID string) {
				t.Helper()
				rec := auditDo(t, h, http.MethodPost, "/detector/"+detectorID+"/threatintelset", map[string]any{
					"name": "upd-threat", "format": "TXT", "location": "s3://b/t.txt", "activate": true,
				})
				var cr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
				setID := cr["threatIntelSetId"].(string)

				activate := false
				rec = auditDo(t, h, http.MethodPost, "/detector/"+detectorID+"/threatintelset/"+setID, map[string]any{
					"activate": activate,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				rec = auditDo(t, h, http.MethodGet, "/detector/"+detectorID+"/threatintelset/"+setID, nil)
				var gr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &gr))
				assert.Equal(t, "INACTIVE", gr["status"])
			},
		},
		{
			name: "delete_threatintelset",
			fn: func(t *testing.T, h *guardduty.Handler, detectorID string) {
				t.Helper()
				rec := auditDo(t, h, http.MethodPost, "/detector/"+detectorID+"/threatintelset", map[string]any{
					"name": "del-threat", "format": "TXT", "location": "s3://b/t.txt",
				})
				var cr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
				setID := cr["threatIntelSetId"].(string)

				rec = auditDo(t, h, http.MethodDelete, "/detector/"+detectorID+"/threatintelset/"+setID, nil)
				require.Equal(t, http.StatusOK, rec.Code)

				rec = auditDo(t, h, http.MethodGet, "/detector/"+detectorID+"/threatintelset/"+setID, nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "list_threatintelsets",
			fn: func(t *testing.T, h *guardduty.Handler, detectorID string) {
				t.Helper()
				for i := range 2 {
					auditDo(t, h, http.MethodPost, "/detector/"+detectorID+"/threatintelset", map[string]any{
						"name": "threat-" + string(rune('a'+i)), "format": "TXT", "location": "s3://b/t.txt",
					})
				}

				rec := auditDo(t, h, http.MethodGet, "/detector/"+detectorID+"/threatintelset", nil)
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				ids, ok := resp["threatIntelSetIds"].([]any)
				require.True(t, ok)
				assert.Len(t, ids, 2)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newAuditHandler(t)
			detectorID := auditCreateDetector(t, h)
			tt.fn(t, h, detectorID)
		})
	}
}

// --- tag tests ---

func TestAudit1_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *guardduty.Handler)
		name string
	}{
		{
			name: "tag_and_list_resource",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()
				arn := "arn:aws:guardduty:us-east-1:123456789012:detector/abc123"

				rec := auditDo(t, h, http.MethodPost, "/tags/"+arn, map[string]any{
					"tags": map[string]string{"env": "prod", "owner": "alice"},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				rec = auditDo(t, h, http.MethodGet, "/tags/"+arn, nil)
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				tags, ok := resp["tags"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "prod", tags["env"])
				assert.Equal(t, "alice", tags["owner"])
			},
		},
		{
			name: "untag_resource",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()
				arn := "arn:aws:guardduty:us-east-1:123456789012:detector/def456"

				auditDo(t, h, http.MethodPost, "/tags/"+arn, map[string]any{
					"tags": map[string]string{"k1": "v1", "k2": "v2"},
				})

				rec := auditDo(t, h, http.MethodDelete, "/tags/"+arn+"?tagKeys=k1", nil)
				require.Equal(t, http.StatusOK, rec.Code)

				rec = auditDo(t, h, http.MethodGet, "/tags/"+arn, nil)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				tags := resp["tags"].(map[string]any)
				assert.NotContains(t, tags, "k1")
				assert.Equal(t, "v2", tags["k2"])
			},
		},
		{
			name: "list_tags_empty",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()
				arn := "arn:aws:guardduty:us-east-1:123456789012:detector/ghi789"

				rec := auditDo(t, h, http.MethodGet, "/tags/"+arn, nil)
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				tags, ok := resp["tags"].(map[string]any)
				require.True(t, ok)
				assert.Empty(t, tags)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newAuditHandler(t)
			tt.fn(t, h)
		})
	}
}
