package inspector2_test

// Stateful CIS scan tests: prove that scan result/report/aggregation operations
// reflect the configuration that produced them (real stored state) rather than
// canned data, and that unknown scan ARNs degrade benignly.

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

// createCisConfig creates a CIS scan configuration over the given target account
// IDs and returns the resulting configuration ARN.
func createCisConfig(t *testing.T, h *inspector2.Handler, name string, accountIDs []string) string {
	t.Helper()

	rec := auditDo(t, h, http.MethodPost, "/cis/scan-configuration/create", map[string]any{
		"scanName": name,
		"targets":  map[string]any{"accountIds": accountIDs},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	arn, ok := resp["scanConfigurationArn"].(string)
	require.True(t, ok)
	require.NotEmpty(t, arn)

	return arn
}

// firstScanArn returns the scan ARN of the single scan materialized for cfgARN.
func firstScanArn(t *testing.T, h *inspector2.Handler, cfgARN string) string {
	t.Helper()

	rec := auditDo(t, h, http.MethodPost, "/cis/scan/list", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	scans, _ := resp["scans"].([]any)

	for _, raw := range scans {
		s, _ := raw.(map[string]any)
		if s["scanConfigurationArn"] == cfgARN {
			arn, _ := s["scanArn"].(string)
			require.NotEmpty(t, arn)

			return arn
		}
	}

	t.Fatalf("no scan found for config %q", cfgARN)

	return ""
}

func TestCisScans_CreatedConfigMaterializesScan(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	// No configs yet => no scans.
	rec := auditDo(t, h, http.MethodPost, "/cis/scan/list", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var empty map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &empty))
	scans, _ := empty["scans"].([]any)
	assert.Empty(t, scans)

	// Creating a config materializes exactly one scan referencing it.
	cfgARN := createCisConfig(t, h, "scan-a", []string{"111111111111"})

	rec = auditDo(t, h, http.MethodPost, "/cis/scan/list", map[string]any{})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &empty))
	scans, _ = empty["scans"].([]any)
	require.Len(t, scans, 1)

	entry, _ := scans[0].(map[string]any)
	assert.Equal(t, cfgARN, entry["scanConfigurationArn"])
	assert.Equal(t, "scan-a", entry["scanName"])
	assert.Equal(t, "COMPLETED", entry["status"])
}

func TestCisScans_ResultDetailsReflectConfig(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)
	cfgARN := createCisConfig(t, h, "scan-detail", []string{"111111111111", "222222222222"})
	scanARN := firstScanArn(t, h, cfgARN)

	rec := auditDo(t, h, http.MethodPost, "/cis/scan-result/details/get", map[string]any{
		"scanArn": scanARN,
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	results, _ := resp["checkResults"].([]any)

	// Two target accounts x catalog checks => non-empty, and every result is
	// tagged with one of the configured accounts.
	require.NotEmpty(t, results)

	accounts := map[string]bool{}

	for _, raw := range results {
		r, _ := raw.(map[string]any)
		assert.Equal(t, scanARN, r["scanArn"])
		acct, _ := r["accountId"].(string)
		accounts[acct] = true
		assert.NotEmpty(t, r["checkId"])
		assert.Contains(t, []any{"PASSED", "FAILED"}, r["status"])
	}

	assert.True(t, accounts["111111111111"])
	assert.True(t, accounts["222222222222"])
}

func TestCisScans_ReportReflectsScan(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)
	cfgARN := createCisConfig(t, h, "scan-report", []string{"111111111111"})
	scanARN := firstScanArn(t, h, cfgARN)

	rec := auditDo(t, h, http.MethodPost, "/cis/scan/report/get", map[string]any{
		"scanArn": scanARN,
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "SUCCEEDED", resp["status"])
	assert.Equal(t, scanARN, resp["scanArn"])
	// totalChecks reported as a JSON number.
	total, ok := resp["totalChecks"].(float64)
	require.True(t, ok)
	assert.Positive(t, total)
}

func TestCisScans_AggregationsConsistent(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)
	cfgARN := createCisConfig(t, h, "scan-agg", []string{"111111111111", "222222222222"})
	scanARN := firstScanArn(t, h, cfgARN)

	// By checks: one aggregation per distinct check ID.
	rec := auditDo(t, h, http.MethodPost, "/cis/scan-result/check/list", map[string]any{
		"scanArn": scanARN,
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var byCheck map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &byCheck))
	checks, _ := byCheck["checkAggregations"].([]any)
	require.NotEmpty(t, checks)

	for _, raw := range checks {
		c, _ := raw.(map[string]any)
		assert.NotEmpty(t, c["checkId"])
		_, ok := c["statusCounts"].(map[string]any)
		assert.True(t, ok)
	}

	// By target resource: one aggregation per target (2 accounts => 2 targets).
	rec = auditDo(t, h, http.MethodPost, "/cis/scan-result/resource/list", map[string]any{
		"scanArn": scanARN,
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var byTarget map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &byTarget))
	targets, _ := byTarget["targetResourceAggregations"].([]any)
	assert.Len(t, targets, 2)
}

func TestCisScans_DeleteConfigRemovesScans(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)
	cfgARN := createCisConfig(t, h, "scan-del", []string{"111111111111"})

	rec := auditDo(t, h, http.MethodPost, "/cis/scan-configuration/delete", map[string]any{
		"scanConfigurationArn": cfgARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = auditDo(t, h, http.MethodPost, "/cis/scan/list", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	scans, _ := resp["scans"].([]any)
	assert.Empty(t, scans)
}

func TestCisScans_UnknownScanArnDegradesBenignly(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)
	unknown := "arn:aws:inspector2:us-east-1:123456789012:cis-scan/does-not-exist"

	cases := []struct {
		path string
		key  string
	}{
		{"/cis/scan/report/get", "status"},
		{"/cis/scan-result/details/get", "checkResults"},
		{"/cis/scan-result/check/list", "checkAggregations"},
		{"/cis/scan-result/resource/list", "targetResourceAggregations"},
	}

	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			t.Parallel()

			rec := auditDo(t, h, http.MethodPost, tc.path, map[string]any{"scanArn": unknown})
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			_, ok := resp[tc.key]
			assert.True(t, ok)
		})
	}
}

// TestCisScans_SnapshotRestoreRoundTrip proves the appendix state (including CIS
// scans) survives a Snapshot/Restore cycle.
func TestCisScans_SnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	src := inspector2.NewInMemoryBackend("123456789012", "us-east-1")
	cfg, err := src.CreateCisScanConfiguration("rt", nil, map[string]any{
		"accountIds": []any{"333333333333"},
	}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, cfg.Arn)

	scansBefore, err := src.ListCisScans()
	require.NoError(t, err)
	require.Len(t, scansBefore, 1)

	data := src.Snapshot(t.Context())

	dst := inspector2.NewInMemoryBackend("000000000000", "us-east-2")
	require.NoError(t, dst.Restore(t.Context(), data))

	scansAfter, err := dst.ListCisScans()
	require.NoError(t, err)
	require.Len(t, scansAfter, 1)
	assert.Equal(t, scansBefore[0]["scanArn"], scansAfter[0]["scanArn"])

	cfgsAfter, err := dst.ListCisScanConfigurations()
	require.NoError(t, err)
	require.Len(t, cfgsAfter, 1)
	assert.Equal(t, cfg.Arn, cfgsAfter[0].Arn)
}

// TestCisScans_ResetClearsState proves Reset clears appendix CIS state.
func TestCisScans_ResetClearsState(t *testing.T) {
	t.Parallel()

	b := inspector2.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.CreateCisScanConfiguration("r", nil, nil, nil)
	require.NoError(t, err)

	scans, err := b.ListCisScans()
	require.NoError(t, err)
	require.Len(t, scans, 1)

	b.Reset()

	scans, err = b.ListCisScans()
	require.NoError(t, err)
	assert.Empty(t, scans)

	cfgs, err := b.ListCisScanConfigurations()
	require.NoError(t, err)
	assert.Empty(t, cfgs)
}

// --- HTTP-level CIS scan configuration / session tests ---

func TestCisScanConfigurationLifecycle(t *testing.T) {
	t.Parallel()

	type step struct {
		body   any
		check  func(t *testing.T, code int, body []byte)
		name   string
		method string
		path   string
	}

	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "Create/List/Update/Delete cycle",
			steps: []step{
				{
					name:   "create",
					method: http.MethodPost,
					path:   "/cis/scan-configuration/create",
					body: map[string]any{
						"scanName": "my-cis-scan",
						"schedule": map[string]any{"daily": map[string]any{"startTime": "12:00"}},
						"targets":  map[string]any{"accountIds": []string{"123456789012"}},
						"tags":     map[string]string{"env": "prod"},
					},
					check: func(t *testing.T, code int, body []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						var resp map[string]any
						require.NoError(t, json.Unmarshal(body, &resp))
						assert.NotEmpty(t, resp["scanConfigurationArn"])
					},
				},
				{
					name:   "list shows config",
					method: http.MethodPost,
					path:   "/cis/scan-configuration/list",
					body:   map[string]any{},
					check: func(t *testing.T, code int, body []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						var resp map[string]any
						require.NoError(t, json.Unmarshal(body, &resp))
						cfgs, _ := resp["scanConfigurations"].([]any)
						assert.Len(t, cfgs, 1)
					},
				},
				{
					name:   "update config name",
					method: http.MethodPost,
					path:   "/cis/scan-configuration/update",
					body: func() any {
						// We need to know the ARN; just pass an empty one to test error path in a separate test.
						// This step will be tested with a real ARN in the sequential flow below.
						return map[string]any{
							"scanConfigurationArn": "placeholder-see-subtests",
							"scanName":             "updated-name",
						}
					}(),
					check: func(t *testing.T, code int, _ []byte) {
						t.Helper()
						// Placeholder ARN will return 404 — that's expected in this static table test.
						assert.Equal(t, http.StatusNotFound, code)
					},
				},
			},
		},
		{
			name: "Create/Delete cycle",
			steps: []step{
				{
					name:   "create",
					method: http.MethodPost,
					path:   "/cis/scan-configuration/create",
					body:   map[string]any{"scanName": "to-delete"},
					check: func(t *testing.T, code int, _ []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "delete unknown ARN returns 404",
					method: http.MethodPost,
					path:   "/cis/scan-configuration/delete",
					body: map[string]any{
						"scanConfigurationArn": "arn:aws:inspector2:us-east-1:123456789012:cis-scan-configuration/nonexistent",
					},
					check: func(t *testing.T, code int, _ []byte) {
						t.Helper()
						assert.Equal(t, http.StatusNotFound, code)
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newAuditHandler(t)

			for _, s := range tc.steps {
				rec := auditDo(t, h, s.method, s.path, s.body)
				s.check(t, rec.Code, rec.Body.Bytes())
			}
		})
	}
}

// TestCisScanConfigurationFullCycle exercises create/update/delete with a real ARN.
func TestCisScanConfigurationFullCycle(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	rec := auditDo(t, h, http.MethodPost, "/cis/scan-configuration/create", map[string]any{
		"scanName": "full-cycle-cis",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	cfgARN, _ := createResp["scanConfigurationArn"].(string)
	require.NotEmpty(t, cfgARN)

	rec = auditDo(t, h, http.MethodPost, "/cis/scan-configuration/update", map[string]any{
		"scanConfigurationArn": cfgARN,
		"scanName":             "renamed-cis",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = auditDo(t, h, http.MethodPost, "/cis/scan-configuration/delete", map[string]any{
		"scanConfigurationArn": cfgARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = auditDo(t, h, http.MethodPost, "/cis/scan-configuration/list", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	cfgs, _ := listResp["scanConfigurations"].([]any)
	assert.Empty(t, cfgs)
}

// TestCisScanConfiguration_ScanNameValidation covers
// CreateCisScanConfiguration/UpdateCisScanConfiguration's real scanName
// length constraint (AWS API Reference for CreateCisScanConfiguration /
// UpdateCisScanConfiguration: "Minimum length of 1. Maximum length of 128.",
// no charset pattern documented). A prior revision accepted any non-empty
// string on create and any string at all (including one exceeding 128
// chars) on update, so real AWS's ValidationException for an out-of-range
// scanName was never modeled.
func TestCisScanConfiguration_ScanNameValidation(t *testing.T) {
	t.Parallel()

	tooLong := make([]byte, 129)
	for i := range tooLong {
		tooLong[i] = 'a'
	}

	tests := []struct {
		name     string
		scanName string
		onUpdate bool
		wantCode int
	}{
		{name: "empty_name_rejected_on_create", scanName: "", wantCode: http.StatusBadRequest},
		{name: "too_long_name_rejected_on_create", scanName: string(tooLong), wantCode: http.StatusBadRequest},
		{name: "single_char_name_accepted_on_create", scanName: "a", wantCode: http.StatusOK},
		{name: "exactly_128_char_name_accepted_on_create", scanName: string(tooLong[:128]), wantCode: http.StatusOK},
		{
			name: "too_long_name_rejected_on_update", scanName: string(tooLong),
			onUpdate: true, wantCode: http.StatusBadRequest,
		},
		{
			name: "valid_name_accepted_on_update", scanName: "renamed-ok",
			onUpdate: true, wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAuditHandler(t)

			if !tt.onUpdate {
				rec := auditDo(t, h, http.MethodPost, "/cis/scan-configuration/create", map[string]any{
					"scanName": tt.scanName,
				})
				assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())

				return
			}

			cfgARN := createCisConfig(t, h, "update-target", nil)
			rec := auditDo(t, h, http.MethodPost, "/cis/scan-configuration/update", map[string]any{
				"scanConfigurationArn": cfgARN,
				"scanName":             tt.scanName,
			})
			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())
		})
	}
}

func TestCisSessionOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   any
		check  func(t *testing.T, code int, body []byte)
		name   string
		method string
		path   string
	}{
		{
			name:   "StartCisSession returns OK",
			method: http.MethodPut,
			path:   "/cissession/start",
			body:   map[string]any{"scanJobId": "job-001", "message": map[string]any{"sessionToken": "tok"}},
			check: func(t *testing.T, code int, _ []byte) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
			},
		},
		{
			name:   "StopCisSession unknown job returns 404",
			method: http.MethodPut,
			path:   "/cissession/stop",
			body:   map[string]any{"scanJobId": "nonexistent"},
			check: func(t *testing.T, code int, _ []byte) {
				t.Helper()
				assert.Equal(t, http.StatusNotFound, code)
			},
		},
		{
			name:   "SendCisSessionHealth returns OK",
			method: http.MethodPut,
			path:   "/cissession/health/send",
			body:   map[string]any{"scanJobId": "job-002"},
			check: func(t *testing.T, code int, _ []byte) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
			},
		},
		{
			name:   "SendCisSessionTelemetry returns OK",
			method: http.MethodPut,
			path:   "/cissession/telemetry/send",
			body:   map[string]any{"scanJobId": "job-002"},
			check: func(t *testing.T, code int, _ []byte) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
			},
		},
		{
			name:   "GetCisScanReport returns status",
			method: http.MethodPost,
			path:   "/cis/scan/report/get",
			body:   map[string]any{"scanArn": "arn:aws:inspector2:us-east-1:123456789012:cis-scan/test"},
			check: func(t *testing.T, code int, body []byte) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "SUCCEEDED", resp["status"])
			},
		},
		{
			name:   "GetCisScanResultDetails returns checkResults",
			method: http.MethodPost,
			path:   "/cis/scan-result/details/get",
			body:   map[string]any{"scanArn": "arn:aws:inspector2:us-east-1:123456789012:cis-scan/test"},
			check: func(t *testing.T, code int, body []byte) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				_, ok := resp["checkResults"]
				assert.True(t, ok)
			},
		},
		{
			name:   "ListCisScans returns empty list",
			method: http.MethodPost,
			path:   "/cis/scan/list",
			body:   map[string]any{},
			check: func(t *testing.T, code int, body []byte) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				scans, _ := resp["scans"].([]any)
				assert.Empty(t, scans)
			},
		},
		{
			name:   "ListCisScanResultsAggregatedByChecks returns empty",
			method: http.MethodPost,
			path:   "/cis/scan-result/check/list",
			body:   map[string]any{"scanArn": "arn:aws:inspector2:us-east-1:123456789012:cis-scan/test"},
			check: func(t *testing.T, code int, body []byte) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				_, ok := resp["checkAggregations"]
				assert.True(t, ok)
			},
		},
		{
			name:   "ListCisScanResultsAggregatedByTargetResource returns empty",
			method: http.MethodPost,
			path:   "/cis/scan-result/resource/list",
			body:   map[string]any{"scanArn": "arn:aws:inspector2:us-east-1:123456789012:cis-scan/test"},
			check: func(t *testing.T, code int, body []byte) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				_, ok := resp["targetResourceAggregations"]
				assert.True(t, ok)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newAuditHandler(t)
			rec := auditDo(t, h, tc.method, tc.path, tc.body)
			tc.check(t, rec.Code, rec.Body.Bytes())
		})
	}
}

// TestCisSessionStartStop exercises a start/stop cycle with a real session.
func TestCisSessionStartStop(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	rec := auditDo(t, h, http.MethodPut, "/cissession/start", map[string]any{
		"scanJobId": "job-ss-001",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = auditDo(t, h, http.MethodPut, "/cissession/stop", map[string]any{
		"scanJobId": "job-ss-001",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}
