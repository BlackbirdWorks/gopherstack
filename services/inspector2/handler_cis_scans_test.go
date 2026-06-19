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
	scans, _ := resp["cisScans"].([]any)

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

	h := axHandler(t)

	// No configs yet => no scans.
	rec := auditDo(t, h, http.MethodPost, "/cis/scan/list", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var empty map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &empty))
	scans, _ := empty["cisScans"].([]any)
	assert.Empty(t, scans)

	// Creating a config materializes exactly one scan referencing it.
	cfgARN := createCisConfig(t, h, "scan-a", []string{"111111111111"})

	rec = auditDo(t, h, http.MethodPost, "/cis/scan/list", map[string]any{})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &empty))
	scans, _ = empty["cisScans"].([]any)
	require.Len(t, scans, 1)

	entry, _ := scans[0].(map[string]any)
	assert.Equal(t, cfgARN, entry["scanConfigurationArn"])
	assert.Equal(t, "scan-a", entry["scanName"])
	assert.Equal(t, "COMPLETED", entry["status"])
}

func TestCisScans_ResultDetailsReflectConfig(t *testing.T) {
	t.Parallel()

	h := axHandler(t)
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

	h := axHandler(t)
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

	h := axHandler(t)
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

	h := axHandler(t)
	cfgARN := createCisConfig(t, h, "scan-del", []string{"111111111111"})

	rec := auditDo(t, h, http.MethodPost, "/cis/scan-configuration/delete", map[string]any{
		"scanConfigurationArn": cfgARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = auditDo(t, h, http.MethodPost, "/cis/scan/list", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	scans, _ := resp["cisScans"].([]any)
	assert.Empty(t, scans)
}

func TestCisScans_UnknownScanArnDegradesBenignly(t *testing.T) {
	t.Parallel()

	h := axHandler(t)
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

	data := src.Snapshot()

	dst := inspector2.NewInMemoryBackend("000000000000", "us-east-2")
	require.NoError(t, dst.Restore(data))

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
