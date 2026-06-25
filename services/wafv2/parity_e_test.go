package wafv2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/wafv2"
)

// TestParity_DispatchTableBuiltOnce verifies the dispatch table is populated at
// construction time so no allocations happen per-request.
func TestParity_DispatchTableBuiltOnce(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	dispatchLen := wafv2.HandlerDispatchOpsLen(h)
	supportedLen := wafv2.HandlerOpsLen(h)

	assert.Equal(t, supportedLen, dispatchLen, "ops dispatch table should cover all supported operations")
	assert.Positive(t, dispatchLen, "dispatch table should not be empty")
}

// TestParity_GetTopPathStatisticsByTraffic verifies input validation.
func TestParity_GetTopPathStatisticsByTraffic(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	backend := wafv2.NewInMemoryBackend("000000000000", "us-east-1")
	acl, err := wafv2.CreateWebACLSimple(backend, "my-acl", "REGIONAL", "", "ALLOW", nil)
	require.NoError(t, err)

	hWithACL := wafv2.NewHandler(backend)

	timeWindow := map[string]any{"StartTime": 1000, "EndTime": 2000}

	cases := []struct {
		body    map[string]any
		handler *wafv2.Handler
		name    string
		wantErr bool
	}{
		{
			name: "valid request returns empty UrlStatistics",
			body: map[string]any{
				"Scope":      "REGIONAL",
				"WebACLName": acl.Name,
				"WebACLId":   acl.ID,
				"TimeWindow": timeWindow,
			},
			handler: hWithACL,
			wantErr: false,
		},
		{
			name: "missing Scope rejected",
			body: map[string]any{
				"WebACLName": "x",
				"WebACLId":   "y",
				"TimeWindow": timeWindow,
			},
			handler: h,
			wantErr: true,
		},
		{
			name: "missing WebACLName rejected",
			body: map[string]any{
				"Scope":      "REGIONAL",
				"WebACLId":   "y",
				"TimeWindow": timeWindow,
			},
			handler: h,
			wantErr: true,
		},
		{
			name: "missing WebACLId rejected",
			body: map[string]any{
				"Scope":      "REGIONAL",
				"WebACLName": "x",
				"TimeWindow": timeWindow,
			},
			handler: h,
			wantErr: true,
		},
		{
			name: "missing TimeWindow rejected",
			body: map[string]any{
				"Scope":      "REGIONAL",
				"WebACLName": "x",
				"WebACLId":   "y",
			},
			handler: h,
			wantErr: true,
		},
		{
			name: "nonexistent WebACL returns 400",
			body: map[string]any{
				"Scope":      "REGIONAL",
				"WebACLName": "no-such",
				"WebACLId":   "dead-beef-0000",
				"TimeWindow": timeWindow,
			},
			handler: h,
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := doWafv2Request(t, tc.handler, "GetTopPathStatisticsByTraffic", tc.body)

			if tc.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			} else {
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				stats, ok := resp["UrlStatistics"]
				assert.True(t, ok, "UrlStatistics key must be present")
				assert.Empty(t, stats, "UrlStatistics should be empty (no traffic in emulator)")
			}
		})
	}
}

// TestParity_GetRateBasedStatementManagedKeys verifies input validation and WebACL existence check.
func TestParity_GetRateBasedStatementManagedKeys(t *testing.T) {
	t.Parallel()

	backend := wafv2.NewInMemoryBackend("000000000000", "us-east-1")
	acl, err := wafv2.CreateWebACLSimple(backend, "rate-acl", "REGIONAL", "", "ALLOW", nil)
	require.NoError(t, err)

	h := wafv2.NewHandler(backend)
	hEmpty := newTestHandler(t)

	cases := []struct {
		body    map[string]any
		handler *wafv2.Handler
		name    string
		wantErr bool
	}{
		{
			name: "valid request returns empty managed keys",
			body: map[string]any{
				"Scope":      "REGIONAL",
				"WebACLName": acl.Name,
				"WebACLId":   acl.ID,
				"RuleName":   "RateLimit",
			},
			handler: h,
			wantErr: false,
		},
		{
			name: "missing Scope rejected",
			body: map[string]any{
				"WebACLName": acl.Name,
				"WebACLId":   acl.ID,
				"RuleName":   "RateLimit",
			},
			handler: h,
			wantErr: true,
		},
		{
			name: "missing WebACLName rejected",
			body: map[string]any{
				"Scope":    "REGIONAL",
				"WebACLId": acl.ID,
				"RuleName": "RateLimit",
			},
			handler: h,
			wantErr: true,
		},
		{
			name: "missing WebACLId rejected",
			body: map[string]any{
				"Scope":      "REGIONAL",
				"WebACLName": acl.Name,
				"RuleName":   "RateLimit",
			},
			handler: h,
			wantErr: true,
		},
		{
			name: "missing RuleName rejected",
			body: map[string]any{
				"Scope":      "REGIONAL",
				"WebACLName": acl.Name,
				"WebACLId":   acl.ID,
			},
			handler: h,
			wantErr: true,
		},
		{
			name: "nonexistent WebACL returns 400",
			body: map[string]any{
				"Scope":      "REGIONAL",
				"WebACLName": "no-such",
				"WebACLId":   "dead-beef-0000",
				"RuleName":   "RateLimit",
			},
			handler: hEmpty,
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := doWafv2Request(t, tc.handler, "GetRateBasedStatementManagedKeys", tc.body)

			if tc.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			} else {
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "ManagedKeysIPV4")
				assert.Contains(t, resp, "ManagedKeysIPV6")
			}
		})
	}
}

// TestParity_GetSampledRequests verifies input validation and WebACL ARN lookup.
func TestParity_GetSampledRequests(t *testing.T) {
	t.Parallel()

	backend := wafv2.NewInMemoryBackend("000000000000", "us-east-1")
	acl, err := wafv2.CreateWebACLSimple(backend, "sample-acl", "REGIONAL", "", "ALLOW", nil)
	require.NoError(t, err)

	h := wafv2.NewHandler(backend)
	hEmpty := newTestHandler(t)

	aclARN := backend.WebACLARN(acl.Name, acl.ID, "REGIONAL")
	timeWindow := map[string]any{"StartTime": 1000, "EndTime": 2000}

	cases := []struct {
		body    map[string]any
		handler *wafv2.Handler
		name    string
		wantErr bool
	}{
		{
			name: "valid request returns empty sampled requests",
			body: map[string]any{
				"WebAclArn":      aclARN,
				"Scope":          "REGIONAL",
				"RuleMetricName": "MyRule",
				"MaxItems":       int64(100),
				"TimeWindow":     timeWindow,
			},
			handler: h,
			wantErr: false,
		},
		{
			name: "missing Scope rejected",
			body: map[string]any{
				"WebAclArn":      aclARN,
				"RuleMetricName": "MyRule",
				"MaxItems":       int64(100),
				"TimeWindow":     timeWindow,
			},
			handler: h,
			wantErr: true,
		},
		{
			name: "missing WebAclArn rejected",
			body: map[string]any{
				"Scope":          "REGIONAL",
				"RuleMetricName": "MyRule",
				"MaxItems":       int64(100),
				"TimeWindow":     timeWindow,
			},
			handler: h,
			wantErr: true,
		},
		{
			name: "missing RuleMetricName rejected",
			body: map[string]any{
				"WebAclArn":  aclARN,
				"Scope":      "REGIONAL",
				"MaxItems":   int64(100),
				"TimeWindow": timeWindow,
			},
			handler: h,
			wantErr: true,
		},
		{
			name: "MaxItems zero rejected",
			body: map[string]any{
				"WebAclArn":      aclARN,
				"Scope":          "REGIONAL",
				"RuleMetricName": "MyRule",
				"MaxItems":       int64(0),
				"TimeWindow":     timeWindow,
			},
			handler: h,
			wantErr: true,
		},
		{
			name: "MaxItems over 500 rejected",
			body: map[string]any{
				"WebAclArn":      aclARN,
				"Scope":          "REGIONAL",
				"RuleMetricName": "MyRule",
				"MaxItems":       int64(501),
				"TimeWindow":     timeWindow,
			},
			handler: h,
			wantErr: true,
		},
		{
			name: "missing TimeWindow rejected",
			body: map[string]any{
				"WebAclArn":      aclARN,
				"Scope":          "REGIONAL",
				"RuleMetricName": "MyRule",
				"MaxItems":       int64(100),
			},
			handler: h,
			wantErr: true,
		},
		{
			name: "nonexistent WebACL ARN returns 400",
			body: map[string]any{
				"WebAclArn":      "arn:aws:wafv2:us-east-1:000000000000:regional/webacl/no-such/dead-beef",
				"Scope":          "REGIONAL",
				"RuleMetricName": "MyRule",
				"MaxItems":       int64(100),
				"TimeWindow":     timeWindow,
			},
			handler: hEmpty,
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := doWafv2Request(t, tc.handler, "GetSampledRequests", tc.body)

			if tc.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			} else {
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "SampledRequests")
				assert.Empty(t, resp["SampledRequests"], "no real traffic in emulator")
				assert.Contains(t, resp, "TimeWindow")
			}
		})
	}
}

// TestParity_RegionCap verifies the region cap constant is exported with the expected value.
func TestParity_RegionCap(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 100, wafv2.MaxRegions, "region cap must be 100")
	assert.Positive(t, wafv2.MaxRegions, "MaxRegions must be positive")
}
