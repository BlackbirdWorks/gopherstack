package wafv2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/wafv2"
)

func TestGetSampledRequestsValidation(t *testing.T) {
	t.Parallel()

	backend := wafv2.NewInMemoryBackend("000000000000", "us-east-1")
	acl, err := wafv2.CreateWebACLSimple(backend, "test", "REGIONAL", "", "ALLOW", nil)
	require.NoError(t, err)

	h := wafv2.NewHandler(backend)
	aclARN := backend.WebACLARN(acl.Name, acl.ID, "REGIONAL")

	// MaxItems 0 — invalid.
	recZero := doWafv2Request(t, h, "GetSampledRequests", map[string]any{
		"WebAclArn":      aclARN,
		"RuleMetricName": "my-rule",
		"Scope":          "REGIONAL",
		"MaxItems":       0,
		"TimeWindow": map[string]any{
			"StartTime": 1000,
			"EndTime":   2000,
		},
	})
	assert.Equal(t, http.StatusBadRequest, recZero.Code)

	// MaxItems 501 — invalid.
	recHigh := doWafv2Request(t, h, "GetSampledRequests", map[string]any{
		"WebAclArn":      aclARN,
		"RuleMetricName": "my-rule",
		"Scope":          "REGIONAL",
		"MaxItems":       501,
		"TimeWindow": map[string]any{
			"StartTime": 1000,
			"EndTime":   2000,
		},
	})
	assert.Equal(t, http.StatusBadRequest, recHigh.Code)

	// MaxItems 100 — valid.
	recOK := doWafv2Request(t, h, "GetSampledRequests", map[string]any{
		"WebAclArn":      aclARN,
		"RuleMetricName": "my-rule",
		"Scope":          "REGIONAL",
		"MaxItems":       100,
		"TimeWindow": map[string]any{
			"StartTime": 1000,
			"EndTime":   2000,
		},
	})
	assert.Equal(t, http.StatusOK, recOK.Code)
}

// ---- Gap 5: Extended WebACL fields -----------------------------------------

func TestRateBasedStatement_WithScopeDown(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":          "acl-rate-scope-down",
		"Scope":         "REGIONAL",
		"DefaultAction": map[string]any{"Allow": map[string]any{}},
		"VisibilityConfig": map[string]any{
			"MetricName": "acl-rate-scope-down",
		},
		"Rules": []map[string]any{
			{
				"Name":     "rate-rule",
				"Priority": 1,
				"Statement": map[string]any{
					"RateBasedStatement": map[string]any{
						"Limit":               500,
						"AggregateKeyType":    "IP",
						"EvaluationWindowSec": 300,
						"ScopeDownStatement": map[string]any{
							"GeoMatchStatement": map[string]any{
								"CountryCodes": []string{"US", "CA"},
							},
						},
					},
				},
				"Action": map[string]any{"Block": map[string]any{}},
				"VisibilityConfig": map[string]any{
					"MetricName": "rate-rule",
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "RateBasedStatement with ScopeDown: %s", rec.Body.String())

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	id := createResp["Summary"].(map[string]any)["Id"].(string)

	// Retrieve and verify the rule is stored with ScopeDownStatement.
	getRec := doWafv2Request(t, h, "GetWebACL", map[string]any{"Id": id})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	rules := getResp["WebACL"].(map[string]any)["Rules"].([]any)
	require.Len(t, rules, 1)

	stmt := rules[0].(map[string]any)["Statement"].(map[string]any)
	rbs, ok := stmt["RateBasedStatement"].(map[string]any)
	require.True(t, ok, "RateBasedStatement should be present")
	_, hasScopeDown := rbs["ScopeDownStatement"]
	assert.True(t, hasScopeDown, "ScopeDownStatement should round-trip")
}

func TestRateBasedStatement_AllValidEvaluationWindows(t *testing.T) {
	t.Parallel()

	validWindows := []int{60, 120, 300, 600, 1800, 3600, 7200, 21600}

	for _, window := range validWindows {
		t.Run(string(rune('0'+window/100)), func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
				"Name":          "acl-ew",
				"Scope":         "REGIONAL",
				"DefaultAction": map[string]any{"Allow": map[string]any{}},
				"VisibilityConfig": map[string]any{
					"MetricName": "acl-ew",
				},
				"Rules": []map[string]any{
					{
						"Name":     "r",
						"Priority": 1,
						"Statement": map[string]any{
							"RateBasedStatement": map[string]any{
								"Limit":               100,
								"AggregateKeyType":    "IP",
								"EvaluationWindowSec": window,
							},
						},
						"Action":           map[string]any{"Block": map[string]any{}},
						"VisibilityConfig": map[string]any{"MetricName": "r"},
					},
				},
			})
			assert.Equal(t, http.StatusOK, rec.Code,
				"EvaluationWindowSec=%d should be accepted: %s", window, rec.Body.String())
		})
	}
}

func TestRateBasedStatement_InvalidEvaluationWindow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		window int
	}{
		{name: "zero", window: 0},
		{name: "one", window: 1},
		{name: "59", window: 59},
		{name: "61", window: 61},
		{name: "3601", window: 3601},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
				"Name":          "acl-ew-bad",
				"Scope":         "REGIONAL",
				"DefaultAction": map[string]any{"Allow": map[string]any{}},
				"VisibilityConfig": map[string]any{
					"MetricName": "acl-ew-bad",
				},
				"Rules": []map[string]any{
					{
						"Name":     "r",
						"Priority": 1,
						"Statement": map[string]any{
							"RateBasedStatement": map[string]any{
								"Limit":               100,
								"AggregateKeyType":    "IP",
								"EvaluationWindowSec": tt.window,
							},
						},
						"Action":           map[string]any{"Block": map[string]any{}},
						"VisibilityConfig": map[string]any{"MetricName": "r"},
					},
				},
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code,
				"EvaluationWindowSec=%d should be rejected", tt.window)
		})
	}
}

func TestRateBasedStatement_LimitBoundaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		limit      int
		wantStatus int
	}{
		{name: "min_valid", limit: 100, wantStatus: http.StatusOK},
		{name: "below_min", limit: 99, wantStatus: http.StatusBadRequest},
		{name: "max_valid", limit: 2_000_000_000, wantStatus: http.StatusOK},
		{name: "above_max", limit: 2_000_000_001, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
				"Name":          "acl-limit",
				"Scope":         "REGIONAL",
				"DefaultAction": map[string]any{"Allow": map[string]any{}},
				"VisibilityConfig": map[string]any{
					"MetricName": "acl-limit",
				},
				"Rules": []map[string]any{
					{
						"Name":     "r",
						"Priority": 1,
						"Statement": map[string]any{
							"RateBasedStatement": map[string]any{
								"Limit":            tt.limit,
								"AggregateKeyType": "IP",
							},
						},
						"Action":           map[string]any{"Block": map[string]any{}},
						"VisibilityConfig": map[string]any{"MetricName": "r"},
					},
				},
			})
			assert.Equal(t, tt.wantStatus, rec.Code,
				"limit=%d expected=%d: %s", tt.limit, tt.wantStatus, rec.Body.String())
		})
	}
}

// ---- Captcha and Challenge actions ------------------------------------------

func TestGetRateBasedStatementManagedKeys_Valid(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	id, _ := createWebACLHelper(t, h, "rate-keys-acl", "REGIONAL")

	rec := doWafv2Request(t, h, "GetRateBasedStatementManagedKeys", map[string]any{
		"Scope":      "REGIONAL",
		"WebACLName": "rate-keys-acl",
		"WebACLId":   id,
		"RuleName":   "my-rate-rule",
	})
	require.Equal(t, http.StatusOK, rec.Code, "GetRateBasedStatementManagedKeys: %s", rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	ipv4, ok := resp["ManagedKeysIPV4"].(map[string]any)
	require.True(t, ok, "ManagedKeysIPV4 should be present")
	assert.Equal(t, "IPV4", ipv4["IPAddressVersion"])
	_, hasIPv4Addrs := ipv4["Addresses"]
	assert.True(t, hasIPv4Addrs)

	ipv6, ok := resp["ManagedKeysIPV6"].(map[string]any)
	require.True(t, ok, "ManagedKeysIPV6 should be present")
	assert.Equal(t, "IPV6", ipv6["IPAddressVersion"])
}

func TestGetRateBasedStatementManagedKeys_MissingScope(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "GetRateBasedStatementManagedKeys", map[string]any{
		"WebACLId": "some-id",
		"RuleName": "some-rule",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- Snapshot / Restore preserves managed rule sets -------------------------

func TestValidation_RuleNameUniqueness(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	dupRules := []map[string]any{
		{
			"Name":     "dupe-rule",
			"Priority": 1,
			"Statement": map[string]any{
				"IPSetReferenceStatement": map[string]any{
					"ARN": "arn:aws:wafv2:us-east-1:000000000000:regional/ipset/x/abc",
				},
			},
			"Action": map[string]any{"Allow": map[string]any{}},
			"VisibilityConfig": map[string]any{
				"MetricName":               "dupe-rule",
				"SampledRequestsEnabled":   false,
				"CloudWatchMetricsEnabled": false,
			},
		},
		{
			"Name":     "dupe-rule", // same name → should fail
			"Priority": 2,
			"Statement": map[string]any{
				"IPSetReferenceStatement": map[string]any{
					"ARN": "arn:aws:wafv2:us-east-1:000000000000:regional/ipset/x/abc",
				},
			},
			"Action": map[string]any{"Block": map[string]any{}},
			"VisibilityConfig": map[string]any{
				"MetricName":               "dupe-rule",
				"SampledRequestsEnabled":   false,
				"CloudWatchMetricsEnabled": false,
			},
		},
	}

	rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":          "acl-dup-rule-name",
		"Scope":         "REGIONAL",
		"DefaultAction": map[string]any{"Allow": map[string]any{}},
		"Rules":         dupRules,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "duplicate rule names should be rejected")

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "WAFInvalidParameterException", errResp["__type"])
}

// ---- Rule Priority range enforcement ----------------------------------------

func TestValidation_RulePriorityRange(t *testing.T) {
	t.Parallel()

	makeRule := func(name string, priority int) map[string]any {
		return map[string]any{
			"Name":     name,
			"Priority": priority,
			"Statement": map[string]any{
				"IPSetReferenceStatement": map[string]any{
					"ARN": "arn:aws:wafv2:us-east-1:000000000000:regional/ipset/x/abc",
				},
			},
			"Action": map[string]any{"Allow": map[string]any{}},
			"VisibilityConfig": map[string]any{
				"MetricName":               name,
				"SampledRequestsEnabled":   false,
				"CloudWatchMetricsEnabled": false,
			},
		}
	}

	tests := []struct {
		name       string
		priority   int
		wantStatus int
	}{
		{name: "priority_0", priority: 0, wantStatus: http.StatusOK},
		{name: "priority_1000", priority: 1000, wantStatus: http.StatusOK},
		{name: "priority_1001", priority: 1001, wantStatus: http.StatusBadRequest},
		{name: "priority_negative", priority: -1, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
				"Name":          "acl-priority-" + tt.name,
				"Scope":         "REGIONAL",
				"DefaultAction": map[string]any{"Allow": map[string]any{}},
				"Rules":         []map[string]any{makeRule("r", tt.priority)},
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "priority=%d body=%s", tt.priority, rec.Body.String())
		})
	}
}

// ---- Scope mismatch on GetWebACL / GetIPSet ----------------------------------

// TestParity_GetTopPathStatisticsByTraffic verifies input validation.
func TestGetTopPathStatisticsByTraffic(t *testing.T) {
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
func TestGetRateBasedStatementManagedKeys(t *testing.T) {
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
func TestGetSampledRequests(t *testing.T) {
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
