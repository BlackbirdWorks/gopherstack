package wafv2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListAvailableManagedRuleGroups_ReturnsCatalog(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doWafv2Request(t, h, "ListAvailableManagedRuleGroups", map[string]any{
		"Scope": "REGIONAL",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	groups, ok := resp["ManagedRuleGroups"].([]any)
	require.True(t, ok)
	assert.Greater(t, len(groups), 5, "should return more than 5 managed rule groups from catalog")

	// Verify a known catalog entry is present.
	found := false

	for _, g := range groups {
		gm := g.(map[string]any)
		if gm["Name"] == "AWSManagedRulesCommonRuleSet" {
			found = true
			assert.Equal(t, "AWS", gm["VendorName"])
		}
	}

	assert.True(t, found, "AWSManagedRulesCommonRuleSet should be in catalog")
}

// ---- Gap 4: DescribeManagedRuleGroup returns catalog data ------------------

func TestDescribeManagedRuleGroup_ReturnsCatalogData(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doWafv2Request(t, h, "DescribeManagedRuleGroup", map[string]any{
		"Scope":      "REGIONAL",
		"VendorName": "AWS",
		"Name":       "AWSManagedRulesCommonRuleSet",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	capacity, _ := resp["Capacity"].(float64)
	assert.InDelta(t, float64(700), capacity, 0, "AWSManagedRulesCommonRuleSet should have capacity 700")
}

// ---- Gap 6: VisibilityConfig MetricName required ---------------------------

func TestGetMobileSdkRelease_KnownVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "GetMobileSdkRelease", map[string]any{
		"Platform":       "Android",
		"ReleaseVersion": "3.1.0",
	})
	require.Equal(t, http.StatusOK, rec.Code, "GetMobileSdkRelease Android/3.1.0: %s", rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	release := resp["MobileSdkRelease"].(map[string]any)
	assert.Equal(t, "3.1.0", release["ReleaseVersion"])
	assert.NotEmpty(t, release["ReleaseNotes"])
	assert.NotNil(t, release["Timestamp"])
	_, tsOk := release["Timestamp"].(float64)
	assert.True(t, tsOk, "Timestamp should be a number")
}

func TestGetMobileSdkRelease_IOSVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "GetMobileSdkRelease", map[string]any{
		"Platform":       "iOS",
		"ReleaseVersion": "3.0.0",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	release := resp["MobileSdkRelease"].(map[string]any)
	assert.Equal(t, "3.0.0", release["ReleaseVersion"])
}

func TestGetMobileSdkRelease_UnknownVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "GetMobileSdkRelease", map[string]any{
		"Platform":       "Android",
		"ReleaseVersion": "99.0.0",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "WAFNonexistentItemException", errResp["__type"])
}

func TestGetMobileSdkRelease_UnknownPlatform(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "GetMobileSdkRelease", map[string]any{
		"Platform":       "WindowsPhone",
		"ReleaseVersion": "3.1.0",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestGetMobileSdkRelease_MissingPlatform(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "GetMobileSdkRelease", map[string]any{
		"ReleaseVersion": "3.1.0",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "WAFInvalidParameterException", errResp["__type"])
}

func TestListMobileSdkReleases_Android(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "ListMobileSdkReleases", map[string]any{
		"Platform": "Android",
		"Scope":    "REGIONAL",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	releases, ok := resp["ReleaseSummaries"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, releases, "should return at least one Android release")

	for _, r := range releases {
		rm := r.(map[string]any)
		assert.NotEmpty(t, rm["ReleaseVersion"])
		assert.NotNil(t, rm["Timestamp"])
	}
}

func TestListMobileSdkReleases_iOS(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "ListMobileSdkReleases", map[string]any{
		"Platform": "iOS",
		"Scope":    "REGIONAL",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	releases, _ := resp["ReleaseSummaries"].([]any)
	assert.NotEmpty(t, releases, "should return at least one iOS release")
}

func TestListMobileSdkReleases_UnknownPlatform(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "ListMobileSdkReleases", map[string]any{
		"Platform": "Blackberry",
		"Scope":    "REGIONAL",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	releases, _ := resp["ReleaseSummaries"].([]any)
	assert.Empty(t, releases, "unknown platform should return empty list")
}

func TestGenerateMobileSdkReleaseUrl_Shape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "GenerateMobileSdkReleaseUrl", map[string]any{
		"Platform":       "Android",
		"ReleaseVersion": "3.1.0",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	url, ok := resp["Url"].(string)
	require.True(t, ok, "Url field should be present")
	assert.NotEmpty(t, url)
}

// ---- Logging configuration: all 3 destination types -------------------------

func TestWebACL_ManagedRuleGroupStatement_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":          "acl-mrg",
		"Scope":         "REGIONAL",
		"DefaultAction": map[string]any{"Allow": map[string]any{}},
		"VisibilityConfig": map[string]any{
			"MetricName": "acl-mrg",
		},
		"Rules": []map[string]any{
			{
				"Name":     "aws-common",
				"Priority": 1,
				"Statement": map[string]any{
					"ManagedRuleGroupStatement": map[string]any{
						"VendorName": "AWS",
						"Name":       "AWSManagedRulesCommonRuleSet",
						"ExcludedRules": []map[string]any{
							{"Name": "SizeRestrictions_BODY"},
						},
					},
				},
				"OverrideAction": map[string]any{"None": map[string]any{}},
				"VisibilityConfig": map[string]any{
					"MetricName": "aws-common",
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "ManagedRuleGroupStatement: %s", rec.Body.String())

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	id := createResp["Summary"].(map[string]any)["Id"].(string)

	getRec := doWafv2Request(t, h, "GetWebACL", map[string]any{"Id": id})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	rules := getResp["WebACL"].(map[string]any)["Rules"].([]any)
	require.Len(t, rules, 1)

	stmt := rules[0].(map[string]any)["Statement"].(map[string]any)
	mrg, ok := stmt["ManagedRuleGroupStatement"].(map[string]any)
	require.True(t, ok, "ManagedRuleGroupStatement should round-trip")
	assert.Equal(t, "AWS", mrg["VendorName"])
	assert.Equal(t, "AWSManagedRulesCommonRuleSet", mrg["Name"])
}

// ---- RuleGroup: update rules, OverrideAction variants -----------------------

func TestDescribeManagedRuleGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		vendor     string
		group      string
		wantType   string
		wantCap    float64
		wantStatus int
	}{
		{
			name:       "known AWS group returns catalog data",
			vendor:     "AWS",
			group:      "AWSManagedRulesCommonRuleSet",
			wantStatus: http.StatusOK,
			wantCap:    700,
		},
		{
			name:       "another known AWS group",
			vendor:     "AWS",
			group:      "AWSManagedRulesKnownBadInputsRuleSet",
			wantStatus: http.StatusOK,
			wantCap:    200,
		},
		{
			name:       "unknown vendor returns WAFNonexistentItemException",
			vendor:     "UnknownVendor",
			group:      "SomeRuleGroup",
			wantStatus: http.StatusBadRequest,
			wantType:   "WAFNonexistentItemException",
		},
		{
			name:       "unknown rule group for known vendor returns WAFNonexistentItemException",
			vendor:     "AWS",
			group:      "NoSuchRuleGroup",
			wantStatus: http.StatusBadRequest,
			wantType:   "WAFNonexistentItemException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doWafv2Request(t, h, "DescribeManagedRuleGroup", map[string]any{
				"Scope":      "REGIONAL",
				"VendorName": tc.vendor,
				"Name":       tc.group,
			})
			require.Equal(t, tc.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			if tc.wantType != "" {
				assert.Equal(t, tc.wantType, resp["__type"])
			}

			if tc.wantCap > 0 {
				gotCap, _ := resp["Capacity"].(float64)
				assert.InDelta(t, tc.wantCap, gotCap, 0)
			}
		})
	}
}

// ---- GenerateMobileSdkReleaseUrl: validates platform+version, rejects unknown combos ----

func TestGenerateMobileSdkReleaseUrl(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		platform   string
		version    string
		wantType   string
		wantStatus int
		checkURL   bool
	}{
		{
			name:       "Android 3.1.0 returns URL",
			platform:   "Android",
			version:    "3.1.0",
			wantStatus: http.StatusOK,
			checkURL:   true,
		},
		{
			name:       "iOS 3.0.0 returns URL",
			platform:   "iOS",
			version:    "3.0.0",
			wantStatus: http.StatusOK,
			checkURL:   true,
		},
		{
			name:       "unknown platform returns WAFNonexistentItemException",
			platform:   "Windows",
			version:    "3.1.0",
			wantStatus: http.StatusBadRequest,
			wantType:   "WAFNonexistentItemException",
		},
		{
			name:       "unknown version returns WAFNonexistentItemException",
			platform:   "Android",
			version:    "99.0.0",
			wantStatus: http.StatusBadRequest,
			wantType:   "WAFNonexistentItemException",
		},
		{
			name:       "missing platform rejected",
			platform:   "",
			version:    "3.1.0",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing version rejected",
			platform:   "Android",
			version:    "",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doWafv2Request(t, h, "GenerateMobileSdkReleaseUrl", map[string]any{
				"Platform":       tc.platform,
				"ReleaseVersion": tc.version,
			})
			require.Equal(t, tc.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			if tc.wantType != "" {
				assert.Equal(t, tc.wantType, resp["__type"])
			}

			if tc.checkURL {
				url, ok := resp["Url"].(string)
				require.True(t, ok, "Url field must be present")
				assert.NotEmpty(t, url)
				assert.Contains(t, url, tc.platform)
				assert.Contains(t, url, tc.version)
			}
		})
	}
}

// TestParity_DescribeManagedRuleGroupRules verifies that DescribeManagedRuleGroup
// returns populated Rules and AvailableLabels for known AWS managed rule groups.
// The previous implementation always returned empty slices for both fields,
// breaking callers that rely on rule names for override/exclusion configuration.
func TestDescribeManagedRuleGroupRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		vendorName    string
		groupName     string
		wantMinRules  int
		wantMinLabels int
	}{
		{
			name:          "crs_has_rules_and_labels",
			vendorName:    "AWS",
			groupName:     "AWSManagedRulesCommonRuleSet",
			wantMinRules:  5,
			wantMinLabels: 5,
		},
		{
			name:          "sqli_has_rules_and_labels",
			vendorName:    "AWS",
			groupName:     "AWSManagedRulesSQLiRuleSet",
			wantMinRules:  4,
			wantMinLabels: 4,
		},
		{
			name:          "known_bad_inputs_has_rules",
			vendorName:    "AWS",
			groupName:     "AWSManagedRulesKnownBadInputsRuleSet",
			wantMinRules:  3,
			wantMinLabels: 3,
		},
		{
			name:          "ip_reputation_has_rules",
			vendorName:    "AWS",
			groupName:     "AWSManagedRulesAmazonIpReputationList",
			wantMinRules:  3,
			wantMinLabels: 3,
		},
		{
			name:          "bot_control_has_rules",
			vendorName:    "AWS",
			groupName:     "AWSManagedRulesBotControlRuleSet",
			wantMinRules:  5,
			wantMinLabels: 5,
		},
		{
			name:         "group_without_rules_returns_empty_not_null",
			vendorName:   "AWS",
			groupName:    "AWSManagedRulesAdminProtectionRuleSet",
			wantMinRules: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doWafv2Request(t, h, "DescribeManagedRuleGroup", map[string]any{
				"Scope":      "REGIONAL",
				"VendorName": tt.vendorName,
				"Name":       tt.groupName,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Rules []struct {
					Action map[string]any `json:"Action"`
					Name   string         `json:"Name"`
				} `json:"Rules"`
				AvailableLabels []struct {
					Name string `json:"Name"`
				} `json:"AvailableLabels"`
				Capacity float64 `json:"Capacity"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			// Rules and AvailableLabels must never be null (always an array).
			assert.NotNil(t, out.Rules, "Rules must not be null")
			assert.NotNil(t, out.AvailableLabels, "AvailableLabels must not be null")

			assert.GreaterOrEqual(t, len(out.Rules), tt.wantMinRules,
				"Rules count for %s", tt.groupName)

			if tt.wantMinLabels > 0 {
				assert.GreaterOrEqual(t, len(out.AvailableLabels), tt.wantMinLabels,
					"AvailableLabels count for %s", tt.groupName)
			}

			// Each rule must have a non-empty Name and a non-empty Action.
			for _, r := range out.Rules {
				assert.NotEmpty(t, r.Name, "rule Name must not be empty")
				assert.NotEmpty(t, r.Action, "rule Action must not be empty")
			}

			// Each available label must have a non-empty Name.
			for _, lbl := range out.AvailableLabels {
				assert.NotEmpty(t, lbl.Name, "label Name must not be empty")
			}

			assert.Positive(t, out.Capacity, "Capacity must be positive")
		})
	}
}

// TestParity_DescribeManagedRuleGroupUnknown verifies that unknown vendor/name
// combinations return a 400 error (not an empty success response).
func TestDescribeManagedRuleGroupUnknown(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doWafv2Request(t, h, "DescribeManagedRuleGroup", map[string]any{
		"Scope":      "REGIONAL",
		"VendorName": "AWS",
		"Name":       "AWSManagedRulesDoesNotExist",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
