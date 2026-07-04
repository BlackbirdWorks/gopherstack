package wafv2_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/wafv2"
)

// ---- helpers ---------------------------------------------------------------

//nolint:unparam // scope kept for call-site readability; callers may vary it in future
func createWebACLWithRules(t *testing.T, h *wafv2.Handler, name, scope string) (string, string) {
	t.Helper()

	rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":          name,
		"Scope":         scope,
		"DefaultAction": map[string]any{"Allow": map[string]any{}},
		"VisibilityConfig": map[string]any{
			"CloudWatchMetricsEnabled": true,
			"MetricName":               name,
			"SampledRequestsEnabled":   true,
		},
		"Rules": []map[string]any{
			{
				"Name":     "rule-1",
				"Priority": 1,
				"Statement": map[string]any{
					"IPSetReferenceStatement": map[string]any{
						"ARN": "arn:aws:wafv2:us-east-1:000000000000:regional/ipset/test/abc",
					},
				},
				"Action": map[string]any{"Allow": map[string]any{}},
				"VisibilityConfig": map[string]any{
					"CloudWatchMetricsEnabled": false,
					"MetricName":               "rule-1",
					"SampledRequestsEnabled":   false,
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "createWebACLWithRules: %s", rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	summary := resp["Summary"].(map[string]any)

	return summary["Id"].(string), summary["LockToken"].(string)
}

func createIPSetHelper2(
	t *testing.T,
	h *wafv2.Handler,
	name, scope, version string,
	addrs []string,
) (string, string) {
	t.Helper()

	body := map[string]any{
		"Name":             name,
		"Scope":            scope,
		"IPAddressVersion": version,
		"Addresses":        addrs,
	}

	rec := doWafv2Request(t, h, "CreateIPSet", body)
	require.Equal(t, http.StatusOK, rec.Code, "createIPSetHelper2: %s", rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	summary := resp["Summary"].(map[string]any)

	return summary["Id"].(string), summary["LockToken"].(string)
}

// ---- Gap 1: WebACL Rules round-trip ----------------------------------------

func TestAccuracy_WebACLRulesRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	id, _ := createWebACLWithRules(t, h, "acl-with-rules", "REGIONAL")

	rec := doWafv2Request(t, h, "GetWebACL", map[string]any{
		"Id":    id,
		"Name":  "acl-with-rules",
		"Scope": "REGIONAL",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	webACL := resp["WebACL"].(map[string]any)

	rules, ok := webACL["Rules"].([]any)
	require.True(t, ok, "Rules should be present in response")
	require.Len(t, rules, 1, "should have exactly 1 rule")

	rule := rules[0].(map[string]any)
	assert.Equal(t, "rule-1", rule["Name"])
	assert.InDelta(t, float64(1), rule["Priority"], 0)
}

// ---- Gap 2: DefaultAction as raw JSON round-trip ----------------------------

func TestAccuracy_DefaultActionRawJSONRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create with Allow.
	recAllow := doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":          "acl-allow",
		"Scope":         "REGIONAL",
		"DefaultAction": map[string]any{"Allow": map[string]any{}},
		"VisibilityConfig": map[string]any{
			"MetricName": "acl-allow",
		},
	})
	require.Equal(t, http.StatusOK, recAllow.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(recAllow.Body.Bytes(), &createResp))
	idAllow := createResp["Summary"].(map[string]any)["Id"].(string)

	recGet := doWafv2Request(t, h, "GetWebACL", map[string]any{"Id": idAllow})
	require.Equal(t, http.StatusOK, recGet.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &getResp))
	da := getResp["WebACL"].(map[string]any)["DefaultAction"].(map[string]any)
	_, hasAllow := da["Allow"]
	assert.True(t, hasAllow, "DefaultAction should contain Allow key")

	// Create with Block.
	recBlock := doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":          "acl-block",
		"Scope":         "REGIONAL",
		"DefaultAction": map[string]any{"Block": map[string]any{}},
		"VisibilityConfig": map[string]any{
			"MetricName": "acl-block",
		},
	})
	require.Equal(t, http.StatusOK, recBlock.Code)

	var createRespBlock map[string]any
	require.NoError(t, json.Unmarshal(recBlock.Body.Bytes(), &createRespBlock))
	idBlock := createRespBlock["Summary"].(map[string]any)["Id"].(string)

	recGetBlock := doWafv2Request(t, h, "GetWebACL", map[string]any{"Id": idBlock})
	require.Equal(t, http.StatusOK, recGetBlock.Code)

	var getRespBlock map[string]any
	require.NoError(t, json.Unmarshal(recGetBlock.Body.Bytes(), &getRespBlock))
	daBlock := getRespBlock["WebACL"].(map[string]any)["DefaultAction"].(map[string]any)
	_, hasBlock := daBlock["Block"]
	assert.True(t, hasBlock, "DefaultAction should contain Block key")

	// Both Allow AND Block together should be rejected.
	recBoth := doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":  "acl-both",
		"Scope": "REGIONAL",
		"DefaultAction": map[string]any{
			"Allow": map[string]any{},
			"Block": map[string]any{},
		},
	})
	assert.Equal(t, http.StatusBadRequest, recBoth.Code)
}

// ---- Gap 3: LockToken enforcement -------------------------------------------

func TestAccuracy_LockTokenEnforcement(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	id, realToken := createWebACLWithRules(t, h, "acl-lock", "REGIONAL")

	// Update with wrong lock token should fail.
	recBad := doWafv2Request(t, h, "UpdateWebACL", map[string]any{
		"Id":          id,
		"LockToken":   "wrong-token",
		"Description": "should fail",
	})
	assert.Equal(t, http.StatusBadRequest, recBad.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(recBad.Body.Bytes(), &errResp))
	assert.Equal(t, "WAFOptimisticLockException", errResp["__type"])

	// Update with correct lock token should succeed.
	recGood := doWafv2Request(t, h, "UpdateWebACL", map[string]any{
		"Id":          id,
		"LockToken":   realToken,
		"Description": "updated successfully",
	})
	assert.Equal(t, http.StatusOK, recGood.Code)
}

// ---- Gap 4: Managed rule group catalog --------------------------------------

func TestAccuracy_ListAvailableManagedRuleGroups_ReturnsCatalog(t *testing.T) {
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

func TestAccuracy_DescribeManagedRuleGroup_ReturnsCatalogData(t *testing.T) {
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

func TestAccuracy_VisibilityConfigMissingMetricName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":          "acl-no-metric",
		"Scope":         "REGIONAL",
		"DefaultAction": map[string]any{"Allow": map[string]any{}},
		"VisibilityConfig": map[string]any{
			"CloudWatchMetricsEnabled": true,
			"MetricName":               "", // missing
			"SampledRequestsEnabled":   false,
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "WAFInvalidParameterException", resp["__type"])
}

// ---- Gap 7: RuleGroup Capacity enforcement ----------------------------------

func TestAccuracy_RuleGroupCapacityValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Capacity too low.
	recLow := doWafv2Request(t, h, "CreateRuleGroup", map[string]any{
		"Name":     "rg-low",
		"Scope":    "REGIONAL",
		"Capacity": 0,
	})
	assert.Equal(t, http.StatusBadRequest, recLow.Code)

	// Capacity too high.
	recHigh := doWafv2Request(t, h, "CreateRuleGroup", map[string]any{
		"Name":     "rg-high",
		"Scope":    "REGIONAL",
		"Capacity": 1501,
	})
	assert.Equal(t, http.StatusBadRequest, recHigh.Code)

	// Valid capacity.
	recValid := doWafv2Request(t, h, "CreateRuleGroup", map[string]any{
		"Name":     "rg-valid",
		"Scope":    "REGIONAL",
		"Capacity": 100,
	})
	assert.Equal(t, http.StatusOK, recValid.Code)
}

// ---- Gap 9: IPSet CIDR validation ------------------------------------------

func TestAccuracy_IPSetCIDRValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Invalid CIDR.
	recBad := doWafv2Request(t, h, "CreateIPSet", map[string]any{
		"Name":             "bad-cidrs",
		"Scope":            "REGIONAL",
		"IPAddressVersion": "IPV4",
		"Addresses":        []string{"not-a-cidr"},
	})
	assert.Equal(t, http.StatusBadRequest, recBad.Code)

	// IPv4 CIDR in IPv6 set.
	recMix := doWafv2Request(t, h, "CreateIPSet", map[string]any{
		"Name":             "ipv4-in-ipv6",
		"Scope":            "REGIONAL",
		"IPAddressVersion": "IPV6",
		"Addresses":        []string{"1.2.3.4/32"},
	})
	assert.Equal(t, http.StatusBadRequest, recMix.Code)

	// Valid IPv4.
	recOK := doWafv2Request(t, h, "CreateIPSet", map[string]any{
		"Name":             "valid-v4",
		"Scope":            "REGIONAL",
		"IPAddressVersion": "IPV4",
		"Addresses":        []string{"10.0.0.0/8", "192.168.1.0/24"},
	})
	assert.Equal(t, http.StatusOK, recOK.Code)

	// Valid IPv6.
	recV6 := doWafv2Request(t, h, "CreateIPSet", map[string]any{
		"Name":             "valid-v6",
		"Scope":            "REGIONAL",
		"IPAddressVersion": "IPV6",
		"Addresses":        []string{"2001:db8::/32"},
	})
	assert.Equal(t, http.StatusOK, recV6.Code)
}

// ---- Gap 10/11: RegexPatternSet object shape --------------------------------

func TestAccuracy_RegexPatternSetObjectShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// AWS object form: [{"RegexString": "..."}]
	recObj := doWafv2Request(t, h, "CreateRegexPatternSet", map[string]any{
		"Name":  "regex-obj",
		"Scope": "REGIONAL",
		"RegularExpressionList": []map[string]any{
			{"RegexString": "^foo"},
			{"RegexString": "bar$"},
		},
	})
	require.Equal(t, http.StatusOK, recObj.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(recObj.Body.Bytes(), &createResp))
	id := createResp["Summary"].(map[string]any)["Id"].(string)

	// Get and verify the entries are returned as objects.
	recGet := doWafv2Request(t, h, "GetRegexPatternSet", map[string]any{"Id": id})
	require.Equal(t, http.StatusOK, recGet.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &getResp))

	rps := getResp["RegexPatternSet"].(map[string]any)
	entries, ok := rps["RegularExpressionList"].([]any)
	require.True(t, ok)
	require.Len(t, entries, 2)

	entry := entries[0].(map[string]any)
	assert.Contains(t, entry, "RegexString")

	// Invalid regex should be rejected.
	recBadRegex := doWafv2Request(t, h, "CreateRegexPatternSet", map[string]any{
		"Name":  "bad-regex",
		"Scope": "REGIONAL",
		"RegularExpressionList": []map[string]any{
			{"RegexString": "[invalid"},
		},
	})
	assert.Equal(t, http.StatusBadRequest, recBadRegex.Code)
}

// ---- Gap 12: Full LoggingConfiguration round-trip --------------------------

func TestAccuracy_LoggingConfigurationFullRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a WebACL to get a real ARN.
	createRec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":             "log-acl",
		"Scope":            "REGIONAL",
		"DefaultAction":    map[string]any{"Allow": map[string]any{}},
		"VisibilityConfig": map[string]any{"MetricName": "log-acl"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	webACLARN := createResp["Summary"].(map[string]any)["ARN"].(string)

	// Put a full logging configuration.
	loggingConfig := map[string]any{
		"ResourceArn":           webACLARN,
		"LogDestinationConfigs": []string{"arn:aws:firehose:us-east-1:000000000000:deliverystream/my-stream"},
		"RedactedFields":        []any{},
	}
	putRec := doWafv2Request(t, h, "PutLoggingConfiguration", map[string]any{
		"LoggingConfiguration": loggingConfig,
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	// Verify the full config is round-tripped in Get.
	getRec := doWafv2Request(t, h, "GetLoggingConfiguration", map[string]any{
		"ResourceArn": webACLARN,
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))

	cfg, ok := getResp["LoggingConfiguration"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, webACLARN, cfg["ResourceArn"])

	dests, ok := cfg["LogDestinationConfigs"].([]any)
	require.True(t, ok)
	require.Len(t, dests, 1)

	// Verify ListLoggingConfigurations returns entries.
	listRec := doWafv2Request(t, h, "ListLoggingConfigurations", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	configs, ok := listResp["LoggingConfigurations"].([]any)
	require.True(t, ok)
	assert.Len(t, configs, 1)
}

// ---- Gap 14: GetWebACLForResource returns Rules ----------------------------

func TestAccuracy_GetWebACLForResourceReturnsRules(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	id, _ := createWebACLWithRules(t, h, "acl-for-resource", "REGIONAL")

	// Get the ARN.
	getRec := doWafv2Request(t, h, "GetWebACL", map[string]any{"Id": id})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getRespW map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getRespW))
	webACLARN := getRespW["WebACL"].(map[string]any)["ARN"].(string)

	// Associate with a resource.
	resourceARN := "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/my-lb/abc"
	assocRec := doWafv2Request(t, h, "AssociateWebACL", map[string]any{
		"WebACLArn":   webACLARN,
		"ResourceArn": resourceARN,
	})
	require.Equal(t, http.StatusOK, assocRec.Code)

	// GetWebACLForResource should return the full WebACL including Rules.
	forResourceRec := doWafv2Request(t, h, "GetWebACLForResource", map[string]any{
		"ResourceArn": resourceARN,
	})
	require.Equal(t, http.StatusOK, forResourceRec.Code)

	var forResourceResp map[string]any
	require.NoError(t, json.Unmarshal(forResourceRec.Body.Bytes(), &forResourceResp))

	webACL := forResourceResp["WebACL"].(map[string]any)
	rules, ok := webACL["Rules"].([]any)
	require.True(t, ok, "Rules should be present")
	assert.Len(t, rules, 1, "should return 1 rule")
}

// ---- Gap 19: DeleteRuleGroup referenced by WebACL -------------------------

func TestAccuracy_DeleteRuleGroupReferencedByWebACL(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a RuleGroup.
	rgRec := doWafv2Request(t, h, "CreateRuleGroup", map[string]any{
		"Name":     "my-rg",
		"Scope":    "REGIONAL",
		"Capacity": 10,
	})
	require.Equal(t, http.StatusOK, rgRec.Code)

	var rgResp map[string]any
	require.NoError(t, json.Unmarshal(rgRec.Body.Bytes(), &rgResp))
	summary := rgResp["Summary"].(map[string]any)
	rgID := summary["Id"].(string)
	rgARN := summary["ARN"].(string)
	rgLockToken := summary["LockToken"].(string)

	// Create a WebACL that references the RuleGroup.
	aclRec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":             "acl-with-rg",
		"Scope":            "REGIONAL",
		"DefaultAction":    map[string]any{"Allow": map[string]any{}},
		"VisibilityConfig": map[string]any{"MetricName": "acl-with-rg"},
		"Rules": []map[string]any{
			{
				"Name":     "use-rule-group",
				"Priority": 1,
				"Statement": map[string]any{
					"RuleGroupReferenceStatement": map[string]any{
						"ARN": rgARN,
					},
				},
				"OverrideAction":   map[string]any{"None": map[string]any{}},
				"VisibilityConfig": map[string]any{"MetricName": "use-rule-group"},
			},
		},
	})
	require.Equal(t, http.StatusOK, aclRec.Code)

	// Try to delete the RuleGroup — should fail with WAFAssociatedItemException.
	delRec := doWafv2Request(t, h, "DeleteRuleGroup", map[string]any{
		"Id":        rgID,
		"LockToken": rgLockToken,
	})
	assert.Equal(t, http.StatusBadRequest, delRec.Code)

	var delResp map[string]any
	require.NoError(t, json.Unmarshal(delRec.Body.Bytes(), &delResp))
	assert.Equal(t, "WAFAssociatedItemException", delResp["__type"])
}

// ---- Gap 20: Tag validation ------------------------------------------------

func TestAccuracy_TagValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a WebACL to tag.
	createRec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":             "tag-test-acl",
		"Scope":            "REGIONAL",
		"DefaultAction":    map[string]any{"Allow": map[string]any{}},
		"VisibilityConfig": map[string]any{"MetricName": "tag-test-acl"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	arnStr := createResp["Summary"].(map[string]any)["ARN"].(string)

	// More than 50 tags should fail.
	tags := make([]map[string]string, 51)
	for i := range tags {
		tags[i] = map[string]string{"Key": fmt.Sprintf("key%d", i), "Value": "v"}
	}

	tooManyRec := doWafv2Request(t, h, "TagResource", map[string]any{
		"ResourceARN": arnStr,
		"Tags":        tags,
	})
	assert.Equal(t, http.StatusBadRequest, tooManyRec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(tooManyRec.Body.Bytes(), &errResp))
	assert.Equal(t, "WAFTagOperationException", errResp["__type"])

	// Reserved prefix "aws:" should fail.
	reservedRec := doWafv2Request(t, h, "TagResource", map[string]any{
		"ResourceARN": arnStr,
		"Tags":        []map[string]string{{"Key": "aws:reserved", "Value": "v"}},
	})
	assert.Equal(t, http.StatusBadRequest, reservedRec.Code)

	var reservedErr map[string]any
	require.NoError(t, json.Unmarshal(reservedRec.Body.Bytes(), &reservedErr))
	assert.Equal(t, "WAFTagOperationException", reservedErr["__type"])
}

// ---- Gap 21: List pagination -----------------------------------------------

func TestAccuracy_ListPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create 5 WebACLs (names are alphabetically ordered).
	for i := range 5 {
		rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
			"Name":          fmt.Sprintf("acl-%02d", i),
			"Scope":         "REGIONAL",
			"DefaultAction": map[string]any{"Allow": map[string]any{}},
			"VisibilityConfig": map[string]any{
				"MetricName": fmt.Sprintf("acl-%02d", i),
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// List with limit 3 — should get first page + NextMarker.
	page1Rec := doWafv2Request(t, h, "ListWebACLs", map[string]any{
		"Scope": "REGIONAL",
		"Limit": 3,
	})
	require.Equal(t, http.StatusOK, page1Rec.Code)

	var page1Resp map[string]any
	require.NoError(t, json.Unmarshal(page1Rec.Body.Bytes(), &page1Resp))

	acls1, ok := page1Resp["WebACLs"].([]any)
	require.True(t, ok)
	assert.Len(t, acls1, 3, "first page should have 3 items")

	nextMarker, ok := page1Resp["NextMarker"].(string)
	require.True(t, ok, "NextMarker should be present after first page")
	assert.NotEmpty(t, nextMarker)

	// Decode and verify it's base64.
	decoded, err := base64.StdEncoding.DecodeString(nextMarker)
	require.NoError(t, err)
	assert.NotEmpty(t, string(decoded))

	// Fetch second page.
	page2Rec := doWafv2Request(t, h, "ListWebACLs", map[string]any{
		"Scope":      "REGIONAL",
		"Limit":      3,
		"NextMarker": nextMarker,
	})
	require.Equal(t, http.StatusOK, page2Rec.Code)

	var page2Resp map[string]any
	require.NoError(t, json.Unmarshal(page2Rec.Body.Bytes(), &page2Resp))

	acls2, ok := page2Resp["WebACLs"].([]any)
	require.True(t, ok)
	assert.Len(t, acls2, 2, "second page should have 2 items")

	_, hasNextMarker := page2Resp["NextMarker"]
	assert.False(t, hasNextMarker, "second page should not have NextMarker")
}

// ---- Gap 22: Scope validation on Get/Update/Delete -------------------------

func TestAccuracy_ScopeValidationOnGet(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a REGIONAL WebACL.
	createRec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":             "scope-test",
		"Scope":            "REGIONAL",
		"DefaultAction":    map[string]any{"Allow": map[string]any{}},
		"VisibilityConfig": map[string]any{"MetricName": "scope-test"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	id := createResp["Summary"].(map[string]any)["Id"].(string)

	// Get with wrong scope should fail.
	getRec := doWafv2Request(t, h, "GetWebACL", map[string]any{
		"Id":    id,
		"Scope": "CLOUDFRONT",
	})
	assert.Equal(t, http.StatusBadRequest, getRec.Code)

	// Get with correct scope should succeed.
	getRec2 := doWafv2Request(t, h, "GetWebACL", map[string]any{
		"Id":    id,
		"Scope": "REGIONAL",
	})
	assert.Equal(t, http.StatusOK, getRec2.Code)
}

// ---- Gap 23: APIKey base64 encoding ----------------------------------------

func TestAccuracy_APIKeyBase64Encoding(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "CreateAPIKey", map[string]any{
		"Scope":        "REGIONAL",
		"TokenDomains": []string{"example.com"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	apiKey, ok := resp["APIKey"].(string)
	require.True(t, ok)
	require.NotEmpty(t, apiKey)

	// The returned key should be base64-encoded.
	decoded, err := base64.StdEncoding.DecodeString(apiKey)
	require.NoError(t, err, "APIKey should be base64-encoded")
	assert.NotEmpty(t, string(decoded))

	// Validate TokenDomains limit (1–5).
	recTooMany := doWafv2Request(t, h, "CreateAPIKey", map[string]any{
		"Scope":        "REGIONAL",
		"TokenDomains": []string{"a.com", "b.com", "c.com", "d.com", "e.com", "f.com"},
	})
	assert.Equal(t, http.StatusBadRequest, recTooMany.Code)

	// Validate TokenDomains minimum.
	recNone := doWafv2Request(t, h, "CreateAPIKey", map[string]any{
		"Scope":        "REGIONAL",
		"TokenDomains": []string{},
	})
	assert.Equal(t, http.StatusBadRequest, recNone.Code)
}

// ---- Gap 24: Error header x-amzn-errortype ---------------------------------

func TestAccuracy_ErrorHeaderXAmznErrortype(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Trigger a not-found error.
	rec := doWafv2Request(t, h, "GetWebACL", map[string]any{"Id": "nonexistent"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, "WAFNonexistentItemException", rec.Header().Get("X-Amzn-Errortype"))

	// Trigger an invalid-parameter error.
	rec2 := doWafv2Request(t, h, "CreateWebACL", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
	assert.Equal(t, "WAFInvalidParameterException", rec2.Header().Get("X-Amzn-Errortype"))
}

// ---- Gap 15: GetSampledRequests validation ----------------------------------

func TestAccuracy_GetSampledRequestsValidation(t *testing.T) {
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

func TestAccuracy_WebACLExtendedFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":             "extended-acl",
		"Scope":            "REGIONAL",
		"DefaultAction":    map[string]any{"Allow": map[string]any{}},
		"VisibilityConfig": map[string]any{"MetricName": "extended-acl"},
		"TokenDomains":     []string{"example.com", "api.example.com"},
		"CaptchaConfig": map[string]any{
			"ImmunityTimeProperty": map[string]any{"ImmunityTime": 300},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	id := createResp["Summary"].(map[string]any)["Id"].(string)

	getRec := doWafv2Request(t, h, "GetWebACL", map[string]any{"Id": id})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	webACL := getResp["WebACL"].(map[string]any)

	// TokenDomains should be round-tripped.
	domains, ok := webACL["TokenDomains"].([]any)
	require.True(t, ok, "TokenDomains should be in response")
	assert.Len(t, domains, 2)

	// CaptchaConfig should be round-tripped.
	cc, ok := webACL["CaptchaConfig"].(map[string]any)
	require.True(t, ok, "CaptchaConfig should be in response")
	assert.Contains(t, cc, "ImmunityTimeProperty")
}

// ---- IPSet pagination -------------------------------------------------------

func TestAccuracy_IPSetListPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 5 {
		rec := doWafv2Request(t, h, "CreateIPSet", map[string]any{
			"Name":             fmt.Sprintf("ipset-%02d", i),
			"Scope":            "REGIONAL",
			"IPAddressVersion": "IPV4",
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	page1Rec := doWafv2Request(t, h, "ListIPSets", map[string]any{
		"Scope": "REGIONAL",
		"Limit": 2,
	})
	require.Equal(t, http.StatusOK, page1Rec.Code)

	var page1Resp map[string]any
	require.NoError(t, json.Unmarshal(page1Rec.Body.Bytes(), &page1Resp))

	sets, _ := page1Resp["IPSets"].([]any)
	assert.Len(t, sets, 2)

	nextMarker, _ := page1Resp["NextMarker"].(string)
	assert.NotEmpty(t, nextMarker)
}

// ---- RuleGroup scope validation --------------------------------------------

func TestAccuracy_RuleGroupScopeValidationOnGet(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doWafv2Request(t, h, "CreateRuleGroup", map[string]any{
		"Name":     "rg-scope",
		"Scope":    "REGIONAL",
		"Capacity": 10,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	id := createResp["Summary"].(map[string]any)["Id"].(string)

	// Get with wrong scope should fail.
	getRec := doWafv2Request(t, h, "GetRuleGroup", map[string]any{
		"Id":    id,
		"Scope": "CLOUDFRONT",
	})
	assert.Equal(t, http.StatusBadRequest, getRec.Code)

	// Get with correct scope should succeed.
	getRec2 := doWafv2Request(t, h, "GetRuleGroup", map[string]any{
		"Id":    id,
		"Scope": "REGIONAL",
	})
	assert.Equal(t, http.StatusOK, getRec2.Code)
}

// ---- IPSet update CIDR validation ------------------------------------------

func TestAccuracy_IPSetUpdateCIDRValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	id, _ := createIPSetHelper2(t, h, "update-set", "REGIONAL", "IPV4", nil)

	// Update with invalid CIDR.
	recBad := doWafv2Request(t, h, "UpdateIPSet", map[string]any{
		"Id":        id,
		"Addresses": []string{"not-valid"},
	})
	assert.Equal(t, http.StatusBadRequest, recBad.Code)

	// Update with wrong IP version.
	recMix := doWafv2Request(t, h, "UpdateIPSet", map[string]any{
		"Id":        id,
		"Addresses": []string{"2001:db8::/32"}, // IPv6 in IPv4 set
	})
	assert.Equal(t, http.StatusBadRequest, recMix.Code)

	// Update with valid CIDR.
	recOK := doWafv2Request(t, h, "UpdateIPSet", map[string]any{
		"Id":        id,
		"Addresses": []string{"10.0.0.0/8"},
	})
	assert.Equal(t, http.StatusOK, recOK.Code)
}
