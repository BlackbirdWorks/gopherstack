package waf_test

// handler_audit1_test.go — WAF Classic AWS-accuracy audit batch-1 (go-l7os).
//
// Covers:
//   - Change tokens: GetChangeToken, GetChangeTokenStatus
//   - WebACL lifecycle: CreateWebACL, GetWebACL, UpdateWebACL (rules + defaultAction), DeleteWebACL, ListWebACLs
//   - Rule lifecycle: CreateRule, GetRule, UpdateRule (predicates INSERT/DELETE), DeleteRule, ListRules
//   - IPSet lifecycle: CreateIPSet, GetIPSet, UpdateIPSet (descriptors), DeleteIPSet, ListIPSets
//   - ByteMatchSet lifecycle: Create, Get, Update, Delete, List
//   - SizeConstraintSet lifecycle: Create, Get, Update, Delete, List
//   - SqlInjectionMatchSet lifecycle: Create, Get, Update, Delete, List
//   - XssMatchSet lifecycle: Create, Get, Update, Delete, List
//   - GeoMatchSet lifecycle: Create, Get, Update, Delete, List
//   - Tags: TagResource, UntagResource, ListTagsForResource
//   - GetSampledRequests stub
//   - Error paths: not-found for all resource types, unknown operation → 400

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/waf"
)

// --- helpers ---

func newWAFHandler(t *testing.T) *waf.Handler {
	t.Helper()

	return waf.NewHandler(waf.NewInMemoryBackend("123456789012", "us-east-1"))
}

func wafDo(t *testing.T, h *waf.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var b []byte
	if body != nil {
		var err error
		b, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSWAF_20150824."+action)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)
	require.NoError(t, h.Handler()(c))

	return rec
}

func wafGetToken(t *testing.T, h *waf.Handler) string {
	t.Helper()

	rec := wafDo(t, h, "GetChangeToken", nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	token, ok := resp["ChangeToken"].(string)
	require.True(t, ok)
	require.NotEmpty(t, token)

	return token
}

func wafCreateWebACL(t *testing.T, h *waf.Handler, name string) string {
	t.Helper()

	token := wafGetToken(t, h)
	rec := wafDo(t, h, "CreateWebACL", map[string]any{
		"ChangeToken":   token,
		"Name":          name,
		"MetricName":    name + "Metric",
		"DefaultAction": map[string]any{"Type": "ALLOW"},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	aclMap, ok := resp["WebACL"].(map[string]any)
	require.True(t, ok)
	id, ok := aclMap["WebACLId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, id)

	return id
}

func wafCreateRule(t *testing.T, h *waf.Handler, name string) string {
	t.Helper()

	token := wafGetToken(t, h)
	rec := wafDo(t, h, "CreateRule", map[string]any{
		"ChangeToken": token,
		"Name":        name,
		"MetricName":  name + "Metric",
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	ruleMap := resp["Rule"].(map[string]any)
	id := ruleMap["RuleId"].(string)
	require.NotEmpty(t, id)

	return id
}

func wafCreateIPSet(t *testing.T, h *waf.Handler, name string) string {
	t.Helper()

	token := wafGetToken(t, h)
	rec := wafDo(t, h, "CreateIPSet", map[string]any{
		"ChangeToken": token,
		"Name":        name,
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	m := resp["IPSet"].(map[string]any)
	id := m["IPSetId"].(string)
	require.NotEmpty(t, id)

	return id
}

// --- §1 Change tokens ---

func TestWAF_ChangeToken_Basic(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	token := wafGetToken(t, h)
	assert.NotEmpty(t, token)
}

func TestWAF_ChangeToken_Unique(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	t1 := wafGetToken(t, h)
	t2 := wafGetToken(t, h)
	assert.NotEqual(t, t1, t2, "each GetChangeToken should return a unique token")
}

func TestWAF_GetChangeTokenStatus_INSYNC(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	token := wafGetToken(t, h)

	rec := wafDo(t, h, "GetChangeTokenStatus", map[string]any{
		"ChangeToken": token,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "INSYNC", resp["ChangeTokenStatus"])
}

// --- §2 WebACL lifecycle ---

func TestWAF_WebACL_CreateGetDeleteList(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)

	// Create
	id := wafCreateWebACL(t, h, "my-acl")
	assert.Equal(t, 1, waf.WebACLCount(h.Backend.(*waf.InMemoryBackend)))

	// Get
	rec := wafDo(t, h, "GetWebACL", map[string]any{"WebACLId": id})
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	acl := getResp["WebACL"].(map[string]any)
	assert.Equal(t, "my-acl", acl["Name"])
	assert.Equal(t, id, acl["WebACLId"])
	assert.NotEmpty(t, acl["WebACLArn"])

	// DefaultAction round-trip
	da := acl["DefaultAction"].(map[string]any)
	assert.Equal(t, "ALLOW", da["Type"])

	// List
	rec = wafDo(t, h, "ListWebACLs", map[string]any{"Limit": 100})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	acls := listResp["WebACLs"].([]any)
	assert.Len(t, acls, 1)

	// Delete
	token := wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteWebACL", map[string]any{
		"ChangeToken": token,
		"WebACLId":    id,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, waf.WebACLCount(h.Backend.(*waf.InMemoryBackend)))
}

func TestWAF_WebACL_DefaultAction_BLOCK(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	token := wafGetToken(t, h)
	rec := wafDo(t, h, "CreateWebACL", map[string]any{
		"ChangeToken":   token,
		"Name":          "block-acl",
		"MetricName":    "blockAclMetric",
		"DefaultAction": map[string]any{"Type": "BLOCK"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	acl := resp["WebACL"].(map[string]any)
	da := acl["DefaultAction"].(map[string]any)
	assert.Equal(t, "BLOCK", da["Type"])
}

func TestWAF_WebACL_UpdateDefaultAction(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	id := wafCreateWebACL(t, h, "update-acl")
	token := wafGetToken(t, h)

	rec := wafDo(t, h, "UpdateWebACL", map[string]any{
		"ChangeToken":   token,
		"WebACLId":      id,
		"DefaultAction": map[string]any{"Type": "BLOCK"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = wafDo(t, h, "GetWebACL", map[string]any{"WebACLId": id})
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	acl := resp["WebACL"].(map[string]any)
	da := acl["DefaultAction"].(map[string]any)
	assert.Equal(t, "BLOCK", da["Type"])
}

func TestWAF_WebACL_UpdateRules_InsertAndDelete(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	aclID := wafCreateWebACL(t, h, "rules-acl")
	ruleID := wafCreateRule(t, h, "rule-1")

	// Insert rule into WebACL
	token := wafGetToken(t, h)
	rec := wafDo(t, h, "UpdateWebACL", map[string]any{
		"ChangeToken": token,
		"WebACLId":    aclID,
		"Updates": []map[string]any{
			{
				"Action": "INSERT",
				"ActivatedRule": map[string]any{
					"Priority": 1,
					"RuleId":   ruleID,
					"Action":   map[string]any{"Type": "BLOCK"},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify rule present
	rec = wafDo(t, h, "GetWebACL", map[string]any{"WebACLId": aclID})
	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	acl := getResp["WebACL"].(map[string]any)
	rules := acl["Rules"].([]any)
	require.Len(t, rules, 1)
	rule := rules[0].(map[string]any)
	assert.Equal(t, ruleID, rule["RuleId"])
	assert.EqualValues(t, 1, rule["Priority"])

	// Delete rule from WebACL
	token = wafGetToken(t, h)
	rec = wafDo(t, h, "UpdateWebACL", map[string]any{
		"ChangeToken": token,
		"WebACLId":    aclID,
		"Updates": []map[string]any{
			{
				"Action": "DELETE",
				"ActivatedRule": map[string]any{
					"Priority": 1,
					"RuleId":   ruleID,
					"Action":   map[string]any{"Type": "BLOCK"},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = wafDo(t, h, "GetWebACL", map[string]any{"WebACLId": aclID})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	acl = getResp["WebACL"].(map[string]any)
	rules = acl["Rules"].([]any)
	assert.Empty(t, rules)
}

func TestWAF_WebACL_RulesPrioritySorted(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	aclID := wafCreateWebACL(t, h, "sorted-acl")
	rule1 := wafCreateRule(t, h, "high-pri")
	rule2 := wafCreateRule(t, h, "low-pri")

	token := wafGetToken(t, h)
	rec := wafDo(t, h, "UpdateWebACL", map[string]any{
		"ChangeToken": token,
		"WebACLId":    aclID,
		"Updates": []map[string]any{
			{
				"Action": "INSERT",
				"ActivatedRule": map[string]any{
					"Priority": 10,
					"RuleId":   rule2,
					"Action":   map[string]any{"Type": "ALLOW"},
				},
			},
			{
				"Action": "INSERT",
				"ActivatedRule": map[string]any{
					"Priority": 1,
					"RuleId":   rule1,
					"Action":   map[string]any{"Type": "BLOCK"},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = wafDo(t, h, "GetWebACL", map[string]any{"WebACLId": aclID})
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	rules := resp["WebACL"].(map[string]any)["Rules"].([]any)
	require.Len(t, rules, 2)
	assert.EqualValues(t, 1, rules[0].(map[string]any)["Priority"])
	assert.EqualValues(t, 10, rules[1].(map[string]any)["Priority"])
}

func TestWAF_WebACL_NotFound(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	rec := wafDo(t, h, "GetWebACL", map[string]any{"WebACLId": "nonexistent"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "WAFNonexistentItemException", errResp["__type"])
}

func TestWAF_WebACL_ChangeTokenRoundTrip(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	token := wafGetToken(t, h)
	rec := wafDo(t, h, "CreateWebACL", map[string]any{
		"ChangeToken":   token,
		"Name":          "token-acl",
		"MetricName":    "tokenAclMetric",
		"DefaultAction": map[string]any{"Type": "ALLOW"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, token, resp["ChangeToken"])
}

// --- §3 Rule lifecycle ---

func TestWAF_Rule_CreateGetDeleteList(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)

	id := wafCreateRule(t, h, "my-rule")
	assert.Equal(t, 1, waf.RuleCount(h.Backend.(*waf.InMemoryBackend)))

	// Get
	rec := wafDo(t, h, "GetRule", map[string]any{"RuleId": id})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	rule := resp["Rule"].(map[string]any)
	assert.Equal(t, "my-rule", rule["Name"])
	assert.Equal(t, id, rule["RuleId"])

	// List
	rec = wafDo(t, h, "ListRules", map[string]any{"Limit": 100})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	rules := listResp["Rules"].([]any)
	assert.Len(t, rules, 1)

	// Delete
	token := wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteRule", map[string]any{
		"ChangeToken": token,
		"RuleId":      id,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, waf.RuleCount(h.Backend.(*waf.InMemoryBackend)))
}

func TestWAF_Rule_UpdatePredicates_InsertDelete(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	id := wafCreateRule(t, h, "pred-rule")
	ipSetID := wafCreateIPSet(t, h, "my-ipset")

	// Insert predicate
	token := wafGetToken(t, h)
	rec := wafDo(t, h, "UpdateRule", map[string]any{
		"ChangeToken": token,
		"RuleId":      id,
		"Updates": []map[string]any{
			{
				"Action": "INSERT",
				"Predicate": map[string]any{
					"Type":    "IPMatch",
					"DataId":  ipSetID,
					"Negated": false,
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify predicate
	rec = wafDo(t, h, "GetRule", map[string]any{"RuleId": id})
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	rule := resp["Rule"].(map[string]any)
	predicates := rule["Predicates"].([]any)
	require.Len(t, predicates, 1)
	pred := predicates[0].(map[string]any)
	assert.Equal(t, "IPMatch", pred["Type"])
	assert.Equal(t, ipSetID, pred["DataId"])

	// Delete predicate
	token = wafGetToken(t, h)
	rec = wafDo(t, h, "UpdateRule", map[string]any{
		"ChangeToken": token,
		"RuleId":      id,
		"Updates": []map[string]any{
			{
				"Action": "DELETE",
				"Predicate": map[string]any{
					"Type":    "IPMatch",
					"DataId":  ipSetID,
					"Negated": false,
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = wafDo(t, h, "GetRule", map[string]any{"RuleId": id})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	rule = resp["Rule"].(map[string]any)
	predicates = rule["Predicates"].([]any)
	assert.Empty(t, predicates)
}

func TestWAF_Rule_NotFound(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	rec := wafDo(t, h, "GetRule", map[string]any{"RuleId": "nonexistent"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- §4 IPSet lifecycle ---

func TestWAF_IPSet_CreateGetUpdateDeleteList(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	id := wafCreateIPSet(t, h, "my-ips")
	assert.Equal(t, 1, waf.IPSetCount(h.Backend.(*waf.InMemoryBackend)))

	// Get - empty descriptors
	rec := wafDo(t, h, "GetIPSet", map[string]any{"IPSetId": id})
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	ipSet := resp["IPSet"].(map[string]any)
	assert.Equal(t, "my-ips", ipSet["Name"])
	descs := ipSet["IPSetDescriptors"].([]any)
	assert.Empty(t, descs)

	// Update: insert descriptor
	token := wafGetToken(t, h)
	rec = wafDo(t, h, "UpdateIPSet", map[string]any{
		"ChangeToken": token,
		"IPSetId":     id,
		"Updates": []map[string]any{
			{
				"Action": "INSERT",
				"IPSetDescriptor": map[string]any{
					"Type":  "IPV4",
					"Value": "192.0.2.0/24",
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify
	rec = wafDo(t, h, "GetIPSet", map[string]any{"IPSetId": id})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	ipSet = resp["IPSet"].(map[string]any)
	descs = ipSet["IPSetDescriptors"].([]any)
	require.Len(t, descs, 1)
	desc := descs[0].(map[string]any)
	assert.Equal(t, "IPV4", desc["Type"])
	assert.Equal(t, "192.0.2.0/24", desc["Value"])

	// Delete descriptor
	token = wafGetToken(t, h)
	rec = wafDo(t, h, "UpdateIPSet", map[string]any{
		"ChangeToken": token,
		"IPSetId":     id,
		"Updates": []map[string]any{
			{
				"Action": "DELETE",
				"IPSetDescriptor": map[string]any{
					"Type":  "IPV4",
					"Value": "192.0.2.0/24",
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = wafDo(t, h, "GetIPSet", map[string]any{"IPSetId": id})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	ipSet = resp["IPSet"].(map[string]any)
	descs = ipSet["IPSetDescriptors"].([]any)
	assert.Empty(t, descs)

	// List
	rec = wafDo(t, h, "ListIPSets", map[string]any{"Limit": 100})
	require.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	sets := listResp["IPSets"].([]any)
	assert.Len(t, sets, 1)

	// Delete
	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteIPSet", map[string]any{
		"ChangeToken": token,
		"IPSetId":     id,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, waf.IPSetCount(h.Backend.(*waf.InMemoryBackend)))
}

func TestWAF_IPSet_IPv6Descriptor(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	id := wafCreateIPSet(t, h, "ipv6-set")

	token := wafGetToken(t, h)
	rec := wafDo(t, h, "UpdateIPSet", map[string]any{
		"ChangeToken": token,
		"IPSetId":     id,
		"Updates": []map[string]any{
			{
				"Action": "INSERT",
				"IPSetDescriptor": map[string]any{
					"Type":  "IPV6",
					"Value": "2001:db8::/32",
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = wafDo(t, h, "GetIPSet", map[string]any{"IPSetId": id})
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	ipSet := resp["IPSet"].(map[string]any)
	descs := ipSet["IPSetDescriptors"].([]any)
	require.Len(t, descs, 1)
	assert.Equal(t, "IPV6", descs[0].(map[string]any)["Type"])
}

func TestWAF_IPSet_NotFound(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	rec := wafDo(t, h, "GetIPSet", map[string]any{"IPSetId": "nonexistent"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- §5 ByteMatchSet lifecycle ---

func TestWAF_ByteMatchSet_CreateGetUpdateDeleteList(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	token := wafGetToken(t, h)
	rec := wafDo(t, h, "CreateByteMatchSet", map[string]any{
		"ChangeToken": token,
		"Name":        "my-bms",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	bmsMap := createResp["ByteMatchSet"].(map[string]any)
	id := bmsMap["ByteMatchSetId"].(string)
	require.NotEmpty(t, id)
	assert.Equal(t, 1, waf.ByteMatchSetCount(h.Backend.(*waf.InMemoryBackend)))

	// Get
	rec = wafDo(t, h, "GetByteMatchSet", map[string]any{"ByteMatchSetId": id})
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	bms := resp["ByteMatchSet"].(map[string]any)
	assert.Equal(t, "my-bms", bms["Name"])

	// Update: insert tuple
	token = wafGetToken(t, h)
	rec = wafDo(t, h, "UpdateByteMatchSet", map[string]any{
		"ChangeToken":    token,
		"ByteMatchSetId": id,
		"Updates": []map[string]any{
			{
				"Action": "INSERT",
				"ByteMatchTuple": map[string]any{
					"FieldToMatch":         map[string]any{"Type": "URI"},
					"TargetString":         "/admin",
					"PositionalConstraint": "STARTS_WITH",
					"TextTransformation":   "NONE",
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = wafDo(t, h, "GetByteMatchSet", map[string]any{"ByteMatchSetId": id})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	bms = resp["ByteMatchSet"].(map[string]any)
	tuples := bms["ByteMatchTuples"].([]any)
	require.Len(t, tuples, 1)
	tuple := tuples[0].(map[string]any)
	assert.Equal(t, "/admin", tuple["TargetString"])
	assert.Equal(t, "STARTS_WITH", tuple["PositionalConstraint"])

	// List
	rec = wafDo(t, h, "ListByteMatchSets", map[string]any{"Limit": 100})
	require.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	sets := listResp["ByteMatchSets"].([]any)
	assert.Len(t, sets, 1)

	// Delete
	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteByteMatchSet", map[string]any{
		"ChangeToken":    token,
		"ByteMatchSetId": id,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, waf.ByteMatchSetCount(h.Backend.(*waf.InMemoryBackend)))
}

func TestWAF_ByteMatchSet_NotFound(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	rec := wafDo(t, h, "GetByteMatchSet", map[string]any{"ByteMatchSetId": "nope"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- §6 SizeConstraintSet lifecycle ---

func TestWAF_SizeConstraintSet_CreateGetUpdateDeleteList(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	token := wafGetToken(t, h)
	rec := wafDo(t, h, "CreateSizeConstraintSet", map[string]any{
		"ChangeToken": token,
		"Name":        "my-scs",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	scsMap := createResp["SizeConstraintSet"].(map[string]any)
	id := scsMap["SizeConstraintSetId"].(string)
	require.NotEmpty(t, id)

	// Update
	token = wafGetToken(t, h)
	rec = wafDo(t, h, "UpdateSizeConstraintSet", map[string]any{
		"ChangeToken":         token,
		"SizeConstraintSetId": id,
		"Updates": []map[string]any{
			{
				"Action": "INSERT",
				"SizeConstraint": map[string]any{
					"FieldToMatch":       map[string]any{"Type": "BODY"},
					"TextTransformation": "NONE",
					"ComparisonOperator": "GT",
					"Size":               8192,
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = wafDo(t, h, "GetSizeConstraintSet", map[string]any{"SizeConstraintSetId": id})
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	scs := resp["SizeConstraintSet"].(map[string]any)
	constraints := scs["SizeConstraints"].([]any)
	require.Len(t, constraints, 1)
	c := constraints[0].(map[string]any)
	assert.Equal(t, "GT", c["ComparisonOperator"])
	assert.EqualValues(t, 8192, c["Size"])

	// List
	rec = wafDo(t, h, "ListSizeConstraintSets", map[string]any{"Limit": 100})
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	sets := listResp["SizeConstraintSets"].([]any)
	assert.Len(t, sets, 1)

	// Delete
	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteSizeConstraintSet", map[string]any{
		"ChangeToken":         token,
		"SizeConstraintSetId": id,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, waf.SizeConstraintSetCount(h.Backend.(*waf.InMemoryBackend)))
}

func TestWAF_SizeConstraintSet_NotFound(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	rec := wafDo(t, h, "GetSizeConstraintSet", map[string]any{"SizeConstraintSetId": "nope"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- §7 SqlInjectionMatchSet lifecycle ---

func TestWAF_SqlInjectionMatchSet_CreateGetUpdateDeleteList(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	token := wafGetToken(t, h)
	rec := wafDo(t, h, "CreateSqlInjectionMatchSet", map[string]any{
		"ChangeToken": token,
		"Name":        "my-sqli",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	simsMap := createResp["SqlInjectionMatchSet"].(map[string]any)
	id := simsMap["SqlInjectionMatchSetId"].(string)
	require.NotEmpty(t, id)

	// Update
	token = wafGetToken(t, h)
	rec = wafDo(t, h, "UpdateSqlInjectionMatchSet", map[string]any{
		"ChangeToken":            token,
		"SqlInjectionMatchSetId": id,
		"Updates": []map[string]any{
			{
				"Action": "INSERT",
				"SqlInjectionMatchTuple": map[string]any{
					"FieldToMatch":       map[string]any{"Type": "QUERY_STRING"},
					"TextTransformation": "URL_DECODE",
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = wafDo(t, h, "GetSqlInjectionMatchSet", map[string]any{"SqlInjectionMatchSetId": id})
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	sims := resp["SqlInjectionMatchSet"].(map[string]any)
	tuples := sims["SqlInjectionMatchTuples"].([]any)
	require.Len(t, tuples, 1)
	tuple := tuples[0].(map[string]any)
	assert.Equal(t, "URL_DECODE", tuple["TextTransformation"])
	fm := tuple["FieldToMatch"].(map[string]any)
	assert.Equal(t, "QUERY_STRING", fm["Type"])

	// List
	rec = wafDo(t, h, "ListSqlInjectionMatchSets", map[string]any{"Limit": 100})
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	sets := listResp["SqlInjectionMatchSets"].([]any)
	assert.Len(t, sets, 1)

	// Delete
	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteSqlInjectionMatchSet", map[string]any{
		"ChangeToken":            token,
		"SqlInjectionMatchSetId": id,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, waf.SqlInjectionMatchSetCount(h.Backend.(*waf.InMemoryBackend)))
}

func TestWAF_SqlInjectionMatchSet_NotFound(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	rec := wafDo(t, h, "GetSqlInjectionMatchSet", map[string]any{"SqlInjectionMatchSetId": "nope"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- §8 XssMatchSet lifecycle ---

func TestWAF_XssMatchSet_CreateGetUpdateDeleteList(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	token := wafGetToken(t, h)
	rec := wafDo(t, h, "CreateXssMatchSet", map[string]any{
		"ChangeToken": token,
		"Name":        "my-xss",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	xmsMap := createResp["XssMatchSet"].(map[string]any)
	id := xmsMap["XssMatchSetId"].(string)
	require.NotEmpty(t, id)

	// Update
	token = wafGetToken(t, h)
	rec = wafDo(t, h, "UpdateXssMatchSet", map[string]any{
		"ChangeToken":   token,
		"XssMatchSetId": id,
		"Updates": []map[string]any{
			{
				"Action": "INSERT",
				"XssMatchTuple": map[string]any{
					"FieldToMatch":       map[string]any{"Type": "BODY"},
					"TextTransformation": "HTML_ENTITY_DECODE",
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = wafDo(t, h, "GetXssMatchSet", map[string]any{"XssMatchSetId": id})
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	xms := resp["XssMatchSet"].(map[string]any)
	tuples := xms["XssMatchTuples"].([]any)
	require.Len(t, tuples, 1)
	assert.Equal(t, "HTML_ENTITY_DECODE", tuples[0].(map[string]any)["TextTransformation"])

	// List
	rec = wafDo(t, h, "ListXssMatchSets", map[string]any{"Limit": 100})
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	sets := listResp["XssMatchSets"].([]any)
	assert.Len(t, sets, 1)

	// Delete
	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteXssMatchSet", map[string]any{
		"ChangeToken":   token,
		"XssMatchSetId": id,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, waf.XssMatchSetCount(h.Backend.(*waf.InMemoryBackend)))
}

func TestWAF_XssMatchSet_NotFound(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	rec := wafDo(t, h, "GetXssMatchSet", map[string]any{"XssMatchSetId": "nope"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- §9 GeoMatchSet lifecycle ---

func TestWAF_GeoMatchSet_CreateGetUpdateDeleteList(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	token := wafGetToken(t, h)
	rec := wafDo(t, h, "CreateGeoMatchSet", map[string]any{
		"ChangeToken": token,
		"Name":        "my-geo",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	gmsMap := createResp["GeoMatchSet"].(map[string]any)
	id := gmsMap["GeoMatchSetId"].(string)
	require.NotEmpty(t, id)

	// Update: insert constraint
	token = wafGetToken(t, h)
	rec = wafDo(t, h, "UpdateGeoMatchSet", map[string]any{
		"ChangeToken":   token,
		"GeoMatchSetId": id,
		"Updates": []map[string]any{
			{
				"Action": "INSERT",
				"GeoMatchConstraint": map[string]any{
					"Type":  "Country",
					"Value": "CN",
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = wafDo(t, h, "GetGeoMatchSet", map[string]any{"GeoMatchSetId": id})
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	gms := resp["GeoMatchSet"].(map[string]any)
	constraints := gms["GeoMatchConstraints"].([]any)
	require.Len(t, constraints, 1)
	c := constraints[0].(map[string]any)
	assert.Equal(t, "Country", c["Type"])
	assert.Equal(t, "CN", c["Value"])

	// Add second country, then delete first
	token = wafGetToken(t, h)
	rec = wafDo(t, h, "UpdateGeoMatchSet", map[string]any{
		"ChangeToken":   token,
		"GeoMatchSetId": id,
		"Updates": []map[string]any{
			{
				"Action": "INSERT",
				"GeoMatchConstraint": map[string]any{
					"Type":  "Country",
					"Value": "RU",
				},
			},
			{
				"Action": "DELETE",
				"GeoMatchConstraint": map[string]any{
					"Type":  "Country",
					"Value": "CN",
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = wafDo(t, h, "GetGeoMatchSet", map[string]any{"GeoMatchSetId": id})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	gms = resp["GeoMatchSet"].(map[string]any)
	constraints = gms["GeoMatchConstraints"].([]any)
	require.Len(t, constraints, 1)
	assert.Equal(t, "RU", constraints[0].(map[string]any)["Value"])

	// List
	rec = wafDo(t, h, "ListGeoMatchSets", map[string]any{"Limit": 100})
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	sets := listResp["GeoMatchSets"].([]any)
	assert.Len(t, sets, 1)

	// Delete
	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteGeoMatchSet", map[string]any{
		"ChangeToken":   token,
		"GeoMatchSetId": id,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, waf.GeoMatchSetCount(h.Backend.(*waf.InMemoryBackend)))
}

func TestWAF_GeoMatchSet_NotFound(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	rec := wafDo(t, h, "GetGeoMatchSet", map[string]any{"GeoMatchSetId": "nope"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- §10 Tags ---

func TestWAF_Tags_TagAndList(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	aclID := wafCreateWebACL(t, h, "tagged-acl")

	// Get the ARN
	rec := wafDo(t, h, "GetWebACL", map[string]any{"WebACLId": aclID})
	require.Equal(t, http.StatusOK, rec.Code)
	var aclResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &aclResp))
	arn := aclResp["WebACL"].(map[string]any)["WebACLArn"].(string)
	require.NotEmpty(t, arn)

	// TagResource
	rec = wafDo(t, h, "TagResource", map[string]any{
		"ResourceARN": arn,
		"Tags": []map[string]any{
			{"Key": "env", "Value": "prod"},
			{"Key": "team", "Value": "security"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// ListTagsForResource
	rec = wafDo(t, h, "ListTagsForResource", map[string]any{"ResourceARN": arn})
	require.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	info := listResp["TagInfoForResource"].(map[string]any)
	assert.Equal(t, arn, info["ResourceARN"])
	tagList := info["TagList"].([]any)
	require.Len(t, tagList, 2)

	// Collect tags
	tagMap := make(map[string]string)
	for _, t := range tagList {
		tMap := t.(map[string]any)
		tagMap[tMap["Key"].(string)] = tMap["Value"].(string)
	}

	assert.Equal(t, "prod", tagMap["env"])
	assert.Equal(t, "security", tagMap["team"])
}

func TestWAF_Tags_UntagResource(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	aclID := wafCreateWebACL(t, h, "untag-acl")

	rec := wafDo(t, h, "GetWebACL", map[string]any{"WebACLId": aclID})
	var aclResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &aclResp))
	arn := aclResp["WebACL"].(map[string]any)["WebACLArn"].(string)

	// Tag
	rec = wafDo(t, h, "TagResource", map[string]any{
		"ResourceARN": arn,
		"Tags": []map[string]any{
			{"Key": "k1", "Value": "v1"},
			{"Key": "k2", "Value": "v2"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Untag k1
	rec = wafDo(t, h, "UntagResource", map[string]any{
		"ResourceARN": arn,
		"TagKeys":     []string{"k1"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify only k2 remains
	rec = wafDo(t, h, "ListTagsForResource", map[string]any{"ResourceARN": arn})
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	tagList := listResp["TagInfoForResource"].(map[string]any)["TagList"].([]any)
	require.Len(t, tagList, 1)
	assert.Equal(t, "k2", tagList[0].(map[string]any)["Key"])
}

func TestWAF_Tags_CreateWebACLWithTags(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	token := wafGetToken(t, h)
	rec := wafDo(t, h, "CreateWebACL", map[string]any{
		"ChangeToken":   token,
		"Name":          "initial-tags-acl",
		"MetricName":    "initialTagsMetric",
		"DefaultAction": map[string]any{"Type": "ALLOW"},
		"Tags": []map[string]any{
			{"Key": "cost-center", "Value": "eng"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	arn := resp["WebACL"].(map[string]any)["WebACLArn"].(string)

	rec = wafDo(t, h, "ListTagsForResource", map[string]any{"ResourceARN": arn})
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	tagList := listResp["TagInfoForResource"].(map[string]any)["TagList"].([]any)
	require.Len(t, tagList, 1)
	assert.Equal(t, "cost-center", tagList[0].(map[string]any)["Key"])
}

// --- §11 GetSampledRequests ---

func TestWAF_GetSampledRequests_EmptyStub(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	aclID := wafCreateWebACL(t, h, "sample-acl")
	ruleID := wafCreateRule(t, h, "sample-rule")

	rec := wafDo(t, h, "GetSampledRequests", map[string]any{
		"WebAclId": aclID,
		"RuleId":   ruleID,
		"MaxItems": 100,
		"TimeWindow": map[string]any{
			"StartTime": "2024-01-01T00:00:00Z",
			"EndTime":   "2024-01-01T01:00:00Z",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	samples := resp["SampledRequests"].([]any)
	assert.Empty(t, samples)
	assert.EqualValues(t, 0, resp["PopulationSize"])
}

// --- §12 Error paths ---

func TestWAF_UnknownOperation_Returns400(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "AWSWAF_20150824.DoSomethingImaginary")
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestWAF_DeleteWebACL_NotFound(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	token := wafGetToken(t, h)
	rec := wafDo(t, h, "DeleteWebACL", map[string]any{
		"ChangeToken": token,
		"WebACLId":    "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "WAFNonexistentItemException", errResp["__type"])
}

func TestWAF_DeleteRule_NotFound(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	token := wafGetToken(t, h)
	rec := wafDo(t, h, "DeleteRule", map[string]any{
		"ChangeToken": token,
		"RuleId":      "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestWAF_DeleteIPSet_NotFound(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	token := wafGetToken(t, h)
	rec := wafDo(t, h, "DeleteIPSet", map[string]any{
		"ChangeToken": token,
		"IPSetId":     "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestWAF_UpdateWebACL_NotFound(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	token := wafGetToken(t, h)
	rec := wafDo(t, h, "UpdateWebACL", map[string]any{
		"ChangeToken": token,
		"WebACLId":    "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestWAF_UpdateRule_NotFound(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	token := wafGetToken(t, h)
	rec := wafDo(t, h, "UpdateRule", map[string]any{
		"ChangeToken": token,
		"RuleId":      "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestWAF_UpdateIPSet_NotFound(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	token := wafGetToken(t, h)
	rec := wafDo(t, h, "UpdateIPSet", map[string]any{
		"ChangeToken": token,
		"IPSetId":     "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- §13 Multiple resources ---

func TestWAF_MultipleWebACLs_List(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	wafCreateWebACL(t, h, "acl-1")
	wafCreateWebACL(t, h, "acl-2")
	wafCreateWebACL(t, h, "acl-3")

	rec := wafDo(t, h, "ListWebACLs", map[string]any{"Limit": 100})
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	acls := resp["WebACLs"].([]any)
	assert.Len(t, acls, 3)
}

func TestWAF_MultipleIPSets_List(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	wafCreateIPSet(t, h, "ips-1")
	wafCreateIPSet(t, h, "ips-2")

	rec := wafDo(t, h, "ListIPSets", map[string]any{"Limit": 100})
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	sets := resp["IPSets"].([]any)
	assert.Len(t, sets, 2)
}

// --- §14 Snapshot/Restore ---

func TestWAF_SnapshotRestore(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	wafCreateWebACL(t, h, "snap-acl")
	wafCreateRule(t, h, "snap-rule")
	wafCreateIPSet(t, h, "snap-ipset")

	snap := h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	h2 := newWAFHandler(t)
	require.NoError(t, h2.Restore(t.Context(), snap))

	assert.Equal(t, 1, waf.WebACLCount(h2.Backend.(*waf.InMemoryBackend)))
	assert.Equal(t, 1, waf.RuleCount(h2.Backend.(*waf.InMemoryBackend)))
	assert.Equal(t, 1, waf.IPSetCount(h2.Backend.(*waf.InMemoryBackend)))
}

// --- §15 SDKCompleteness meta-test ---

func TestWAF_HandlerOpsNonZero(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	assert.Greater(t, waf.HandlerOpsLen(h), 40,
		"WAF Classic handler should implement at least 40 operations")
}
