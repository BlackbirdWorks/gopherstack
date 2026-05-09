package route53resolver_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func decodeJSON(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&m))

	return m
}

// TestR53R_FirewallRuleGroupCRUD covers CreateFirewallRuleGroup, GetFirewallRuleGroup,
// ListFirewallRuleGroups, DeleteFirewallRuleGroup, GetFirewallRuleGroupPolicy,
// PutFirewallRuleGroupPolicy.
func TestR53R_FirewallRuleGroupCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// CreateFirewallRuleGroup.
	rec := doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{
		"Name":             "test-rule-group",
		"CreatorRequestId": "req-1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	createResp := decodeJSON(t, rec)
	rg, _ := createResp["FirewallRuleGroup"].(map[string]any)
	rgID, _ := rg["Id"].(string)
	require.NotEmpty(t, rgID)

	// GetFirewallRuleGroup.
	rec = doRequest(t, h, "GetFirewallRuleGroup", map[string]any{"FirewallRuleGroupId": rgID})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListFirewallRuleGroups.
	rec = doRequest(t, h, "ListFirewallRuleGroups", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	// PutFirewallRuleGroupPolicy.
	rec = doRequest(t, h, "PutFirewallRuleGroupPolicy", map[string]any{
		"Arn":                     "arn:aws:route53resolver:us-east-1:000000000000:firewall-rule-group/" + rgID,
		"FirewallRuleGroupPolicy": `{"Version":"2012-10-17","Statement":[]}`,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetFirewallRuleGroupPolicy.
	rec = doRequest(t, h, "GetFirewallRuleGroupPolicy", map[string]any{
		"Arn": "arn:aws:route53resolver:us-east-1:000000000000:firewall-rule-group/" + rgID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeleteFirewallRuleGroup.
	rec = doRequest(t, h, "DeleteFirewallRuleGroup", map[string]any{"FirewallRuleGroupId": rgID})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestR53R_FirewallDomainListCRUD covers CreateFirewallDomainList, GetFirewallDomainList,
// ListFirewallDomainLists, ListFirewallDomains, UpdateFirewallDomains,
// ImportFirewallDomains, DeleteFirewallDomainList.
func TestR53R_FirewallDomainListCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// CreateFirewallDomainList.
	rec := doRequest(t, h, "CreateFirewallDomainList", map[string]any{
		"Name":             "test-domain-list",
		"CreatorRequestId": "req-2",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	createResp := decodeJSON(t, rec)
	dl, _ := createResp["FirewallDomainList"].(map[string]any)
	dlID, _ := dl["Id"].(string)
	require.NotEmpty(t, dlID)

	// GetFirewallDomainList.
	rec = doRequest(t, h, "GetFirewallDomainList", map[string]any{"FirewallDomainListId": dlID})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListFirewallDomainLists.
	rec = doRequest(t, h, "ListFirewallDomainLists", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	// UpdateFirewallDomains.
	rec = doRequest(t, h, "UpdateFirewallDomains", map[string]any{
		"FirewallDomainListId": dlID,
		"Operation":            "ADD",
		"Domains":              []string{"example.com"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListFirewallDomains.
	rec = doRequest(t, h, "ListFirewallDomains", map[string]any{"FirewallDomainListId": dlID})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ImportFirewallDomains.
	rec = doRequest(t, h, "ImportFirewallDomains", map[string]any{
		"FirewallDomainListId": dlID,
		"Operation":            "REPLACE",
		"DomainFileUrl":        "s3://my-bucket/domains.txt",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeleteFirewallDomainList.
	rec = doRequest(t, h, "DeleteFirewallDomainList", map[string]any{"FirewallDomainListId": dlID})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestR53R_FirewallRuleCRUD covers CreateFirewallRule, ListFirewallRules,
// UpdateFirewallRule, DeleteFirewallRule.
func TestR53R_FirewallRuleCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a rule group and domain list.
	rec := doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{
		"Name":             "rule-test-group",
		"CreatorRequestId": "req-3",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	rgResp := decodeJSON(t, rec)
	rg, _ := rgResp["FirewallRuleGroup"].(map[string]any)
	rgID, _ := rg["Id"].(string)

	rec = doRequest(t, h, "CreateFirewallDomainList", map[string]any{
		"Name":             "rule-test-domains",
		"CreatorRequestId": "req-4",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	dlResp := decodeJSON(t, rec)
	dl, _ := dlResp["FirewallDomainList"].(map[string]any)
	dlID, _ := dl["Id"].(string)

	// CreateFirewallRule.
	rec = doRequest(t, h, "CreateFirewallRule", map[string]any{
		"FirewallRuleGroupId":  rgID,
		"FirewallDomainListId": dlID,
		"Priority":             100,
		"Action":               "BLOCK",
		"BlockResponse":        "NODATA",
		"Name":                 "test-rule",
		"CreatorRequestId":     "req-5",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	ruleResp := decodeJSON(t, rec)
	rule, _ := ruleResp["FirewallRule"].(map[string]any)
	ruleID, _ := rule["Id"].(string)
	require.NotEmpty(t, ruleID)

	// ListFirewallRules.
	rec = doRequest(t, h, "ListFirewallRules", map[string]any{"FirewallRuleGroupId": rgID})
	assert.Equal(t, http.StatusOK, rec.Code)

	// UpdateFirewallRule.
	rec = doRequest(t, h, "UpdateFirewallRule", map[string]any{
		"FirewallRuleId": ruleID,
		"Priority":       200,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeleteFirewallRule.
	rec = doRequest(t, h, "DeleteFirewallRule", map[string]any{
		"FirewallRuleId": ruleID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestR53R_FirewallRuleGroupAssociation covers AssociateFirewallRuleGroup,
// GetFirewallRuleGroupAssociation, ListFirewallRuleGroupAssociations,
// UpdateFirewallRuleGroupAssociation, DisassociateFirewallRuleGroup.
func TestR53R_FirewallRuleGroupAssociation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a rule group.
	rec := doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{
		"Name":             "assoc-test-group",
		"CreatorRequestId": "req-6",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	rgResp := decodeJSON(t, rec)
	rg, _ := rgResp["FirewallRuleGroup"].(map[string]any)
	rgID, _ := rg["Id"].(string)

	vpcID := "vpc-12345678"

	// AssociateFirewallRuleGroup.
	rec = doRequest(t, h, "AssociateFirewallRuleGroup", map[string]any{
		"FirewallRuleGroupId": rgID,
		"VpcId":               vpcID,
		"Priority":            100,
		"Name":                "test-assoc",
		"CreatorRequestId":    "req-7",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assocResp := decodeJSON(t, rec)
	assoc, _ := assocResp["FirewallRuleGroupAssociation"].(map[string]any)
	assocID, _ := assoc["Id"].(string)
	require.NotEmpty(t, assocID)

	// GetFirewallRuleGroupAssociation.
	rec = doRequest(t, h, "GetFirewallRuleGroupAssociation", map[string]any{"FirewallRuleGroupAssociationId": assocID})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListFirewallRuleGroupAssociations.
	rec = doRequest(t, h, "ListFirewallRuleGroupAssociations", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	// UpdateFirewallRuleGroupAssociation.
	rec = doRequest(t, h, "UpdateFirewallRuleGroupAssociation", map[string]any{
		"FirewallRuleGroupAssociationId": assocID,
		"Priority":                       200,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// DisassociateFirewallRuleGroup.
	rec = doRequest(t, h, "DisassociateFirewallRuleGroup", map[string]any{"FirewallRuleGroupAssociationId": assocID})
	assert.Equal(t, http.StatusOK, rec.Code)
}
