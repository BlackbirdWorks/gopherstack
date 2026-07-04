package route53resolver_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_ListResolverEndpointIpAddresses_Pagination verifies NextToken/MaxResults
// on ListResolverEndpointIpAddresses. Real AWS paginates IP addresses per endpoint.
func TestParity_ListResolverEndpointIpAddresses_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
		"Name":        "ep-paginate",
		"Direction":   "INBOUND",
		"IpAddresses": []map[string]string{{"SubnetId": "subnet-1", "Ip": "10.0.0.1"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var createOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	epID := createOut["ResolverEndpoint"].(map[string]any)["Id"].(string)

	for i := 2; i <= 4; i++ {
		ipRec := doRequest(t, h, "AssociateResolverEndpointIpAddress", map[string]any{
			"ResolverEndpointId": epID,
			"IpAddress": map[string]string{
				"SubnetId": fmt.Sprintf("subnet-%d", i),
				"Ip":       fmt.Sprintf("10.0.0.%d", i),
			},
		})
		require.Equal(t, http.StatusOK, ipRec.Code)
	}

	tests := []struct {
		body          map[string]any
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			body:          map[string]any{"ResolverEndpointId": epID},
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			body:          map[string]any{"ResolverEndpointId": epID, "MaxResults": float64(2)},
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			listRec := doRequest(t, h, "ListResolverEndpointIpAddresses", tt.body)
			require.Equal(t, http.StatusOK, listRec.Code)
			var out map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
			ips, _ := out["IpAddresses"].([]any)
			assert.Len(t, ips, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out["NextToken"])
			} else {
				assert.Empty(t, out["NextToken"])
			}
		})
	}
}

// TestParity_ListFirewallRuleGroupAssociations_Pagination verifies NextToken/MaxResults
// on ListFirewallRuleGroupAssociations. Real AWS paginates associations.
func TestParity_ListFirewallRuleGroupAssociations_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 3 {
		grpRec := doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{
			"Name": fmt.Sprintf("grp-%d", i),
		})
		require.Equal(t, http.StatusOK, grpRec.Code)
		var grpOut map[string]any
		require.NoError(t, json.Unmarshal(grpRec.Body.Bytes(), &grpOut))
		grpID := grpOut["FirewallRuleGroup"].(map[string]any)["Id"].(string)

		assocRec := doRequest(t, h, "AssociateFirewallRuleGroup", map[string]any{
			"FirewallRuleGroupId": grpID,
			"VpcId":               fmt.Sprintf("vpc-fwassoc-%d", i),
			"Priority":            int32(100 + i),
		})
		require.Equal(t, http.StatusOK, assocRec.Code)
	}

	tests := []struct {
		body          map[string]any
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			body:          map[string]any{},
			wantLen:       3,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			body:          map[string]any{"MaxResults": float64(2)},
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			listRec := doRequest(t, h, "ListFirewallRuleGroupAssociations", tt.body)
			require.Equal(t, http.StatusOK, listRec.Code)
			var out map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
			assocs, _ := out["FirewallRuleGroupAssociations"].([]any)
			assert.Len(t, assocs, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out["NextToken"])
			} else {
				assert.Empty(t, out["NextToken"])
			}
		})
	}
}

// TestParity_ListFirewallDomains_Pagination verifies NextToken/MaxResults on
// ListFirewallDomains. Real AWS paginates domain entries within a list.
func TestParity_ListFirewallDomains_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	dlRec := doRequest(t, h, "CreateFirewallDomainList", map[string]any{"Name": "dl-paginate"})
	require.Equal(t, http.StatusOK, dlRec.Code)
	var dlOut map[string]any
	require.NoError(t, json.Unmarshal(dlRec.Body.Bytes(), &dlOut))
	dlID := dlOut["FirewallDomainList"].(map[string]any)["Id"].(string)

	domains := []string{"example.com", "foo.com", "bar.com", "baz.com"}
	updRec := doRequest(t, h, "UpdateFirewallDomains", map[string]any{
		"FirewallDomainListId": dlID,
		"Operation":            "ADD",
		"Domains":              domains,
	})
	require.Equal(t, http.StatusOK, updRec.Code)

	tests := []struct {
		body          map[string]any
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			body:          map[string]any{"FirewallDomainListId": dlID},
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			body:          map[string]any{"FirewallDomainListId": dlID, "MaxResults": float64(2)},
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			listRec := doRequest(t, h, "ListFirewallDomains", tt.body)
			require.Equal(t, http.StatusOK, listRec.Code)
			var out map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
			domList, _ := out["Domains"].([]any)
			assert.Len(t, domList, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out["NextToken"])
			} else {
				assert.Empty(t, out["NextToken"])
			}
		})
	}
}

// TestParity_ListFirewallConfigs_Pagination verifies NextToken/MaxResults on
// ListFirewallConfigs. Configs are auto-created on GetFirewallConfig access.
func TestParity_ListFirewallConfigs_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 3 {
		rec := doRequest(t, h, "GetFirewallConfig", map[string]any{
			"ResourceId": fmt.Sprintf("vpc-fwcfg-%d", i),
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		body          map[string]any
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			body:          map[string]any{},
			wantLen:       3,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			body:          map[string]any{"MaxResults": float64(2)},
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			listRec := doRequest(t, h, "ListFirewallConfigs", tt.body)
			require.Equal(t, http.StatusOK, listRec.Code)
			var out map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
			cfgs, _ := out["FirewallConfigs"].([]any)
			assert.Len(t, cfgs, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out["NextToken"])
			} else {
				assert.Empty(t, out["NextToken"])
			}
		})
	}
}

// TestParity_ListOutpostResolvers_Pagination verifies NextToken/MaxResults on
// ListOutpostResolvers.
func TestParity_ListOutpostResolvers_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 3 {
		rec := doRequest(t, h, "CreateOutpostResolver", map[string]any{
			"Name":                  fmt.Sprintf("op-res-%d", i),
			"OutpostArn":            fmt.Sprintf("arn:aws:outposts:us-east-1:000000000000:outpost/op-%d", i),
			"PreferredInstanceType": "m5.xlarge",
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		body          map[string]any
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			body:          map[string]any{},
			wantLen:       3,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			body:          map[string]any{"MaxResults": float64(2)},
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			listRec := doRequest(t, h, "ListOutpostResolvers", tt.body)
			require.Equal(t, http.StatusOK, listRec.Code)
			var out map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
			resolvers, _ := out["OutpostResolvers"].([]any)
			assert.Len(t, resolvers, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out["NextToken"])
			} else {
				assert.Empty(t, out["NextToken"])
			}
		})
	}
}

// TestParity_ListResolverQueryLogConfigs_Pagination verifies NextToken/MaxResults on
// ListResolverQueryLogConfigs.
func TestParity_ListResolverQueryLogConfigs_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 3 {
		rec := doRequest(t, h, "CreateResolverQueryLogConfig", map[string]any{
			"Name":           fmt.Sprintf("cfg-%d", i),
			"DestinationArn": fmt.Sprintf("arn:aws:s3:::bucket-%d", i),
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		body          map[string]any
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			body:          map[string]any{},
			wantLen:       3,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			body:          map[string]any{"MaxResults": float64(2)},
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			listRec := doRequest(t, h, "ListResolverQueryLogConfigs", tt.body)
			require.Equal(t, http.StatusOK, listRec.Code)
			var out map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
			cfgs, _ := out["ResolverQueryLogConfigs"].([]any)
			assert.Len(t, cfgs, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out["NextToken"])
			} else {
				assert.Empty(t, out["NextToken"])
			}
		})
	}
}

// TestParity_ListResolverQueryLogConfigAssociations_Pagination verifies NextToken/MaxResults
// on ListResolverQueryLogConfigAssociations.
func TestParity_ListResolverQueryLogConfigAssociations_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	cfgRec := doRequest(t, h, "CreateResolverQueryLogConfig", map[string]any{
		"Name":           "cfg-assoc-pag",
		"DestinationArn": "arn:aws:s3:::bucket-assoc-pag",
	})
	require.Equal(t, http.StatusOK, cfgRec.Code)
	var cfgOut map[string]any
	require.NoError(t, json.Unmarshal(cfgRec.Body.Bytes(), &cfgOut))
	cfgID := cfgOut["ResolverQueryLogConfig"].(map[string]any)["Id"].(string)

	for i := range 3 {
		assocRec := doRequest(t, h, "AssociateResolverQueryLogConfig", map[string]any{
			"ResolverQueryLogConfigId": cfgID,
			"ResourceId":               fmt.Sprintf("vpc-qlassoc-%d", i),
		})
		require.Equal(t, http.StatusOK, assocRec.Code)
	}

	tests := []struct {
		body          map[string]any
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			body:          map[string]any{},
			wantLen:       3,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			body:          map[string]any{"MaxResults": float64(2)},
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			listRec := doRequest(t, h, "ListResolverQueryLogConfigAssociations", tt.body)
			require.Equal(t, http.StatusOK, listRec.Code)
			var out map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
			assocs, _ := out["ResolverQueryLogConfigAssociations"].([]any)
			assert.Len(t, assocs, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out["NextToken"])
			} else {
				assert.Empty(t, out["NextToken"])
			}
		})
	}
}

// TestParity_ListResolverRuleAssociations_Pagination verifies NextToken/MaxResults on
// ListResolverRuleAssociations.
func TestParity_ListResolverRuleAssociations_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	ruleRec := doRequest(t, h, "CreateResolverRule", map[string]any{
		"Name":       "rule-pag",
		"DomainName": "pag.example.com",
		"RuleType":   "FORWARD",
		"TargetIps":  []map[string]any{{"Ip": "10.0.0.1", "Port": 53}},
	})
	require.Equal(t, http.StatusOK, ruleRec.Code)
	var ruleOut map[string]any
	require.NoError(t, json.Unmarshal(ruleRec.Body.Bytes(), &ruleOut))
	ruleID := ruleOut["ResolverRule"].(map[string]any)["Id"].(string)

	for i := range 3 {
		assocRec := doRequest(t, h, "AssociateResolverRule", map[string]any{
			"ResolverRuleId": ruleID,
			"VPCId":          fmt.Sprintf("vpc-rassoc-%d", i),
			"Name":           fmt.Sprintf("assoc-%d", i),
		})
		require.Equal(t, http.StatusOK, assocRec.Code)
	}

	tests := []struct {
		body          map[string]any
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			body:          map[string]any{},
			wantLen:       3,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			body:          map[string]any{"MaxResults": float64(2)},
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			listRec := doRequest(t, h, "ListResolverRuleAssociations", tt.body)
			require.Equal(t, http.StatusOK, listRec.Code)
			var out map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
			assocs, _ := out["ResolverRuleAssociations"].([]any)
			assert.Len(t, assocs, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out["NextToken"])
			} else {
				assert.Empty(t, out["NextToken"])
			}
		})
	}
}

// TestParity_ListResolverConfigs_Pagination verifies NextToken/MaxResults on
// ListResolverConfigs. Configs are auto-created on GetResolverConfig access.
func TestParity_ListResolverConfigs_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 3 {
		rec := doRequest(t, h, "GetResolverConfig", map[string]any{
			"ResourceId": fmt.Sprintf("vpc-rescfg-%d", i),
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		body          map[string]any
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			body:          map[string]any{},
			wantLen:       3,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			body:          map[string]any{"MaxResults": float64(2)},
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			listRec := doRequest(t, h, "ListResolverConfigs", tt.body)
			require.Equal(t, http.StatusOK, listRec.Code)
			var out map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
			cfgs, _ := out["ResolverConfigs"].([]any)
			assert.Len(t, cfgs, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out["NextToken"])
			} else {
				assert.Empty(t, out["NextToken"])
			}
		})
	}
}

// TestParity_ListResolverDnssecConfigs_Pagination verifies NextToken/MaxResults on
// ListResolverDnssecConfigs. Configs are auto-created on GetResolverDnssecConfig access.
func TestParity_ListResolverDnssecConfigs_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 3 {
		rec := doRequest(t, h, "GetResolverDnssecConfig", map[string]any{
			"ResourceId": fmt.Sprintf("vpc-dnssec-pag-%d", i),
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		body          map[string]any
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			body:          map[string]any{},
			wantLen:       3,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			body:          map[string]any{"MaxResults": float64(2)},
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			listRec := doRequest(t, h, "ListResolverDnssecConfigs", tt.body)
			require.Equal(t, http.StatusOK, listRec.Code)
			var out map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
			cfgs, _ := out["ResolverDnssecConfigs"].([]any)
			assert.Len(t, cfgs, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out["NextToken"])
			} else {
				assert.Empty(t, out["NextToken"])
			}
		})
	}
}

// TestParity_ResolverDnssecConfig_ValidationField verifies the DNSSEC config response
// uses the "Validation" field name (not "ValidationStatus") matching the real AWS API.
func TestParity_ResolverDnssecConfig_ValidationField(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "GetResolverDnssecConfig", map[string]any{
		"ResourceId": "vpc-dnssec-field",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	cfg, ok := out["ResolverDNSSECConfig"].(map[string]any)
	require.True(t, ok, "ResolverDNSSECConfig must be present")

	_, hasValidation := cfg["Validation"]
	_, hasValidationStatus := cfg["ValidationStatus"]
	assert.True(t, hasValidation, "response must have Validation field (not ValidationStatus)")
	assert.False(t, hasValidationStatus, "response must not have ValidationStatus field")
}
