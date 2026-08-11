package route53resolver_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53resolver"
)

func TestFirewallRuleGroup_ShareStatusAndTimestamps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{
		"Name":             "rg-share-test",
		"CreatorRequestId": "req-rg-1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	rg := resp["FirewallRuleGroup"].(map[string]any)

	assert.Equal(t, "NOT_SHARED", rg["ShareStatus"])
	assert.NotEmpty(t, rg["CreationTime"])
	assert.NotEmpty(t, rg["ModificationTime"])
}

// --- Issue 24: FirewallRuleGroupAssociation MutationProtection ---

func TestFirewallRuleGroupAssociation_MutationProtection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		mutationProtection string
		tryDisassociate    bool
		wantAssocCode      int
		wantDisassocCode   int
	}{
		{
			name:               "disabled_allows_disassociate",
			mutationProtection: "DISABLED",
			tryDisassociate:    true,
			wantAssocCode:      http.StatusOK,
			wantDisassocCode:   http.StatusOK,
		},
		{
			name:               "enabled_blocks_disassociate",
			mutationProtection: "ENABLED",
			tryDisassociate:    true,
			wantAssocCode:      http.StatusOK,
			wantDisassocCode:   http.StatusBadRequest,
		},
		{
			name:               "default_disabled",
			mutationProtection: "",
			tryDisassociate:    false,
			wantAssocCode:      http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			grpRec := doRequest(
				t,
				h,
				"CreateFirewallRuleGroup",
				map[string]any{"Name": "mut-prot-grp"},
			)
			require.Equal(t, http.StatusOK, grpRec.Code)
			var grpResp map[string]any
			require.NoError(t, json.Unmarshal(grpRec.Body.Bytes(), &grpResp))
			groupID := grpResp["FirewallRuleGroup"].(map[string]any)["Id"].(string)

			assocBody := map[string]any{
				"FirewallRuleGroupId": groupID,
				"VpcId":               "vpc-mp-test",
				"Priority":            100,
				"Name":                "mp-assoc",
			}
			if tt.mutationProtection != "" {
				assocBody["MutationProtection"] = tt.mutationProtection
			}

			assocRec := doRequest(t, h, "AssociateFirewallRuleGroup", assocBody)
			assert.Equal(t, tt.wantAssocCode, assocRec.Code)

			if tt.wantAssocCode == http.StatusOK && tt.tryDisassociate {
				var assocResp map[string]any
				require.NoError(t, json.Unmarshal(assocRec.Body.Bytes(), &assocResp))
				assocID := assocResp["FirewallRuleGroupAssociation"].(map[string]any)["Id"].(string)

				disassocRec := doRequest(t, h, "DisassociateFirewallRuleGroup", map[string]any{
					"FirewallRuleGroupAssociationId": assocID,
				})
				assert.Equal(t, tt.wantDisassocCode, disassocRec.Code)
			}
		})
	}
}

// --- MutationProtection update ---

func TestUpdateFirewallRuleGroupAssociation_MutationProtection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		mutationProtection string
		wantCode           int
	}{
		{name: "set_enabled", mutationProtection: "ENABLED", wantCode: http.StatusOK},
		{name: "set_disabled", mutationProtection: "DISABLED", wantCode: http.StatusOK},
		{name: "invalid_value", mutationProtection: "MAYBE", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			grpRec := doRequest(
				t,
				h,
				"CreateFirewallRuleGroup",
				map[string]any{"Name": "upd-mp-grp"},
			)
			require.Equal(t, http.StatusOK, grpRec.Code)
			var grpResp map[string]any
			require.NoError(t, json.Unmarshal(grpRec.Body.Bytes(), &grpResp))
			groupID := grpResp["FirewallRuleGroup"].(map[string]any)["Id"].(string)

			assocRec := doRequest(t, h, "AssociateFirewallRuleGroup", map[string]any{
				"FirewallRuleGroupId": groupID,
				"VpcId":               "vpc-upd-mp",
				"Priority":            100,
				"Name":                "upd-mp-assoc",
			})
			require.Equal(t, http.StatusOK, assocRec.Code)
			var assocResp map[string]any
			require.NoError(t, json.Unmarshal(assocRec.Body.Bytes(), &assocResp))
			assocID := assocResp["FirewallRuleGroupAssociation"].(map[string]any)["Id"].(string)

			rec := doRequest(t, h, "UpdateFirewallRuleGroupAssociation", map[string]any{
				"FirewallRuleGroupAssociationId": assocID,
				"MutationProtection":             tt.mutationProtection,
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assoc := resp["FirewallRuleGroupAssociation"].(map[string]any)
				assert.Equal(t, tt.mutationProtection, assoc["MutationProtection"])
			}
		})
	}
}

// TestUpdateFirewallRuleGroupAssociation_RejectedMutationProtectionLeavesStateUnchanged
// asserts that when MutationProtection fails validation, Name and Priority
// from the same request are not partially applied. The backend mutates
// assoc.Name/assoc.Priority in place on the live stored pointer before it
// validates MutationProtection (firewall_rule_groups.go
// UpdateFirewallRuleGroupAssociation), so a request that fails validation
// was still leaving its other field changes committed.
func TestUpdateFirewallRuleGroupAssociation_RejectedMutationProtectionLeavesStateUnchanged(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	grpRec := doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{"Name": "upd-mp-partial-grp"})
	require.Equal(t, http.StatusOK, grpRec.Code)
	var grpResp map[string]any
	require.NoError(t, json.Unmarshal(grpRec.Body.Bytes(), &grpResp))
	groupID := grpResp["FirewallRuleGroup"].(map[string]any)["Id"].(string)

	assocRec := doRequest(t, h, "AssociateFirewallRuleGroup", map[string]any{
		"FirewallRuleGroupId": groupID,
		"VpcId":               "vpc-upd-mp-partial",
		"Priority":            100,
		"Name":                "original-name",
	})
	require.Equal(t, http.StatusOK, assocRec.Code)
	var assocResp map[string]any
	require.NoError(t, json.Unmarshal(assocRec.Body.Bytes(), &assocResp))
	assocID := assocResp["FirewallRuleGroupAssociation"].(map[string]any)["Id"].(string)

	updateRec := doRequest(t, h, "UpdateFirewallRuleGroupAssociation", map[string]any{
		"FirewallRuleGroupAssociationId": assocID,
		"Name":                           "changed-name",
		"Priority":                       999,
		"MutationProtection":             "MAYBE",
	})
	require.Equal(t, http.StatusBadRequest, updateRec.Code)

	getRec := doRequest(t, h, "GetFirewallRuleGroupAssociation", map[string]any{
		"FirewallRuleGroupAssociationId": assocID,
	})
	require.Equal(t, http.StatusOK, getRec.Code)
	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	assoc := getResp["FirewallRuleGroupAssociation"].(map[string]any)
	assert.Equal(t, "original-name", assoc["Name"])
	assert.InDelta(t, float64(100), assoc["Priority"], 0)
}

// --- AssociationCount decrements on disassociate ---

func TestFirewallRuleGroupAssociation_CreatorAndTimestamps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	grpRec := doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{"Name": "assoc-ts-grp"})
	require.Equal(t, http.StatusOK, grpRec.Code)
	var grpResp map[string]any
	require.NoError(t, json.Unmarshal(grpRec.Body.Bytes(), &grpResp))
	groupID := grpResp["FirewallRuleGroup"].(map[string]any)["Id"].(string)

	assocRec := doRequest(t, h, "AssociateFirewallRuleGroup", map[string]any{
		"FirewallRuleGroupId": groupID,
		"VpcId":               "vpc-assoc-ts",
		"Priority":            100,
		"Name":                "assoc-ts",
		"CreatorRequestId":    "req-assoc-ts-1",
	})
	require.Equal(t, http.StatusOK, assocRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(assocRec.Body.Bytes(), &resp))
	assoc := resp["FirewallRuleGroupAssociation"].(map[string]any)

	assert.Equal(t, "req-assoc-ts-1", assoc["CreatorRequestId"])
	assert.NotEmpty(t, assoc["CreationTime"])
	assert.NotEmpty(t, assoc["ModificationTime"])
	assert.Equal(t, "DISABLED", assoc["MutationProtection"])
}

// --- Backend direct: CreateResolverEndpoint type enum validation ---

func TestBackend_MutationProtectionDefault(t *testing.T) {
	t.Parallel()

	b := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
	grp, _ := b.CreateFirewallRuleGroup(context.Background(), "grp", "req-1")

	assoc, err := b.AssociateFirewallRuleGroup(context.Background(), grp.ID, "vpc-test", "assoc", "req-2", "", 100)
	require.NoError(t, err)
	assert.Equal(t, "DISABLED", assoc.MutationProtection)
}

// --- Endpoint timestamps survive round trip ---

func TestAddFirewallRuleGroupInternal(t *testing.T) {
	t.Parallel()

	b := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
	g := b.AddFirewallRuleGroupInternal("seeded-grp")
	require.NotNil(t, g)
	assert.Equal(t, "seeded-grp", g.Name)
	assert.Equal(t, 1, route53resolver.FirewallRuleGroupCount(b))
}

// --- Tags on Create ---

func TestAssociateFirewallRuleGroup_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "AssociateFirewallRuleGroup", map[string]any{
		"FirewallRuleGroupId": "rslvr-frg-nonexistent",
		"VpcId":               "vpc-12345",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- CreateFirewallRule increments RuleCount ---

func TestListFirewallRuleGroups_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxResults int
		wantLen    int
		wantToken  bool
	}{
		{"MaxResults=1 limits to 1", 1, 1, true},
		{"MaxResults=100 returns all 3", 100, 3, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			for _, name := range []string{"grp-a", "grp-b", "grp-c"} {
				doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{"Name": name})
			}

			rec := doRequest(t, h, "ListFirewallRuleGroups", map[string]any{"MaxResults": tt.maxResults})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			groups, _ := resp["FirewallRuleGroups"].([]any)
			assert.Len(t, groups, tt.wantLen)
			nextToken, _ := resp["NextToken"].(string)
			if tt.wantToken {
				assert.NotEmpty(t, nextToken)
			} else {
				assert.Empty(t, nextToken)
			}
		})
	}
}

func TestCreateFirewallRuleGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		wantName string
		wantCode int
	}{
		{
			name: "success",
			body: map[string]any{
				"Name":             "my-rule-group",
				"CreatorRequestId": "req-1",
			},
			wantCode: http.StatusOK,
			wantName: "my-rule-group",
		},
		{
			name:     "missing_name_returns_bad_request",
			body:     map[string]any{"CreatorRequestId": "req-2"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateFirewallRuleGroup", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				grp, ok := resp["FirewallRuleGroup"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantName, grp["Name"])
				assert.Contains(t, grp["Arn"].(string), "arn:aws:route53resolver:")
				assert.NotEmpty(t, grp["Id"])
				assert.Equal(t, "COMPLETE", grp["Status"])
			}
		})
	}
}

// --- AssociateFirewallRuleGroup ---

func TestAssociateFirewallRuleGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupExtra func(t *testing.T, h *route53resolver.Handler) string
		body       func(groupID string) map[string]any
		name       string
		wantStatus string
		wantCode   int
	}{
		{
			name: "success",
			setupExtra: func(t *testing.T, h *route53resolver.Handler) string {
				t.Helper()
				rec := doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{"Name": "grp-assoc"})
				require.Equal(t, http.StatusOK, rec.Code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["FirewallRuleGroup"].(map[string]any)["Id"].(string)
			},
			body: func(groupID string) map[string]any {
				return map[string]any{
					"FirewallRuleGroupId": groupID,
					"VpcId":               "vpc-12345",
					"Name":                "assoc-1",
					"Priority":            200,
					"CreatorRequestId":    "req-assoc",
				}
			},
			wantCode:   http.StatusOK,
			wantStatus: "COMPLETE",
		},
		{
			name:       "missing_group_id",
			setupExtra: func(_ *testing.T, _ *route53resolver.Handler) string { return "" },
			body: func(_ string) map[string]any {
				return map[string]any{"VpcId": "vpc-12345"}
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:       "missing_vpc_id",
			setupExtra: func(_ *testing.T, _ *route53resolver.Handler) string { return "" },
			body: func(_ string) map[string]any {
				return map[string]any{"FirewallRuleGroupId": "rslvr-frg-notexist"}
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:       "group_not_found",
			setupExtra: func(_ *testing.T, _ *route53resolver.Handler) string { return "" },
			body: func(_ string) map[string]any {
				return map[string]any{
					"FirewallRuleGroupId": "rslvr-frg-notexist",
					"VpcId":               "vpc-12345",
				}
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			groupID := tt.setupExtra(t, h)
			rec := doRequest(t, h, "AssociateFirewallRuleGroup", tt.body(groupID))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assoc, ok := resp["FirewallRuleGroupAssociation"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantStatus, assoc["Status"])
				assert.NotEmpty(t, assoc["Id"])
				assert.Equal(t, groupID, assoc["FirewallRuleGroupId"])
			}
		})
	}
}

// --- AssociateResolverEndpointIpAddress ---

// TestParity_AssociateFirewallRuleGroup_RequiresFields verifies that
// AssociateFirewallRuleGroup rejects requests missing FirewallRuleGroupId or
// VpcId. Real AWS returns 400 for both; the emulator had the validation but
// lacked handler-level tests.
func TestAssociateFirewallRuleGroup_RequiresFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "missing_group_id_rejected",
			body:     map[string]any{"VpcId": "vpc-12345", "Priority": 100},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_vpc_id_rejected",
			body:     map[string]any{"FirewallRuleGroupId": "rslvr-frg-abc", "Priority": 100},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "AssociateFirewallRuleGroup", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code,
				"AssociateFirewallRuleGroup status for case %q", tt.name)
		})
	}
}

// TestParity_ListFirewallRuleGroupAssociations_Pagination verifies NextToken/MaxResults
// on ListFirewallRuleGroupAssociations. Real AWS paginates associations.
func TestListFirewallRuleGroupAssociations_Pagination(t *testing.T) {
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

// TestListFirewallRuleGroupAssociations_PriorityStatusFilters asserts that
// Priority and Status (ListFirewallRuleGroupAssociationsRequest members,
// botocore route53resolver 2018-04-01 service-2.json.gz) actually narrow
// the result set instead of being silently dropped by the decoder.
func TestListFirewallRuleGroupAssociations_PriorityStatusFilters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 2 {
		grpRec := doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{
			"Name": fmt.Sprintf("grp-pf-%d", i),
		})
		require.Equal(t, http.StatusOK, grpRec.Code)
		var grpOut map[string]any
		require.NoError(t, json.Unmarshal(grpRec.Body.Bytes(), &grpOut))
		grpID := grpOut["FirewallRuleGroup"].(map[string]any)["Id"].(string)

		assocRec := doRequest(t, h, "AssociateFirewallRuleGroup", map[string]any{
			"FirewallRuleGroupId": grpID,
			"VpcId":               fmt.Sprintf("vpc-pf-%d", i),
			"Priority":            int32(200 + i),
		})
		require.Equal(t, http.StatusOK, assocRec.Code)
	}

	tests := []struct {
		body    map[string]any
		name    string
		wantLen int
	}{
		{name: "priority_narrows_to_one", body: map[string]any{"Priority": float64(200)}, wantLen: 1},
		{name: "priority_no_match", body: map[string]any{"Priority": float64(999)}, wantLen: 0},
		{name: "status_matches_all", body: map[string]any{"Status": "COMPLETE"}, wantLen: 2},
		{name: "status_no_match", body: map[string]any{"Status": "DELETING"}, wantLen: 0},
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
		})
	}
}

// TestR53R_FirewallRuleGroupCRUD covers CreateFirewallRuleGroup, GetFirewallRuleGroup,
// ListFirewallRuleGroups, DeleteFirewallRuleGroup, GetFirewallRuleGroupPolicy,
// PutFirewallRuleGroupPolicy.
func TestFirewallRuleGroupCRUD(t *testing.T) {
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

// TestR53R_FirewallRuleGroupAssociation covers AssociateFirewallRuleGroup,
// GetFirewallRuleGroupAssociation, ListFirewallRuleGroupAssociations,
// UpdateFirewallRuleGroupAssociation, DisassociateFirewallRuleGroup.
func TestFirewallRuleGroupAssociation(t *testing.T) {
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
