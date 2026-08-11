package route53resolver_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53resolver"
)

func TestResolverQueryLogConfigAssociation_Fields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, "CreateResolverQueryLogConfig", map[string]any{
		"Name":           "qlc-assoc-fields",
		"DestinationArn": "arn:aws:logs:us-east-1:000000000000:log-group:/test",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	cfgID := createResp["ResolverQueryLogConfig"].(map[string]any)["Id"].(string)

	assocRec := doRequest(t, h, "AssociateResolverQueryLogConfig", map[string]any{
		"ResolverQueryLogConfigId": cfgID,
		"ResourceId":               "vpc-fields-test",
	})
	require.Equal(t, http.StatusOK, assocRec.Code)

	var assocResp map[string]any
	require.NoError(t, json.Unmarshal(assocRec.Body.Bytes(), &assocResp))
	assoc := assocResp["ResolverQueryLogConfigAssociation"].(map[string]any)

	assert.NotEmpty(t, assoc["Id"])
	assert.NotEmpty(t, assoc["CreationTime"])
	assert.Equal(t, "ACTIVE", assoc["Status"])
}

// --- CreateFirewallRule stores CreatorRequestId and timestamps ---

func TestQueryLogConfigAssociationGetAndList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "get_and_list", wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create config and associate.
			rec := doRequest(t, h, "CreateResolverQueryLogConfig", map[string]any{
				"Name":           "cfg-assoc",
				"DestinationArn": "arn:aws:s3:::bucket-assoc",
			})
			require.Equal(t, tt.wantCode, rec.Code)
			resp := decodeJSON(t, rec)
			cfgID := resp["ResolverQueryLogConfig"].(map[string]any)["Id"].(string)

			rec = doRequest(t, h, "AssociateResolverQueryLogConfig", map[string]any{
				"ResolverQueryLogConfigId": cfgID,
				"ResourceId":               "vpc-assoc-1",
			})
			require.Equal(t, http.StatusOK, rec.Code)
			assocResp := decodeJSON(t, rec)
			assocID := assocResp["ResolverQueryLogConfigAssociation"].(map[string]any)["Id"].(string)

			// GetResolverQueryLogConfigAssociation.
			rec = doRequest(t, h, "GetResolverQueryLogConfigAssociation", map[string]any{
				"ResolverQueryLogConfigAssociationId": assocID,
			})
			assert.Equal(t, http.StatusOK, rec.Code)
			getResp := decodeJSON(t, rec)
			assocObj, _ := getResp["ResolverQueryLogConfigAssociation"].(map[string]any)
			assert.Equal(t, assocID, assocObj["Id"])

			// ListResolverQueryLogConfigAssociations.
			rec = doRequest(t, h, "ListResolverQueryLogConfigAssociations", map[string]any{})
			assert.Equal(t, http.StatusOK, rec.Code)
			listResp := decodeJSON(t, rec)
			items, _ := listResp["ResolverQueryLogConfigAssociations"].([]any)
			assert.Len(t, items, 1)
		})
	}
}

func TestQueryLogConfigAssociationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "get_missing_id",
			action:   "GetResolverQueryLogConfigAssociation",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get_not_found",
			action:   "GetResolverQueryLogConfigAssociation",
			body:     map[string]any{"ResolverQueryLogConfigAssociationId": "rqlca-notexist"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// --- GetResolverQueryLogConfigPolicy / PutResolverQueryLogConfigPolicy ---

func TestAssociateResolverQueryLogConfig_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "AssociateResolverQueryLogConfig", map[string]any{
		"ResolverQueryLogConfigId": "rqlc-nonexistent",
		"ResourceId":               "vpc-12345",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- AssociateResolverRule not-found ---

func TestAssociateResolverQueryLogConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupExtra func(t *testing.T, h *route53resolver.Handler) string
		body       func(cfgID string) map[string]any
		name       string
		wantStatus string
		wantCode   int
	}{
		{
			name: "success",
			setupExtra: func(t *testing.T, h *route53resolver.Handler) string {
				t.Helper()
				rec := doRequest(t, h, "CreateResolverQueryLogConfig", map[string]any{
					"Name": "cfg", "DestinationArn": "arn:aws:s3:::bkt",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["ResolverQueryLogConfig"].(map[string]any)["Id"].(string)
			},
			body: func(cfgID string) map[string]any {
				return map[string]any{
					"ResolverQueryLogConfigId": cfgID,
					"ResourceId":               "vpc-12345",
				}
			},
			wantCode:   http.StatusOK,
			wantStatus: "ACTIVE",
		},
		{
			name:       "missing_config_id",
			setupExtra: func(_ *testing.T, _ *route53resolver.Handler) string { return "" },
			body:       func(_ string) map[string]any { return map[string]any{"ResourceId": "vpc-1"} },
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "missing_resource_id",
			setupExtra: func(_ *testing.T, _ *route53resolver.Handler) string { return "" },
			body: func(_ string) map[string]any {
				return map[string]any{"ResolverQueryLogConfigId": "rqlc-notexist"}
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:       "config_not_found",
			setupExtra: func(_ *testing.T, _ *route53resolver.Handler) string { return "" },
			body: func(_ string) map[string]any {
				return map[string]any{
					"ResolverQueryLogConfigId": "rqlc-notexist",
					"ResourceId":               "vpc-1",
				}
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			cfgID := tt.setupExtra(t, h)
			rec := doRequest(t, h, "AssociateResolverQueryLogConfig", tt.body(cfgID))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assoc, ok := resp["ResolverQueryLogConfigAssociation"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantStatus, assoc["Status"])
				assert.NotEmpty(t, assoc["Id"])
				assert.Equal(t, cfgID, assoc["ResolverQueryLogConfigId"])
			}
		})
	}
}

// --- AssociateResolverRule ---

// TestParity_AssociateResolverQueryLogConfig_RequiresFields verifies that
// AssociateResolverQueryLogConfig rejects requests missing required fields.
// Real AWS returns 400 InvalidRequest for missing ResolverQueryLogConfigId or
// ResourceId; the emulator had the validation but it lacked handler-level tests.
func TestAssociateResolverQueryLogConfig_RequiresFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "missing_config_id_rejected",
			body:     map[string]any{"ResourceId": "vpc-12345"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty_config_id_rejected",
			body:     map[string]any{"ResolverQueryLogConfigId": "", "ResourceId": "vpc-12345"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_resource_id_rejected",
			body:     map[string]any{"ResolverQueryLogConfigId": "rqlc-abc"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty_resource_id_rejected",
			body:     map[string]any{"ResolverQueryLogConfigId": "rqlc-abc", "ResourceId": ""},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "AssociateResolverQueryLogConfig", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code,
				"AssociateResolverQueryLogConfig status for case %q", tt.name)
		})
	}
}

// TestParity_ListResolverQueryLogConfigAssociations_Pagination verifies NextToken/MaxResults
// on ListResolverQueryLogConfigAssociations.
func TestListResolverQueryLogConfigAssociations_Pagination(t *testing.T) {
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

func TestListResolverQueryLogConfigAssociations_Filters(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T) (*route53resolver.Handler, string, string) {
		t.Helper()
		h := newTestHandler(t)

		mkCfg := func(name, destArn string) string {
			rec := doRequest(t, h, "CreateResolverQueryLogConfig", map[string]any{
				"Name": name, "DestinationArn": destArn,
			})
			require.Equal(t, http.StatusOK, rec.Code)
			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			return out["ResolverQueryLogConfig"].(map[string]any)["Id"].(string)
		}
		cfg1ID := mkCfg("cfg-one", "arn:aws:s3:::bucket-one")
		cfg2ID := mkCfg("cfg-two", "arn:aws:s3:::bucket-two")

		assocRec1 := doRequest(t, h, "AssociateResolverQueryLogConfig", map[string]any{
			"ResolverQueryLogConfigId": cfg1ID, "ResourceId": "vpc-one",
		})
		require.Equal(t, http.StatusOK, assocRec1.Code)
		assocRec2 := doRequest(t, h, "AssociateResolverQueryLogConfig", map[string]any{
			"ResolverQueryLogConfigId": cfg2ID, "ResourceId": "vpc-two",
		})
		require.Equal(t, http.StatusOK, assocRec2.Code)

		return h, cfg1ID, cfg2ID
	}

	tests := []struct {
		buildFilter func(cfg1, cfg2 string) map[string]any
		name        string
		wantResIDs  []string
	}{
		{
			name: "resource_id_canonical",
			buildFilter: func(string, string) map[string]any {
				return map[string]any{"Name": "ResourceId", "Values": []string{"vpc-one"}}
			},
			wantResIDs: []string{"vpc-one"},
		},
		{
			name: "resource_id_legacy_uppercase",
			buildFilter: func(string, string) map[string]any {
				return map[string]any{"Name": "RESOURCE_ID", "Values": []string{"vpc-two"}}
			},
			wantResIDs: []string{"vpc-two"},
		},
		{
			name: "resolver_query_log_config_id",
			buildFilter: func(cfg1, _ string) map[string]any {
				return map[string]any{"Name": "ResolverQueryLogConfigId", "Values": []string{cfg1}}
			},
			wantResIDs: []string{"vpc-one"},
		},
		{
			name: "status_values_or_combined",
			buildFilter: func(string, string) map[string]any {
				return map[string]any{"Name": "Status", "Values": []string{"ACTIVE"}}
			},
			wantResIDs: []string{"vpc-one", "vpc-two"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, cfg1, cfg2 := setup(t)

			rec := doRequest(t, h, "ListResolverQueryLogConfigAssociations", map[string]any{
				"Filters": []map[string]any{tt.buildFilter(cfg1, cfg2)},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assocs, _ := resp["ResolverQueryLogConfigAssociations"].([]any)
			gotResIDs := make([]string, len(assocs))
			for i, a := range assocs {
				gotResIDs[i] = a.(map[string]any)["ResourceId"].(string)
			}
			assert.ElementsMatch(t, tt.wantResIDs, gotResIDs)
		})
	}
}

func TestListResolverQueryLogConfigAssociations_UnknownFilterNameRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListResolverQueryLogConfigAssociations", map[string]any{
		"Filters": []map[string]any{
			{"Name": "NotARealFilter", "Values": []string{"x"}},
		},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "InvalidParameterException", resp["__type"])
}
