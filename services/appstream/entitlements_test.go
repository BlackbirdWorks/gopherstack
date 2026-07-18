package appstream_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appstream"
)

// TestAppStream_Entitlements covers Entitlement CRUD and application associations.
func TestAppStream_Entitlements(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *appstream.Handler)
		check    func(t *testing.T, body []byte)
		body     any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "CreateEntitlement returns entitlement",
			action: "CreateEntitlement",
			body: map[string]any{
				"Name":          "my-ent",
				"StackName":     "stk",
				"AppVisibility": "ALL",
				"Attributes": []map[string]string{
					{"Name": "department", "Value": "eng"},
				},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				ent := resp["Entitlement"].(map[string]any)
				assert.Equal(t, "my-ent", ent["Name"])
				assert.Equal(t, "ALL", ent["AppVisibility"])
			},
		},
		{
			name:   "DescribeEntitlements lists by stack",
			action: "DescribeEntitlements",
			setup: func(h *appstream.Handler) {
				rec := doRequest(t, h, "CreateEntitlement", map[string]any{
					"Name":          "ent-1",
					"StackName":     "stk-1",
					"AppVisibility": "ALL",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"StackName": "stk-1"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				ents := resp["Entitlements"].([]any)
				assert.Len(t, ents, 1)
			},
		},
		{
			name:   "UpdateEntitlement changes description",
			action: "UpdateEntitlement",
			setup: func(h *appstream.Handler) {
				rec := doRequest(t, h, "CreateEntitlement", map[string]any{
					"Name": "upd-ent", "StackName": "stk", "AppVisibility": "ALL",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body: map[string]any{
				"Name": "upd-ent", "StackName": "stk", "Description": "updated",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				ent := resp["Entitlement"].(map[string]any)
				assert.Equal(t, "updated", ent["Description"])
			},
		},
		{
			name:   "DeleteEntitlement removes it",
			action: "DeleteEntitlement",
			setup: func(h *appstream.Handler) {
				rec := doRequest(t, h, "CreateEntitlement", map[string]any{
					"Name": "del-ent", "StackName": "stk", "AppVisibility": "ALL",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"Name": "del-ent", "StackName": "stk"},
			wantCode: http.StatusOK,
		},
		{
			name:   "AssociateApplicationToEntitlement adds app",
			action: "AssociateApplicationToEntitlement",
			setup: func(h *appstream.Handler) {
				rec := doRequest(t, h, "CreateEntitlement", map[string]any{
					"Name": "ent-assoc", "StackName": "stk", "AppVisibility": "ALL",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body: map[string]any{
				"ApplicationIdentifier": "app-001",
				"EntitlementName":       "ent-assoc",
				"StackName":             "stk",
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "ListEntitledApplications returns apps",
			action: "ListEntitledApplications",
			setup: func(h *appstream.Handler) {
				rec := doRequest(t, h, "CreateEntitlement", map[string]any{
					"Name": "ent-list", "StackName": "stk", "AppVisibility": "ALL",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				rec = doRequest(t, h, "AssociateApplicationToEntitlement", map[string]any{
					"ApplicationIdentifier": "app-x",
					"EntitlementName":       "ent-list",
					"StackName":             "stk",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"EntitlementName": "ent-list", "StackName": "stk"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				apps := resp["EntitledApplications"].([]any)
				assert.Len(t, apps, 1)
			},
		},
		{
			name:   "DisassociateApplicationFromEntitlement removes app",
			action: "DisassociateApplicationFromEntitlement",
			setup: func(h *appstream.Handler) {
				rec := doRequest(t, h, "CreateEntitlement", map[string]any{
					"Name": "ent-dis", "StackName": "stk", "AppVisibility": "ALL",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				rec = doRequest(t, h, "AssociateApplicationToEntitlement", map[string]any{
					"ApplicationIdentifier": "app-y",
					"EntitlementName":       "ent-dis",
					"StackName":             "stk",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body: map[string]any{
				"ApplicationIdentifier": "app-y",
				"EntitlementName":       "ent-dis",
				"StackName":             "stk",
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// TestAppStream_EntitlementRoundtrip verifies CreateEntitlement and DescribeEntitlements.
func TestAppStream_EntitlementRoundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateStack", map[string]any{"Name": "ent-stack"})

	rec := doRequest(t, h, "CreateEntitlement", map[string]any{
		"Name":          "my-entitlement",
		"StackName":     "ent-stack",
		"AppVisibility": "ALL",
		"Attributes": []any{
			map[string]any{"Name": "saml:sub_type", "Value": "persistent"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	recDesc := doRequest(t, h, "DescribeEntitlements", map[string]any{
		"Name":      "my-entitlement",
		"StackName": "ent-stack",
	})
	require.Equal(t, http.StatusOK, recDesc.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(recDesc.Body.Bytes(), &resp))
	entitlements := resp["Entitlements"].([]any)
	require.Len(t, entitlements, 1)
	ent := entitlements[0].(map[string]any)
	assert.Equal(t, "my-entitlement", ent["Name"])
	assert.Equal(t, "ALL", ent["AppVisibility"])
}
