package lakeformation_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lakeformation"
)

func TestHandler_GrantRevokeListPermissions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		principal   string
		resourceArn string
		wantStatus  int
	}{
		{
			name:        "grant_list_revoke",
			principal:   "arn:aws:iam::123456789012:user/alice",
			resourceArn: "arn:aws:s3:::my-bucket",
			wantStatus:  http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			grantBody := `{"Principal":{"DataLakePrincipalIdentifier":"` + tt.principal +
				`"},"Resource":{"DataLocation":{"ResourceArn":"` + tt.resourceArn +
				`"}},"Permissions":["DATA_LOCATION_ACCESS"]}`
			rec := doLFRequest(t, h, "/GrantPermissions", grantBody)
			assert.Equal(t, tt.wantStatus, rec.Code)

			listBody := `{"Resource":{"DataLocation":{"ResourceArn":"` + tt.resourceArn + `"}}}`
			rec = doLFRequest(t, h, "/ListPermissions", listBody)
			assert.Equal(t, http.StatusOK, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			perms, _ := listResp["PrincipalResourcePermissions"].([]any)
			assert.Len(t, perms, 1)

			rec = doLFRequest(t, h, "/RevokePermissions", grantBody)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_BatchGrantRevokePermissions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "batch_grant_success",
			body: `{"Entries":[{"Id":"entry-1","Principal":{"DataLakePrincipalIdentifier":"arn:aws:iam::123:user/a"},` +
				`"Resource":{"DataLocation":{"ResourceArn":"arn:aws:s3:::b"}},` +
				`"Permissions":["DATA_LOCATION_ACCESS"]}]}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			rec := doLFRequest(t, h, "/BatchGrantPermissions", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			failures, _ := resp["Failures"].([]any)
			assert.Empty(t, failures)

			rec = doLFRequest(t, h, "/BatchRevokePermissions", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestBatchGrantPermissions_EmptyEntries(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/BatchGrantPermissions", map[string]any{
		"Entries": []any{},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Empty(t, out["Failures"])
}

func TestGrantAndListPermissions(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/GrantPermissions", map[string]any{
		"Principal":   map[string]any{"DataLakePrincipalIdentifier": "arn:aws:iam::123:user/alice"},
		"Resource":    map[string]any{"Database": map[string]any{"Name": "mydb"}},
		"Permissions": []string{"SELECT"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, b.PermissionCount())

	rec2 := postJSON(t, h, "/ListPermissions", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out))
	perms := out["PrincipalResourcePermissions"].([]any)
	assert.Len(t, perms, 1)
}

func TestGrantPermissions_NilPrincipalReturns400(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/GrantPermissions", map[string]any{
		"Resource":    map[string]any{"Database": map[string]any{"Name": "db"}},
		"Permissions": []string{"SELECT"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestBatchGrant_ErrorCodeMapping(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	// Pass a nil principal entry to trigger validation error
	rec := postJSON(t, h, "/BatchGrantPermissions", map[string]any{
		"Entries": []any{
			map[string]any{
				"Id": "entry-1",
				// Missing Principal and Resource → validation error in GrantPermissions
				"Permissions": []string{"SELECT"},
			},
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	failures := out["Failures"].([]any)
	assert.Len(t, failures, 1)
	failure := failures[0].(map[string]any)
	errDetail := failure["Error"].(map[string]any)
	// validation errors should map to InvalidInputException
	assert.Equal(t, "InvalidInputException", errDetail["ErrorCode"])
}

func TestBatchGrantPermissions_MissingIDRejected(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	// The real BatchPermissionsRequestEntry.Id member is required so
	// BatchFailureEntry.RequestEntry can be correlated back to the caller's
	// request; omitting it must be rejected up front (400), not silently
	// accepted or surfaced only inside a per-entry Failures[] error.
	rec := postJSON(t, h, "/BatchGrantPermissions", map[string]any{
		"Entries": []any{
			map[string]any{
				"Principal":   map[string]any{"DataLakePrincipalIdentifier": "arn:aws:iam::123:user/a"},
				"Resource":    map[string]any{"Database": map[string]any{"Name": "db"}},
				"Permissions": []any{"SELECT"},
			},
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestListPermissions_ResourceShapedFilter(t *testing.T) {
	t.Parallel()

	// The real ListPermissionsInput filters by a nested Resource object (the
	// same shape GrantPermissions/RevokePermissions use), not a flat
	// ResourceArn string -- a real aws-sdk-go-v2 client would never send
	// ResourceArn for this operation.
	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	postJSON(t, h, "/GrantPermissions", map[string]any{
		"Principal":   map[string]any{"DataLakePrincipalIdentifier": "arn:aws:iam::123:user/alice"},
		"Resource":    map[string]any{"Database": map[string]any{"Name": "matchdb"}},
		"Permissions": []any{"SELECT"},
	})
	postJSON(t, h, "/GrantPermissions", map[string]any{
		"Principal":   map[string]any{"DataLakePrincipalIdentifier": "arn:aws:iam::123:user/bob"},
		"Resource":    map[string]any{"Database": map[string]any{"Name": "otherdb"}},
		"Permissions": []any{"SELECT"},
	})

	rec := postJSON(t, h, "/ListPermissions", map[string]any{
		"Resource": map[string]any{"Database": map[string]any{"Name": "matchdb"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	entries := out["PrincipalResourcePermissions"].([]any)
	require.Len(t, entries, 1)
	entry := entries[0].(map[string]any)
	principal := entry["Principal"].(map[string]any)
	assert.Equal(t, "arn:aws:iam::123:user/alice", principal["DataLakePrincipalIdentifier"])

	// LastUpdated must be a JSON number (epoch seconds), never an RFC3339 string.
	_, isNumber := entry["LastUpdated"].(float64)
	assert.True(t, isNumber, "LastUpdated must serialize as epoch seconds, got %T: %v",
		entry["LastUpdated"], entry["LastUpdated"])
}

func TestGetEffectivePermissionsForPath_Empty(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/GetEffectivePermissionsForPath", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	assert.NotNil(t, out["PrincipalResourcePermissions"])
}

// --- ListTableStorageOptimizers type filter ---

func TestGrantPermissions_InvalidEnum(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		permissions []any
		wantStatus  int
	}{
		{
			name:        "valid SELECT permission accepted",
			permissions: []any{"SELECT"},
			wantStatus:  http.StatusOK,
		},
		{
			name:        "invalid FAKE_PERM rejected",
			permissions: []any{"FAKE_PERM"},
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "ALL accepted",
			permissions: []any{"ALL"},
			wantStatus:  http.StatusOK,
		},
		{
			name:        "mixed valid and invalid rejected",
			permissions: []any{"SELECT", "NOPE"},
			wantStatus:  http.StatusBadRequest,
		},
		{
			// CREATE_LAKE_FORMATION_OPT_IN does not exist in the real
			// types.Permission enum (aws-sdk-go-v2/service/lakeformation) --
			// it must be rejected, not silently accepted.
			name:        "CREATE_LAKE_FORMATION_OPT_IN not a real permission, rejected",
			permissions: []any{"CREATE_LAKE_FORMATION_OPT_IN"},
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "DROP accepted",
			permissions: []any{"DROP"},
			wantStatus:  http.StatusOK,
		},
		{
			name:        "SUPER_USER accepted",
			permissions: []any{"SUPER_USER"},
			wantStatus:  http.StatusOK,
		},
		{
			// SUPER is a gopherstack-invented typo for SUPER_USER; must be rejected.
			name:        "SUPER (typo of SUPER_USER) rejected",
			permissions: []any{"SUPER"},
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "CREATE_LF_TAG_EXPRESSION accepted",
			permissions: []any{"CREATE_LF_TAG_EXPRESSION"},
			wantStatus:  http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()
			h := lakeformation.NewHandler(b)

			rec := postJSON(t, h, "/GrantPermissions", map[string]any{
				"Principal":   map[string]any{"DataLakePrincipalIdentifier": "arn:aws:iam::123:user/u"},
				"Resource":    map[string]any{"Database": map[string]any{"Name": "db"}},
				"Permissions": tt.permissions,
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "test case: %s", tt.name)
		})
	}
}

// --- #5: GrantPermissions merges duplicate entries ---

func TestGrantPermissions_MergesDuplicates(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	principal := map[string]any{"DataLakePrincipalIdentifier": "arn:aws:iam::123:user/alice"}
	resource := map[string]any{"Database": map[string]any{"Name": "db1"}}

	postJSON(t, h, "/GrantPermissions", map[string]any{
		"Principal":   principal,
		"Resource":    resource,
		"Permissions": []any{"SELECT"},
	})

	postJSON(t, h, "/GrantPermissions", map[string]any{
		"Principal":   principal,
		"Resource":    resource,
		"Permissions": []any{"INSERT"},
	})

	rec := postJSON(t, h, "/ListPermissions", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	entries := out["PrincipalResourcePermissions"].([]any)
	assert.Len(t, entries, 1, "should merge into single entry")

	entry := entries[0].(map[string]any)
	perms := entry["Permissions"].([]any)
	assert.Len(t, perms, 2, "merged entry should contain both SELECT and INSERT")
}

// --- #6: RevokePermissions subtracts, does not delete entire entry ---

func TestRevokePermissions_Subtracts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		grant         []any
		revoke        []any
		wantRemaining []string
		wantDeleted   bool
	}{
		{
			name:          "partial revoke leaves remainder",
			grant:         []any{"SELECT", "INSERT"},
			revoke:        []any{"SELECT"},
			wantRemaining: []string{"INSERT"},
			wantDeleted:   false,
		},
		{
			name:        "full revoke deletes entry",
			grant:       []any{"SELECT"},
			revoke:      []any{"SELECT"},
			wantDeleted: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()
			h := lakeformation.NewHandler(b)

			principal := map[string]any{"DataLakePrincipalIdentifier": "arn:aws:iam::123:user/u"}
			resource := map[string]any{"Database": map[string]any{"Name": "db1"}}

			postJSON(t, h, "/GrantPermissions", map[string]any{
				"Principal":   principal,
				"Resource":    resource,
				"Permissions": tt.grant,
			})

			postJSON(t, h, "/RevokePermissions", map[string]any{
				"Principal":   principal,
				"Resource":    resource,
				"Permissions": tt.revoke,
			})

			rec := postJSON(t, h, "/ListPermissions", map[string]any{})
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, jsonDecode(rec.Body, &out))
			entries := out["PrincipalResourcePermissions"].([]any)

			if tt.wantDeleted {
				assert.Empty(t, entries)
			} else {
				require.Len(t, entries, 1)
				entry := entries[0].(map[string]any)
				perms := toStringSlice(entry["Permissions"].([]any))
				assert.ElementsMatch(t, tt.wantRemaining, perms)
			}
		})
	}
}

func toStringSlice(in []any) []string {
	out := make([]string, len(in))
	for i, v := range in {
		out[i] = v.(string)
	}

	return out
}

// --- #7: ListPermissions filters ---

func TestListPermissions_Filters(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	alice := map[string]any{"DataLakePrincipalIdentifier": "arn:aws:iam::123:user/alice"}
	bob := map[string]any{"DataLakePrincipalIdentifier": "arn:aws:iam::123:user/bob"}
	dbResource := map[string]any{"Database": map[string]any{"Name": "mydb"}}
	tableResource := map[string]any{"Table": map[string]any{"DatabaseName": "mydb", "Name": "mytable"}}

	postJSON(t, h, "/GrantPermissions", map[string]any{
		"Principal": alice, "Resource": dbResource, "Permissions": []any{"SELECT"},
	})
	postJSON(t, h, "/GrantPermissions", map[string]any{
		"Principal": bob, "Resource": tableResource, "Permissions": []any{"INSERT"},
	})

	tests := []struct {
		filterBody  map[string]any
		name        string
		wantPrincID string
		wantCount   int
	}{
		{
			name:        "filter by principal returns only alice's entries",
			filterBody:  map[string]any{"Principal": alice},
			wantCount:   1,
			wantPrincID: "arn:aws:iam::123:user/alice",
		},
		{
			name:       "filter by ResourceType DATABASE returns only database entries",
			filterBody: map[string]any{"ResourceType": "DATABASE"},
			wantCount:  1,
		},
		{
			name:       "filter by ResourceType TABLE returns only table entries",
			filterBody: map[string]any{"ResourceType": "TABLE"},
			wantCount:  1,
		},
		{
			name:       "no filter returns all entries",
			filterBody: map[string]any{},
			wantCount:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := postJSON(t, h, "/ListPermissions", tt.filterBody)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, jsonDecode(rec.Body, &out))
			entries := out["PrincipalResourcePermissions"].([]any)
			assert.Len(t, entries, tt.wantCount, "test: %s", tt.name)

			if tt.wantPrincID != "" && len(entries) > 0 {
				entry := entries[0].(map[string]any)
				principal := entry["Principal"].(map[string]any)
				assert.Equal(t, tt.wantPrincID, principal["DataLakePrincipalIdentifier"])
			}
		})
	}
}

// --- #8: AssumeDecoratedRoleWithSAML random creds ---

func TestGrantPermissions_TableWithColumns(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/GrantPermissions", map[string]any{
		"Principal": map[string]any{"DataLakePrincipalIdentifier": "arn:aws:iam::123:user/u"},
		"Resource": map[string]any{
			"TableWithColumns": map[string]any{
				"DatabaseName": "mydb",
				"Name":         "mytable",
				"ColumnNames":  []any{"col1", "col2"},
			},
		},
		"Permissions": []any{"SELECT"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := postJSON(t, h, "/ListPermissions", map[string]any{"ResourceType": "TABLE"})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec2.Body, &out))
	entries := out["PrincipalResourcePermissions"].([]any)
	assert.Len(t, entries, 1)
}

// --- GetWorkUnits returns non-empty ranges ---

func TestGrantPermissions_GrantOptionSubsetValidation(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/GrantPermissions", map[string]any{
		"Principal":                  map[string]any{"DataLakePrincipalIdentifier": "arn:aws:iam::123:user/u"},
		"Resource":                   map[string]any{"Database": map[string]any{"Name": "db"}},
		"Permissions":                []any{"SELECT"},
		"PermissionsWithGrantOption": []any{"INSERT"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "GrantOption not in Permissions should fail")
}
