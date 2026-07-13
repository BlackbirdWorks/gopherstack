package lakeformation_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lakeformation"
)

// --- #1: DataLakeSettings full round-trip ---

func TestRefinement4_DataLakeSettings_FullRoundTrip(t *testing.T) {
	t.Parallel()

	trueVal := true

	tests := []struct {
		name     string
		settings map[string]any
		checkKey string
	}{
		{
			name: "ReadOnlyAdmins persisted",
			settings: map[string]any{
				"DataLakeSettings": map[string]any{
					"ReadOnlyAdmins": []any{
						map[string]any{"DataLakePrincipalIdentifier": "arn:aws:iam::123:user/readonly"},
					},
				},
			},
			checkKey: "ReadOnlyAdmins",
		},
		{
			name: "Parameters persisted",
			settings: map[string]any{
				"DataLakeSettings": map[string]any{
					"Parameters": map[string]any{
						"CROSS_ACCOUNT_VERSION": "3",
					},
				},
			},
			checkKey: "Parameters",
		},
		{
			name: "AllowExternalDataFiltering persisted",
			settings: map[string]any{
				"DataLakeSettings": map[string]any{
					"AllowExternalDataFiltering": trueVal,
				},
			},
			checkKey: "AllowExternalDataFiltering",
		},
		{
			name: "AuthorizedSessionTagValueList persisted",
			settings: map[string]any{
				"DataLakeSettings": map[string]any{
					"AuthorizedSessionTagValueList": []any{"val1", "val2"},
				},
			},
			checkKey: "AuthorizedSessionTagValueList",
		},
		{
			name: "AllowFullTableExternalDataAccess persisted",
			settings: map[string]any{
				"DataLakeSettings": map[string]any{
					"AllowFullTableExternalDataAccess": trueVal,
				},
			},
			checkKey: "AllowFullTableExternalDataAccess",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()
			h := lakeformation.NewHandler(b)

			rec := postJSON(t, h, "/PutDataLakeSettings", tt.settings)
			require.Equal(t, http.StatusOK, rec.Code)

			rec2 := postJSON(t, h, "/GetDataLakeSettings", map[string]any{})
			require.Equal(t, http.StatusOK, rec2.Code)

			var out map[string]any
			require.NoError(t, jsonDecode(rec2.Body, &out))

			settings, ok := out["DataLakeSettings"].(map[string]any)
			require.True(t, ok, "DataLakeSettings missing from response")
			assert.NotNil(t, settings[tt.checkKey], "field %q not persisted", tt.checkKey)
		})
	}
}

// --- #2: RegisterResource UseServiceLinkedRole ---

func TestRefinement4_RegisterResource_UseServiceLinkedRole(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body            map[string]any
		name            string
		wantStatus      int
		wantSLRoleInARN bool
	}{
		{
			name: "UseServiceLinkedRole=true synthesises role ARN",
			body: map[string]any{
				"ResourceArn":          "arn:aws:s3:::my-datalake",
				"UseServiceLinkedRole": true,
			},
			wantStatus:      http.StatusOK,
			wantSLRoleInARN: true,
		},
		{
			name: "UseServiceLinkedRole=true with explicit RoleArn rejected",
			body: map[string]any{
				"ResourceArn":          "arn:aws:s3:::my-datalake-2",
				"UseServiceLinkedRole": true,
				"RoleArn":              "arn:aws:iam::123:role/MyRole",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "WithFederation flag stored",
			body: map[string]any{
				"ResourceArn":    "arn:aws:s3:::federated-lake",
				"RoleArn":        "arn:aws:iam::123:role/R",
				"WithFederation": true,
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "HybridAccessEnabled flag stored",
			body: map[string]any{
				"ResourceArn":         "arn:aws:s3:::hybrid-lake",
				"RoleArn":             "arn:aws:iam::123:role/R",
				"HybridAccessEnabled": true,
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()
			h := lakeformation.NewHandler(b)

			rec := postJSON(t, h, "/RegisterResource", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantSLRoleInARN && tt.wantStatus == http.StatusOK {
				arn := tt.body["ResourceArn"].(string)
				rec2 := postJSON(t, h, "/DescribeResource", map[string]any{"ResourceArn": arn})
				require.Equal(t, http.StatusOK, rec2.Code)

				var out map[string]any
				require.NoError(t, jsonDecode(rec2.Body, &out))
				info := out["ResourceInfo"].(map[string]any)
				assert.Contains(t, info["RoleArn"].(string), "AWSServiceRoleForLakeFormationDataAccess")
			}
		})
	}
}

// --- #4: Permission enum validation ---

func TestRefinement4_GrantPermissions_InvalidEnum(t *testing.T) {
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
			name:        "CREATE_LAKE_FORMATION_OPT_IN accepted",
			permissions: []any{"CREATE_LAKE_FORMATION_OPT_IN"},
			wantStatus:  http.StatusOK,
		},
		{
			name:        "DROP accepted",
			permissions: []any{"DROP"},
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

func TestRefinement4_GrantPermissions_MergesDuplicates(t *testing.T) {
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

func TestRefinement4_RevokePermissions_Subtracts(t *testing.T) {
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

func TestRefinement4_ListPermissions_Filters(t *testing.T) {
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

func TestRefinement4_AssumeDecoratedRoleWithSAML_RandomCreds(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	body := map[string]any{
		"PrincipalArn":  "arn:aws:iam::123:saml-provider/IdP",
		"RoleArn":       "arn:aws:iam::123:role/MyRole",
		"SAMLAssertion": "dGVzdC1zYW1sLWFzc2VydGlvbg==",
	}

	rec1 := postJSON(t, h, "/AssumeDecoratedRoleWithSAML", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := postJSON(t, h, "/AssumeDecoratedRoleWithSAML", body)
	require.Equal(t, http.StatusOK, rec2.Code)

	var out1, out2 map[string]any
	require.NoError(t, jsonDecode(rec1.Body, &out1))
	require.NoError(t, jsonDecode(rec2.Body, &out2))

	assert.NotEmpty(t, out1["AccessKeyId"])
	assert.NotEmpty(t, out1["SessionToken"])

	// Credentials must differ between calls
	assert.NotEqual(t, out1["AccessKeyId"], out2["AccessKeyId"], "AccessKeyId should be unique per call")
	assert.NotEqual(t, out1["SessionToken"], out2["SessionToken"], "SessionToken should be unique per call")
}

// --- #9: GetTemporaryCredentials unique per request ---

func TestRefinement4_GetTemporaryGlueTableCredentials_UniquePerCall(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	body := map[string]any{
		"TableArn": "arn:aws:glue:us-east-1:123:table/db/tbl",
	}

	sessions := make([]string, 0, 3)

	for range 3 {
		rec := postJSON(t, h, "/GetTemporaryGlueTableCredentials", body)
		require.Equal(t, http.StatusOK, rec.Code)

		var out map[string]any
		require.NoError(t, jsonDecode(rec.Body, &out))
		// GetTemporaryGlueTableCredentialsOutput returns credential fields
		// flat (no nested "Credentials" object), unlike
		// GetTemporaryDataLocationCredentials -- see the real
		// aws-sdk-go-v2 GetTemporaryGlueTableCredentialsOutput shape.
		sessions = append(sessions, out["SessionToken"].(string))
	}

	// All session tokens must be distinct
	seen := make(map[string]bool)
	for _, s := range sessions {
		assert.False(t, seen[s], "duplicate SessionToken: %s", s)
		seen[s] = true
	}
}

// --- #10: StartTransaction respects TransactionType ---

func TestRefinement4_StartTransaction_ReadOnly_RejectsWrites(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/StartTransaction", map[string]any{"TransactionType": "READ_ONLY"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	txID := out["TransactionId"].(string)

	// Write to READ_ONLY transaction must fail
	rec2 := postJSON(t, h, "/UpdateTableObjects", map[string]any{
		"TransactionId": txID,
		"DatabaseName":  "db1",
		"TableName":     "t1",
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestRefinement4_StartTransaction_ReadAndWrite_AllowsWrites(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/StartTransaction", map[string]any{"TransactionType": "READ_AND_WRITE"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	txID := out["TransactionId"].(string)

	rec2 := postJSON(t, h, "/UpdateTableObjects", map[string]any{
		"TransactionId": txID,
		"DatabaseName":  "db1",
		"TableName":     "t1",
	})
	assert.Equal(t, http.StatusOK, rec2.Code)
}

// --- #11: Transaction timestamps ---

func TestRefinement4_DescribeTransaction_HasTimestamps(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec1 := postJSON(t, h, "/StartTransaction", nil)
	require.Equal(t, http.StatusOK, rec1.Code)

	var startOut map[string]any
	require.NoError(t, jsonDecode(rec1.Body, &startOut))
	txID := startOut["TransactionId"].(string)

	rec2 := postJSON(t, h, "/DescribeTransaction", map[string]any{"TransactionId": txID})
	require.Equal(t, http.StatusOK, rec2.Code)

	var descOut map[string]any
	require.NoError(t, jsonDecode(rec2.Body, &descOut))

	desc := descOut["TransactionDescription"].(map[string]any)
	assert.NotEmpty(t, desc["TransactionStartTime"], "TransactionStartTime should be set")
}

func TestRefinement4_CommitTransaction_SetsEndTime(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec1 := postJSON(t, h, "/StartTransaction", nil)
	var startOut map[string]any
	require.NoError(t, jsonDecode(rec1.Body, &startOut))
	txID := startOut["TransactionId"].(string)

	postJSON(t, h, "/CommitTransaction", map[string]any{"TransactionId": txID})

	rec3 := postJSON(t, h, "/DescribeTransaction", map[string]any{"TransactionId": txID})
	var descOut map[string]any
	require.NoError(t, jsonDecode(rec3.Body, &descOut))

	desc := descOut["TransactionDescription"].(map[string]any)
	assert.NotEmpty(t, desc["TransactionEndTime"], "TransactionEndTime should be set after commit")
}

// --- #13: ListTransactions StatusFilter enum ---

func TestRefinement4_ListTransactions_COMPLETED_Filter(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	// committed transaction
	postJSON(t, h, "/CommitTransaction", map[string]any{"TransactionId": "tx-commit"})
	// aborted transaction
	postJSON(t, h, "/CancelTransaction", map[string]any{"TransactionId": "tx-abort"})
	// active transaction
	postJSON(t, h, "/StartTransaction", nil)

	rec := postJSON(t, h, "/ListTransactions", map[string]any{"StatusFilter": "COMPLETED"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	txns := out["Transactions"].([]any)
	assert.Len(t, txns, 2, "COMPLETED should return both COMMITTED and ABORTED")

	for _, tx := range txns {
		status := tx.(map[string]any)["TransactionStatus"].(string)
		assert.True(t, status == "COMMITTED" || status == "ABORTED", "unexpected status: %s", status)
	}
}

func TestRefinement4_ListTransactions_ALL_Filter(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	postJSON(t, h, "/CommitTransaction", map[string]any{"TransactionId": "tx1"})
	postJSON(t, h, "/StartTransaction", nil)

	rec := postJSON(t, h, "/ListTransactions", map[string]any{"StatusFilter": "ALL"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	txns := out["Transactions"].([]any)
	assert.Len(t, txns, 2, "ALL should return every transaction")
}

// --- #14: DataCellsFilter RowFilter and ColumnNames ---

func TestRefinement4_DataCellsFilter_RowFilterAndColumns(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	expr := "col1 > 100"

	rec := postJSON(t, h, "/CreateDataCellsFilter", map[string]any{
		"TableData": map[string]any{
			"TableCatalogId": "123456789012",
			"DatabaseName":   "mydb",
			"TableName":      "mytable",
			"Name":           "filter1",
			"RowFilter": map[string]any{
				"FilterExpression": expr,
			},
			"ColumnNames": []any{"col1", "col2"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := postJSON(t, h, "/GetDataCellsFilter", map[string]any{
		"TableCatalogId": "123456789012",
		"DatabaseName":   "mydb",
		"TableName":      "mytable",
		"Name":           "filter1",
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec2.Body, &out))
	filter := out["DataCellsFilter"].(map[string]any)

	rowFilter, ok := filter["RowFilter"].(map[string]any)
	require.True(t, ok, "RowFilter should be present")
	assert.Equal(t, expr, rowFilter["FilterExpression"])

	cols := filter["ColumnNames"].([]any)
	assert.Len(t, cols, 2)
}

// --- #15: ListDataCellsFilter requires Table ---

func TestRefinement4_ListDataCellsFilter_RequiresTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "no Table field returns 400",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "Table with missing DatabaseName returns 400",
			body: map[string]any{
				"Table": map[string]any{"Name": "mytable"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "valid Table returns 200",
			body: map[string]any{
				"Table": map[string]any{
					"DatabaseName": "mydb",
					"Name":         "mytable",
				},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()
			h := lakeformation.NewHandler(b)

			rec := postJSON(t, h, "/ListDataCellsFilter", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code, "test: %s", tt.name)
		})
	}
}

// --- #16: UpdateLFTagExpression pointer-style optionals ---

func TestRefinement4_UpdateLFTagExpression_PartialUpdate(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	// Create expression
	postJSON(t, h, "/CreateLFTagExpression", map[string]any{
		"Name":        "myexpr",
		"Description": "original description",
		"Expression":  []any{map[string]any{"TagKey": "env", "TagValues": []any{"dev"}}},
	})

	// Update description only (no Expression field)
	rec := postJSON(t, h, "/UpdateLFTagExpression", map[string]any{
		"Name":        "myexpr",
		"Description": "updated description",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify expression preserved
	rec2 := postJSON(t, h, "/GetLFTagExpression", map[string]any{"Name": "myexpr"})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec2.Body, &out))
	assert.Equal(t, "updated description", out["Description"])
	expr := out["Expression"].([]any)
	assert.Len(t, expr, 1, "Expression should be preserved when not updated")
}

func TestRefinement4_UpdateLFTagExpression_EmptyExpressionRejected(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	postJSON(t, h, "/CreateLFTagExpression", map[string]any{
		"Name":       "myexpr2",
		"Expression": []any{map[string]any{"TagKey": "env", "TagValues": []any{"dev"}}},
	})

	// Send empty Expression array — should fail
	rec := postJSON(t, h, "/UpdateLFTagExpression", map[string]any{
		"Name":       "myexpr2",
		"Expression": []any{},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- #17: CreateLakeFormationIdentityCenterConfiguration AlreadyExists ---

func TestRefinement4_CreateIdentityCenter_AlreadyExists(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	body := map[string]any{
		"CatalogId":   "123456789012",
		"InstanceArn": "arn:aws:sso:::instance/ssoins-abc",
	}

	rec1 := postJSON(t, h, "/CreateLakeFormationIdentityCenterConfiguration", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := postJSON(t, h, "/CreateLakeFormationIdentityCenterConfiguration", body)
	assert.Equal(t, http.StatusConflict, rec2.Code, "duplicate create should return 409")
}

func TestRefinement4_CreateIdentityCenter_WithExternalFiltering(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/CreateLakeFormationIdentityCenterConfiguration", map[string]any{
		"CatalogId":   "123456789012",
		"InstanceArn": "arn:aws:sso:::instance/ssoins-abc",
		"ExternalFiltering": map[string]any{
			"Status": "ENABLED",
		},
		"ShareRecipients": []any{
			map[string]any{"DataLakePrincipalIdentifier": "arn:aws:iam::456:root"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := postJSON(t, h, "/DescribeLakeFormationIdentityCenterConfiguration", map[string]any{
		"CatalogId": "123456789012",
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec2.Body, &out))
	assert.NotNil(t, out["ExternalFiltering"])
	assert.NotEmpty(t, out["ShareRecipients"])
}

// --- #18: UpdateLakeFormationIdentityCenterConfiguration ApplicationStatus ---

func TestRefinement4_UpdateIdentityCenter_ApplicationStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		appStatus  string
		wantStatus int
	}{
		{
			name:       "ENABLED accepted",
			appStatus:  "ENABLED",
			wantStatus: http.StatusOK,
		},
		{
			name:       "DISABLED accepted",
			appStatus:  "DISABLED",
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid status rejected",
			appStatus:  "BROKEN",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()
			h := lakeformation.NewHandler(b)

			// First create the config
			_, _ = b.CreateLakeFormationIdentityCenterConfiguration(
				"123456789012",
				"arn:aws:sso:::instance/i",
				nil,
				nil,
			)

			rec := postJSON(t, h, "/UpdateLakeFormationIdentityCenterConfiguration", map[string]any{
				"CatalogId":         "123456789012",
				"ApplicationStatus": tt.appStatus,
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "test: %s", tt.name)

			if tt.wantStatus == http.StatusOK {
				rec2 := postJSON(t, h, "/DescribeLakeFormationIdentityCenterConfiguration", map[string]any{
					"CatalogId": "123456789012",
				})
				require.Equal(t, http.StatusOK, rec2.Code)

				var out map[string]any
				require.NoError(t, jsonDecode(rec2.Body, &out))
				assert.Equal(t, tt.appStatus, out["ApplicationStatus"])
			}
		})
	}
}

// --- #19: CreateLakeFormationOptIn timestamps ---

func TestRefinement4_CreateLakeFormationOptIn_HasTimestamps(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/CreateLakeFormationOptIn", map[string]any{
		"Principal": map[string]any{"DataLakePrincipalIdentifier": "arn:aws:iam::123:user/alice"},
		"Resource":  map[string]any{"Database": map[string]any{"Name": "db1"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := postJSON(t, h, "/ListLakeFormationOptIns", map[string]any{})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec2.Body, &out))

	list, ok := out["LakeFormationOptInsInfoList"].([]any)
	require.True(t, ok && len(list) > 0)

	entry := list[0].(map[string]any)
	assert.NotEmpty(t, entry["LastModified"], "LastModified should be set")
	assert.NotEmpty(t, entry["LastUpdatedBy"], "LastUpdatedBy should be set")
}

// --- #20: ListLakeFormationOptIns resource filter ---

func TestRefinement4_ListLakeFormationOptIns_ResourceFilter(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	alice := map[string]any{"DataLakePrincipalIdentifier": "arn:aws:iam::123:user/alice"}
	db1 := map[string]any{"Database": map[string]any{"Name": "db1"}}
	db2 := map[string]any{"Database": map[string]any{"Name": "db2"}}

	postJSON(t, h, "/CreateLakeFormationOptIn", map[string]any{"Principal": alice, "Resource": db1})
	postJSON(t, h, "/CreateLakeFormationOptIn", map[string]any{"Principal": alice, "Resource": db2})

	rec := postJSON(t, h, "/ListLakeFormationOptIns", map[string]any{
		"Resource": db1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))

	list, _ := out["LakeFormationOptInsInfoList"].([]any)
	assert.Len(t, list, 1, "should only return opt-ins for db1")
}

// --- #25: AddLFTagsToResource validates tag values ---

func TestRefinement4_AddLFTagsToResource_ValidatesTagValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		wantFailureCode string
		allowedValues   []string
		tagValues       []any
		wantFailures    bool
	}{
		{
			name:          "valid value succeeds",
			allowedValues: []string{"dev", "prod"},
			tagValues:     []any{"dev"},
			wantFailures:  false,
		},
		{
			name:            "invalid value fails",
			allowedValues:   []string{"dev", "prod"},
			tagValues:       []any{"staging"},
			wantFailures:    true,
			wantFailureCode: "InvalidInputException",
		},
		{
			name:            "mix of valid and invalid fails for that tag",
			allowedValues:   []string{"dev", "prod"},
			tagValues:       []any{"dev", "nope"},
			wantFailures:    true,
			wantFailureCode: "InvalidInputException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()
			h := lakeformation.NewHandler(b)

			require.NoError(t, b.CreateLFTag("", "env", tt.allowedValues))

			rec := postJSON(t, h, "/AddLFTagsToResource", map[string]any{
				"Resource": map[string]any{"Database": map[string]any{"Name": "db1"}},
				"LFTags": []any{
					map[string]any{"TagKey": "env", "TagValues": tt.tagValues},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, jsonDecode(rec.Body, &out))
			failures, _ := out["Failures"].([]any)

			if tt.wantFailures {
				require.NotEmpty(t, failures, "should have failures")
				failure := failures[0].(map[string]any)
				errDetail := failure["Error"].(map[string]any)
				assert.Equal(t, tt.wantFailureCode, errDetail["ErrorCode"])
			} else {
				assert.Empty(t, failures)
			}
		})
	}
}

// --- Persistence round-trip (issue #30) ---

func TestRefinement4_Persistence_IncludesQueriesAndOptimizers(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	// Seed a query
	rec := postJSON(t, h, "/StartQueryPlanning", map[string]any{
		"QueryString":          "SELECT * FROM db.t",
		"QueryPlanningContext": map[string]any{"DatabaseName": "db"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Seed a storage optimizer
	postJSON(t, h, "/UpdateTableStorageOptimizer", map[string]any{
		"DatabaseName": "db",
		"TableName":    "t",
		"StorageOptimizerConfig": map[string]any{
			"compaction": map[string]any{"enabled": "true"},
		},
	})

	// Snapshot
	snap, err := b.Snapshot()
	require.NoError(t, err)

	// Restore to fresh backend
	b2 := lakeformation.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	// Verify queries survived restore (GetQueryState should work)
	var qOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &qOut))
	queryID := qOut["QueryId"].(string)

	state, err := b2.GetQueryState(queryID)
	require.NoError(t, err)
	assert.Equal(t, "WORKUNITS_AVAILABLE", state)

	// Verify optimizer survived restore
	opts := b2.ListTableStorageOptimizers("", "db", "t", "")
	assert.Len(t, opts, 1)
}

// --- Resource variants (#3): TableWithColumns ---

func TestRefinement4_GrantPermissions_TableWithColumns(t *testing.T) {
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

func TestRefinement4_GetWorkUnits_ReturnsRanges(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/StartQueryPlanning", map[string]any{
		"QueryString":          "SELECT 1",
		"QueryPlanningContext": map[string]any{"DatabaseName": "db"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var pOut map[string]any
	require.NoError(t, jsonDecode(rec.Body, &pOut))
	queryID := pOut["QueryId"].(string)

	rec2 := postJSON(t, h, "/GetWorkUnits", map[string]any{"QueryId": queryID})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec2.Body, &out))
	ranges := out["WorkUnitRanges"].([]any)
	assert.NotEmpty(t, ranges, "GetWorkUnits should return at least one range")
}

// --- PermissionsWithGrantOption must be subset of Permissions ---

func TestRefinement4_GrantPermissions_GrantOptionSubsetValidation(t *testing.T) {
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
