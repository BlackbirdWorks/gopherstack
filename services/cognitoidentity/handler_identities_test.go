package cognitoidentity_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_GetID_UnauthDisabled_EmptyLogins(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "no-unauth-handler-pool",
		"AllowUnauthenticatedIdentities": false,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

	rec := doCognitoIdentityRequest(t, h, "GetId", map[string]any{
		"AccountId":      "000000000000",
		"IdentityPoolId": created["IdentityPoolId"],
	})
	assert.Equal(t, http.StatusForbidden, rec.Code)
}

func TestHandler_ListIdentities_MaxResultsZero(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "maxresults-handler-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

	rec := doCognitoIdentityRequest(t, h, "ListIdentities", map[string]any{
		"IdentityPoolId": created["IdentityPoolId"],
		"MaxResults":     0,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "getid-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

	rec := doCognitoIdentityRequest(t, h, "GetId", map[string]any{
		"AccountId":      "000000000000",
		"IdentityPoolId": created["IdentityPoolId"],
		"Logins":         map[string]string{},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out["IdentityId"])
}

func TestHandler_GetID_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "GetId", map[string]any{
		"AccountId":      "000000000000",
		"IdentityPoolId": "us-east-1:nonexistent",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DeleteIdentities(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
		wantLen  int
	}{
		{
			name: "success_deletes_identities",
			body: nil, // set in test
			// wantLen 0 unprocessed = success
			wantCode: http.StatusOK,
			wantLen:  0,
		},
		{
			name: "empty_ids_rejected",
			body: map[string]any{
				"IdentityIdsToDelete": []string{},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "nonexistent_ids_silently_ignored",
			body: map[string]any{
				"IdentityIdsToDelete": []string{"us-east-1:nonexistent"},
			},
			wantCode: http.StatusOK,
			wantLen:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			body := tt.body

			if tt.name == "success_deletes_identities" {
				createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
					"IdentityPoolName":               "del-id-pool",
					"AllowUnauthenticatedIdentities": true,
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				var created map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

				idRec := doCognitoIdentityRequest(t, h, "GetId", map[string]any{
					"AccountId":      "000000000000",
					"IdentityPoolId": created["IdentityPoolId"],
				})
				require.Equal(t, http.StatusOK, idRec.Code)

				var idOut map[string]any
				require.NoError(t, json.Unmarshal(idRec.Body.Bytes(), &idOut))

				body = map[string]any{
					"IdentityIdsToDelete": []string{idOut["IdentityId"].(string)},
				}
			}

			rec := doCognitoIdentityRequest(t, h, "DeleteIdentities", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				unprocessed, _ := out["UnprocessedIdentityIds"].([]any)
				assert.Len(t, unprocessed, tt.wantLen)
			}
		})
	}
}

func TestHandler_DescribeIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		identityID string
		wantCode   int
	}{
		{name: "success", wantCode: http.StatusOK},
		{name: "not_found", identityID: "us-east-1:nonexistent", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			identityID := tt.identityID

			if tt.name == "success" {
				createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
					"IdentityPoolName":               "desc-id-pool",
					"AllowUnauthenticatedIdentities": true,
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				var created map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

				idRec := doCognitoIdentityRequest(t, h, "GetId", map[string]any{
					"AccountId":      "000000000000",
					"IdentityPoolId": created["IdentityPoolId"],
					"Logins": map[string]string{
						"accounts.google.com": "google-token-123",
					},
				})
				require.Equal(t, http.StatusOK, idRec.Code)

				var idOut map[string]any
				require.NoError(t, json.Unmarshal(idRec.Body.Bytes(), &idOut))
				identityID = idOut["IdentityId"].(string)
			}

			rec := doCognitoIdentityRequest(t, h, "DescribeIdentity", map[string]any{
				"IdentityId": identityID,
			})

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, identityID, out["IdentityId"])
				assert.NotEmpty(t, out["CreationDate"])
				logins, _ := out["Logins"].([]any)
				assert.Contains(t, logins, "accounts.google.com")
			}
		})
	}
}

func TestHandler_ListIdentities(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		poolID   string
		wantCode int
		wantLen  int
	}{
		{name: "success_with_identities", wantCode: http.StatusOK, wantLen: 2},
		{name: "pool_not_found", poolID: "us-east-1:nonexistent", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID := tt.poolID

			if tt.name == "success_with_identities" {
				createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
					"IdentityPoolName":               "list-id-pool",
					"AllowUnauthenticatedIdentities": true,
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				var created map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
				poolID = created["IdentityPoolId"].(string)

				for _, login := range []map[string]string{
					{"provider.a.com": "token1"},
					{"provider.b.com": "token2"},
				} {
					idRec := doCognitoIdentityRequest(t, h, "GetId", map[string]any{
						"AccountId":      "000000000000",
						"IdentityPoolId": poolID,
						"Logins":         login,
					})
					require.Equal(t, http.StatusOK, idRec.Code)
				}
			}

			rec := doCognitoIdentityRequest(t, h, "ListIdentities", map[string]any{
				"IdentityPoolId": poolID,
				"MaxResults":     10,
			})

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, poolID, out["IdentityPoolId"])

				identities, _ := out["Identities"].([]any)
				assert.Len(t, identities, tt.wantLen)
			}
		})
	}
}

func TestSortedListIdentities(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "sorted-id-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	poolID := created["IdentityPoolId"].(string)

	for _, login := range []map[string]string{
		{"provider.a.com": "t1"},
		{"provider.b.com": "t2"},
		{"provider.c.com": "t3"},
	} {
		idRec := doCognitoIdentityRequest(t, h, "GetId", map[string]any{
			"AccountId":      "000000000000",
			"IdentityPoolId": poolID,
			"Logins":         login,
		})
		require.Equal(t, http.StatusOK, idRec.Code)
	}

	rec := doCognitoIdentityRequest(t, h, "ListIdentities", map[string]any{
		"IdentityPoolId": poolID,
		"MaxResults":     10,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listOut))

	identities, _ := listOut["Identities"].([]any)
	require.Len(t, identities, 3)

	returnedIDs := make([]string, len(identities))
	for i, identity := range identities {
		im, _ := identity.(map[string]any)
		returnedIDs[i] = im["IdentityId"].(string)
	}

	// Verify IDs are sorted.
	for i := 1; i < len(returnedIDs); i++ {
		assert.Less(t, returnedIDs[i-1], returnedIDs[i], "identities should be sorted by IdentityId")
	}
}

func TestDeleteIdentities_MaxLimit(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// 61 IDs should be rejected.
	ids := make([]string, 61)
	for i := range ids {
		ids[i] = fmt.Sprintf("us-east-1:fake-id-%d", i)
	}

	rec := doCognitoIdentityRequest(t, h, "DeleteIdentities", map[string]any{
		"IdentityIdsToDelete": ids,
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestListIdentities_MaxResultsValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxResults int
		wantCode   int
	}{
		{name: "valid_60", maxResults: 60, wantCode: http.StatusOK},
		{name: "zero_rejected", maxResults: 0, wantCode: http.StatusBadRequest},
		{name: "too_large_61", maxResults: 61, wantCode: http.StatusBadRequest},
		{name: "negative", maxResults: -1, wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
				"IdentityPoolName":               "max-results-pool",
				"AllowUnauthenticatedIdentities": true,
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var created map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

			rec := doCognitoIdentityRequest(t, h, "ListIdentities", map[string]any{
				"IdentityPoolId": created["IdentityPoolId"],
				"MaxResults":     tt.maxResults,
			})

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestDescribeIdentity_EmptyIdRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doCognitoIdentityRequest(t, h, "DescribeIdentity", map[string]any{
		"IdentityId": "",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestSortedLogins(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "sorted-logins-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	poolID := created["IdentityPoolId"].(string)

	idRec := doCognitoIdentityRequest(t, h, "GetId", map[string]any{
		"AccountId":      "000000000000",
		"IdentityPoolId": poolID,
		"Logins": map[string]string{
			"zzz.provider.com": "token3",
			"aaa.provider.com": "token1",
			"mmm.provider.com": "token2",
		},
	})
	require.Equal(t, http.StatusOK, idRec.Code)

	var idOut map[string]any
	require.NoError(t, json.Unmarshal(idRec.Body.Bytes(), &idOut))
	identityID := idOut["IdentityId"].(string)

	rec := doCognitoIdentityRequest(t, h, "DescribeIdentity", map[string]any{
		"IdentityId": identityID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descOut))

	logins, _ := descOut["Logins"].([]any)
	require.Len(t, logins, 3)

	loginStrs := make([]string, len(logins))
	for i, l := range logins {
		loginStrs[i] = l.(string)
	}

	// Verify logins are sorted.
	for i := 1; i < len(loginStrs); i++ {
		assert.Less(t, loginStrs[i-1], loginStrs[i], "logins should be sorted")
	}
}

func TestHandler_ListIdentities_EmptyPoolId_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "ListIdentities", map[string]any{
		"IdentityPoolId": "",
		"MaxResults":     10,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetID_EmptyPoolId_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "GetId", map[string]any{
		"IdentityPoolId": "",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetID_MergesLoginProviders(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "login-merge-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut map[string]any
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))
	poolID := createOut["IdentityPoolId"].(string)

	// First call: only google.
	rec1 := doCognitoIdentityRequest(t, h, "GetId", map[string]any{
		"IdentityPoolId": poolID,
		"Logins":         map[string]string{"accounts.google.com": "g-tok"},
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.NewDecoder(rec1.Body).Decode(&out1))
	id1 := out1["IdentityId"].(string)

	// Second call: same google + new facebook → must return same ID.
	rec2 := doCognitoIdentityRequest(t, h, "GetId", map[string]any{
		"IdentityPoolId": poolID,
		"Logins": map[string]string{
			"accounts.google.com": "g-tok",
			"graph.facebook.com":  "fb-tok",
		},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&out2))
	id2 := out2["IdentityId"].(string)

	assert.Equal(t, id1, id2, "GetId must return the same identity when any login provider matches")
}

func TestHandler_ListIdentities_NextToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "page-id-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut map[string]any
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))
	poolID := createOut["IdentityPoolId"].(string)

	for i := range 3 {
		doCognitoIdentityRequest(t, h, "GetId", map[string]any{
			"IdentityPoolId": poolID,
			"Logins":         map[string]string{"accounts.google.com": fmt.Sprintf("tok-%d", i)},
		})
	}

	// Page 1.
	rec1 := doCognitoIdentityRequest(t, h, "ListIdentities", map[string]any{
		"IdentityPoolId": poolID,
		"MaxResults":     2,
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.NewDecoder(rec1.Body).Decode(&out1))
	assert.Len(t, out1["Identities"], 2)
	nextToken, _ := out1["NextToken"].(string)
	assert.NotEmpty(t, nextToken)

	// Page 2.
	rec2 := doCognitoIdentityRequest(t, h, "ListIdentities", map[string]any{
		"IdentityPoolId": poolID,
		"MaxResults":     2,
		"NextToken":      nextToken,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&out2))
	assert.Len(t, out2["Identities"], 1)
}
