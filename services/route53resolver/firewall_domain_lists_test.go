package route53resolver_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53resolver"
)

func TestFirewallDomainList_ManagedOwnerName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateFirewallDomainList", map[string]any{
		"Name":             "my-domain-list",
		"CreatorRequestId": "req-dl-1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	dl := resp["FirewallDomainList"].(map[string]any)

	// ManagedOwnerName should be empty for user-created domain lists.
	assert.Empty(t, dl["ManagedOwnerName"])
}

// --- Issue 20 + 21: FirewallRule BlockOverride* fields ---

func TestDeleteFirewallDomainList_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DeleteFirewallDomainList", map[string]any{
		"FirewallDomainListId": "rslvr-fdl-nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- CreateOutpostResolver defaults ---

func TestCreateFirewallDomainList(t *testing.T) {
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
				"Name":             "my-domain-list",
				"CreatorRequestId": "req-dl-1",
			},
			wantCode: http.StatusOK,
			wantName: "my-domain-list",
		},
		{
			name:     "missing_name",
			body:     map[string]any{"CreatorRequestId": "req-dl-2"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateFirewallDomainList", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				dl, ok := resp["FirewallDomainList"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantName, dl["Name"])
				assert.Equal(t, "COMPLETE", dl["Status"])
				assert.NotEmpty(t, dl["Id"])
				assert.Contains(t, dl["Arn"].(string), "arn:aws:route53resolver:")
			}
		})
	}
}

// --- DeleteFirewallDomainList ---

func TestDeleteFirewallDomainList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupExtra func(t *testing.T, h *route53resolver.Handler) string
		body       func(id string) map[string]any
		name       string
		wantCode   int
	}{
		{
			name: "success",
			setupExtra: func(t *testing.T, h *route53resolver.Handler) string {
				t.Helper()
				rec := doRequest(t, h, "CreateFirewallDomainList", map[string]any{"Name": "dl-del"})
				require.Equal(t, http.StatusOK, rec.Code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["FirewallDomainList"].(map[string]any)["Id"].(string)
			},
			body:     func(id string) map[string]any { return map[string]any{"FirewallDomainListId": id} },
			wantCode: http.StatusOK,
		},
		{
			name:       "missing_id",
			setupExtra: func(_ *testing.T, _ *route53resolver.Handler) string { return "" },
			body:       func(_ string) map[string]any { return map[string]any{} },
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "not_found",
			setupExtra: func(_ *testing.T, _ *route53resolver.Handler) string { return "" },
			body:       func(_ string) map[string]any { return map[string]any{"FirewallDomainListId": "rslvr-fdl-notexist"} },
			wantCode:   http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.setupExtra(t, h)
			rec := doRequest(t, h, "DeleteFirewallDomainList", tt.body(id))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				dl, ok := resp["FirewallDomainList"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, id, dl["Id"])
			}
		})
	}
}

// --- CreateFirewallRule ---

// TestParity_ListFirewallDomains_Pagination verifies NextToken/MaxResults on
// ListFirewallDomains. Real AWS paginates domain entries within a list.
func TestListFirewallDomains_Pagination(t *testing.T) {
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

// TestR53R_FirewallDomainListCRUD covers CreateFirewallDomainList, GetFirewallDomainList,
// ListFirewallDomainLists, ListFirewallDomains, UpdateFirewallDomains,
// ImportFirewallDomains, DeleteFirewallDomainList.
func TestFirewallDomainListCRUD(t *testing.T) {
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

// TestListFirewallDomainLists_NoOverwideLeak asserts the raw JSON body of a
// ListFirewallDomainLists item has none of Status/DomainCount/CreationTime/
// ModificationTime/StatusMessage -- the real ListFirewallDomainListsOutput.
// FirewallDomainLists is []types.FirewallDomainListMetadata
// (route53resolver@v1.48.4 types/types.go:584, deserializer at
// deserializers.go:10568), which has none of those members. An SDK client
// silently drops the unrecognized keys, so only a raw-body assertion
// catches the leak.
func TestListFirewallDomainLists_NoOverwideLeak(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateFirewallDomainList", map[string]any{
		"Name":             "leak-check-list",
		"CreatorRequestId": "req-leak",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "ListFirewallDomainLists", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var raw struct {
		FirewallDomainLists []map[string]any `json:"FirewallDomainLists"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
	require.Len(t, raw.FirewallDomainLists, 1)

	item := raw.FirewallDomainLists[0]
	for _, leaked := range []string{"Status", "DomainCount", "CreationTime", "ModificationTime", "StatusMessage"} {
		_, has := item[leaked]
		assert.Falsef(
			t,
			has,
			"ListFirewallDomainLists item leaked %s; real types.FirewallDomainListMetadata has no such member",
			leaked,
		)
	}
	assert.Contains(t, item, "Id", "FirewallDomainListMetadata must still emit Id")
	assert.Contains(t, item, "Name", "FirewallDomainListMetadata must still emit Name")
}
