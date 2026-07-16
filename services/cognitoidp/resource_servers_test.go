package cognitoidp_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResourceServers_Persist(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("rs-pool")
	require.NoError(t, err)

	rs, err := b.CreateResourceServer(pool.ID, "https://api.example.com", "My API", []cognitoidp.ResourceServerScope{
		{ScopeName: "read", ScopeDescription: "Read access"},
		{ScopeName: "write", ScopeDescription: "Write access"},
	})
	require.NoError(t, err)
	assert.Equal(t, "https://api.example.com", rs.Identifier)
	assert.Equal(t, "My API", rs.Name)
	assert.Len(t, rs.Scopes, 2)

	got, err := b.DescribeResourceServer(pool.ID, "https://api.example.com")
	require.NoError(t, err)
	assert.Equal(t, "My API", got.Name)

	servers, err := b.ListResourceServers(pool.ID)
	require.NoError(t, err)
	assert.Len(t, servers, 1)
}

func TestHandler_ResourceServers_Accurate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "rs-handler-pool")

	rec := doCognitoRequest(t, h, "CreateResourceServer", map[string]any{
		"UserPoolId": poolID,
		"Identifier": "https://api.example.com",
		"Name":       "My API",
		"Scopes": []map[string]string{
			{"ScopeName": "read", "ScopeDescription": "Read access"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec := doCognitoRequest(t, h, "DescribeResourceServer", map[string]any{
		"UserPoolId": poolID,
		"Identifier": "https://api.example.com",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var resp struct {
		ResourceServer struct {
			Name   string `json:"Name,omitempty"`
			Scopes []struct {
				ScopeName string `json:"ScopeName,omitempty"`
			} `json:"Scopes"`
		} `json:"ResourceServer"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))
	assert.Equal(t, "My API", resp.ResourceServer.Name)
	assert.Len(t, resp.ResourceServer.Scopes, 1)
	assert.Equal(t, "read", resp.ResourceServer.Scopes[0].ScopeName)
}

func TestResourceServer_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "rs-pool")

	createRec := doCognitoRequest(t, h, "CreateResourceServer", map[string]any{
		"UserPoolId": poolID,
		"Identifier": "https://api.example.com",
		"Name":       "My API",
		"Scopes": []map[string]string{
			{"ScopeName": "read", "ScopeDescription": "Read access"},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code, createRec.Body.String())

	listRec := doCognitoRequest(t, h, "ListResourceServers", map[string]any{
		"UserPoolId": poolID,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp struct {
		ResourceServers []struct {
			Identifier string `json:"Identifier"`
		} `json:"ResourceServers"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	require.Len(t, listResp.ResourceServers, 1)
	assert.Equal(t, "https://api.example.com", listResp.ResourceServers[0].Identifier)
}

func TestBackend_UpdateResourceServer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget   error
		name        string
		wantErr     bool
		poolMissing bool
		rsMissing   bool
	}{
		{
			name: "update_name_and_scopes",
		},
		{
			name:        "pool_not_found",
			wantErr:     true,
			errTarget:   cognitoidp.ErrUserPoolNotFound,
			poolMissing: true,
		},
		{
			name:      "resource_server_not_found",
			wantErr:   true,
			errTarget: cognitoidp.ErrUserPoolNotFound,
			rsMissing: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if tt.poolMissing {
				_, err := b.UpdateResourceServer("bad-pool", "https://api.example.com", "new-name", nil)
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			pool, err := b.CreateUserPool("rs-update-pool")
			require.NoError(t, err)

			if tt.rsMissing {
				_, err = b.UpdateResourceServer(pool.ID, "https://nonexistent.example.com", "x", nil)
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			_, err = b.CreateResourceServer(
				pool.ID,
				"https://api.example.com",
				"My API",
				[]cognitoidp.ResourceServerScope{
					{ScopeName: "read", ScopeDescription: "Read access"},
				},
			)
			require.NoError(t, err)

			updated, err := b.UpdateResourceServer(
				pool.ID,
				"https://api.example.com",
				"Updated API",
				[]cognitoidp.ResourceServerScope{{ScopeName: "write", ScopeDescription: "Write access"}},
			)

			require.NoError(t, err)
			require.NotNil(t, updated)
			assert.Equal(t, "Updated API", updated.Name)
			require.Len(t, updated.Scopes, 1)
			assert.Equal(t, "write", updated.Scopes[0].ScopeName)
		})
	}
}

func TestBackend_DeleteResourceServer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget   error
		name        string
		wantErr     bool
		poolMissing bool
		rsMissing   bool
	}{
		{name: "success"},
		{
			name:        "pool_not_found",
			wantErr:     true,
			errTarget:   cognitoidp.ErrUserPoolNotFound,
			poolMissing: true,
		},
		{
			name:      "resource_server_not_found",
			wantErr:   true,
			errTarget: cognitoidp.ErrUserPoolNotFound,
			rsMissing: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if tt.poolMissing {
				err := b.DeleteResourceServer("bad-pool", "https://api.example.com")
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			pool, err := b.CreateUserPool("rs-del-pool")
			require.NoError(t, err)

			if tt.rsMissing {
				err = b.DeleteResourceServer(pool.ID, "https://nonexistent.example.com")
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			_, err = b.CreateResourceServer(pool.ID, "https://del.example.com", "Del API", nil)
			require.NoError(t, err)

			err = b.DeleteResourceServer(pool.ID, "https://del.example.com")
			require.NoError(t, err)

			// Second delete must fail.
			err = b.DeleteResourceServer(pool.ID, "https://del.example.com")
			require.Error(t, err)
		})
	}
}

func TestHandler_ListResourceServers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
		count    int
	}{
		{name: "empty_list", wantCode: http.StatusOK, count: 0},
		{name: "one_server", wantCode: http.StatusOK, count: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, _ := setupHandlerPoolAndClient(t, h, "list-rs-pool")

			for i := range tt.count {
				doCognitoRequest(t, h, "CreateResourceServer", map[string]any{
					"UserPoolId": poolID,
					"Identifier": "https://api" + string(rune('0'+i)) + ".example.com",
					"Name":       "API " + string(rune('0'+i)),
				})
			}

			rec := doCognitoRequest(t, h, "ListResourceServers", map[string]any{
				"UserPoolId": poolID,
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp struct {
				ResourceServers []map[string]any `json:"ResourceServers,omitempty"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Len(t, resp.ResourceServers, tt.count)
		})
	}
}

func TestHandler_UpdateResourceServer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
		badPool  bool
	}{
		{name: "success", wantCode: http.StatusOK},
		{name: "pool_not_found", wantCode: http.StatusBadRequest, badPool: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, _ := setupHandlerPoolAndClient(t, h, "upd-rs-pool")

			identifier := "https://update.example.com"

			doCognitoRequest(t, h, "CreateResourceServer", map[string]any{
				"UserPoolId": poolID,
				"Identifier": identifier,
				"Name":       "Original",
			})

			reqPoolID := poolID
			if tt.badPool {
				reqPoolID = "bad-pool-id"
			}

			rec := doCognitoRequest(t, h, "UpdateResourceServer", map[string]any{
				"UserPoolId": reqPoolID,
				"Identifier": identifier,
				"Name":       "Updated",
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_DeleteResourceServer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
		badPool  bool
	}{
		{name: "success", wantCode: http.StatusOK},
		{name: "not_found", wantCode: http.StatusBadRequest, badPool: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, _ := setupHandlerPoolAndClient(t, h, "del-rs-pool")

			identifier := "https://delete.example.com"

			doCognitoRequest(t, h, "CreateResourceServer", map[string]any{
				"UserPoolId": poolID,
				"Identifier": identifier,
				"Name":       "ToDelete",
			})

			reqPoolID := poolID
			if tt.badPool {
				reqPoolID = "bad-pool"
			}

			rec := doCognitoRequest(t, h, "DeleteResourceServer", map[string]any{
				"UserPoolId": reqPoolID,
				"Identifier": identifier,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
