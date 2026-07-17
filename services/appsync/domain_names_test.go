package appsync_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/appsync"
)

func TestDomainName_AssociateAPI_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	domain, err := b.CreateDomainName(
		"example.com",
		"arn:aws:acm:us-east-1:000000000000:certificate/abc",
		"test domain",
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, "example.com", domain.DomainName)

	assoc, err := b.AssociateAPI("example.com", api.APIID)
	require.NoError(t, err)
	assert.Equal(t, api.APIID, assoc.APIID)

	got, err := b.GetAPIAssociation("example.com")
	require.NoError(t, err)
	assert.Equal(t, api.APIID, got.APIID)
}

func TestInMemoryBackend_AssociateAPI_UpdatesDomainName(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	dn, err := b.CreateDomainName("api.example.com", "arn:aws:acm:us-east-1:000000000000:certificate/abc", "", nil)
	require.NoError(t, err)
	assert.Empty(t, dn.APIID)

	// Create a GraphQL API and associate it.
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	assoc, err := b.AssociateAPI("api.example.com", api.APIID)
	require.NoError(t, err)
	assert.Equal(t, api.APIID, assoc.APIID)
}

func TestInMemoryBackend_CreateDomainName_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		domainName string
		wantErr    bool
	}{
		{name: "valid_domain", domainName: "api.example.com"},
		{name: "valid_subdomain", domainName: "my.api.example.co.uk"},
		{name: "empty_rejected", domainName: "", wantErr: true},
		{name: "no_dot_rejected", domainName: "localhost", wantErr: true},
		{name: "leading_dot_rejected", domainName: ".example.com", wantErr: true},
		{name: "trailing_dot_rejected", domainName: "example.com.", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			_, err := b.CreateDomainName(tt.domainName, "arn:aws:acm:us-east-1:000000000000:certificate/abc", "", nil)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInMemoryBackend_DomainNameCRUD(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	certARN := "arn:aws:acm:us-east-1:000000000000:certificate/abc"

	// Create.
	_, err := b.CreateDomainName("api.example.com", certARN, "desc", nil)
	require.NoError(t, err)

	// Get.
	dn, err := b.GetDomainName("api.example.com")
	require.NoError(t, err)
	assert.Equal(t, "api.example.com", dn.DomainName)

	// List.
	dns, err := b.ListDomainNames()
	require.NoError(t, err)
	assert.Len(t, dns, 1)

	// Delete.
	err = b.DeleteDomainName("api.example.com")
	require.NoError(t, err)

	// Get after delete returns error.
	_, err = b.GetDomainName("api.example.com")
	require.ErrorIs(t, err, awserr.ErrNotFound)

	// List returns 0.
	dns, err = b.ListDomainNames()
	require.NoError(t, err)
	assert.Empty(t, dns)
}

func TestInMemoryBackend_GetAPIAssociation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*appsync.InMemoryBackend)
		domainName string
		wantStatus string
		wantErr    bool
	}{
		{
			name:       "no_association_returns_not_found_status",
			domainName: "api.example.com",
			setup: func(b *appsync.InMemoryBackend) {
				_, _ = b.CreateDomainName("api.example.com",
					"arn:aws:acm:us-east-1:000000000000:certificate/abc", "", nil)
			},
			wantStatus: "NOT_FOUND",
		},
		{
			name: "with_association_returns_success_status",
			setup: func(b *appsync.InMemoryBackend) {
				_, _ = b.CreateDomainName("api.example.com",
					"arn:aws:acm:us-east-1:000000000000:certificate/abc", "", nil)
				api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
				_, _ = b.AssociateAPI("api.example.com", api.APIID)
			},
			domainName: "api.example.com",
			wantStatus: "SUCCESS",
		},
		{
			name:       "domain_not_found_returns_error",
			domainName: "missing.example.com",
			setup:      func(_ *appsync.InMemoryBackend) {},
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			tt.setup(b)

			assoc, err := b.GetAPIAssociation(tt.domainName)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, assoc.AssociationStatus)
		})
	}
}

func TestInMemoryBackend_DeleteDomainName_CascadesAssociation(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	certARN := "arn:aws:acm:us-east-1:000000000000:certificate/abc"

	_, err := b.CreateDomainName("api.example.com", certARN, "", nil)
	require.NoError(t, err)

	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.AssociateAPI("api.example.com", api.APIID)
	require.NoError(t, err)

	// Delete domain name.
	err = b.DeleteDomainName("api.example.com")
	require.NoError(t, err)

	// Get association after domain name deleted returns error.
	_, err = b.GetAPIAssociation("api.example.com")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_DisassociateAPI(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	certARN := "arn:aws:acm:us-east-1:000000000000:certificate/abc"

	_, err := b.CreateDomainName("api.example.com", certARN, "", nil)
	require.NoError(t, err)

	gqlAPI, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.AssociateAPI("api.example.com", gqlAPI.APIID)
	require.NoError(t, err)

	// Disassociate.
	err = b.DisassociateAPI("api.example.com")
	require.NoError(t, err)

	// Association no longer exists.
	assoc, err := b.GetAPIAssociation("api.example.com")
	require.NoError(t, err)
	assert.Equal(t, "NOT_FOUND", assoc.AssociationStatus)

	// Second disassociate returns 404.
	err = b.DisassociateAPI("api.example.com")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_UpdateDomainName(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	certARN := "arn:aws:acm:us-east-1:000000000000:certificate/abc"

	_, err := b.CreateDomainName("api.example.com", certARN, "orig desc", nil)
	require.NoError(t, err)

	updated, err := b.UpdateDomainName("api.example.com", "new desc", "")
	require.NoError(t, err)
	assert.Equal(t, "new desc", updated.Description)

	// Not found.
	_, err = b.UpdateDomainName("missing.example.com", "x", "")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_DeleteDomainName_APIAssoc_Cascade(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	certARN := "arn:aws:acm:us-east-1:000000000000:certificate/abc"

	_, err := b.CreateDomainName("api.example.com", certARN, "desc", nil)
	require.NoError(t, err)

	gqlAPI, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.AssociateAPI("api.example.com", gqlAPI.APIID)
	require.NoError(t, err)

	// Delete domain name - should cascade delete the association.
	err = b.DeleteDomainName("api.example.com")
	require.NoError(t, err)

	// Get domain name should return not found.
	_, err = b.GetDomainName("api.example.com")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_AssociateAPI_ValidatesAPIExists(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	_, err := b.CreateDomainName("example.com", "arn:aws:acm:us-east-1:000:certificate/abc", "", nil)
	require.NoError(t, err)

	// Associating a non-existent API should fail.
	_, err = b.AssociateAPI("example.com", "nonexistent-api-id")
	require.Error(t, err)
	assert.ErrorIs(t, err, appsync.ErrNotFound)
}

func TestInMemoryBackend_AssociateAPI_ValidAPISucceeds(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	_, err := b.CreateDomainName("api.example.com", "arn:aws:acm:us-east-1:000:certificate/abc", "", nil)
	require.NoError(t, err)

	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	assoc, err := b.AssociateAPI("api.example.com", api.APIID)
	require.NoError(t, err)
	assert.Equal(t, api.APIID, assoc.APIID)
}

// TestListDomainNames_Pagination verifies maxResults/nextToken on ListDomainNames.
func TestListDomainNames_Pagination(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	for i := range 4 {
		rec := doRequest(t, h, http.MethodPost, "/v1/domainnames", map[string]any{
			"domainName":     fmt.Sprintf("domain%d.example.com", i),
			"certificateArn": fmt.Sprintf("arn:aws:acm:us-east-1:000000000000:certificate/cert-%d", i),
			"description":    "test",
		})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	tests := []struct {
		name          string
		path          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			path:          "/v1/domainnames",
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			path:          "/v1/domainnames?maxResults=2",
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, http.MethodGet, tt.path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				NextToken         string           `json:"nextToken"`
				DomainNameConfigs []map[string]any `json:"domainNameConfigs"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.DomainNameConfigs, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out.NextToken)
			} else {
				assert.Empty(t, out.NextToken)
			}
		})
	}
}
