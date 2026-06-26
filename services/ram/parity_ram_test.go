package ram_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ram"
)

// ---------------------------------------------------------------------------
// DeleteResourceShare — share is gone after delete (no map leak)
// ---------------------------------------------------------------------------

func TestParity_DeleteResourceShare_RemovesFromMap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createName string
	}{
		{name: "single share deleted from map", createName: "my-share"},
		{name: "share with dashes deleted cleanly", createName: "share-with-dashes"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create a share.
			createResp := doRAMRequest(t, h, "/createresourceshare", map[string]any{
				"name": tc.createName,
			})
			require.Equal(t, http.StatusOK, createResp.Code)

			var createBody map[string]any
			require.NoError(t, json.Unmarshal(createResp.Body.Bytes(), &createBody))
			shareARN := createBody["resourceShare"].(map[string]any)["resourceShareArn"].(string)
			require.NotEmpty(t, shareARN)

			// Delete via query param.
			rec := doRAMRawRequest(
				t, h, http.MethodDelete,
				"/deleteresourceshare?resourceShareArn="+shareARN,
				nil,
			)
			require.Equal(t, http.StatusOK, rec.Code)

			// Re-listing SELF shares must not return the deleted share.
			listResp := doRAMRequest(t, h, "/getresourceshares", map[string]any{
				"resourceOwner": "SELF",
			})
			require.Equal(t, http.StatusOK, listResp.Code)

			var listBody map[string]any
			require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listBody))
			shares := listBody["resourceShares"].([]any)

			for _, s := range shares {
				arn := s.(map[string]any)["resourceShareArn"].(string)
				assert.NotEqual(t, shareARN, arn, "deleted share must not appear in list")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ListResources — resourceOwner filter (SELF vs OTHER-ACCOUNTS)
// ---------------------------------------------------------------------------

func TestParity_ListResources_ResourceOwnerFilter(t *testing.T) {
	t.Parallel()

	const (
		selfAccount  = "000000000000"
		otherAccount = "111111111111"
		resourceARN  = "arn:aws:ec2:us-east-1:000000000000:subnet/subnet-aabbccdd"
	)

	tests := []struct {
		name          string
		owningAccount string
		filterOwner   string
		wantCount     int
	}{
		{
			name:          "SELF returns own shares",
			owningAccount: selfAccount,
			filterOwner:   "SELF",
			wantCount:     1,
		},
		{
			name:          "OTHER-ACCOUNTS returns nothing for own share",
			owningAccount: selfAccount,
			filterOwner:   "OTHER-ACCOUNTS",
			wantCount:     0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ram.NewInMemoryBackend(selfAccount, "us-east-1")
			h := ram.NewHandler(b)

			shareARN := fmt.Sprintf(
				"arn:aws:ram:us-east-1:%s:resource-share/test-share", tc.owningAccount,
			)
			rs := ram.NewTestResourceShare(shareARN, "test-share")
			ram.AddResourceShareInternal(b, rs)

			_, err := b.AssociateResourceShare(shareARN, nil, []string{resourceARN})
			require.NoError(t, err)

			rec := doRAMRequest(t, h, "/listresources", map[string]any{
				"resourceOwner": tc.filterOwner,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			resources := resp["resources"].([]any)
			assert.Len(t, resources, tc.wantCount)
		})
	}
}

// ---------------------------------------------------------------------------
// ListResources — resourceType filter
// ---------------------------------------------------------------------------

func TestParity_ListResources_ResourceTypeFilter(t *testing.T) {
	t.Parallel()

	const (
		accountID  = "000000000000"
		shareARN   = "arn:aws:ram:us-east-1:000000000000:resource-share/type-filter-test"
		subnetARN  = "arn:aws:ec2:us-east-1:000000000000:subnet/subnet-aabbccdd"
		transitARN = "arn:aws:ec2:us-east-1:000000000000:transit-gateway/tgw-aabbccdd"
	)

	tests := []struct {
		name         string
		resourceType string
		wantCount    int
	}{
		{
			name:         "filter by ec2:Subnet returns only subnets",
			resourceType: "ec2:Subnet",
			wantCount:    1,
		},
		{
			name:         "filter by ec2:TransitGateway returns only TGWs",
			resourceType: "ec2:TransitGateway",
			wantCount:    1,
		},
		{
			name:         "no filter returns all resources",
			resourceType: "",
			wantCount:    2,
		},
		{
			name:         "unknown type returns empty",
			resourceType: "ec2:VPC",
			wantCount:    0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ram.NewInMemoryBackend(accountID, "us-east-1")
			h := ram.NewHandler(b)

			rs := ram.NewTestResourceShare(shareARN, "type-filter-share")
			ram.AddResourceShareInternal(b, rs)

			_, err := b.AssociateResourceShare(shareARN, nil, []string{subnetARN, transitARN})
			require.NoError(t, err)

			body := map[string]any{"resourceOwner": "SELF"}
			if tc.resourceType != "" {
				body["resourceType"] = tc.resourceType
			}

			rec := doRAMRequest(t, h, "/listresources", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			resources := resp["resources"].([]any)
			assert.Len(t, resources, tc.wantCount)
		})
	}
}

// ---------------------------------------------------------------------------
// ListPrincipals — resourceOwner filter
// ---------------------------------------------------------------------------

func TestParity_ListPrincipals_ResourceOwnerFilter(t *testing.T) {
	t.Parallel()

	const (
		selfAccount = "000000000000"
		// Use same-account format so AllowExternalPrincipals=false doesn't block.
		principal = "111111111111"
	)

	tests := []struct {
		name        string
		filterOwner string
		wantCount   int
	}{
		{
			name:        "SELF returns principals on own shares",
			filterOwner: "SELF",
			wantCount:   1,
		},
		{
			name:        "OTHER-ACCOUNTS returns empty for own share",
			filterOwner: "OTHER-ACCOUNTS",
			wantCount:   0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ram.NewInMemoryBackend(selfAccount, "us-east-1")
			h := ram.NewHandler(b)

			shareARN := "arn:aws:ram:us-east-1:000000000000:resource-share/principal-test"
			rs := ram.NewTestResourceShare(shareARN, "principal-share")
			rs.AllowExternalPrincipals = true
			ram.AddResourceShareInternal(b, rs)

			_, err := b.AssociateResourceShare(shareARN, []string{principal}, nil)
			require.NoError(t, err)

			rec := doRAMRequest(t, h, "/listprincipals", map[string]any{
				"resourceOwner": tc.filterOwner,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			principals := resp["principals"].([]any)
			assert.Len(t, principals, tc.wantCount)
		})
	}
}

// ---------------------------------------------------------------------------
// GetResourcePolicies — only returns entries for shared resources
// ---------------------------------------------------------------------------

func TestParity_GetResourcePolicies_OnlySharedResourcesGetPolicies(t *testing.T) {
	t.Parallel()

	const (
		accountID   = "000000000000"
		sharedARN   = "arn:aws:ec2:us-east-1:000000000000:subnet/subnet-shared"
		unsharedARN = "arn:aws:ec2:us-east-1:000000000000:subnet/subnet-unshared"
		shareARN    = "arn:aws:ram:us-east-1:000000000000:resource-share/policy-test"
	)

	tests := []struct {
		name         string
		resourceARNs []string
		wantCount    int
	}{
		{
			name:         "shared resource gets a policy entry",
			resourceARNs: []string{sharedARN},
			wantCount:    1,
		},
		{
			name:         "unshared resource gets no policy entry",
			resourceARNs: []string{unsharedARN},
			wantCount:    0,
		},
		{
			name:         "mix: only shared ARN gets policy",
			resourceARNs: []string{sharedARN, unsharedARN},
			wantCount:    1,
		},
		{
			name:         "empty input returns empty",
			resourceARNs: []string{},
			wantCount:    0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ram.NewInMemoryBackend(accountID, "us-east-1")
			h := ram.NewHandler(b)

			// Associate sharedARN with a resource share.
			rs := ram.NewTestResourceShare(shareARN, "policy-share")
			ram.AddResourceShareInternal(b, rs)

			_, err := b.AssociateResourceShare(shareARN, nil, []string{sharedARN})
			require.NoError(t, err)

			rec := doRAMRequest(t, h, "/getresourcepolicies", map[string]any{
				"resourceArns": tc.resourceARNs,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			var policies []any
			if p, ok := resp["policies"]; ok {
				policies = p.([]any)
			}

			assert.Len(t, policies, tc.wantCount)
		})
	}
}

// ---------------------------------------------------------------------------
// PromotePermissionCreatedFromPolicy — type changes to CUSTOMER_MANAGED
// ---------------------------------------------------------------------------

func TestParity_PromotePermissionCreatedFromPolicy(t *testing.T) {
	t.Parallel()

	const (
		accountID    = "000000000000"
		permARN      = "arn:aws:ram::aws:permission/from-policy-perm"
		resourceType = "ec2:Subnet"
	)

	tests := []struct {
		name     string
		permType string
		newName  string
		wantType string
		wantName string
		wantErr  bool
	}{
		{
			name:     "CREATED_FROM_POLICY promotes to CUSTOMER_MANAGED",
			permType: "CREATED_FROM_POLICY",
			newName:  "my-promoted-perm",
			wantType: "CUSTOMER_MANAGED",
			wantName: "my-promoted-perm",
		},
		{
			name:     "CUSTOMER_MANAGED permission rejected",
			permType: "CUSTOMER_MANAGED",
			newName:  "irrelevant",
			wantErr:  true,
		},
		{
			name:     "AWS_MANAGED permission rejected",
			permType: "AWS_MANAGED",
			newName:  "irrelevant",
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ram.NewInMemoryBackend(accountID, "us-east-1")
			h := ram.NewHandler(b)

			// Inject a permission with the requested type.
			p := ram.NewTestPermission(permARN, "base-perm", resourceType)
			p.PermissionType = tc.permType
			ram.AddPermissionInternal(b, p)

			rec := doRAMRequest(t, h, "/promotepermissioncreatedfrompolicy", map[string]any{
				"permissionArn": permARN,
				"name":          tc.newName,
			})

			if tc.wantErr {
				assert.NotEqual(t, http.StatusOK, rec.Code)

				return
			}

			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			perm := resp["permission"].(map[string]any)
			assert.Equal(t, tc.wantType, perm["permissionType"])
			assert.Equal(t, tc.wantName, perm["name"])
		})
	}
}

// ---------------------------------------------------------------------------
// Pagination — tokens are opaque base64 strings
// ---------------------------------------------------------------------------

func TestParity_RAM_PaginationTokenIsOpaque(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")
	h := ram.NewHandler(b)

	// Seed 5 shares.
	for i := range 5 {
		arn := fmt.Sprintf("arn:aws:ram:us-east-1:000000000000:resource-share/share-%02d", i)
		rs := ram.NewTestResourceShare(arn, fmt.Sprintf("share-%02d", i))
		ram.AddResourceShareInternal(b, rs)
	}

	maxResults := int32(2)
	rec := doRAMRequest(t, h, "/getresourceshares", map[string]any{
		"resourceOwner": "SELF",
		"maxResults":    maxResults,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	nextToken, _ := resp["nextToken"].(string)
	require.NotEmpty(t, nextToken, "expected a nextToken for multi-page result")

	// The token must not be a plain integer.
	var plainInt int
	err := json.Unmarshal([]byte(nextToken), &plainInt)
	require.Error(t, err, "nextToken should not be a plain integer")

	// Following the token must yield the next page without overlap.
	rec2 := doRAMRequest(t, h, "/getresourceshares", map[string]any{
		"resourceOwner": "SELF",
		"maxResults":    maxResults,
		"nextToken":     nextToken,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))

	page1Shares := resp["resourceShares"].([]any)
	page2Shares := resp2["resourceShares"].([]any)

	// Collect page1 ARNs.
	p1ARNs := make(map[string]struct{}, len(page1Shares))
	for _, s := range page1Shares {
		arn := s.(map[string]any)["resourceShareArn"].(string)
		p1ARNs[arn] = struct{}{}
	}

	// No page2 ARN should appear in page1.
	for _, s := range page2Shares {
		arn := s.(map[string]any)["resourceShareArn"].(string)
		assert.NotContains(t, p1ARNs, arn, "page 2 must not overlap page 1")
	}
}
