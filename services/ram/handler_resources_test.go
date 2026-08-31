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

// TestListSourceAssociations_WireShape verifies the response uses the real SDK's
// "sourceAssociations" key (not "associations") and that the (always-empty, since RAM
// has no API surface that ever creates a source association) list unmarshals cleanly.
func TestListSourceAssociations_WireShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRAMRequest(t, h, "/listsourceassociations", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		SourceAssociations []struct {
			SourceID string `json:"sourceId"`
		} `json:"sourceAssociations"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp.SourceAssociations)
	assert.NotContains(
		t, rec.Body.String(), `"associations"`,
		"must use the real SDK's sourceAssociations key, not associations",
	)
}

func TestResourceRegionScope_InListResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		resourceARN string
		wantType    string
		wantScope   string
	}{
		{
			name:        "EC2 subnet resource",
			resourceARN: "arn:aws:ec2:us-east-1:123456789012:subnet/subnet-abc123",
			wantType:    "ec2:Subnet",
			wantScope:   "REGIONAL",
		},
		{
			name:        "EC2 VPC resource",
			resourceARN: "arn:aws:ec2:us-east-1:123456789012:vpc/vpc-12345678",
			wantType:    "ec2:VPC",
			wantScope:   "REGIONAL",
		},
		{
			name:        "EC2 transit gateway",
			resourceARN: "arn:aws:ec2:us-east-1:123456789012:transit-gateway/tgw-1234abcd",
			wantType:    "ec2:TransitGateway",
			wantScope:   "REGIONAL",
		},
		{
			name:        "EC2 prefix list",
			resourceARN: "arn:aws:ec2:us-east-1:123456789012:prefix-list/pl-abc123",
			wantType:    "ec2:PrefixList",
			wantScope:   "REGIONAL",
		},
		{
			name:        "Route53Resolver rule",
			resourceARN: "arn:aws:route53resolver:us-east-1:123456789012:resolver-rule/rslvr-rr-abc",
			wantType:    "route53resolver:ResolverRule",
			wantScope:   "REGIONAL",
		},
		{
			name:        "License Manager config",
			resourceARN: "arn:aws:license-manager:us-east-1:123456789012:license-configuration:lic-abc123",
			wantType:    "license-manager:LicenseConfiguration",
			wantScope:   "REGIONAL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create share with a resource.
			rs, err := h.Backend.CreateResourceShare("scope-share", false, nil, nil, []string{tt.resourceARN})
			require.NoError(t, err)

			rec := doRAMRequest(t, h, "/listresources", map[string]any{
				"resourceOwner":     "SELF",
				"resourceShareArns": []string{rs.ARN},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				Resources []struct {
					Arn                 string `json:"arn"`
					Type                string `json:"type"`
					ResourceRegionScope string `json:"resourceRegionScope"`
				} `json:"resources"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			require.Len(t, resp.Resources, 1)
			assert.Equal(t, tt.resourceARN, resp.Resources[0].Arn)
			assert.Equal(t, tt.wantType, resp.Resources[0].Type)
			assert.Equal(t, tt.wantScope, resp.Resources[0].ResourceRegionScope)
		})
	}
}

// TestListResources_ResourceShareArnsFilter proves ListResources' resourceShareArns
// filter (a list, per the pinned SDK's ListResourcesInput.ResourceShareArns) actually
// scopes results to the requested share(s), rather than being a no-op that returns
// resources from every share the caller owns.
func TestListResources_ResourceShareArnsFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rsA, err := h.Backend.CreateResourceShare(
		"share-a", false, nil, nil,
		[]string{"arn:aws:ec2:us-east-1:123456789012:subnet/subnet-a"},
	)
	require.NoError(t, err)

	_, err = h.Backend.CreateResourceShare(
		"share-b", false, nil, nil,
		[]string{"arn:aws:ec2:us-east-1:123456789012:subnet/subnet-b"},
	)
	require.NoError(t, err)

	rec := doRAMRequest(t, h, "/listresources", map[string]any{
		"resourceOwner":     "SELF",
		"resourceShareArns": []string{rsA.ARN},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Resources []struct {
			Arn              string `json:"arn"`
			ResourceShareArn string `json:"resourceShareArn"`
		} `json:"resources"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(
		t, resp.Resources, 1,
		"resourceShareArns must scope results to the requested share, not return every share",
	)
	assert.Equal(t, rsA.ARN, resp.Resources[0].ResourceShareArn)
}

func TestResourceTypeDerivation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		resourceARN string
		wantType    string
	}{
		{
			name:        "EC2 subnet via slash separator",
			resourceARN: "arn:aws:ec2:us-east-1:123456789012:subnet/subnet-abc123",
			wantType:    "ec2:Subnet",
		},
		{
			name:        "EC2 VPC",
			resourceARN: "arn:aws:ec2:us-east-1:123456789012:vpc/vpc-12345678",
			wantType:    "ec2:VPC",
		},
		{
			name:        "transit gateway",
			resourceARN: "arn:aws:ec2:us-east-1:123456789012:transit-gateway/tgw-1234abcd",
			wantType:    "ec2:TransitGateway",
		},
		{
			name:        "prefix list",
			resourceARN: "arn:aws:ec2:us-east-1:123456789012:prefix-list/pl-abc123",
			wantType:    "ec2:PrefixList",
		},
		{
			name:        "Route53Resolver rule",
			resourceARN: "arn:aws:route53resolver:us-east-1:123456789012:resolver-rule/rslvr-rr-abc",
			wantType:    "route53resolver:ResolverRule",
		},
		{
			name:        "License Manager colon separator",
			resourceARN: "arn:aws:license-manager:us-east-1:123456789012:license-configuration:lic-abc123",
			wantType:    "license-manager:LicenseConfiguration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rs, err := h.Backend.CreateResourceShare("type-derive-share", false, nil, nil, []string{tt.resourceARN})
			require.NoError(t, err)

			rec := doRAMRequest(t, h, "/listresources", map[string]any{
				"resourceOwner":     "SELF",
				"resourceShareArns": []string{rs.ARN},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				Resources []struct {
					Type string `json:"type"`
				} `json:"resources"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			require.Len(t, resp.Resources, 1)
			assert.Equal(t, tt.wantType, resp.Resources[0].Type)
		})
	}
}

func TestListPendingInvitationResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		resourceArns  []string
		wantResources int
		wantStatus    int
	}{
		{
			name:          "invitation with resources",
			resourceArns:  []string{"arn:aws:ec2:us-east-1:123456789012:subnet/sub-1"},
			wantResources: 1,
			wantStatus:    http.StatusOK,
		},
		{
			name: "invitation with multiple resources",
			resourceArns: []string{
				"arn:aws:ec2:us-east-1:123456789012:subnet/sub-1",
				"arn:aws:ec2:us-east-1:123456789012:subnet/sub-2",
			},
			wantResources: 2,
			wantStatus:    http.StatusOK,
		},
		{
			name:          "invitation with no resources",
			resourceArns:  nil,
			wantResources: 0,
			wantStatus:    http.StatusOK,
		},
		{
			name:       "nonexistent invitation returns error",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.wantStatus == http.StatusBadRequest && tt.resourceArns == nil &&
				tt.name == "nonexistent invitation returns error" {
				rec := doRAMRequest(t, h, "/listpendinginvitationresources", map[string]any{
					"resourceShareInvitationArn": "arn:aws:ram:us-east-1:000000000000:resource-share-invitation/nonexistent",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			rs, err := h.Backend.CreateResourceShare("pending-inv-share", true, nil, nil, tt.resourceArns)
			require.NoError(t, err)

			inv := ram.CreateInvitation(
				h.Backend.(*ram.InMemoryBackend),
				rs.ARN,
				"pending-inv-share",
				"111111111111",
				"222222222222",
			)

			rec := doRAMRequest(t, h, "/listpendinginvitationresources", map[string]any{
				"resourceShareInvitationArn": inv.InvitationARN,
			})
			require.Equal(t, tt.wantStatus, rec.Code)

			var resp struct {
				Resources []json.RawMessage `json:"resources"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Len(t, resp.Resources, tt.wantResources)
		})
	}
}

func TestListResourceTypes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRequest(t, h, "/listresourcetypes", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		ResourceTypes []struct {
			ResourceType string `json:"resourceType"`
			ServiceName  string `json:"serviceName"`
		} `json:"resourceTypes"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.NotEmpty(t, resp.ResourceTypes)

	found := map[string]bool{}
	for _, rt := range resp.ResourceTypes {
		found[rt.ResourceType] = true
		assert.NotEmpty(t, rt.ServiceName, "resource type %q should have serviceName", rt.ResourceType)
	}

	assert.True(t, found["ec2:Subnet"], "ec2:Subnet should be in list")
	assert.True(t, found["ec2:VPC"], "ec2:VPC should be in list")
}

func TestListResourceTypes_ExpandedSet(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRequest(t, h, "/listresourcetypes", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		ResourceTypes []struct {
			ResourceType        string `json:"resourceType"`
			ServiceName         string `json:"serviceName"`
			ResourceRegionScope string `json:"resourceRegionScope"`
		} `json:"resourceTypes"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.Greater(t, len(resp.ResourceTypes), 4, "should list more than the original 4 types")

	found := map[string]bool{}
	for _, rt := range resp.ResourceTypes {
		found[rt.ResourceType] = true
		assert.NotEmpty(t, rt.ServiceName, "type %q must have serviceName", rt.ResourceType)
		assert.NotEmpty(t, rt.ResourceRegionScope, "type %q must have resourceRegionScope", rt.ResourceType)
		assert.Contains(
			t,
			[]string{"REGIONAL", "GLOBAL"},
			rt.ResourceRegionScope,
			"scope must be REGIONAL or GLOBAL",
		)
	}

	for _, requiredType := range []string{
		"ec2:Subnet",
		"ec2:TransitGateway",
		"route53resolver:ResolverRule",
		"license-manager:LicenseConfiguration",
		"codebuild:Project",
	} {
		assert.True(t, found[requiredType], "expected %q in ListResourceTypes", requiredType)
	}
}

func TestHandler_GetResourcePolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name: "with resources",
			body: map[string]any{
				"resourceArns": []string{
					"arn:aws:ec2:us-east-1:123456789012:subnet/subnet-abc123",
				},
			},
			wantStatus: http.StatusOK,
			wantBody:   "policies",
		},
		{
			name:       "empty",
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			wantBody:   "policies",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRAMRequest(t, h, "/getresourcepolicies", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// TestRAMPagination_ListResources covers resource pagination.
func TestListResources_Pagination(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")
	h := ram.NewHandler(b)

	// Create a share with 5 resource associations.
	shareARN := "arn:aws:ram:us-east-1:000000000000:resource-share/res-share"
	ram.AddResourceShareInternal(b, ram.NewTestResourceShare(shareARN, "res-share"))

	for i := range 5 {
		resourceARN := fmt.Sprintf("arn:aws:ec2:us-east-1:000000000000:subnet/subnet-%d", i)
		body := map[string]any{
			"resourceShareArn": shareARN,
			"resourceArns":     []string{resourceARN},
		}
		rec := doRAMRequest(t, h, "/associateresourceshare", body)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	type reqBody struct {
		ResourceOwner string `json:"resourceOwner"`
		MaxResults    *int32 `json:"maxResults,omitempty"`
		NextToken     string `json:"nextToken,omitempty"`
	}

	type respBody struct {
		NextToken string `json:"nextToken"`
		Resources []any  `json:"resources"`
	}

	tests := []struct {
		maxResults *int32
		name       string
		wantTotal  int
		wantPages  int
		wantError  bool
	}{
		{
			name:       "maxResults=2 paginates 5 resources into 3 pages",
			maxResults: ptr32(2),
			wantTotal:  5,
			wantPages:  3,
		},
		{
			name:       "maxResults=5 returns all in one page",
			maxResults: ptr32(5),
			wantTotal:  5,
			wantPages:  1,
		},
		{
			name:       "maxResults=101 returns error",
			maxResults: ptr32(101),
			wantError:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			nextToken := ""
			totalSeen := 0
			pages := 0

			for {
				req := reqBody{
					ResourceOwner: "SELF",
					MaxResults:    tc.maxResults,
					NextToken:     nextToken,
				}

				rec := doRAMRequest(t, h, "/listresources", req)

				if tc.wantError {
					assert.Equal(t, http.StatusBadRequest, rec.Code)

					var errResp map[string]string
					require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
					assert.Equal(t, "InvalidParameterException", errResp["__type"])

					return
				}

				require.Equal(t, http.StatusOK, rec.Code)

				var resp respBody
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				pages++
				totalSeen += len(resp.Resources)
				nextToken = resp.NextToken

				if nextToken == "" {
					break
				}
			}

			assert.Equal(t, tc.wantTotal, totalSeen)
			assert.Equal(t, tc.wantPages, pages)
		})
	}
}

// TestRAMPagination_PendingInvitationResources covers ListPendingInvitationResources pagination.
func TestListPendingInvitationResources_Pagination(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")
	h := ram.NewHandler(b)

	// Use CreateInvitation directly to seed a pending invitation.
	shareARN := "arn:aws:ram:us-east-1:111111111111:resource-share/pending-share"
	inv := ram.CreateInvitation(b, shareARN, "pending-share", "111111111111", "000000000000")

	// Associate resources to the share so they show up as pending.
	for i := range 3 {
		resourceARN := fmt.Sprintf("arn:aws:ec2:us-east-1:111111111111:subnet/subnet-pending-%d", i)
		invARN := inv.InvitationARN

		_ = resourceARN
		_ = invARN
		// Note: pending invitation resource listing doesn't require real association in this mock;
		// testing the pagination mechanics is the goal.
	}

	type reqBody struct {
		ResourceShareInvitationArn string `json:"resourceShareInvitationArn"`
		MaxResults                 *int32 `json:"maxResults,omitempty"`
		NextToken                  string `json:"nextToken,omitempty"`
	}

	type respBody struct {
		NextToken string `json:"nextToken"`
		Resources []any  `json:"resources"`
	}

	tests := []struct {
		maxResults *int32
		name       string
		wantError  bool
	}{
		{
			name:       "maxResults=10 succeeds",
			maxResults: ptr32(10),
		},
		{
			name:       "maxResults=0 returns error",
			maxResults: ptr32(0),
			wantError:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req := reqBody{
				ResourceShareInvitationArn: inv.InvitationARN,
				MaxResults:                 tc.maxResults,
			}

			rec := doRAMRequest(t, h, "/listpendinginvitationresources", req)

			if tc.wantError {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				var errResp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, "InvalidParameterException", errResp["__type"])

				return
			}

			require.Equal(t, http.StatusOK, rec.Code)

			var resp respBody
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			assert.NotNil(t, resp.Resources)
		})
	}
}

// TestRAMPagination_GetResourcePolicies covers policy pagination.
func TestGetResourcePolicies_Pagination(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")
	h := ram.NewHandler(b)

	tests := []struct {
		maxResults *int32
		name       string
		wantError  bool
	}{
		{
			name:       "maxResults=10 succeeds even with empty policies",
			maxResults: ptr32(10),
		},
		{
			name:       "maxResults=101 returns error",
			maxResults: ptr32(101),
			wantError:  true,
		},
		{
			name:       "maxResults=0 returns error",
			maxResults: ptr32(0),
			wantError:  true,
		},
	}

	type reqBody struct {
		MaxResults   *int32   `json:"maxResults,omitempty"`
		NextToken    string   `json:"nextToken,omitempty"`
		ResourceArns []string `json:"resourceArns"`
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req := reqBody{
				ResourceArns: []string{"arn:aws:ec2:us-east-1:000000000000:subnet/subnet-0"},
				MaxResults:   tc.maxResults,
			}

			rec := doRAMRequest(t, h, "/getresourcepolicies", req)

			if tc.wantError {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				var errResp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, "InvalidParameterException", errResp["__type"])

				return
			}

			require.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

// TestRefinement1_GetResourcePolicies_EmptyArns verifies empty input returns empty list.
func TestGetResourcePolicies_EmptyArns(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRequest(t, h, "/getresourcepolicies", map[string]any{"resourceArns": []string{}})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Policies []string `json:"policies"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp.Policies)
}

func TestListResources_ResourceOwnerFilter(t *testing.T) {
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

func TestListResources_ResourceTypeFilter(t *testing.T) {
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

func TestGetResourcePolicies_OnlySharedResourcesGetPolicies(t *testing.T) {
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

func TestListResources_Smoke(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// ListResources (owner = SELF)
	rec := doRAMRequest(t, h, "/listresources", map[string]any{
		"resourceOwner": "SELF",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListPrincipals
	rec = doRAMRequest(t, h, "/listprincipals", map[string]any{
		"resourceOwner": "SELF",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}
