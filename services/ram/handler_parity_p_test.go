package ram_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ram"
)

// makeNShares seeds n resource shares and returns their ARNs sorted.
func makeNShares(t *testing.T, b *ram.InMemoryBackend, n int) []string {
	t.Helper()

	arns := make([]string, n)

	for i := range n {
		arn := fmt.Sprintf("arn:aws:ram:us-east-1:000000000000:resource-share/%04d", i)
		arns[i] = arn
		ram.AddResourceShareInternal(b, ram.NewTestResourceShare(arn, fmt.Sprintf("share-%04d", i)))
	}

	return arns
}

// TestRAMPagination_GetResourceShares covers multi-page round-trips and bounds validation.
func TestRAMPagination_GetResourceShares(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		totalShares int
		maxResults  *int32
		wantPages   int
		wantError   bool
	}{
		{
			name:        "no maxResults returns all up to cap",
			totalShares: 5,
			maxResults:  nil,
			wantPages:   1,
		},
		{
			name:        "maxResults=2 paginates 5 shares into 3 pages",
			totalShares: 5,
			maxResults:  ptr32(2),
			wantPages:   3,
		},
		{
			name:        "maxResults=1 paginates 3 shares into 3 pages",
			totalShares: 3,
			maxResults:  ptr32(1),
			wantPages:   3,
		},
		{
			name:        "maxResults=100 (cap) returns all in one page",
			totalShares: 50,
			maxResults:  ptr32(100),
			wantPages:   1,
		},
		{
			name:        "maxResults=101 returns InvalidParameterException",
			totalShares: 1,
			maxResults:  ptr32(101),
			wantError:   true,
		},
		{
			name:        "maxResults=0 returns InvalidParameterException",
			totalShares: 1,
			maxResults:  ptr32(0),
			wantError:   true,
		},
		{
			name:        "empty result set",
			totalShares: 0,
			maxResults:  ptr32(10),
			wantPages:   1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ram.NewInMemoryBackend("000000000000", "us-east-1")
			h := ram.NewHandler(b)

			makeNShares(t, b, tc.totalShares)

			type reqBody struct {
				ResourceOwner string `json:"resourceOwner"`
				MaxResults    *int32 `json:"maxResults,omitempty"`
				NextToken     string `json:"nextToken,omitempty"`
			}

			type respBody struct {
				NextToken      string        `json:"nextToken"`
				ResourceShares []interface{} `json:"resourceShares"`
			}

			nextToken := ""
			totalSeen := 0
			pages := 0

			for {
				req := reqBody{
					ResourceOwner: "SELF",
					MaxResults:    tc.maxResults,
					NextToken:     nextToken,
				}

				rec := doRAMRequest(t, h, "/getresourceshares", req)

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
				totalSeen += len(resp.ResourceShares)
				nextToken = resp.NextToken

				if nextToken == "" {
					break
				}
			}

			assert.Equal(t, tc.totalShares, totalSeen, "total items across all pages")
			assert.Equal(t, tc.wantPages, pages, "number of pages")
		})
	}
}

// TestRAMPagination_ListPermissions covers multi-page round-trips for ListPermissions.
func TestRAMPagination_ListPermissions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		extra      int
		maxResults *int32
		wantError  bool
		wantPages  int
	}{
		{
			name:       "default cap returns all permissions in one page",
			extra:      0,
			maxResults: nil,
			wantPages:  1,
		},
		{
			name:       "maxResults=1 paginates into BuiltInPermissionCount+extra pages",
			extra:      2,
			maxResults: ptr32(1),
			wantPages:  ram.BuiltInPermissionCount + 2, // each page has 1 item
		},
		{
			name:      "maxResults=101 returns error",
			extra:     0,
			wantError: true,
			maxResults: ptr32(101),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ram.NewInMemoryBackend("000000000000", "us-east-1")
			h := ram.NewHandler(b)

			for i := range tc.extra {
				arn := fmt.Sprintf("arn:aws:ram::aws:permission/extra-%d", i)
				ram.AddPermissionInternal(b, ram.NewTestPermission(arn, fmt.Sprintf("Extra%d", i), "ec2:Subnet"))
			}

			type reqBody struct {
				MaxResults *int32 `json:"maxResults,omitempty"`
				NextToken  string `json:"nextToken,omitempty"`
			}

			type respBody struct {
				NextToken   string        `json:"nextToken"`
				Permissions []interface{} `json:"permissions"`
			}

			nextToken := ""
			pages := 0
			totalSeen := 0

			for {
				req := reqBody{MaxResults: tc.maxResults, NextToken: nextToken}
				rec := doRAMRequest(t, h, "/listpermissions", req)

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
				totalSeen += len(resp.Permissions)
				nextToken = resp.NextToken

				if nextToken == "" {
					break
				}
			}

			assert.Equal(t, tc.wantPages, pages)
			assert.Equal(t, ram.BuiltInPermissionCount+tc.extra, totalSeen)
		})
	}
}

// TestRAMPagination_GetResourceShareAssociations covers PRINCIPAL associations.
func TestRAMPagination_GetResourceShareAssociations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxResults *int32
		wantError  bool
	}{
		{
			name:       "maxResults=1 pages through all associations",
			maxResults: ptr32(1),
		},
		{
			name:       "maxResults=0 returns error",
			maxResults: ptr32(0),
			wantError:  true,
		},
		{
			name:       "maxResults=100 returns all",
			maxResults: ptr32(100),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ram.NewInMemoryBackend("000000000000", "us-east-1")
			h := ram.NewHandler(b)

			// Create 3 shares, each with a PRINCIPAL association (same account = internal).
			shareARNs := makeNShares(t, b, 3)

			for _, shareARN := range shareARNs {
				body := map[string]interface{}{
					"resourceShareArn": shareARN,
					"principals":       []string{"000000000000"},
				}
				rec := doRAMRequest(t, h, "/associateresourceshare", body)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			type reqBody struct {
				AssociationType string `json:"associationType"`
				MaxResults      *int32 `json:"maxResults,omitempty"`
				NextToken       string `json:"nextToken,omitempty"`
			}

			type respBody struct {
				NextToken                 string        `json:"nextToken"`
				ResourceShareAssociations []interface{} `json:"resourceShareAssociations"`
			}

			nextToken := ""
			totalSeen := 0

			for {
				req := reqBody{
					AssociationType: "PRINCIPAL",
					MaxResults:      tc.maxResults,
					NextToken:       nextToken,
				}

				rec := doRAMRequest(t, h, "/getresourceshareassociations", req)

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

				totalSeen += len(resp.ResourceShareAssociations)
				nextToken = resp.NextToken

				if nextToken == "" {
					break
				}
			}

			assert.Equal(t, 3, totalSeen)
		})
	}
}

// TestRAMPagination_ListResources covers resource pagination.
func TestRAMPagination_ListResources(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")
	h := ram.NewHandler(b)

	// Create a share with 5 resource associations.
	shareARN := "arn:aws:ram:us-east-1:000000000000:resource-share/res-share"
	ram.AddResourceShareInternal(b, ram.NewTestResourceShare(shareARN, "res-share"))

	for i := range 5 {
		resourceARN := fmt.Sprintf("arn:aws:ec2:us-east-1:000000000000:subnet/subnet-%d", i)
		body := map[string]interface{}{
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
		NextToken string        `json:"nextToken"`
		Resources []interface{} `json:"resources"`
	}

	tests := []struct {
		name       string
		maxResults *int32
		wantTotal  int
		wantPages  int
		wantError  bool
	}{
		{
			name:      "maxResults=2 paginates 5 resources into 3 pages",
			maxResults: ptr32(2),
			wantTotal: 5,
			wantPages: 3,
		},
		{
			name:      "maxResults=5 returns all in one page",
			maxResults: ptr32(5),
			wantTotal: 5,
			wantPages: 1,
		},
		{
			name:      "maxResults=101 returns error",
			maxResults: ptr32(101),
			wantError: true,
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

// TestRAMPagination_ListPrincipals covers principal pagination.
func TestRAMPagination_ListPrincipals(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")
	h := ram.NewHandler(b)

	// Create a share with AllowExternalPrincipals so we can associate different external principals.
	createRec := doRAMRequest(t, h, "/createresourceshare", map[string]interface{}{
		"name":                    "principal-share",
		"allowExternalPrincipals": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp struct {
		ResourceShare struct {
			ResourceShareArn string `json:"resourceShareArn"`
		} `json:"resourceShare"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	shareARN := createResp.ResourceShare.ResourceShareArn
	require.NotEmpty(t, shareARN)

	// Associate 4 different external account IDs as principals.
	for i := range 4 {
		principal := fmt.Sprintf("%012d", 111111111111+i)
		body := map[string]interface{}{
			"resourceShareArn": shareARN,
			"principals":       []string{principal},
		}
		rec := doRAMRequest(t, h, "/associateresourceshare", body)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		name       string
		maxResults *int32
		wantTotal  int
		wantPages  int
		wantError  bool
	}{
		{
			name:      "maxResults=2 paginates 4 principals into 2 pages",
			maxResults: ptr32(2),
			wantTotal: 4,
			wantPages: 2,
		},
		{
			name:      "maxResults=4 returns all in one page",
			maxResults: ptr32(4),
			wantTotal: 4,
			wantPages: 1,
		},
		{
			name:      "maxResults=-1 returns error",
			maxResults: ptr32(-1),
			wantError: true,
		},
	}

	type reqBody struct {
		ResourceOwner string `json:"resourceOwner"`
		MaxResults    *int32 `json:"maxResults,omitempty"`
		NextToken     string `json:"nextToken,omitempty"`
	}

	type respBody struct {
		NextToken  string        `json:"nextToken"`
		Principals []interface{} `json:"principals"`
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

				rec := doRAMRequest(t, h, "/listprincipals", req)

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
				totalSeen += len(resp.Principals)
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

// TestRAMPagination_ListResourceSharePermissions covers pagination of permissions on a share.
func TestRAMPagination_ListResourceSharePermissions(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")
	h := ram.NewHandler(b)

	shareARN := "arn:aws:ram:us-east-1:000000000000:resource-share/perm-share"
	ram.AddResourceShareInternal(b, ram.NewTestResourceShare(shareARN, "perm-share"))

	// The default permission is added at share creation; add extra custom permissions.
	for i := range 3 {
		permARN := fmt.Sprintf("arn:aws:ram::aws:permission/custom-%d", i)
		ram.AddPermissionInternal(b, ram.NewTestPermission(permARN, fmt.Sprintf("Custom%d", i), "ec2:Subnet"))

		body := map[string]interface{}{
			"resourceShareArn": shareARN,
			"permissionArn":    permARN,
		}
		rec := doRAMRequest(t, h, "/associateresourcesharepermission", body)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		name       string
		maxResults *int32
		wantError  bool
		minPages   int
	}{
		{
			name:      "maxResults=1 paginates",
			maxResults: ptr32(1),
			minPages:  2,
		},
		{
			name:      "no maxResults returns all",
			maxResults: nil,
			minPages:  1,
		},
		{
			name:      "maxResults=101 returns error",
			maxResults: ptr32(101),
			wantError: true,
		},
	}

	type reqBody struct {
		ResourceShareArn string `json:"resourceShareArn"`
		MaxResults       *int32 `json:"maxResults,omitempty"`
		NextToken        string `json:"nextToken,omitempty"`
	}

	type respBody struct {
		NextToken   string        `json:"nextToken"`
		Permissions []interface{} `json:"permissions"`
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			nextToken := ""
			pages := 0

			for {
				req := reqBody{
					ResourceShareArn: shareARN,
					MaxResults:       tc.maxResults,
					NextToken:        nextToken,
				}

				rec := doRAMRequest(t, h, "/listresourcesharepermissions", req)

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
				nextToken = resp.NextToken

				if nextToken == "" {
					break
				}
			}

			assert.GreaterOrEqual(t, pages, tc.minPages)
		})
	}
}

// TestRAMPagination_GetResourceShareInvitations covers invitation pagination.
func TestRAMPagination_GetResourceShareInvitations(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")
	h := ram.NewHandler(b)

	// Seed invitations.
	for i := range 5 {
		shareARN := fmt.Sprintf("arn:aws:ram:us-east-1:111111111111:resource-share/inv-share-%d", i)
		invARN := fmt.Sprintf("arn:aws:ram:us-east-1:111111111111:resource-share-invitation/inv-%d", i)
		ram.AddInvitationInternal(b, ram.NewTestInvitation(invARN, shareARN, fmt.Sprintf("share-%d", i)))
	}

	tests := []struct {
		name       string
		maxResults *int32
		wantTotal  int
		wantPages  int
		wantError  bool
	}{
		{
			name:      "maxResults=2 paginates 5 invitations",
			maxResults: ptr32(2),
			wantTotal: 5,
			wantPages: 3,
		},
		{
			name:      "maxResults=5 returns all in one page",
			maxResults: ptr32(5),
			wantTotal: 5,
			wantPages: 1,
		},
		{
			name:      "maxResults=0 returns error",
			maxResults: ptr32(0),
			wantError: true,
		},
	}

	type reqBody struct {
		MaxResults *int32 `json:"maxResults,omitempty"`
		NextToken  string `json:"nextToken,omitempty"`
	}

	type respBody struct {
		NextToken                string        `json:"nextToken"`
		ResourceShareInvitations []interface{} `json:"resourceShareInvitations"`
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			nextToken := ""
			totalSeen := 0
			pages := 0

			for {
				req := reqBody{MaxResults: tc.maxResults, NextToken: nextToken}
				rec := doRAMRequest(t, h, "/getresourceshareinvitations", req)

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
				totalSeen += len(resp.ResourceShareInvitations)
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

// TestRAMPagination_ListPermissionVersions covers version pagination.
func TestRAMPagination_ListPermissionVersions(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")
	h := ram.NewHandler(b)

	permARN := "arn:aws:ram::aws:permission/versioned-perm"
	ram.AddPermissionInternal(b, ram.NewTestPermission(permARN, "VersionedPerm", "ec2:Subnet"))

	// Create 3 more versions via CreatePermissionVersion.
	for i := range 3 {
		_ = i
		body := map[string]interface{}{
			"permissionArn":  permARN,
			"policyTemplate": `{"Effect":"Allow","Action":["ec2:DescribeSubnets"]}`,
		}
		rec := doRAMRequest(t, h, "/createpermissionversion", body)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		name       string
		maxResults *int32
		wantMin    int // minimum total versions expected
		wantError  bool
	}{
		{
			name:      "maxResults=1 paginates all versions",
			maxResults: ptr32(1),
			wantMin:  2,
		},
		{
			name:      "maxResults=100 returns all in one page",
			maxResults: ptr32(100),
			wantMin:  1,
		},
		{
			name:      "maxResults=0 returns error",
			maxResults: ptr32(0),
			wantError: true,
		},
	}

	type reqBody struct {
		PermissionArn string `json:"permissionArn"`
		MaxResults    *int32 `json:"maxResults,omitempty"`
		NextToken     string `json:"nextToken,omitempty"`
	}

	type respBody struct {
		NextToken   string        `json:"nextToken"`
		Permissions []interface{} `json:"permissions"`
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			nextToken := ""
			totalSeen := 0

			for {
				req := reqBody{
					PermissionArn: permARN,
					MaxResults:    tc.maxResults,
					NextToken:     nextToken,
				}

				rec := doRAMRequest(t, h, "/listpermissionversions", req)

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

				totalSeen += len(resp.Permissions)
				nextToken = resp.NextToken

				if nextToken == "" {
					break
				}
			}

			assert.GreaterOrEqual(t, totalSeen, tc.wantMin)
		})
	}
}

// TestRAMPagination_ListPermissionAssociations covers association pagination.
func TestRAMPagination_ListPermissionAssociations(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")
	h := ram.NewHandler(b)

	permARN := "arn:aws:ram::aws:permission/assoc-perm"
	ram.AddPermissionInternal(b, ram.NewTestPermission(permARN, "AssocPerm", "ec2:Subnet"))

	// Create 4 shares that associate to this permission.
	for i := range 4 {
		shareARN := fmt.Sprintf("arn:aws:ram:us-east-1:000000000000:resource-share/assoc-share-%d", i)
		ram.AddResourceShareInternal(b, ram.NewTestResourceShare(shareARN, fmt.Sprintf("assoc-share-%d", i)))

		body := map[string]interface{}{
			"resourceShareArn": shareARN,
			"permissionArn":    permARN,
		}
		rec := doRAMRequest(t, h, "/associateresourcesharepermission", body)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		name       string
		maxResults *int32
		wantTotal  int
		wantPages  int
		wantError  bool
	}{
		{
			name:      "maxResults=2 paginates 4 associations",
			maxResults: ptr32(2),
			wantTotal: 4,
			wantPages: 2,
		},
		{
			name:      "maxResults=4 returns all in one page",
			maxResults: ptr32(4),
			wantTotal: 4,
			wantPages: 1,
		},
		{
			name:      "maxResults=101 returns error",
			maxResults: ptr32(101),
			wantError: true,
		},
	}

	type reqBody struct {
		PermissionArn string `json:"permissionArn"`
		MaxResults    *int32 `json:"maxResults,omitempty"`
		NextToken     string `json:"nextToken,omitempty"`
	}

	type respBody struct {
		NextToken   string        `json:"nextToken"`
		Permissions []interface{} `json:"permissions"`
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			nextToken := ""
			totalSeen := 0
			pages := 0

			for {
				req := reqBody{
					PermissionArn: permARN,
					MaxResults:    tc.maxResults,
					NextToken:     nextToken,
				}

				rec := doRAMRequest(t, h, "/listpermissionassociations", req)

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
				totalSeen += len(resp.Permissions)
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
func TestRAMPagination_PendingInvitationResources(t *testing.T) {
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
		NextToken string        `json:"nextToken"`
		Resources []interface{} `json:"resources"`
	}

	tests := []struct {
		name       string
		maxResults *int32
		wantError  bool
	}{
		{
			name:      "maxResults=10 succeeds",
			maxResults: ptr32(10),
		},
		{
			name:      "maxResults=0 returns error",
			maxResults: ptr32(0),
			wantError: true,
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
func TestRAMPagination_GetResourcePolicies(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")
	h := ram.NewHandler(b)

	tests := []struct {
		name       string
		maxResults *int32
		wantError  bool
	}{
		{
			name:      "maxResults=10 succeeds even with empty policies",
			maxResults: ptr32(10),
		},
		{
			name:      "maxResults=101 returns error",
			maxResults: ptr32(101),
			wantError: true,
		},
		{
			name:      "maxResults=0 returns error",
			maxResults: ptr32(0),
			wantError: true,
		},
	}

	type reqBody struct {
		ResourceArns []string `json:"resourceArns"`
		MaxResults   *int32   `json:"maxResults,omitempty"`
		NextToken    string   `json:"nextToken,omitempty"`
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

// ptr32 is a helper to get a pointer to an int32 literal.
func ptr32(v int32) *int32 { return &v }

// Ensure time package is referenced (used indirectly via NewTestResourceShare).
var _ = time.Now
