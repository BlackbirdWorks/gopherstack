package lakeformation_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/lakeformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_RegisterDeregisterDescribeResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		resourceArn     string
		roleArn         string
		wantRegStatus   int
		wantDescStatus  int
		wantDeregStatus int
	}{
		{
			name:            "full_lifecycle",
			resourceArn:     "arn:aws:s3:::my-bucket",
			roleArn:         "arn:aws:iam::123456789012:role/MyRole",
			wantRegStatus:   http.StatusOK,
			wantDescStatus:  http.StatusOK,
			wantDeregStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// Register
			body := `{"ResourceArn":"` + tt.resourceArn + `","RoleArn":"` + tt.roleArn + `"}`
			rec := doLFRequest(t, h, "/RegisterResource", body)
			assert.Equal(t, tt.wantRegStatus, rec.Code)

			// Describe
			descBody := `{"ResourceArn":"` + tt.resourceArn + `"}`
			rec = doLFRequest(t, h, "/DescribeResource", descBody)
			assert.Equal(t, tt.wantDescStatus, rec.Code)

			var descResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
			ri, ok := descResp["ResourceInfo"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.resourceArn, ri["ResourceArn"])

			// Deregister
			rec = doLFRequest(t, h, "/DeregisterResource", descBody)
			assert.Equal(t, tt.wantDeregStatus, rec.Code)

			// Describe after deregister → 404
			rec = doLFRequest(t, h, "/DescribeResource", descBody)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestHandler_RegisterResource_AlreadyExists(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	body := `{"ResourceArn":"arn:aws:s3:::bucket","RoleArn":"arn:aws:iam::123:role/R"}`
	rec := doLFRequest(t, h, "/RegisterResource", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doLFRequest(t, h, "/RegisterResource", body)
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestHandler_ListResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		setupArns  []string
		wantStatus int
		wantCount  int
	}{
		{
			name:       "empty",
			body:       "{}",
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name:       "with_resources",
			setupArns:  []string{"arn:aws:s3:::a", "arn:aws:s3:::b"},
			body:       "{}",
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()
			h := lakeformation.NewHandler(b)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			for _, arn := range tt.setupArns {
				require.NoError(
					t,
					b.RegisterResource(arn, "arn:aws:iam::123:role/R", lakeformation.RegisterResourceOptions{}),
				)
			}

			rec := doLFRequest(t, h, "/ListResources", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			list, _ := resp["ResourceInfoList"].([]any)
			assert.Len(t, list, tt.wantCount)
		})
	}
}

func TestRegisterResource_DuplicateReturns409(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	b.AddResourceInternal("arn:aws:s3:::existing", "role")

	rec := postJSON(t, h, "/RegisterResource", map[string]any{
		"ResourceArn": "arn:aws:s3:::existing",
		"RoleArn":     "arn:aws:iam::123:role/r",
	})
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestListResources_Sorted(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	b.AddResourceInternal("arn:aws:s3:::zz-bucket", "role")
	b.AddResourceInternal("arn:aws:s3:::aa-bucket", "role")
	b.AddResourceInternal("arn:aws:s3:::mm-bucket", "role")

	rec := postJSON(t, h, "/ListResources", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	list := out["ResourceInfoList"].([]any)
	require.Len(t, list, 3)
	assert.Equal(t, "arn:aws:s3:::aa-bucket", list[0].(map[string]any)["ResourceArn"])
	assert.Equal(t, "arn:aws:s3:::mm-bucket", list[1].(map[string]any)["ResourceArn"])
	assert.Equal(t, "arn:aws:s3:::zz-bucket", list[2].(map[string]any)["ResourceArn"])
}

func TestDeregisterResourceCleansPermissions(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	arn := "arn:aws:s3:::cleanup-bucket"

	rec := postJSON(t, h, "/RegisterResource", map[string]any{
		"ResourceArn": arn,
		"RoleArn":     "arn:aws:iam::123:role/r",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	b.AddPermissionInternal(&lakeformation.PermissionEntry{
		Principal: &lakeformation.DataLakePrincipal{DataLakePrincipalIdentifier: "arn:aws:iam::123:user/a"},
		Resource:  &lakeformation.Resource{DataLocation: &lakeformation.DataLocationResource{ResourceArn: arn}},
	})
	require.Equal(t, 1, b.PermissionCount())

	rec2 := postJSON(t, h, "/DeregisterResource", map[string]any{"ResourceArn": arn})
	require.Equal(t, http.StatusOK, rec2.Code)

	assert.Equal(t, 0, b.ResourceCount())
	assert.Equal(t, 0, b.PermissionCount(), "permissions should be cleaned up on deregister")
}

func TestUpdateResource_Success(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	b.AddResourceInternal("arn:aws:s3:::my-bucket", "old-role")

	rec := postJSON(t, h, "/UpdateResource", map[string]any{
		"ResourceArn": "arn:aws:s3:::my-bucket",
		"RoleArn":     "arn:aws:iam::000000000000:role/new-role",
	})

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestUpdateResource_ExtendedFieldsRoundTrip(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	b.AddResourceInternal("arn:aws:s3:::my-bucket", "old-role")

	rec := postJSON(t, h, "/UpdateResource", map[string]any{
		"ResourceArn":                  "arn:aws:s3:::my-bucket",
		"RoleArn":                      "arn:aws:iam::000000000000:role/new-role",
		"ExpectedResourceOwnerAccount": "111111111111",
		"WithFederation":               true,
		"HybridAccessEnabled":          true,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := postJSON(t, h, "/DescribeResource", map[string]any{"ResourceArn": "arn:aws:s3:::my-bucket"})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec2.Body, &out))
	info := out["ResourceInfo"].(map[string]any)
	assert.Equal(t, "arn:aws:iam::000000000000:role/new-role", info["RoleArn"])
	assert.Equal(t, "111111111111", info["ExpectedResourceOwnerAccount"])
	assert.Equal(t, true, info["WithFederation"])
	assert.Equal(t, true, info["HybridAccessEnabled"])
}

func TestUpdateResource_NotFound(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/UpdateResource", map[string]any{
		"ResourceArn": "arn:aws:s3:::missing",
		"RoleArn":     "arn:aws:iam::000000000000:role/r",
	})

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestUpdateResource_MissingRoleArn(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	b.AddResourceInternal("arn:aws:s3:::my-bucket", "role")

	rec := postJSON(t, h, "/UpdateResource", map[string]any{
		"ResourceArn": "arn:aws:s3:::my-bucket",
		"RoleArn":     "",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- StartTransaction / DescribeTransaction / ListTransactions tests ---

func TestRegisterResource_UseServiceLinkedRole(t *testing.T) {
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

// TestRegisterResource_ExtendedFieldsRoundTrip verifies that
// WithFederation/HybridAccessEnabled/ExpectedResourceOwnerAccount/
// WithPrivilegedAccess actually persist and surface back through
// DescribeResource, and that VerificationStatus is populated -- these fields
// previously existed on registerResourceInput/ResourceInfo but were dropped
// on the floor because the backend's RegisterResource(resourceArn, roleArn
// string) signature had nowhere to carry them.
func TestRegisterResource_ExtendedFieldsRoundTrip(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/RegisterResource", map[string]any{
		"ResourceArn":                  "arn:aws:s3:::extended-lake",
		"RoleArn":                      "arn:aws:iam::123456789012:role/R",
		"ExpectedResourceOwnerAccount": "999999999999",
		"WithFederation":               true,
		"HybridAccessEnabled":          true,
		"WithPrivilegedAccess":         true,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := postJSON(t, h, "/DescribeResource", map[string]any{"ResourceArn": "arn:aws:s3:::extended-lake"})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec2.Body, &out))
	info := out["ResourceInfo"].(map[string]any)

	assert.Equal(t, "999999999999", info["ExpectedResourceOwnerAccount"])
	assert.Equal(t, true, info["WithFederation"])
	assert.Equal(t, true, info["HybridAccessEnabled"])
	assert.Equal(t, true, info["WithPrivilegedAccess"])
	assert.NotEmpty(t, info["VerificationStatus"])
}
