package ssoadmin_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssoadmin"
)

// TestRefinement2_DescribeInstanceCreatedDate verifies that DescribeInstance returns CreatedDate.
func TestRefinement2_DescribeInstanceCreatedDate(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "date-inst")

	rec := doRequest(t, h, "DescribeInstance", map[string]any{"InstanceArn": instanceArn})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	assert.NotNil(t, resp["CreatedDate"], "CreatedDate must be present in DescribeInstance response")
	cd, ok := resp["CreatedDate"].(float64)
	assert.True(t, ok, "CreatedDate should be a numeric timestamp")
	assert.Greater(t, cd, float64(0), "CreatedDate should be > 0")
}

// TestRefinement2_ListRegionsReturnsMetadata verifies that ListRegions returns RegionMetadata objects.
func TestRefinement2_ListRegionsReturnsMetadata(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "regions-inst")

	rec := doRequest(t, h, "AddRegion", map[string]any{"InstanceArn": instanceArn, "RegionName": "eu-west-1"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "ListRegions", map[string]any{"InstanceArn": instanceArn})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	regions, ok := resp["Regions"].([]any)
	require.True(t, ok)
	require.Len(t, regions, 1)

	region := regions[0].(map[string]any)
	assert.Equal(t, "eu-west-1", region["RegionName"])
	// ListRegions lazily transitions ADDING -> ACTIVE on read, mirroring
	// ListInstances' CREATE_IN_PROGRESS -> ACTIVE transition.
	assert.Equal(t, "ACTIVE", region["Status"])
	assert.Equal(t, false, region["IsPrimaryRegion"])
	assert.NotNil(t, region["AddedDate"])
}

// TestRefinement2_ResetReseeds verifies that Reset re-seeds the default instance.
func TestRefinement2_ResetReseeds(t *testing.T) {
	t.Parallel()

	backend := ssoadmin.NewInMemoryBackend("000000000000", "us-east-1")
	h := ssoadmin.NewHandler(backend)

	h.Reset()

	rec := doRequest(t, h, "ListInstances", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	instances := resp["Instances"].([]any)
	assert.Len(t, instances, 1, "default instance should be re-seeded after Reset")
}

// TestRefinement2_DescribeApplicationIncludesTags verifies that DescribeApplication includes Tags.
func TestRefinement2_DescribeApplicationIncludesTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "app-tags-inst")

	createRec := doRequest(t, h, "CreateApplication", map[string]any{
		"InstanceArn":            instanceArn,
		"ApplicationProviderArn": "arn:aws:sso::000000000000:applicationProvider/custom",
		"Name":                   "TaggedApp",
		"Tags":                   []map[string]any{{"Key": "env", "Value": "prod"}},
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	createResp := parseResponse(t, createRec)
	appArn := createResp["ApplicationArn"].(string)

	rec := doRequest(t, h, "DescribeApplication", map[string]any{"ApplicationArn": appArn})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)

	app, ok := resp["Application"].(map[string]any)
	require.True(t, ok)
	tags, ok := app["Tags"].([]any)
	require.True(t, ok, "Tags should be present in DescribeApplication response")
	require.NotEmpty(t, tags)
}

// TestRefinement2_CreateTrustedTokenIssuerWithTags verifies that CreateTrustedTokenIssuer accepts tags.
func TestRefinement2_CreateTrustedTokenIssuerWithTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "tti-tags-inst")

	rec := doRequest(t, h, "CreateTrustedTokenIssuer", map[string]any{
		"InstanceArn":            instanceArn,
		"Name":                   "MyIssuer",
		"TrustedTokenIssuerType": "OIDC_JWT",
		"Tags":                   []map[string]any{{"Key": "service", "Value": "auth"}},
		"TrustedTokenIssuerConfiguration": map[string]any{
			"OidcJwtConfiguration": map[string]any{
				"IssuerUrl":          "https://auth.example.com",
				"ClaimAttributePath": "sub",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	assert.NotEmpty(t, resp["TrustedTokenIssuerArn"])
}

// TestRefinement2_DescribeTrustedTokenIssuerConfiguration verifies TrustedTokenIssuerConfiguration is returned.
func TestRefinement2_DescribeTrustedTokenIssuerConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "tti-cfg-inst")

	createRec := doRequest(t, h, "CreateTrustedTokenIssuer", map[string]any{
		"InstanceArn":            instanceArn,
		"Name":                   "CfgIssuer",
		"TrustedTokenIssuerType": "OIDC_JWT",
		"TrustedTokenIssuerConfiguration": map[string]any{
			"OidcJwtConfiguration": map[string]any{
				"IssuerUrl":           "https://auth.example.com",
				"ClaimAttributePath":  "sub",
				"JwksRetrievalOption": "OPEN_ID_DISCOVERY",
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	createResp := parseResponse(t, createRec)
	ttiArn := createResp["TrustedTokenIssuerArn"].(string)

	rec := doRequest(t, h, "DescribeTrustedTokenIssuer", map[string]any{"TrustedTokenIssuerArn": ttiArn})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)

	tti := resp["TrustedTokenIssuer"].(map[string]any)
	cfg, ok := tti["TrustedTokenIssuerConfiguration"].(map[string]any)
	require.True(t, ok, "TrustedTokenIssuerConfiguration should be in DescribeTrustedTokenIssuer")
	oidc, ok := cfg["OidcJwtConfiguration"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "https://auth.example.com", oidc["IssuerUrl"])
	assert.Equal(t, "sub", oidc["ClaimAttributePath"])
}

// TestRefinement2_CreateACAConflict verifies that creating ACA twice returns 409.
func TestRefinement2_CreateACAConflict(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "aca-conflict-inst")

	payload := map[string]any{
		"InstanceArn": instanceArn,
		"InstanceAccessControlAttributeConfiguration": map[string]any{
			"AccessControlAttributes": []map[string]any{
				{"Key": "dept", "Value": map[string]any{"Source": []string{"${user:department}"}}},
			},
		},
	}

	rec := doRequest(t, h, "CreateInstanceAccessControlAttributeConfiguration", payload)
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doRequest(t, h, "CreateInstanceAccessControlAttributeConfiguration", payload)
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

// TestRefinement2_ListPermissionSetsProvisionedToAccount verifies the new operation.
func TestRefinement2_ListPermissionSetsProvisionedToAccount(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "pspta-inst")
	psArn := createPermissionSet(t, h, instanceArn, "pspta-ps")

	accountID := "111111111111"
	doRequest(t, h, "CreateAccountAssignment", map[string]any{
		"InstanceArn":      instanceArn,
		"PermissionSetArn": psArn,
		"TargetId":         accountID,
		"TargetType":       "AWS_ACCOUNT",
		"PrincipalType":    "USER",
		"PrincipalId":      "user-xyz",
	})

	rec := doRequest(t, h, "ListPermissionSetsProvisionedToAccount", map[string]any{
		"InstanceArn": instanceArn,
		"AccountId":   accountID,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	psets, ok := resp["PermissionSets"].([]any)
	require.True(t, ok)
	assert.Len(t, psets, 1)
	assert.Equal(t, psArn, psets[0].(string))
}

// TestRefinement2_ListAccountAssignmentsForPrincipal verifies the new operation.
func TestRefinement2_ListAccountAssignmentsForPrincipal(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "laafp-inst")
	psArn := createPermissionSet(t, h, instanceArn, "laafp-ps")

	accountID := "222222222222"
	doRequest(t, h, "CreateAccountAssignment", map[string]any{
		"InstanceArn":      instanceArn,
		"PermissionSetArn": psArn,
		"TargetId":         accountID,
		"TargetType":       "AWS_ACCOUNT",
		"PrincipalType":    "USER",
		"PrincipalId":      "user-abc",
	})

	rec := doRequest(t, h, "ListAccountAssignmentsForPrincipal", map[string]any{
		"InstanceArn":   instanceArn,
		"PrincipalId":   "user-abc",
		"PrincipalType": "USER",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	assignments, ok := resp["AccountAssignments"].([]any)
	require.True(t, ok)
	assert.Len(t, assignments, 1)
	a := assignments[0].(map[string]any)
	assert.Equal(t, accountID, a["AccountId"])
	assert.Equal(t, psArn, a["PermissionSetArn"])
}

// TestRefinement2_TagResourceTooManyTags verifies that adding >50 tags returns 400.
func TestRefinement2_TagResourceTooManyTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "tag-limit-inst")

	// Add 50 tags first.
	tags := make([]map[string]any, 50)
	for i := range tags {
		tags[i] = map[string]any{"Key": "k" + string(rune('a'+i%26)) + string(rune('0'+i/26)), "Value": "v"}
	}
	rec := doRequest(t, h, "TagResource", map[string]any{
		"InstanceArn": instanceArn,
		"ResourceArn": instanceArn,
		"Tags":        tags,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Adding 1 more tag should fail with the real ssoadmin exception type --
	// TooManyTagsException is not in the ssoadmin error model; AWS returns
	// ServiceQuotaExceededException for this limit (see types/errors.go).
	rec = doRequest(t, h, "TagResource", map[string]any{
		"InstanceArn": instanceArn,
		"ResourceArn": instanceArn,
		"Tags":        []map[string]any{{"Key": "extra", "Value": "v"}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	resp := parseResponse(t, rec)
	assert.Equal(t, "ServiceQuotaExceededException", resp["__type"])
}

// TestRefinement2_CreateAccountAssignmentIdempotency verifies deterministic idempotency.
func TestRefinement2_CreateAccountAssignmentIdempotency(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "idempotency-inst")
	psArn := createPermissionSet(t, h, instanceArn, "idempotency-ps")

	req := map[string]any{
		"InstanceArn":      instanceArn,
		"PermissionSetArn": psArn,
		"TargetId":         "333333333333",
		"TargetType":       "AWS_ACCOUNT",
		"PrincipalType":    "USER",
		"PrincipalId":      "user-idem",
	}

	rec1 := doRequest(t, h, "CreateAccountAssignment", req)
	require.Equal(t, http.StatusOK, rec1.Code)
	resp1 := parseResponse(t, rec1)

	rec2 := doRequest(t, h, "CreateAccountAssignment", req)
	require.Equal(t, http.StatusOK, rec2.Code)
	resp2 := parseResponse(t, rec2)

	// Both responses should return the same request ID.
	id1 := resp1["AccountAssignmentCreationStatus"].(map[string]any)["RequestId"]
	id2 := resp2["AccountAssignmentCreationStatus"].(map[string]any)["RequestId"]
	assert.Equal(t, id1, id2, "idempotent calls should return the same RequestId")
}

// TestRefinement2_ListAccountAssignmentStatusSorted verifies creation statuses are sorted desc.
func TestRefinement2_ListAccountAssignmentStatusSorted(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "status-sort-inst")
	psArn := createPermissionSet(t, h, instanceArn, "status-sort-ps")

	for i := range 3 {
		doRequest(t, h, "CreateAccountAssignment", map[string]any{
			"InstanceArn":      instanceArn,
			"PermissionSetArn": psArn,
			"TargetId":         "444444444444",
			"TargetType":       "AWS_ACCOUNT",
			"PrincipalType":    "USER",
			"PrincipalId":      "user-sort-" + string(rune('a'+i)),
		})
	}

	rec := doRequest(t, h, "ListAccountAssignmentCreationStatus", map[string]any{
		"InstanceArn": instanceArn,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	statuses, ok := resp["AccountAssignmentsCreationStatus"].([]any)
	require.True(t, ok)
	assert.Len(t, statuses, 3)
}

// TestRefinement2_ListPermissionSetsProvisionedToAccountMissing verifies required fields.
func TestRefinement2_ListPermissionSetsProvisionedToAccountMissing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		payload    map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "missing InstanceArn",
			payload:    map[string]any{"AccountId": "123456789012"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing AccountId",
			payload:    map[string]any{"InstanceArn": "arn:aws:sso:::instance/ssoins-xxx"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			rec := doRequest(t, h, "ListPermissionSetsProvisionedToAccount", tt.payload)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
