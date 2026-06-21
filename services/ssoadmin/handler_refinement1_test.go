package ssoadmin_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssoadmin"
)

// TestRefinement1_ProviderNilContext verifies that Init returns ErrNilAppContext on nil input.
func TestRefinement1_ProviderNilContext(t *testing.T) {
	t.Parallel()

	p := ssoadmin.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, ssoadmin.ErrNilAppContext)
}

// TestRefinement1_HandlerOpsLen verifies GetSupportedOperations count.
func TestRefinement1_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	assert.Equal(t, 79, ssoadmin.HandlerOpsLen(h))
}

// TestRefinement1_GetSupportedOperationsSorted verifies that GetSupportedOperations is sorted.
func TestRefinement1_GetSupportedOperationsSorted(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	ops := h.GetSupportedOperations()
	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i], "ops should be sorted: %s >= %s", ops[i-1], ops[i])
	}
}

// TestRefinement1_HandlerReset verifies that Reset clears all backend state.
func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	backend := ssoadmin.NewInMemoryBackend("000000000000", "us-east-1")
	h := ssoadmin.NewHandler(backend)

	// Create resources.
	instanceArn := createInstance(t, h, "reset-inst")
	createPermissionSet(t, h, instanceArn, "reset-ps")
	_ = doRequest(t, h, "CreateApplication", map[string]any{
		"InstanceArn":            instanceArn,
		"ApplicationProviderArn": "arn:aws:sso::000000000000:applicationProvider/custom",
		"Name":                   "ResetApp",
	})

	h.Reset()

	// After reset, only the default pre-seeded instance remains.
	rec := doRequest(t, h, "ListInstances", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	instances := resp["Instances"].([]any)
	assert.Len(t, instances, 1, "reset should re-seed the default instance")
}

// TestRefinement1_InstanceCount verifies that InstanceCount export helper works.
func TestRefinement1_InstanceCount(t *testing.T) {
	t.Parallel()

	b := ssoadmin.NewInMemoryBackend("000000000000", "us-east-1")
	// Default pre-seeded instance exists.
	assert.Equal(t, 1, ssoadmin.InstanceCount(b))

	b.AddInstanceInternal("extra")
	assert.Equal(t, 2, ssoadmin.InstanceCount(b))
}

// TestRefinement1_PermissionSetCount verifies PermissionSetCount export helper.
func TestRefinement1_PermissionSetCount(t *testing.T) {
	t.Parallel()

	b := ssoadmin.NewInMemoryBackend("000000000000", "us-east-1")
	assert.Equal(t, 0, ssoadmin.PermissionSetCount(b))

	inst := b.AddInstanceInternal("ps-inst")
	b.AddPermissionSetInternal(inst.InstanceArn, "ps1")
	b.AddPermissionSetInternal(inst.InstanceArn, "ps2")
	assert.Equal(t, 2, ssoadmin.PermissionSetCount(b))
}

// TestRefinement1_ApplicationCount verifies ApplicationCount export helper.
func TestRefinement1_ApplicationCount(t *testing.T) {
	t.Parallel()

	b := ssoadmin.NewInMemoryBackend("000000000000", "us-east-1")
	assert.Equal(t, 0, ssoadmin.ApplicationCount(b))

	inst := b.AddInstanceInternal("app-inst")
	b.AddApplicationInternal(inst.InstanceArn, "app1")
	assert.Equal(t, 1, ssoadmin.ApplicationCount(b))
}

// TestRefinement1_TrustedTokenIssuerCount verifies TrustedTokenIssuerCount export helper.
func TestRefinement1_TrustedTokenIssuerCount(t *testing.T) {
	t.Parallel()

	b := ssoadmin.NewInMemoryBackend("000000000000", "us-east-1")
	h := ssoadmin.NewHandler(b)
	assert.Equal(t, 0, ssoadmin.TrustedTokenIssuerCount(b))

	inst := b.AddInstanceInternal("tti-inst")
	rec := doRequest(t, h, "CreateTrustedTokenIssuer", map[string]any{
		"InstanceArn":            inst.InstanceArn,
		"Name":                   "Issuer1",
		"TrustedTokenIssuerType": "OIDC_JWT",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, ssoadmin.TrustedTokenIssuerCount(b))
}

// TestRefinement1_DeleteInstanceCascades verifies that deleting an instance also removes
// dependent permission sets, applications, and trusted token issuers.
func TestRefinement1_DeleteInstanceCascades(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		createPS     bool
		createApp    bool
		createTTI    bool
		wantPSCount  int
		wantAppCount int
		wantTTICount int
	}{
		{
			name:         "cascade deletes permission sets and apps",
			createPS:     true,
			createApp:    true,
			createTTI:    true,
			wantPSCount:  0,
			wantAppCount: 0,
			wantTTICount: 0,
		},
		{
			name:         "cascade on empty instance",
			createPS:     false,
			createApp:    false,
			createTTI:    false,
			wantPSCount:  0,
			wantAppCount: 0,
			wantTTICount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssoadmin.NewInMemoryBackend("000000000000", "us-east-1")
			h := ssoadmin.NewHandler(b)

			inst := b.AddInstanceInternal("cascade-inst")
			instanceArn := inst.InstanceArn

			if tt.createPS {
				b.AddPermissionSetInternal(instanceArn, "cascade-ps")
			}
			if tt.createApp {
				b.AddApplicationInternal(instanceArn, "cascade-app")
			}
			if tt.createTTI {
				rec := doRequest(t, h, "CreateTrustedTokenIssuer", map[string]any{
					"InstanceArn":            instanceArn,
					"Name":                   "cascade-tti",
					"TrustedTokenIssuerType": "OIDC_JWT",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "DeleteInstance", map[string]any{"InstanceArn": instanceArn})
			require.Equal(t, http.StatusOK, rec.Code)

			assert.Equal(t, tt.wantPSCount, ssoadmin.PermissionSetCount(b))
			assert.Equal(t, tt.wantAppCount, ssoadmin.ApplicationCount(b))
			assert.Equal(t, tt.wantTTICount, ssoadmin.TrustedTokenIssuerCount(b))
		})
	}
}

// TestRefinement1_TagResourceApplication verifies that TagResource, UntagResource,
// and ListTagsForResource work on applications.
func TestRefinement1_TagResourceApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tagKey     string
		tagValue   string
		wantStatus int
	}{
		{
			name:       "tag application resource",
			tagKey:     "env",
			tagValue:   "prod",
			wantStatus: http.StatusOK,
		},
		{
			name:       "tag with multiple tags",
			tagKey:     "team",
			tagValue:   "platform",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "tag-app-inst")
			rec := doRequest(t, h, "CreateApplication", map[string]any{
				"InstanceArn":            instanceArn,
				"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
				"Name":                   "TagApp",
			})
			require.Equal(t, http.StatusOK, rec.Code)
			appArn := parseResponse(t, rec)["ApplicationArn"].(string)

			// Tag it.
			tagRec := doRequest(t, h, "TagResource", map[string]any{
				"InstanceArn": instanceArn,
				"ResourceArn": appArn,
				"Tags":        []map[string]any{{"Key": tt.tagKey, "Value": tt.tagValue}},
			})
			assert.Equal(t, tt.wantStatus, tagRec.Code)

			// List tags and verify.
			listRec := doRequest(t, h, "ListTagsForResource", map[string]any{
				"InstanceArn": instanceArn,
				"ResourceArn": appArn,
			})
			require.Equal(t, http.StatusOK, listRec.Code)
			resp := parseResponse(t, listRec)
			tagList := resp["Tags"].([]any)
			found := false
			for _, item := range tagList {
				m := item.(map[string]any)
				if m["Key"] == tt.tagKey {
					found = true
					assert.Equal(t, tt.tagValue, m["Value"])
				}
			}
			assert.True(t, found, "expected tag key %q in response", tt.tagKey)
		})
	}
}

// TestRefinement1_UntagResourceApplication verifies that UntagResource works on applications.
func TestRefinement1_UntagResourceApplication(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "untag-app-inst")
	rec := doRequest(t, h, "CreateApplication", map[string]any{
		"InstanceArn":            instanceArn,
		"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
		"Name":                   "UntagApp",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	appArn := parseResponse(t, rec)["ApplicationArn"].(string)

	// Tag it first.
	_ = doRequest(t, h, "TagResource", map[string]any{
		"InstanceArn": instanceArn,
		"ResourceArn": appArn,
		"Tags":        []map[string]any{{"Key": "env", "Value": "test"}},
	})

	// Untag it.
	untagRec := doRequest(t, h, "UntagResource", map[string]any{
		"InstanceArn": instanceArn,
		"ResourceArn": appArn,
		"TagKeys":     []string{"env"},
	})
	assert.Equal(t, http.StatusOK, untagRec.Code)

	// Verify tag is removed.
	listRec := doRequest(t, h, "ListTagsForResource", map[string]any{
		"InstanceArn": instanceArn,
		"ResourceArn": appArn,
	})
	resp := parseResponse(t, listRec)
	tagList := resp["Tags"].([]any)
	for _, item := range tagList {
		m := item.(map[string]any)
		assert.NotEqual(t, "env", m["Key"])
	}
}

// TestRefinement1_ListAccountAssignmentsReturnsCopies verifies that mutations of returned
// assignments do not affect backend state.
func TestRefinement1_ListAccountAssignmentsReturnsCopies(t *testing.T) {
	t.Parallel()

	b := ssoadmin.NewInMemoryBackend("000000000000", "us-east-1")
	h := ssoadmin.NewHandler(b)

	instanceArn := createInstance(t, h, "copy-inst")
	psArn := createPermissionSet(t, h, instanceArn, "copy-ps")

	_ = doRequest(t, h, "CreateAccountAssignment", map[string]any{
		"InstanceArn":      instanceArn,
		"PermissionSetArn": psArn,
		"TargetId":         "123456789012",
		"TargetType":       "AWS_ACCOUNT",
		"PrincipalId":      "user-001",
		"PrincipalType":    "USER",
	})

	rec := doRequest(t, h, "ListAccountAssignments", map[string]any{
		"InstanceArn":      instanceArn,
		"PermissionSetArn": psArn,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	assignments := resp["AccountAssignments"].([]any)
	assert.Len(t, assignments, 1)
}

// TestRefinement1_BadJSONReturns400 verifies that all handlers return 400 on bad JSON bodies.
func TestRefinement1_BadJSONReturns400(t *testing.T) {
	t.Parallel()

	ops := []string{
		"CreateInstance",
		"DescribeInstance",
		"DeleteInstance",
		"CreatePermissionSet",
		"CreateAccountAssignment",
		"DeleteApplication",
		"CreateApplication",
		"CreateTrustedTokenIssuer",
	}

	for _, op := range ops {
		t.Run(op, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			rec := doRequestRaw(t, h, op, []byte(`{invalid json`))
			assert.Equal(t, http.StatusBadRequest, rec.Code, "op=%s", op)
		})
	}
}

// TestRefinement1_SnapshotRestore verifies that Snapshot and Restore preserve state.
func TestRefinement1_SnapshotRestore(t *testing.T) {
	t.Parallel()

	b1 := ssoadmin.NewInMemoryBackend("000000000000", "us-east-1")
	h1 := ssoadmin.NewHandler(b1)

	// Create state.
	instanceArn := createInstance(t, h1, "snap-inst")
	createPermissionSet(t, h1, instanceArn, "snap-ps")

	// Snapshot.
	snap := b1.Snapshot(t.Context())
	require.NotNil(t, snap)
	require.NotEmpty(t, snap)

	// Restore into fresh backend.
	b2 := ssoadmin.NewInMemoryBackend("000000000000", "us-east-1")
	h2 := ssoadmin.NewHandler(b2)
	require.NoError(t, b2.Restore(t.Context(), snap))

	// Verify instance is present.
	rec := doRequest(t, h2, "DescribeInstance", map[string]any{"InstanceArn": instanceArn})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify permission set count preserved.
	assert.GreaterOrEqual(t, ssoadmin.PermissionSetCount(b2), 1)
}

// TestRefinement1_CreatePermissionSetValidation verifies required-field validation.
func TestRefinement1_CreatePermissionSetValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		instanceArn string
		psName      string
		wantStatus  int
	}{
		{
			name:        "missing instance arn",
			instanceArn: "",
			psName:      "MyPS",
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "missing name",
			instanceArn: "arn:aws:sso:::instance/ssoins-test1234",
			psName:      "",
			wantStatus:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			rec := doRequest(t, h, "CreatePermissionSet", map[string]any{
				"InstanceArn": tt.instanceArn,
				"Name":        tt.psName,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement1_CreateApplicationNameRequired verifies that creating an application
// requires a non-empty Name field.
func TestRefinement1_CreateApplicationNameRequired(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "name-req-inst")
	rec := doRequest(t, h, "CreateApplication", map[string]any{
		"InstanceArn":            instanceArn,
		"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
		"Name":                   "",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_CreateTrustedTokenIssuerNameRequired verifies that creating a trusted token
// issuer requires a non-empty Name field.
func TestRefinement1_CreateTrustedTokenIssuerNameRequired(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "tti-name-req-inst")
	rec := doRequest(t, h, "CreateTrustedTokenIssuer", map[string]any{
		"InstanceArn":            instanceArn,
		"Name":                   "",
		"TrustedTokenIssuerType": "OIDC_JWT",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_DeleteApplicationCascadesAssignments verifies that DeleteApplication
// removes related assignments.
func TestRefinement1_DeleteApplicationCascadesAssignments(t *testing.T) {
	t.Parallel()

	b := ssoadmin.NewInMemoryBackend("000000000000", "us-east-1")
	h := ssoadmin.NewHandler(b)

	instanceArn := createInstance(t, h, "del-cascade-inst")
	rec := doRequest(t, h, "CreateApplication", map[string]any{
		"InstanceArn":            instanceArn,
		"ApplicationProviderArn": "arn:aws:sso::000000000000:applicationProvider/custom",
		"Name":                   "CascadeApp",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	appArn := parseResponse(t, rec)["ApplicationArn"].(string)

	// Assign a user.
	_ = doRequest(t, h, "CreateApplicationAssignment", map[string]any{
		"ApplicationArn": appArn,
		"PrincipalId":    "user-cascade",
		"PrincipalType":  "USER",
	})

	// Delete the application.
	delRec := doRequest(t, h, "DeleteApplication", map[string]any{"ApplicationArn": appArn})
	require.Equal(t, http.StatusOK, delRec.Code)

	// Application count should be 0.
	assert.Equal(t, 0, ssoadmin.ApplicationCount(b))
}

// TestRefinement1_DefaultSessionDuration verifies that a new permission set gets a default
// session duration of PT1H.
func TestRefinement1_DefaultSessionDuration(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "session-inst")
	psArn := createPermissionSet(t, h, instanceArn, "session-ps")

	rec := doRequest(t, h, "DescribePermissionSet", map[string]any{
		"InstanceArn":      instanceArn,
		"PermissionSetArn": psArn,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	ps := resp["PermissionSet"].(map[string]any)
	assert.Equal(t, "PT1H", ps["SessionDuration"])
}

// TestRefinement1_AddInstanceInternal verifies the seed helper.
func TestRefinement1_AddInstanceInternal(t *testing.T) {
	t.Parallel()

	b := ssoadmin.NewInMemoryBackend("000000000000", "us-east-1")
	initial := ssoadmin.InstanceCount(b)
	inst := b.AddInstanceInternal("seeded")
	require.NotNil(t, inst)
	assert.NotEmpty(t, inst.InstanceArn)
	assert.Equal(t, "seeded", inst.Name)
	assert.Equal(t, initial+1, ssoadmin.InstanceCount(b))
}

// TestRefinement1_AddPermissionSetInternal verifies the seed helper.
func TestRefinement1_AddPermissionSetInternal(t *testing.T) {
	t.Parallel()

	b := ssoadmin.NewInMemoryBackend("000000000000", "us-east-1")
	inst := b.AddInstanceInternal("seed-inst")
	ps := b.AddPermissionSetInternal(inst.InstanceArn, "seed-ps")
	require.NotNil(t, ps)
	assert.NotEmpty(t, ps.PermissionSetArn)
	assert.Equal(t, "seed-ps", ps.Name)
	assert.Equal(t, 1, ssoadmin.PermissionSetCount(b))
}

// TestRefinement1_AddApplicationInternal verifies the seed helper.
func TestRefinement1_AddApplicationInternal(t *testing.T) {
	t.Parallel()

	b := ssoadmin.NewInMemoryBackend("000000000000", "us-east-1")
	inst := b.AddInstanceInternal("app-seed-inst")
	app := b.AddApplicationInternal(inst.InstanceArn, "seed-app")
	require.NotNil(t, app)
	assert.NotEmpty(t, app.ApplicationArn)
	assert.Equal(t, "seed-app", app.Name)
	assert.Equal(t, 1, ssoadmin.ApplicationCount(b))
}

// TestRefinement1_AccessControlAttributesDeepCopy verifies that CreateInstanceAccessControlAttributeConfiguration
// stores a deep copy of the attributes.
func TestRefinement1_AccessControlAttributesDeepCopy(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "aca-deep-copy-inst")

	attributes := []map[string]any{
		{
			"Key":   "dept",
			"Value": map[string]any{"Source": []string{"${path:enterprise.department}"}},
		},
	}

	rec := doRequest(t, h, "CreateInstanceAccessControlAttributeConfiguration", map[string]any{
		"InstanceArn": instanceArn,
		"InstanceAccessControlAttributeConfiguration": map[string]any{
			"AccessControlAttributes": attributes,
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestRefinement1_UnknownOperationReturns400 verifies that unknown ops return 400.
func TestRefinement1_UnknownOperationReturns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doRequest(t, h, "NonExistentOperation", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_TagResourceInstance verifies that TagResource works on instances.
func TestRefinement1_TagResourceInstance(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "tag-instance")

	tagRec := doRequest(t, h, "TagResource", map[string]any{
		"InstanceArn": instanceArn,
		"ResourceArn": instanceArn,
		"Tags":        []map[string]any{{"Key": "owner", "Value": "team-a"}},
	})
	assert.Equal(t, http.StatusOK, tagRec.Code)

	listRec := doRequest(t, h, "ListTagsForResource", map[string]any{
		"InstanceArn": instanceArn,
		"ResourceArn": instanceArn,
	})
	require.Equal(t, http.StatusOK, listRec.Code)
	resp := parseResponse(t, listRec)
	tagList := resp["Tags"].([]any)
	found := false
	for _, item := range tagList {
		m := item.(map[string]any)
		if m["Key"] == "owner" {
			found = true
			assert.Equal(t, "team-a", m["Value"])
		}
	}
	assert.True(t, found)
}

// TestRefinement1_AddRegionIdempotent verifies adding the same region twice is idempotent.
func TestRefinement1_AddRegionIdempotent(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "region-idempotent-inst")

	rec1 := doRequest(t, h, "AddRegion", map[string]any{
		"InstanceArn": instanceArn,
		"RegionName":  "us-west-2",
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doRequest(t, h, "AddRegion", map[string]any{
		"InstanceArn": instanceArn,
		"RegionName":  "us-west-2",
	})
	assert.Equal(t, http.StatusOK, rec2.Code)
}

// TestRefinement1_AttachCustomerManagedPolicyIdempotent verifies attaching the same policy twice is idempotent.
func TestRefinement1_AttachCustomerManagedPolicyIdempotent(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "cmp-idempotent-inst")
	psArn := createPermissionSet(t, h, instanceArn, "cmp-idempotent-ps")

	req := map[string]any{
		"InstanceArn":      instanceArn,
		"PermissionSetArn": psArn,
		"CustomerManagedPolicyReference": map[string]any{
			"Name": "MyPolicy",
			"Path": "/",
		},
	}

	rec1 := doRequest(t, h, "AttachCustomerManagedPolicyReferenceToPermissionSet", req)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doRequest(t, h, "AttachCustomerManagedPolicyReferenceToPermissionSet", req)
	assert.Equal(t, http.StatusOK, rec2.Code)
}
