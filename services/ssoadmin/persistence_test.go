package ssoadmin_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/ssoadmin"
)

// TestInMemoryBackend_SnapshotRestore_FullState exercises a Snapshot->Restore
// round trip across every resource kind the backend owns -- both the
// store.Table-backed "clean"/"dirty" tables (Phase 3.3) and the raw maps left
// untouched by that conversion -- verifying every field survives the round
// trip into a completely fresh backend.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := ssoadmin.NewInMemoryBackend("123456789012", config.DefaultRegion)

	instanceArn := original.AddInstanceInternal("full-state-inst").InstanceArn
	_, err := original.AddRegion(instanceArn, "us-west-2")
	require.NoError(t, err)

	ps, err := original.CreatePermissionSet(instanceArn, "full-state-ps", "desc", "PT2H", "relay", map[string]string{
		"env": "prod",
	})
	require.NoError(t, err)
	psArn := ps.PermissionSetArn

	require.NoError(t, original.AttachManagedPolicyToPermissionSet(
		instanceArn, psArn, "arn:aws:iam::aws:policy/ReadOnly", "ReadOnly",
	))
	require.NoError(t, original.PutInlinePolicyToPermissionSet(instanceArn, psArn, `{"Version":"2012-10-17"}`))
	require.NoError(t, original.AttachCustomerManagedPolicyReferenceToPermissionSet(
		instanceArn, psArn, "cmp-name", "/path/",
	))
	require.NoError(t, original.PutPermissionsBoundaryToPermissionSet(instanceArn, psArn, &ssoadmin.PermissionsBoundary{
		ManagedPolicyArn: "arn:aws:iam::aws:policy/Boundary",
	}))

	requestID, err := original.CreateAccountAssignment(instanceArn, psArn, "111111111111", "user-1", "USER")
	require.NoError(t, err)

	app, err := original.CreateApplication(
		instanceArn, "arn:aws:sso::aws:applicationProvider/custom", "full-state-app", "app desc",
		map[string]string{"team": "platform"}, nil,
	)
	require.NoError(t, err)
	appArn := app.ApplicationArn

	require.NoError(t, original.CreateApplicationAssignment(appArn, "user-2", "USER"))
	require.NoError(t, original.PutApplicationAccessScope(
		appArn, "scope:read", []string{"arn:aws:sso::aws:applicationProvider/custom"},
	))
	require.NoError(t, original.PutApplicationAuthenticationMethod(appArn, "IAM", []byte(`{"Policy":"{}"}`)))
	require.NoError(t, original.PutApplicationGrant(
		appArn, "authorization_code", []byte(`{"RedirectUris":["https://x"]}`),
	))
	require.NoError(t, original.PutApplicationSessionConfiguration(appArn, "PT4H"))
	require.NoError(t, original.PutApplicationAssignmentConfiguration(appArn, true))

	tti, err := original.CreateTrustedTokenIssuer(
		instanceArn, "full-state-tti", "OIDC_JWT", map[string]string{"k": "v"}, nil,
	)
	require.NoError(t, err)
	ttiArn := tti.TrustedTokenIssuerArn

	require.NoError(t, original.CreateInstanceAccessControlAttributeConfiguration(
		instanceArn,
		[]ssoadmin.AccessControlAttribute{
			{Key: "department", Value: ssoadmin.AccessControlAttributeValue{Source: []string{"${path:department}"}}},
		},
	))

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)
	require.NotEmpty(t, snap)

	fresh := ssoadmin.NewInMemoryBackend("999999999999", "us-east-2")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// Instance + region.
	inst, err := fresh.DescribeInstance(instanceArn)
	require.NoError(t, err)
	assert.Equal(t, "full-state-inst", inst.Name)

	regions, err := fresh.ListRegions(instanceArn)
	require.NoError(t, err)
	require.Len(t, regions, 1)
	assert.Equal(t, "us-west-2", regions[0].RegionName)

	// Permission set + attachments.
	restoredPS, err := fresh.DescribePermissionSet(instanceArn, psArn)
	require.NoError(t, err)
	assert.Equal(t, "full-state-ps", restoredPS.Name)
	assert.Equal(t, "prod", restoredPS.Tags["env"])

	policies, err := fresh.ListManagedPoliciesInPermissionSet(instanceArn, psArn)
	require.NoError(t, err)
	require.Len(t, policies, 1)
	assert.Equal(t, "arn:aws:iam::aws:policy/ReadOnly", policies[0].Arn)

	inline, err := fresh.GetInlinePolicyForPermissionSet(instanceArn, psArn)
	require.NoError(t, err)
	assert.JSONEq(t, `{"Version":"2012-10-17"}`, inline)

	cmprs, err := fresh.ListCustomerManagedPolicyReferencesInPermissionSet(instanceArn, psArn)
	require.NoError(t, err)
	require.Len(t, cmprs, 1)
	assert.Equal(t, "cmp-name", cmprs[0].Name)

	boundary, err := fresh.GetPermissionsBoundaryForPermissionSet(instanceArn, psArn)
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::aws:policy/Boundary", boundary.ManagedPolicyArn)

	// Account assignment + creation status.
	assignments := fresh.ListAccountAssignments(instanceArn, psArn, "")
	require.Len(t, assignments, 1)
	assert.Equal(t, "user-1", assignments[0].PrincipalID)

	status, err := fresh.DescribeAccountAssignmentCreationStatus(instanceArn, requestID)
	require.NoError(t, err)
	assert.Equal(t, psArn, status.PermissionSetArn)

	// Application + nested state.
	restoredApp, err := fresh.DescribeApplication(appArn)
	require.NoError(t, err)
	assert.Equal(t, "full-state-app", restoredApp.Name)
	assert.Equal(t, "platform", restoredApp.Tags["team"])

	appAssignments, err := fresh.ListApplicationAssignments(appArn)
	require.NoError(t, err)
	require.Len(t, appAssignments, 1)
	assert.Equal(t, "user-2", appAssignments[0].PrincipalID)

	scopes, err := fresh.ListApplicationAccessScopes(appArn)
	require.NoError(t, err)
	require.Len(t, scopes, 1)
	assert.Equal(t, "scope:read", scopes[0].Scope)
	assert.Equal(t, []string{"arn:aws:sso::aws:applicationProvider/custom"}, scopes[0].AuthorizedTargets)

	authMethods, err := fresh.ListApplicationAuthenticationMethods(appArn)
	require.NoError(t, err)
	require.Len(t, authMethods, 1)
	assert.Equal(t, "IAM", authMethods[0].AuthMethodType)

	grants, err := fresh.ListApplicationGrants(appArn)
	require.NoError(t, err)
	require.Len(t, grants, 1)
	assert.Equal(t, "authorization_code", grants[0].GrantType)

	sessionDur, err := fresh.GetApplicationSessionConfiguration(appArn)
	require.NoError(t, err)
	assert.Equal(t, "PT4H", sessionDur)

	assignRequired, err := fresh.GetApplicationAssignmentConfiguration(appArn)
	require.NoError(t, err)
	assert.True(t, assignRequired)

	// Trusted token issuer.
	restoredTTI, err := fresh.DescribeTrustedTokenIssuer(ttiArn)
	require.NoError(t, err)
	assert.Equal(t, "full-state-tti", restoredTTI.Name)
	assert.Equal(t, "v", restoredTTI.Tags["k"])

	// Instance ABAC configuration (a "dirty" DTO-backed table).
	aca, err := fresh.DescribeInstanceAccessControlAttributeConfiguration(instanceArn)
	require.NoError(t, err)
	require.Len(t, aca.AccessControlAttributes, 1)
	assert.Equal(t, "department", aca.AccessControlAttributes[0].Key)

	// AccountID/Region are overwritten unconditionally by Restore, matching
	// pre-Phase-3.3 behaviour.
	assert.Equal(t, "123456789012", fresh.AccountID())
	assert.Equal(t, config.DefaultRegion, fresh.Region())
}

// TestInMemoryBackend_SnapshotRestore_Empty verifies an empty backend
// round-trips cleanly (only the pre-seeded default instance survives).
func TestInMemoryBackend_SnapshotRestore_Empty(t *testing.T) {
	t.Parallel()

	original := ssoadmin.NewInMemoryBackend("123456789012", config.DefaultRegion)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := ssoadmin.NewInMemoryBackend("123456789012", config.DefaultRegion)
	require.NoError(t, fresh.Restore(t.Context(), snap))

	assert.Equal(t, 1, ssoadmin.InstanceCount(fresh))
	assert.Equal(t, 0, ssoadmin.PermissionSetCount(fresh))
	assert.Equal(t, 0, ssoadmin.ApplicationCount(fresh))
	assert.Equal(t, 0, ssoadmin.TrustedTokenIssuerCount(fresh))
}

// TestInMemoryBackend_RestoreInvalidData verifies Restore rejects malformed JSON.
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := ssoadmin.NewInMemoryBackend("123456789012", config.DefaultRegion)
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreIncompatibleVersion verifies that a snapshot
// carrying an incompatible (or absent, i.e. pre-Phase-3.3) version field is
// discarded -- the backend resets to empty (re-seeded default instance)
// rather than partially decoding a foreign shape.
func TestInMemoryBackend_RestoreIncompatibleVersion(t *testing.T) {
	t.Parallel()

	original := ssoadmin.NewInMemoryBackend("123456789012", config.DefaultRegion)
	instanceArn := original.AddInstanceInternal("versioned-inst").InstanceArn
	_, err := original.CreatePermissionSet(instanceArn, "versioned-ps", "", "", "", nil)
	require.NoError(t, err)

	// Simulate a pre-Phase-3.3 snapshot (no "version" field at all, so it
	// unmarshals to Version == 0, which never matches the current version).
	legacyLikeSnapshot := []byte(`{"tables":{},"accountID":"123456789012","region":"us-east-1"}`)

	fresh := ssoadmin.NewInMemoryBackend("123456789012", config.DefaultRegion)
	require.NoError(t, fresh.Restore(t.Context(), legacyLikeSnapshot))

	// Discarded cleanly: back to just the re-seeded default instance, no error.
	assert.Equal(t, 1, ssoadmin.InstanceCount(fresh))
	assert.Equal(t, 0, ssoadmin.PermissionSetCount(fresh))
}

// TestHandler_SnapshotRestoreDelegate verifies Handler.Snapshot/Restore
// delegate to the backend.
func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	backend := ssoadmin.NewInMemoryBackend("123456789012", config.DefaultRegion)
	h := ssoadmin.NewHandler(backend)

	instanceArn := backend.AddInstanceInternal("delegate-inst").InstanceArn

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	freshBackend := ssoadmin.NewInMemoryBackend("123456789012", config.DefaultRegion)
	freshHandler := ssoadmin.NewHandler(freshBackend)
	require.NoError(t, freshHandler.Restore(t.Context(), snap))

	_, err := freshBackend.DescribeInstance(instanceArn)
	require.NoError(t, err)
}

// TestSnapshotRestorePreservesState verifies that Snapshot and Restore preserve state.
func TestSnapshotRestorePreservesState(t *testing.T) {
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
