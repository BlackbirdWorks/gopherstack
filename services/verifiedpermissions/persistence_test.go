package verifiedpermissions_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/verifiedpermissions"
)

// TestInMemoryBackend_RestoreInvalidData verifies that malformed JSON is
// reported as an error rather than silently discarded or partially applied.
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := verifiedpermissions.NewInMemoryBackend("123456789012", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := verifiedpermissions.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.CreatePolicyStore("seed", nil, verifiedpermissions.ValidationModeOff, "")
	require.NoError(t, err)

	// A syntactically valid but version-mismatched snapshot.
	err = b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Equal(t, 0, verifiedpermissions.PolicyStoreCount(b))
}

// TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero verifies that a
// snapshot with no version field at all decodes with Version == 0, which
// mismatches the current snapshot version and is discarded the same way any
// other incompatible version is -- not partially applied. This also covers
// the pre-Phase-3.3 on-disk shape (policyStoreID-keyed maps with no
// "version"/"tables" keys).
func TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero(t *testing.T) {
	t.Parallel()

	b := verifiedpermissions.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.CreatePolicyStore("seed", nil, verifiedpermissions.ValidationModeOff, "")
	require.NoError(t, err)

	// The pre-refactor shape: flat/nested maps keyed by resource type, no
	// version/tables keys.
	oldShape := `{"policyStores":{"ps-1":{"policyStoreID":"ps-1"}},"region":"us-east-1"}`
	err = b.Restore(t.Context(), []byte(oldShape))
	require.NoError(t, err)

	assert.Equal(t, 0, verifiedpermissions.PolicyStoreCount(b))
}

// TestInMemoryBackend_SnapshotRestore_FullState exercises a Snapshot->Restore
// round trip across every store.Table-backed resource family the Phase 3.3
// conversion touched (policyStores, policies, policyTemplates,
// identitySources -- all "clean" tables -- plus the "dirty" schemas table,
// which carries a hidden policyStoreID field through a DTO) and the plain
// resourceTags map left unconverted.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := verifiedpermissions.NewInMemoryBackend("111122223333", "us-west-2")
	ctx := t.Context()

	ps, err := original.CreatePolicyStore(
		"a store", map[string]string{"env": "test"},
		verifiedpermissions.ValidationModeOff, verifiedpermissions.DeletionProtectionDisabled,
	)
	require.NoError(t, err)

	pol, err := original.CreatePolicy(ps.PolicyStoreID, verifiedpermissions.CreatePolicyParams{
		PolicyType: "STATIC",
		Statement:  `permit(principal, action, resource);`,
	})
	require.NoError(t, err)

	tmpl, err := original.CreatePolicyTemplate(
		ps.PolicyStoreID, "a template", `permit(principal == ?principal, action, resource);`,
	)
	require.NoError(t, err)

	src, err := original.CreateIdentitySource(ps.PolicyStoreID, "User", verifiedpermissions.IdentitySourceConfig{
		UserPoolArn: "arn:aws:cognito-idp:us-west-2:111122223333:userpool/us-west-2_abc123",
		ClientIDs:   []string{"client-1"},
	})
	require.NoError(t, err)

	namespaces, err := original.PutSchema(ps.PolicyStoreID, `{"MyNamespace":{}}`)
	require.NoError(t, err)
	require.Equal(t, []string{"MyNamespace"}, namespaces)

	snap := original.Snapshot(ctx)
	require.NotNil(t, snap)

	fresh := verifiedpermissions.NewInMemoryBackend("999999999999", "us-east-1")
	require.NoError(t, fresh.Restore(ctx, snap))

	// Scalars: accountID/region surface indirectly through a freshly created
	// resource (there is no direct accessor).
	newPS, err := fresh.CreatePolicyStore("new store", nil, verifiedpermissions.ValidationModeOff, "")
	require.NoError(t, err)
	assert.Contains(t, newPS.Arn, "111122223333")

	// policyStores table ("clean") + Tags.
	gotPS, err := fresh.GetPolicyStore(ps.PolicyStoreID)
	require.NoError(t, err)
	assert.Equal(t, ps.Arn, gotPS.Arn)
	assert.Equal(t, "a store", gotPS.Description)
	tagVals, err := fresh.ListTagsForResource(ps.Arn)
	require.NoError(t, err)
	assert.Equal(t, "test", tagVals["env"])

	// policies table ("clean").
	gotPol, err := fresh.GetPolicy(ps.PolicyStoreID, pol.PolicyID)
	require.NoError(t, err)
	assert.Equal(t, pol.Statement, gotPol.Statement)

	// policyTemplates table ("clean").
	gotTmpl, err := fresh.GetPolicyTemplate(ps.PolicyStoreID, tmpl.PolicyTemplateID)
	require.NoError(t, err)
	assert.Equal(t, tmpl.Statement, gotTmpl.Statement)

	// identitySources table ("clean").
	gotSrc, err := fresh.GetIdentitySource(ps.PolicyStoreID, src.IdentitySourceID)
	require.NoError(t, err)
	assert.Equal(t, src.UserPoolArn, gotSrc.UserPoolArn)
	assert.Equal(t, []string{"client-1"}, gotSrc.ClientIDs)

	// schemas table ("dirty": hidden policyStoreID field via a DTO).
	gotSchema, err := fresh.GetSchema(ps.PolicyStoreID)
	require.NoError(t, err)
	assert.JSONEq(t, `{"MyNamespace":{}}`, gotSchema.Schema)
	assert.Equal(t, []string{"MyNamespace"}, gotSchema.Namespaces)

	// DeletePolicyStore cascade still works post-restore -- proves the
	// byPolicyStore indexes and the ARN index rebuilt from the restored
	// values (not just the primary lookups).
	require.NoError(t, fresh.DeletePolicyStore(ps.PolicyStoreID))
	_, err = fresh.GetPolicy(ps.PolicyStoreID, pol.PolicyID)
	require.Error(t, err)
	_, err = fresh.GetPolicyTemplate(ps.PolicyStoreID, tmpl.PolicyTemplateID)
	require.Error(t, err)
	_, err = fresh.GetIdentitySource(ps.PolicyStoreID, src.IdentitySourceID)
	require.Error(t, err)
	_, err = fresh.GetPolicyStore(ps.PolicyStoreID)
	require.Error(t, err)
}

// TestInMemoryBackend_SnapshotRestore_ResourceTags verifies that the plain
// (unconverted) resourceTags map -- ARN -> tags for every resource type --
// round-trips through Snapshot/Restore.
func TestInMemoryBackend_SnapshotRestore_ResourceTags(t *testing.T) {
	t.Parallel()

	original := verifiedpermissions.NewInMemoryBackend("123456789012", "us-east-1")
	ctx := t.Context()

	ps, err := original.CreatePolicyStore("a store", nil, verifiedpermissions.ValidationModeOff, "")
	require.NoError(t, err)

	require.NoError(t, original.TagResource(ps.Arn, map[string]string{"k1": "v1"}))

	snap := original.Snapshot(ctx)
	require.NotNil(t, snap)

	fresh := verifiedpermissions.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, fresh.Restore(ctx, snap))

	tagVals, err := fresh.ListTagsForResource(ps.Arn)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"k1": "v1"}, tagVals)
}
