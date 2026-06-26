package secretsmanager_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sm "github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// TestParity_PutSecretValue_PendingOnlyDoesNotForceAWSCURRENT verifies that when
// VersionStages contains only AWSPENDING (as during Lambda rotation's createSecret step),
// the new version does NOT get AWSCURRENT. Real AWS honors the caller's label list exactly.
func TestParity_PutSecretValue_PendingOnlyDoesNotForceAWSCURRENT(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	ctx := context.Background()

	_, err := b.CreateSecret(ctx, &sm.CreateSecretInput{
		Name:         "rotation-secret",
		SecretString: "initial-value",
	})
	require.NoError(t, err)

	// Lambda rotation step: createSecret — puts a new version with only AWSPENDING.
	out, err := b.PutSecretValue(ctx, &sm.PutSecretValueInput{
		SecretID:      "rotation-secret",
		SecretString:  "new-value",
		VersionStages: []string{"AWSPENDING"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"AWSPENDING"}, out.VersionStages,
		"real AWS: AWSPENDING-only PutSecretValue must not add AWSCURRENT")

	// Original AWSCURRENT version must still be intact.
	current, err := b.GetSecretValue(ctx, &sm.GetSecretValueInput{SecretID: "rotation-secret"})
	require.NoError(t, err)
	assert.Equal(t, "initial-value", current.SecretString,
		"AWSCURRENT version must not be disturbed by AWSPENDING-only PutSecretValue")
}

// TestParity_PutSecretValue_BothSecretStringAndBinaryRejected verifies that providing
// both SecretString and SecretBinary returns InvalidParameterException (400).
// Real AWS rejects this combination.
func TestParity_PutSecretValue_BothSecretStringAndBinaryRejected(t *testing.T) {
	t.Parallel()

	h := newSMHandler()

	// Create a secret first.
	rec := doSMRequest(t, h, "secretsmanager.CreateSecret",
		`{"Name":"both-vals-secret","SecretString":"v1"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doSMRequest(t, h, "secretsmanager.PutSecretValue",
		`{"SecretId":"both-vals-secret","SecretString":"str","SecretBinary":"YmluYXJ5"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"providing both SecretString and SecretBinary must return 400; body: %s", rec.Body.String())
}

// TestParity_CreateSecret_BothSecretStringAndBinaryRejected verifies that CreateSecret
// rejects requests providing both SecretString and SecretBinary.
func TestParity_CreateSecret_BothSecretStringAndBinaryRejected(t *testing.T) {
	t.Parallel()

	h := newSMHandler()

	rec := doSMRequest(t, h, "secretsmanager.CreateSecret",
		`{"Name":"both-at-create","SecretString":"str","SecretBinary":"YmluYXJ5"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"CreateSecret with both SecretString and SecretBinary must return 400; body: %s", rec.Body.String())
}

// TestParity_CancelRotateSecret_RotationConfigPreserved verifies that CancelRotateSecret
// does not disable rotation. Real AWS only removes the AWSPENDING label; the Lambda ARN
// and rotation rules remain, and RotationEnabled stays true.
func TestParity_CancelRotateSecret_RotationConfigPreserved(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	ctx := context.Background()

	_, err := b.CreateSecret(ctx, &sm.CreateSecretInput{
		Name:         "cancel-rot-config",
		SecretString: "v1",
	})
	require.NoError(t, err)

	_, err = b.RotateSecret(ctx, &sm.RotateSecretInput{SecretID: "cancel-rot-config"})
	require.NoError(t, err)

	_, err = b.CancelRotateSecret(ctx, &sm.CancelRotateSecretInput{SecretID: "cancel-rot-config"})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(ctx, &sm.DescribeSecretInput{SecretID: "cancel-rot-config"})
	require.NoError(t, err)
	assert.True(t, desc.RotationEnabled,
		"real AWS: CancelRotateSecret must not disable RotationEnabled")
}

// TestParity_TagResource_UpdateExistingKeyDoesNotCountAsNew verifies that updating an
// existing tag key does not count against the 50-tag limit. Real AWS counts net new keys.
func TestParity_TagResource_UpdateExistingKeyDoesNotCountAsNew(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	ctx := context.Background()

	// Create a secret with 48 tags.
	tags := make([]sm.Tag, 48)
	for i := range 48 {
		tags[i] = sm.Tag{Key: fmt.Sprintf("key-%02d", i), Value: "val"}
	}

	_, err := b.CreateSecret(ctx, &sm.CreateSecretInput{
		Name:         "near-limit-secret",
		SecretString: "v",
		Tags:         tags,
	})
	require.NoError(t, err)

	// Add 2 new tags (reaches 50 — allowed).
	err = b.TagResource(ctx, &sm.TagResourceInput{
		SecretID: "near-limit-secret",
		Tags:     []sm.Tag{{Key: "key-48", Value: "v"}, {Key: "key-49", Value: "v"}},
	})
	require.NoError(t, err)

	// Now update 3 existing tags — no net new keys, must succeed even though at limit.
	err = b.TagResource(ctx, &sm.TagResourceInput{
		SecretID: "near-limit-secret",
		Tags: []sm.Tag{
			{Key: "key-00", Value: "updated"},
			{Key: "key-01", Value: "updated"},
			{Key: "key-02", Value: "updated"},
		},
	})
	assert.NoError(t, err,
		"real AWS: updating existing tag keys must not count against the 50-tag limit")
}

// TestParity_ReplicateSecretToRegions_ExistingRegionRejectedWithoutForce verifies that
// replicating to a region that already has a replica returns ResourceExistsException
// when ForceOverwriteReplicaSecret is false. Real AWS behavior.
func TestParity_ReplicateSecretToRegions_ExistingRegionRejectedWithoutForce(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	ctx := context.Background()

	_, err := b.CreateSecret(ctx, &sm.CreateSecretInput{
		Name:         "replicated-secret",
		SecretString: "v",
	})
	require.NoError(t, err)

	// First replication succeeds.
	_, err = b.ReplicateSecretToRegions(ctx, &sm.ReplicateSecretToRegionsInput{
		SecretID:          "replicated-secret",
		AddReplicaRegions: []sm.ReplicaRegion{{Region: "us-east-2"}},
	})
	require.NoError(t, err)

	// Second replication to same region without force must fail.
	_, err = b.ReplicateSecretToRegions(ctx, &sm.ReplicateSecretToRegionsInput{
		SecretID:          "replicated-secret",
		AddReplicaRegions: []sm.ReplicaRegion{{Region: "us-east-2"}},
	})
	assert.ErrorIs(t, err, sm.ErrSecretAlreadyExists,
		"real AWS: replicating to existing region without ForceOverwriteReplicaSecret"+
			" must return ResourceExistsException")
}

// TestParity_ReplicateSecretToRegions_ForceOverwriteAllowed verifies that
// ForceOverwriteReplicaSecret=true allows overwriting an existing replica.
func TestParity_ReplicateSecretToRegions_ForceOverwriteAllowed(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	ctx := context.Background()

	_, err := b.CreateSecret(ctx, &sm.CreateSecretInput{
		Name:         "force-overwrite-secret",
		SecretString: "v",
	})
	require.NoError(t, err)

	_, err = b.ReplicateSecretToRegions(ctx, &sm.ReplicateSecretToRegionsInput{
		SecretID:          "force-overwrite-secret",
		AddReplicaRegions: []sm.ReplicaRegion{{Region: "eu-west-1"}},
	})
	require.NoError(t, err)

	// Force overwrite succeeds.
	out, err := b.ReplicateSecretToRegions(ctx, &sm.ReplicateSecretToRegionsInput{
		SecretID:                    "force-overwrite-secret",
		AddReplicaRegions:           []sm.ReplicaRegion{{Region: "eu-west-1", KmsKeyID: "new-key"}},
		ForceOverwriteReplicaSecret: true,
	})
	require.NoError(t, err)
	require.Len(t, out.ReplicationStatus, 1)
	assert.Equal(t, "new-key", out.ReplicationStatus[0].KmsKeyID,
		"ForceOverwriteReplicaSecret=true must update the existing replica")
}

// TestParity_GetSecretValue_LastAccessedDateReturned verifies that GetSecretValue
// returns LastAccessedDate in its response. Real AWS includes this field.
func TestParity_GetSecretValue_LastAccessedDateReturned(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	ctx := context.Background()

	_, err := b.CreateSecret(ctx, &sm.CreateSecretInput{
		Name:         "access-date-secret",
		SecretString: "v",
	})
	require.NoError(t, err)

	out, err := b.GetSecretValue(ctx, &sm.GetSecretValueInput{SecretID: "access-date-secret"})
	require.NoError(t, err)
	require.NotNil(t, out.LastAccessedDate,
		"real AWS: GetSecretValue must return LastAccessedDate after first access")
	assert.Greater(t, *out.LastAccessedDate, float64(0))
}

// TestParity_BatchGetSecretValue_SecretIdListAndFiltersRejected verifies that providing
// both SecretIdList and Filters returns InvalidParameterException. Real AWS behavior.
func TestParity_BatchGetSecretValue_SecretIdListAndFiltersRejected(t *testing.T) {
	t.Parallel()

	h := newSMHandler()

	rec := doSMRequest(t, h, "secretsmanager.BatchGetSecretValue",
		`{"SecretIdList":["s1"],"Filters":[{"Key":"name","Values":["s"]}]}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"BatchGetSecretValue with both SecretIdList and Filters must return 400; body: %s", rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "InvalidParameterException", resp["__type"])
}

// TestParity_BatchGetSecretValue_FilterUsesPrefix verifies that BatchGetSecretValue
// filter matching uses prefix (not exact) matching, consistent with ListSecrets.
func TestParity_BatchGetSecretValue_FilterUsesPrefix(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	h := sm.NewHandler(b)
	ctx := context.Background()

	_, err := b.CreateSecret(ctx, &sm.CreateSecretInput{Name: "prefix-match-abc", SecretString: "v"})
	require.NoError(t, err)

	_, err = b.CreateSecret(ctx, &sm.CreateSecretInput{Name: "prefix-match-xyz", SecretString: "v"})
	require.NoError(t, err)

	// Filter by prefix "prefix-match-" — should return both.
	rec := doSMRequest(t, h, "secretsmanager.BatchGetSecretValue",
		`{"Filters":[{"Key":"name","Values":["prefix-match-"]}]}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		SecretValues []map[string]any `json:"SecretValues"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Len(t, out.SecretValues, 2,
		"real AWS: BatchGetSecretValue filter uses prefix matching; body: %s", rec.Body.String())
}
