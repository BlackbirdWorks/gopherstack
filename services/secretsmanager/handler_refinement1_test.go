package secretsmanager_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// doR1Request is a helper to invoke an SM handler via an X-Amz-Target action.
func doR1Request(t *testing.T, h *secretsmanager.Handler, action, body string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", action)

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))

	return rec
}

// TestRefinement1_HandlerOpsLen verifies all 21 ops are registered in the dispatch table.
func TestRefinement1_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	assert.Equal(t, len(h.GetSupportedOperations()), secretsmanager.HandlerOpsLen(h))
}

// TestRefinement1_AccountIDAndRegion verifies AccountID() and Region() return configured values.
func TestRefinement1_AccountIDAndRegion(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackendWithConfig("123456789012", "eu-west-1")
	assert.Equal(t, "123456789012", b.AccountID())
	assert.Equal(t, "eu-west-1", b.Region())
}

// TestRefinement1_ErrNilAppContext verifies provider.Init(nil) returns ErrNilAppContext.
func TestRefinement1_ErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &secretsmanager.Provider{}
	_, err := p.Init(nil)

	require.ErrorIs(t, err, secretsmanager.ErrNilAppContext)
}

// TestRefinement1_SecretCount verifies SecretCount tracks secrets correctly.
func TestRefinement1_SecretCount(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	require.Equal(t, 0, secretsmanager.SecretCount(b))

	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{Name: "a", SecretString: "v"})
	require.NoError(t, err)
	assert.Equal(t, 1, secretsmanager.SecretCount(b))

	_, err = b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{Name: "b", SecretString: "v"})
	require.NoError(t, err)
	assert.Equal(t, 2, secretsmanager.SecretCount(b))
}

// TestRefinement1_ResourcePolicyCount verifies ResourcePolicyCount tracks policies.
func TestRefinement1_ResourcePolicyCount(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "pol-secret", SecretString: "v"},
	)
	require.NoError(t, err)
	require.Equal(t, 0, secretsmanager.ResourcePolicyCount(b))

	_, err = b.PutResourcePolicy(context.Background(), &secretsmanager.PutResourcePolicyInput{
		SecretID:       "pol-secret",
		ResourcePolicy: `{"Version":"2012-10-17"}`,
	})
	require.NoError(t, err)
	assert.Equal(t, 1, secretsmanager.ResourcePolicyCount(b))

	_, err = b.DeleteResourcePolicy(
		context.Background(),
		&secretsmanager.DeleteResourcePolicyInput{SecretID: "pol-secret"},
	)
	require.NoError(t, err)
	assert.Equal(t, 0, secretsmanager.ResourcePolicyCount(b))
}

// TestRefinement1_ReplicationConfigCount verifies ReplicationConfigCount tracks replicas.
func TestRefinement1_ReplicationConfigCount(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "rep-cnt", SecretString: "v"},
	)
	require.NoError(t, err)
	require.Equal(t, 0, secretsmanager.ReplicationConfigCount(b))

	_, err = b.ReplicateSecretToRegions(context.Background(), &secretsmanager.ReplicateSecretToRegionsInput{
		SecretID:          "rep-cnt",
		AddReplicaRegions: []secretsmanager.ReplicaRegion{{Region: "us-west-2"}},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, secretsmanager.ReplicationConfigCount(b))

	_, err = b.StopReplicationToReplica(
		context.Background(),
		&secretsmanager.StopReplicationToReplicaInput{SecretID: "rep-cnt"},
	)
	require.NoError(t, err)
	assert.Equal(t, 0, secretsmanager.ReplicationConfigCount(b))
}

// TestRefinement1_AddSecretInternal verifies AddSecretInternal seeds correctly.
func TestRefinement1_AddSecretInternal(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	b.AddSecretInternal(&secretsmanager.Secret{
		ARN:  "arn:aws:secretsmanager:us-east-1:123456789012:secret:my-seed-AABBCC",
		Name: "my-seed",
	})
	assert.Equal(t, 1, secretsmanager.SecretCount(b))

	got, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "my-seed"})
	require.NoError(t, err)
	assert.Equal(t, "my-seed", got.Name)
}

// TestRefinement1_UpdateSecretVersionStageAutoStrip verifies a staging label is stripped from all
// other versions when moved, not just RemoveFromVersionID.
func TestRefinement1_UpdateSecretVersionStageAutoStrip(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "vs-strip", SecretString: "v1"},
	)
	require.NoError(t, err)

	desc1, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "vs-strip"})
	require.NoError(t, err)
	v1ID := desc1.VersionIDsToStages
	var v1 string
	for id := range v1ID {
		v1 = id
	}

	// Create second version.
	put, err := b.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
		SecretID:     "vs-strip",
		SecretString: "v2",
	})
	require.NoError(t, err)
	v2 := put.VersionID

	// Move AWSPREVIOUS label to v2 (stripping from v1 where it may be).
	_, err = b.UpdateSecretVersionStage(context.Background(), &secretsmanager.UpdateSecretVersionStageInput{
		SecretID:        "vs-strip",
		VersionStage:    secretsmanager.StagingLabelPrevious,
		MoveToVersionID: v2,
	})
	require.NoError(t, err)

	// Verify v1 no longer has AWSPREVIOUS.
	desc2, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "vs-strip"})
	require.NoError(t, err)

	for _, lbl := range desc2.VersionIDsToStages[v1] {
		assert.NotEqual(t, secretsmanager.StagingLabelPrevious, lbl,
			"v1 should not have AWSPREVIOUS after moving to v2")
	}

	assert.Contains(t, desc2.VersionIDsToStages[v2], secretsmanager.StagingLabelPrevious)
}

// TestRefinement1_UpdateSecretVersionStageAWSCURRENT verifies moving AWSCURRENT updates CurrentVersionID.
func TestRefinement1_UpdateSecretVersionStageAWSCURRENT(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "vs-curr", SecretString: "v1"},
	)
	require.NoError(t, err)

	put, err := b.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
		SecretID:     "vs-curr",
		SecretString: "v2",
	})
	require.NoError(t, err)
	v2 := put.VersionID

	// Move AWSCURRENT back to the original v1.
	desc1, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "vs-curr"})
	require.NoError(t, err)
	var v1 string
	for id, labels := range desc1.VersionIDsToStages {
		for _, l := range labels {
			if l == secretsmanager.StagingLabelPrevious {
				v1 = id
			}
		}
	}
	require.NotEmpty(t, v1)

	_, err = b.UpdateSecretVersionStage(context.Background(), &secretsmanager.UpdateSecretVersionStageInput{
		SecretID:            "vs-curr",
		VersionStage:        secretsmanager.StagingLabelCurrent,
		MoveToVersionID:     v1,
		RemoveFromVersionID: v2,
	})
	require.NoError(t, err)

	// v1 should now be AWSCURRENT.
	got, err := b.GetSecretValue(context.Background(), &secretsmanager.GetSecretValueInput{
		SecretID:     "vs-curr",
		VersionStage: secretsmanager.StagingLabelCurrent,
	})
	require.NoError(t, err)
	assert.Equal(t, v1, got.VersionID)
}

// TestRefinement1_DeleteSecretCascade verifies force-delete clears resource policy + replication.
func TestRefinement1_DeleteSecretCascade(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "cascade", SecretString: "v"},
	)
	require.NoError(t, err)

	_, err = b.PutResourcePolicy(context.Background(), &secretsmanager.PutResourcePolicyInput{
		SecretID:       "cascade",
		ResourcePolicy: `{"Version":"2012-10-17"}`,
	})
	require.NoError(t, err)

	_, err = b.ReplicateSecretToRegions(context.Background(), &secretsmanager.ReplicateSecretToRegionsInput{
		SecretID:          "cascade",
		AddReplicaRegions: []secretsmanager.ReplicaRegion{{Region: "us-west-2"}},
	})
	require.NoError(t, err)

	require.Equal(t, 1, secretsmanager.ResourcePolicyCount(b))
	require.Equal(t, 1, secretsmanager.ReplicationConfigCount(b))

	_, err = b.DeleteSecret(context.Background(), &secretsmanager.DeleteSecretInput{
		SecretID:                   "cascade",
		ForceDeleteWithoutRecovery: true,
	})
	require.NoError(t, err)

	assert.Equal(t, 0, secretsmanager.SecretCount(b))
	assert.Equal(t, 0, secretsmanager.ResourcePolicyCount(b))
	assert.Equal(t, 0, secretsmanager.ReplicationConfigCount(b))
}

// TestRefinement1_PutResourcePolicyEmptyRejects verifies empty ResourcePolicy returns 400.
func TestRefinement1_PutResourcePolicyEmptyRejects(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "ep-secret", SecretString: "v"},
	)
	require.NoError(t, err)

	_, err = b.PutResourcePolicy(context.Background(), &secretsmanager.PutResourcePolicyInput{
		SecretID:       "ep-secret",
		ResourcePolicy: "",
	})
	require.ErrorIs(t, err, secretsmanager.ErrInvalidParameter)
}

// TestRefinement1_PutResourcePolicyEmptyHTTP verifies empty policy returns HTTP 400.
func TestRefinement1_PutResourcePolicyEmptyHTTP(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "ep-http", SecretString: "v"},
	)
	require.NoError(t, err)

	h := secretsmanager.NewHandler(b)
	rec := doR1Request(t, h, "secretsmanager.PutResourcePolicy",
		`{"SecretId":"ep-http","ResourcePolicy":""}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_KmsKeyIdRoundTrip verifies KmsKeyId is stored and returned in DescribeSecret.
func TestRefinement1_KmsKeyIdRoundTrip(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "kms-test",
		SecretString: "v",
		KmsKeyID:     "alias/my-key",
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "kms-test"})
	require.NoError(t, err)
	assert.Equal(t, "alias/my-key", desc.KmsKeyID)
}

// TestRefinement1_RotationLambdaARNStored verifies RotationLambdaARN is stored and returned.
func TestRefinement1_RotationLambdaARNStored(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "rla-test", SecretString: "v"},
	)
	require.NoError(t, err)

	_, err = b.RotateSecret(context.Background(), &secretsmanager.RotateSecretInput{
		SecretID:          "rla-test",
		RotationLambdaARN: "arn:aws:lambda:us-east-1:123:function:my-rotator",
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "rla-test"})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:lambda:us-east-1:123:function:my-rotator", desc.RotationLambdaARN)
}

// TestRefinement1_LastRotatedDate verifies LastRotatedDate is set after rotation.
func TestRefinement1_LastRotatedDate(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "lrd-test", SecretString: "v"},
	)
	require.NoError(t, err)

	desc0, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "lrd-test"})
	require.NoError(t, err)
	assert.Nil(t, desc0.LastRotatedDate)

	_, err = b.RotateSecret(context.Background(), &secretsmanager.RotateSecretInput{SecretID: "lrd-test"})
	require.NoError(t, err)

	desc1, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "lrd-test"})
	require.NoError(t, err)
	require.NotNil(t, desc1.LastRotatedDate)
	assert.Greater(t, *desc1.LastRotatedDate, float64(0))
}

// TestRefinement1_DescribeSecretReplicationStatus verifies ReplicationStatus in DescribeSecret.
func TestRefinement1_DescribeSecretReplicationStatus(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "rep-desc", SecretString: "v"},
	)
	require.NoError(t, err)

	_, err = b.ReplicateSecretToRegions(context.Background(), &secretsmanager.ReplicateSecretToRegionsInput{
		SecretID:          "rep-desc",
		AddReplicaRegions: []secretsmanager.ReplicaRegion{{Region: "ap-northeast-1"}},
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "rep-desc"})
	require.NoError(t, err)
	require.Len(t, desc.ReplicationStatus, 1)
	assert.Equal(t, "ap-northeast-1", desc.ReplicationStatus[0].Region)
}

// TestRefinement1_ListSecretsFilterByName verifies ListSecrets with a name filter.
func TestRefinement1_ListSecretsFilterByName(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()

	for _, name := range []string{"alpha-1", "alpha-2", "beta-1"} {
		_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{Name: name, SecretString: "v"})
		require.NoError(t, err)
	}

	out, err := b.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{
		Filters: []secretsmanager.SecretFilter{{Key: "name", Values: []string{"alpha"}}},
	})
	require.NoError(t, err)
	require.Len(t, out.SecretList, 2)
	assert.Equal(t, "alpha-1", out.SecretList[0].Name)
	assert.Equal(t, "alpha-2", out.SecretList[1].Name)
}

// TestRefinement1_ListSecretsFilterByDescription verifies ListSecrets with description filter.
func TestRefinement1_ListSecretsFilterByDescription(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "desc-a",
		SecretString: "v",
		Description:  "production secret",
	})
	require.NoError(t, err)
	_, err = b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "desc-b",
		SecretString: "v",
		Description:  "staging secret",
	})
	require.NoError(t, err)

	out, err := b.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{
		Filters: []secretsmanager.SecretFilter{{Key: "description", Values: []string{"prod"}}},
	})
	require.NoError(t, err)
	require.Len(t, out.SecretList, 1)
	assert.Equal(t, "desc-a", out.SecretList[0].Name)
}

// TestRefinement1_ListSecretsFilterByTagKey verifies ListSecrets with tag-key filter.
func TestRefinement1_ListSecretsFilterByTagKey(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "tagged",
		SecretString: "v",
		Tags:         []secretsmanager.Tag{{Key: "env", Value: "prod"}},
	})
	require.NoError(t, err)
	_, err = b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "untagged",
		SecretString: "v",
	})
	require.NoError(t, err)

	out, err := b.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{
		Filters: []secretsmanager.SecretFilter{{Key: "tag-key", Values: []string{"env"}}},
	})
	require.NoError(t, err)
	require.Len(t, out.SecretList, 1)
	assert.Equal(t, "tagged", out.SecretList[0].Name)
}

// TestRefinement1_ListSecretsFilterByTagValue verifies ListSecrets with tag-value filter.
func TestRefinement1_ListSecretsFilterByTagValue(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "tv-a",
		SecretString: "v",
		Tags:         []secretsmanager.Tag{{Key: "env", Value: "prod"}},
	})
	require.NoError(t, err)
	_, err = b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "tv-b",
		SecretString: "v",
		Tags:         []secretsmanager.Tag{{Key: "env", Value: "dev"}},
	})
	require.NoError(t, err)

	out, err := b.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{
		Filters: []secretsmanager.SecretFilter{{Key: "tag-value", Values: []string{"prod"}}},
	})
	require.NoError(t, err)
	require.Len(t, out.SecretList, 1)
	assert.Equal(t, "tv-a", out.SecretList[0].Name)
}

// TestRefinement1_BatchGetFilterTagKey verifies BatchGetSecretValue with a tag-key filter.
func TestRefinement1_BatchGetFilterTagKey(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "batch-tag",
		SecretString: "v",
		Tags:         []secretsmanager.Tag{{Key: "class", Value: "database"}},
	})
	require.NoError(t, err)
	_, err = b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "batch-notag", SecretString: "v"},
	)
	require.NoError(t, err)

	out, err := b.BatchGetSecretValue(context.Background(), &secretsmanager.BatchGetSecretValueInput{
		Filters: []secretsmanager.BatchGetSecretValueFilter{{Key: "tag-key", Values: []string{"class"}}},
	})
	require.NoError(t, err)
	require.Len(t, out.SecretValues, 1)
	assert.Equal(t, "batch-tag", out.SecretValues[0].Name)
}

// TestRefinement1_BatchGetPagination verifies NextToken pagination in filter mode.
func TestRefinement1_BatchGetPagination(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()

	for i := range 5 {
		_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
			Name:         fmt.Sprintf("pag-%02d", i),
			SecretString: "v",
		})
		require.NoError(t, err)
	}

	maxResults := int32(2)
	out1, err := b.BatchGetSecretValue(context.Background(), &secretsmanager.BatchGetSecretValueInput{
		MaxResults: &maxResults,
	})
	require.NoError(t, err)
	require.Len(t, out1.SecretValues, 2)
	assert.NotEmpty(t, out1.NextToken)

	out2, err := b.BatchGetSecretValue(context.Background(), &secretsmanager.BatchGetSecretValueInput{
		MaxResults: &maxResults,
		NextToken:  out1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, out2.SecretValues, 2)
	assert.NotEmpty(t, out2.NextToken)

	out3, err := b.BatchGetSecretValue(context.Background(), &secretsmanager.BatchGetSecretValueInput{
		MaxResults: &maxResults,
		NextToken:  out2.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, out3.SecretValues, 1)
	assert.Empty(t, out3.NextToken)
}

// TestRefinement1_ListSecretVersionsDeleted verifies ListSecretVersionIds works on deleted secrets.
func TestRefinement1_ListSecretVersionsDeleted(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "del-ver", SecretString: "v"},
	)
	require.NoError(t, err)

	_, err = b.DeleteSecret(context.Background(), &secretsmanager.DeleteSecretInput{SecretID: "del-ver"})
	require.NoError(t, err)

	out, err := b.ListSecretVersionIDs(
		context.Background(),
		&secretsmanager.ListSecretVersionIDsInput{SecretID: "del-ver"},
	)
	require.NoError(t, err)
	assert.Len(t, out.Versions, 1)
}

// TestRefinement1_CreateSecretClientRequestToken verifies ClientRequestToken is used as version ID.
func TestRefinement1_CreateSecretClientRequestToken(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	token := "11111111-2222-3333-4444-555555555555"

	out, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:               "crt-test",
		SecretString:       "v",
		ClientRequestToken: token,
	})
	require.NoError(t, err)
	assert.Equal(t, token, out.VersionID)
}

// TestRefinement1_GenerateVersionID verifies generated version IDs are UUID-formatted.
func TestRefinement1_GenerateVersionID(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "uuid-ver", SecretString: "v"},
	)
	require.NoError(t, err)

	_, err = b.RotateSecret(context.Background(), &secretsmanager.RotateSecretInput{SecretID: "uuid-ver"})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "uuid-ver"})
	require.NoError(t, err)

	uuidRE := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	for id := range desc.VersionIDsToStages {
		assert.True(t, uuidRE.MatchString(id), "version ID %q is not a valid v4 UUID", id)
	}
}

// TestRefinement1_Snapshot_Restore verifies KmsKeyId, RotationLambdaARN, and LastRotatedDate survive round-trip.
func TestRefinement1_Snapshot_Restore(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "snap-test",
		SecretString: "v",
		KmsKeyID:     "alias/snap-key",
	})
	require.NoError(t, err)

	_, err = b.RotateSecret(context.Background(), &secretsmanager.RotateSecretInput{
		SecretID:          "snap-test",
		RotationLambdaARN: "arn:aws:lambda:us-east-1:123:function:rotator",
	})
	require.NoError(t, err)

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := secretsmanager.NewInMemoryBackend()
	require.NoError(t, b2.Restore(snap))

	desc, err := b2.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "snap-test"})
	require.NoError(t, err)
	assert.Equal(t, "alias/snap-key", desc.KmsKeyID)
	assert.Equal(t, "arn:aws:lambda:us-east-1:123:function:rotator", desc.RotationLambdaARN)
	assert.NotNil(t, desc.LastRotatedDate)
}

// TestRefinement1_ResetCleansAllMaps verifies Reset clears secrets + policies + replications.
func TestRefinement1_ResetCleansAllMaps(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "reset-s", SecretString: "v"},
	)
	require.NoError(t, err)
	_, err = b.PutResourcePolicy(context.Background(), &secretsmanager.PutResourcePolicyInput{
		SecretID:       "reset-s",
		ResourcePolicy: `{}`,
	})
	require.NoError(t, err)
	_, err = b.ReplicateSecretToRegions(context.Background(), &secretsmanager.ReplicateSecretToRegionsInput{
		SecretID:          "reset-s",
		AddReplicaRegions: []secretsmanager.ReplicaRegion{{Region: "us-west-2"}},
	})
	require.NoError(t, err)

	b.Reset()

	assert.Equal(t, 0, secretsmanager.SecretCount(b))
	assert.Equal(t, 0, secretsmanager.ResourcePolicyCount(b))
	assert.Equal(t, 0, secretsmanager.ReplicationConfigCount(b))
}

// TestRefinement1_CancelRotateSecretNoRotation verifies CancelRotateSecret without rotation
// removes AWSPENDING gracefully.
func TestRefinement1_CancelRotateSecretNoRotation(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{Name: "norot", SecretString: "v"})
	require.NoError(t, err)

	// CancelRotate on a non-rotating secret should succeed (idempotent).
	out, err := b.CancelRotateSecret(context.Background(), &secretsmanager.CancelRotateSecretInput{SecretID: "norot"})
	require.NoError(t, err)
	assert.Equal(t, "norot", out.Name)
}

// TestRefinement1_RestoreEnsuresNonNilMaps verifies ensureNonNilMaps protects Restore from nil maps.
func TestRefinement1_RestoreEnsuresNonNilMaps(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	// Restore with minimal valid JSON that has no map keys.
	err := b.Restore([]byte(`{"accountID":"acct","region":"us-east-1"}`))
	require.NoError(t, err)
	// Should be able to create secrets without panics.
	_, err = b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "post-restore", SecretString: "v"},
	)
	require.NoError(t, err)
}

// TestRefinement1_ListSecretsNoFilter verifies ListSecrets returns all when no filters.
func TestRefinement1_ListSecretsNoFilter(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()

	for _, name := range []string{"x", "y", "z"} {
		_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{Name: name, SecretString: "v"})
		require.NoError(t, err)
	}

	out, err := b.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{})
	require.NoError(t, err)
	assert.Len(t, out.SecretList, 3)
}

// TestRefinement1_DescribeSecretHTTP verifies DescribeSecret via HTTP returns KmsKeyId.
func TestRefinement1_DescribeSecretHTTP(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "http-kms",
		SecretString: "v",
		KmsKeyID:     "alias/http-key",
	})
	require.NoError(t, err)

	h := secretsmanager.NewHandler(b)
	rec := doR1Request(t, h, "secretsmanager.DescribeSecret", `{"SecretId":"http-kms"}`)

	require.Equal(t, http.StatusOK, rec.Code)

	var out secretsmanager.DescribeSecretOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "alias/http-key", out.KmsKeyID)
}

// TestRefinement1_ListSecretsHTTPFilter verifies ListSecrets filter via HTTP.
func TestRefinement1_ListSecretsHTTPFilter(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()

	for _, name := range []string{"http-flt-a", "http-flt-b", "other"} {
		_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{Name: name, SecretString: "v"})
		require.NoError(t, err)
	}

	h := secretsmanager.NewHandler(b)
	rec := doR1Request(t, h, "secretsmanager.ListSecrets",
		`{"Filters":[{"Key":"name","Values":["http-flt"]}]}`)

	require.Equal(t, http.StatusOK, rec.Code)

	var out secretsmanager.ListSecretsOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Len(t, out.SecretList, 2)
}
