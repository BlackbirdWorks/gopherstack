package secretsmanager_test

// describesecret_test.go consolidates every DescribeSecret-specific test that was
// previously scattered across several older test files. Ported verbatim
// (assertions unchanged).

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// ---------------------------------------------------------------------------
// DescribeSecret comprehensive
// ---------------------------------------------------------------------------

func TestDescribeSecret_AllMetadataFields(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackendWithConfig("123456789012", "us-west-2")
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "full-desc",
		Description:  "my description",
		SecretString: "v",
		KmsKeyID:     "alias/my-key",
		Tags:         []secretsmanager.Tag{{Key: "env", Value: "prod"}},
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "full-desc"})
	require.NoError(t, err)
	assert.Equal(t, "full-desc", desc.Name)
	assert.Equal(t, "my description", desc.Description)
	assert.Equal(t, "alias/my-key", desc.KmsKeyID)
	assert.Equal(t, "us-west-2", desc.PrimaryRegion)
	assert.NotNil(t, desc.CreatedDate)
	assert.NotNil(t, desc.LastChangedDate)
	assert.NotNil(t, desc.VersionIDsToStages)
}

func TestDescribeSecret_DeletedSecretStillReturnsMetadata(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "desc-del", SecretString: "v"},
	)
	require.NoError(t, err)
	_, err = b.DeleteSecret(context.Background(), &secretsmanager.DeleteSecretInput{SecretID: "desc-del"})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "desc-del"})
	require.NoError(t, err)
	assert.NotNil(t, desc.DeletedDate, "DeletedDate must be present for deleted secrets")
	assert.Equal(t, "desc-del", desc.Name)
}

func TestDescribeSecret_NotFound(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "missing"})
	require.ErrorIs(t, err, secretsmanager.ErrSecretNotFound)
}

func TestDescribeSecret_VersionIDsToStages(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:               "versions-check",
		SecretString:       "v1",
		ClientRequestToken: "ver-1",
	})
	require.NoError(t, err)

	_, err = b.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
		SecretID:           "versions-check",
		SecretString:       "v2",
		ClientRequestToken: "ver-2",
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "versions-check"})
	require.NoError(t, err)
	require.NotNil(t, desc.VersionIDsToStages)

	assert.Contains(t, desc.VersionIDsToStages["ver-2"], secretsmanager.StagingLabelCurrent)
	assert.Contains(t, desc.VersionIDsToStages["ver-1"], secretsmanager.StagingLabelPrevious)
}

func TestDescribeSecret_ARN(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	created, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "arn-desc", SecretString: "v"},
	)
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "arn-desc"})
	require.NoError(t, err)
	assert.Equal(t, created.ARN, desc.ARN)
}

// ---------------------------------------------------------------------------
// DescribeSecret round-trip
// ---------------------------------------------------------------------------

func TestDescribeSecret_AllFields(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	ctx := context.Background()

	_, err := b.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         "describe-test",
		SecretString: "v",
		Description:  "test description",
		KmsKeyID:     "arn:aws:kms:us-east-1:123456789012:key/abc",
		Tags:         []secretsmanager.Tag{{Key: "app", Value: "myapp"}},
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{SecretID: "describe-test"})
	require.NoError(t, err)

	assert.Equal(t, "describe-test", desc.Name)
	assert.Equal(t, "test description", desc.Description)
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/abc", desc.KmsKeyID)
	assert.NotEmpty(t, desc.ARN)
	assert.NotNil(t, desc.CreatedDate)
	assert.Nil(t, desc.DeletedDate)
	require.Len(t, desc.Tags, 1)
	assert.Equal(t, "app", desc.Tags[0].Key)
	assert.Equal(t, "myapp", desc.Tags[0].Value)
}

// ---------------------------------------------------------------------------
// Cron-driven NextRotationDate
// ---------------------------------------------------------------------------

// TestDescribeSecret_NextRotationDateFromCron verifies that NextRotationDate is computed
// correctly from a cron ScheduleExpression.
func TestDescribeSecret_NextRotationDateFromCron(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "cron-next-date",
		SecretString: "v",
	})
	require.NoError(t, err)

	_, err = b.RotateSecret(context.Background(), &secretsmanager.RotateSecretInput{
		SecretID: "cron-next-date",
		RotationRules: &secretsmanager.RotationRulesType{
			ScheduleExpression: "cron(0 0 * * ? *)",
		},
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "cron-next-date"})
	require.NoError(t, err)
	require.NotNil(t, desc.NextRotationDate, "NextRotationDate must be set for cron schedule")

	nextTime := time.Unix(int64(*desc.NextRotationDate), 0).UTC()
	assert.Equal(t, 0, nextTime.Hour(), "daily midnight cron must give hour=0")
	assert.Equal(t, 0, nextTime.Minute())
}

// ---------------------------------------------------------------------------
// PrimaryRegion
// ---------------------------------------------------------------------------

func TestDescribeSecret_PrimaryRegion(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackendWithConfig("000000000001", "eu-west-2")

	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{Name: "owned", SecretString: "v"})
	require.NoError(t, err)

	out, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "owned"})
	require.NoError(t, err)
	assert.Equal(t, "eu-west-2", out.PrimaryRegion)
}

// ---------------------------------------------------------------------------
// Replication status
// ---------------------------------------------------------------------------

// TestDescribeSecret_ReplicationStatus verifies ReplicationStatus in DescribeSecret.
func TestDescribeSecret_ReplicationStatus(t *testing.T) {
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

// TestDescribeSecret_HTTP verifies DescribeSecret via HTTP returns KmsKeyId.
func TestDescribeSecret_HTTP(t *testing.T) {
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

// ---------------------------------------------------------------------------
// All fields + CreatedDate via HTTP
// ---------------------------------------------------------------------------

// TestDescribeSecret_HasAllFields verifies DescribeSecret includes all metadata fields
// via the HTTP handler.
func TestDescribeSecret_HasAllFields(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	// Create secret with KMS key.
	rec := doR1Request(
		t,
		h,
		"secretsmanager.CreateSecret",
		`{"Name":"full-meta","SecretString":"v","Description":"full metadata","KmsKeyId":"alias/mykey"}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe it.
	rec = doR1Request(t, h, "secretsmanager.DescribeSecret", `{"SecretId":"full-meta"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var desc secretsmanager.DescribeSecretOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &desc))
	assert.Equal(t, "full-meta", desc.Name)
	assert.Equal(t, "full metadata", desc.Description)
	assert.Equal(t, "alias/mykey", desc.KmsKeyID)
	assert.NotNil(t, desc.LastChangedDate)
}

// TestDescribeSecret_HasCreatedDate verifies DescribeSecret returns CreatedDate via HTTP.
func TestDescribeSecret_HasCreatedDate(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	rec := doR1Request(t, h, "secretsmanager.CreateSecret",
		`{"Name":"createdate-test","SecretString":"v"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doR1Request(t, h, "secretsmanager.DescribeSecret",
		`{"SecretId":"createdate-test"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var desc secretsmanager.DescribeSecretOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &desc))
	assert.NotNil(t, desc.CreatedDate)
	assert.Greater(t, *desc.CreatedDate, float64(0))
}

// ---------------------------------------------------------------------------
// Backend scenarios (ported from table-style subtests)
// ---------------------------------------------------------------------------

func TestDescribeSecret_BackendScenarios(t *testing.T) {
	t.Parallel()

	t.Run("Found", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()
		_, _ = backend.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
			Name:         "described",
			Description:  "my description",
			SecretString: "value",
			Tags: []secretsmanager.Tag{
				{Key: "env", Value: "prod"},
			},
		})

		out, err := backend.DescribeSecret(
			context.Background(),
			&secretsmanager.DescribeSecretInput{SecretID: "described"},
		)
		require.NoError(t, err)
		assert.Equal(t, "described", out.Name)
		assert.Equal(t, "my description", out.Description)
		assert.NotEmpty(t, out.VersionIDsToStages)
	})

	t.Run("NotFound", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()
		_, err := backend.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "missing"})
		require.ErrorIs(t, err, secretsmanager.ErrSecretNotFound)
	})
}
