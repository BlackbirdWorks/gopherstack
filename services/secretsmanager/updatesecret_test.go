package secretsmanager_test

// updatesecret_test.go consolidates every UpdateSecret-specific test that was
// previously scattered across several older test files. Ported verbatim
// (assertions unchanged).

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// ---------------------------------------------------------------------------
// UpdateSecret comprehensive
// ---------------------------------------------------------------------------

func TestUpdateSecret_Description(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "upd-desc", SecretString: "v"},
	)
	require.NoError(t, err)

	_, err = b.UpdateSecret(context.Background(), &secretsmanager.UpdateSecretInput{
		SecretID:    "upd-desc",
		Description: "new description",
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "upd-desc"})
	require.NoError(t, err)
	assert.Equal(t, "new description", desc.Description)
}

func TestUpdateSecret_KmsKeyIDBasic(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "upd-kms", SecretString: "v"},
	)
	require.NoError(t, err)

	_, err = b.UpdateSecret(context.Background(), &secretsmanager.UpdateSecretInput{
		SecretID: "upd-kms",
		KmsKeyID: aws.String("alias/new-key"),
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "upd-kms"})
	require.NoError(t, err)
	assert.Equal(t, "alias/new-key", desc.KmsKeyID)
}

func TestUpdateSecret_ValueCreatesNewVersion(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:               "upd-val",
		SecretString:       "v1",
		ClientRequestToken: "v1-id",
	})
	require.NoError(t, err)

	out, err := b.UpdateSecret(context.Background(), &secretsmanager.UpdateSecretInput{
		SecretID:           "upd-val",
		SecretString:       "v2",
		ClientRequestToken: "v2-id",
	})
	require.NoError(t, err)
	assert.Equal(t, "v2-id", out.VersionID)

	val, err := b.GetSecretValue(context.Background(), &secretsmanager.GetSecretValueInput{SecretID: "upd-val"})
	require.NoError(t, err)
	assert.Equal(t, "v2", val.SecretString)
}

func TestUpdateSecret_DeletedFails(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "upd-del", SecretString: "v"},
	)
	require.NoError(t, err)
	_, err = b.DeleteSecret(context.Background(), &secretsmanager.DeleteSecretInput{SecretID: "upd-del"})
	require.NoError(t, err)

	_, err = b.UpdateSecret(context.Background(), &secretsmanager.UpdateSecretInput{
		SecretID:    "upd-del",
		Description: "new desc",
	})
	require.ErrorIs(t, err, secretsmanager.ErrSecretDeleted)
}

func TestUpdateSecret_NotFound(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.UpdateSecret(context.Background(), &secretsmanager.UpdateSecretInput{
		SecretID:    "missing",
		Description: "d",
	})
	require.ErrorIs(t, err, secretsmanager.ErrSecretNotFound)
}

// ---------------------------------------------------------------------------
// UpdateSecret value + metadata (table-driven)
// ---------------------------------------------------------------------------

func TestUpdateSecret_ValueAndMeta(t *testing.T) {
	t.Parallel()

	tests := []struct {
		checkFn     func(t *testing.T, desc *secretsmanager.DescribeSecretOutput, val *secretsmanager.GetSecretValueOutput)
		name        string
		updateInput secretsmanager.UpdateSecretInput
	}{
		{
			name: "update_description_only",
			updateInput: secretsmanager.UpdateSecretInput{
				SecretID:    "update-test",
				Description: "updated description",
			},
			checkFn: func(t *testing.T, desc *secretsmanager.DescribeSecretOutput, val *secretsmanager.GetSecretValueOutput) {
				t.Helper()
				assert.Equal(t, "updated description", desc.Description)
				assert.Equal(
					t,
					"original",
					val.SecretString,
					"value must not change on meta-only update",
				)
			},
		},
		{
			name: "update_value",
			updateInput: secretsmanager.UpdateSecretInput{
				SecretID:     "update-test",
				SecretString: "updated-value",
			},
			checkFn: func(t *testing.T, _ *secretsmanager.DescribeSecretOutput, val *secretsmanager.GetSecretValueOutput) {
				t.Helper()
				assert.Equal(t, "updated-value", val.SecretString)
			},
		},
		{
			name: "update_kms_key",
			updateInput: secretsmanager.UpdateSecretInput{
				SecretID: "update-test",
				KmsKeyID: aws.String("new-key-id"),
			},
			checkFn: func(t *testing.T, desc *secretsmanager.DescribeSecretOutput, _ *secretsmanager.GetSecretValueOutput) {
				t.Helper()
				assert.Equal(t, "new-key-id", desc.KmsKeyID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := secretsmanager.NewInMemoryBackend()
			ctx := context.Background()

			_, err := b.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
				Name:         "update-test",
				SecretString: "original",
				Description:  "original description",
			})
			require.NoError(t, err)

			_, err = b.UpdateSecret(ctx, &tc.updateInput)
			require.NoError(t, err)

			desc, err := b.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{SecretID: "update-test"})
			require.NoError(t, err)

			val, err := b.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretID: "update-test"})
			require.NoError(t, err)

			tc.checkFn(t, desc, val)
		})
	}
}

// ---------------------------------------------------------------------------
// UpdateSecret KMS key via HTTP
// ---------------------------------------------------------------------------

// TestUpdateSecret_KmsKeyID verifies UpdateSecret can change the KMS key via the HTTP
// handler.
func TestUpdateSecret_KmsKeyID(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	// Create a secret.
	rec := doR1Request(t, h, "secretsmanager.CreateSecret", `{"Name":"kms-update-test","SecretString":"v"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// Update KMS key.
	rec = doR1Request(
		t,
		h,
		"secretsmanager.UpdateSecret",
		`{"SecretId":"kms-update-test","KmsKeyId":"arn:aws:kms:us-east-1:123456789012:key/test-key-id"}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify via DescribeSecret.
	rec = doR1Request(t, h, "secretsmanager.DescribeSecret", `{"SecretId":"kms-update-test"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var desc secretsmanager.DescribeSecretOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &desc))
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/test-key-id", desc.KmsKeyID)
}

// TestUpdateSecret_ClientRequestTokenIdempotency verifies that UpdateSecret with the
// same ClientRequestToken and content is idempotent.
func TestUpdateSecret_ClientRequestTokenIdempotency(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	rec := doR1Request(t, h, "secretsmanager.CreateSecret",
		`{"Name":"update-idempotent","SecretString":"initial"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	const token = "550e8400-e29b-41d4-a716-446655440001"
	body, _ := json.Marshal(map[string]any{
		"SecretId":           "update-idempotent",
		"SecretString":       "updated-value",
		"ClientRequestToken": token,
	})

	rec = doR1Request(t, h, "secretsmanager.UpdateSecret", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	var out1 secretsmanager.UpdateSecretOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out1))

	rec = doR1Request(t, h, "secretsmanager.UpdateSecret", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	var out2 secretsmanager.UpdateSecretOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out2))

	assert.Equal(t, out1.VersionID, out2.VersionID)
}

// ---------------------------------------------------------------------------
// Backend scenarios (ported from table-style subtests)
// ---------------------------------------------------------------------------

func TestUpdateSecret_BackendScenarios(t *testing.T) {
	t.Parallel()

	t.Run("UpdateDescription", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()
		_, _ = backend.CreateSecret(
			context.Background(),
			&secretsmanager.CreateSecretInput{Name: "updatable", SecretString: "original"},
		)

		out, err := backend.UpdateSecret(context.Background(), &secretsmanager.UpdateSecretInput{
			SecretID:    "updatable",
			Description: "new description",
		})
		require.NoError(t, err)
		assert.Equal(t, "updatable", out.Name)
		assert.Empty(t, out.VersionID) // no new version for description-only update

		desc, _ := backend.DescribeSecret(
			context.Background(),
			&secretsmanager.DescribeSecretInput{SecretID: "updatable"},
		)
		assert.Equal(t, "new description", desc.Description)
	})

	t.Run("UpdateValue", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()
		_, _ = backend.CreateSecret(
			context.Background(),
			&secretsmanager.CreateSecretInput{Name: "with-value", SecretString: "v1"},
		)

		out, err := backend.UpdateSecret(context.Background(), &secretsmanager.UpdateSecretInput{
			SecretID:     "with-value",
			SecretString: "v2",
		})
		require.NoError(t, err)
		assert.NotEmpty(t, out.VersionID)
	})

	t.Run("NotFound", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()
		_, err := backend.UpdateSecret(context.Background(), &secretsmanager.UpdateSecretInput{SecretID: "missing"})
		require.ErrorIs(t, err, secretsmanager.ErrSecretNotFound)
	})
}

// TestUpdateSecret_FailedValueUpdate_LeavesDescriptionAndKmsKeyIDUnchanged verifies
// that when a same-call value update fails (here, a KMS encryption error), the
// Description and KmsKeyId changes requested in the same UpdateSecret call are not
// left applied -- a "state mutated before validation" bug (parity-principles.md)
// where a rejected request had already written its fields and the caller got an
// error but the change stood anyway.
func TestUpdateSecret_FailedValueUpdate_LeavesDescriptionAndKmsKeyIDUnchanged(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	fake := &fakeKMSEncryptor{}
	b.SetKMSEncryptor(fake)

	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "atomic-update",
		SecretString: "v1",
		KmsKeyID:     "alias/original-key",
	})
	require.NoError(t, err)

	fake.encryptErr = assert.AnError

	_, err = b.UpdateSecret(context.Background(), &secretsmanager.UpdateSecretInput{
		SecretID:     "atomic-update",
		Description:  "should not apply",
		KmsKeyID:     aws.String("alias/new-key"),
		SecretString: "v2",
	})
	require.Error(t, err)

	desc, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "atomic-update"})
	require.NoError(t, err)
	assert.Empty(t, desc.Description, "a rejected UpdateSecret must not apply Description")
	assert.Equal(t, "alias/original-key", desc.KmsKeyID, "a rejected UpdateSecret must not apply KmsKeyId")
}
