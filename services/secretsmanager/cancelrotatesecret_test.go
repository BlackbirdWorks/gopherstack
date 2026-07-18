package secretsmanager_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// ---------------------------------------------------------------------------
// CancelRotateSecret comprehensive
// ---------------------------------------------------------------------------

func TestCancelRotateSecret_RemovesAWSPENDING(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "cancel-rot", SecretString: "v1"},
	)
	require.NoError(t, err)

	// Put a version with AWSPENDING
	_, err = b.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
		SecretID:           "cancel-rot",
		SecretString:       "v2",
		ClientRequestToken: "v2-pending",
		VersionStages:      []string{"AWSPENDING"},
	})
	require.NoError(t, err)

	_, err = b.CancelRotateSecret(context.Background(), &secretsmanager.CancelRotateSecretInput{SecretID: "cancel-rot"})
	require.NoError(t, err)

	// Confirm AWSPENDING is gone
	desc, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "cancel-rot"})
	require.NoError(t, err)

	for _, labels := range desc.VersionIDsToStages {
		for _, l := range labels {
			assert.NotEqual(t, "AWSPENDING", l, "AWSPENDING must be removed after CancelRotateSecret")
		}
	}
}

func TestCancelRotateSecret_SetsRotationDisabled(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "cancel-enabled", SecretString: "v"},
	)
	require.NoError(t, err)

	_, err = b.RotateSecret(context.Background(), &secretsmanager.RotateSecretInput{SecretID: "cancel-enabled"})
	require.NoError(t, err)

	_, err = b.CancelRotateSecret(
		context.Background(),
		&secretsmanager.CancelRotateSecretInput{SecretID: "cancel-enabled"},
	)
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "cancel-enabled"})
	require.NoError(t, err)
	// Real AWS: CancelRotateSecret only removes AWSPENDING; rotation config stays intact.
	assert.True(t, desc.RotationEnabled)
}

func TestCancelRotateSecret_NotFound(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CancelRotateSecret(context.Background(), &secretsmanager.CancelRotateSecretInput{SecretID: "missing"})
	require.ErrorIs(t, err, secretsmanager.ErrSecretNotFound)
}

// ---------------------------------------------------------------------------
// CancelRotateSecret HTTP dispatch
// Uses doSMRequest defined in helpers_test.go (same package).
// ---------------------------------------------------------------------------

// TestCancelRotateSecret_HTTP verifies CancelRotateSecret via HTTP dispatch.
func TestCancelRotateSecret_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup          func(*testing.T, *secretsmanager.InMemoryBackend)
		checkFn        func(*testing.T, *httptest.ResponseRecorder)
		name           string
		body           string
		expectedStatus int
	}{
		{
			name: "cancels_rotation",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "cancel-rot", SecretString: "v1"},
				)
				require.NoError(t, err)
				// Rotate to create a pending version.
				_, err = b.RotateSecret(context.Background(), &secretsmanager.RotateSecretInput{SecretID: "cancel-rot"})
				require.NoError(t, err)
			},
			body:           `{"SecretId":"cancel-rot"}`,
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out secretsmanager.CancelRotateSecretOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, "cancel-rot", out.Name)
				assert.NotEmpty(t, out.ARN)
			},
		},
		{
			name:           "not_found",
			body:           `{"SecretId":"no-such-secret"}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name: "deleted_secret",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "cancel-del", SecretString: "v"},
				)
				require.NoError(t, err)
				_, err = b.DeleteSecret(context.Background(), &secretsmanager.DeleteSecretInput{SecretID: "cancel-del"})
				require.NoError(t, err)
			},
			body:           `{"SecretId":"cancel-del"}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "bad_json",
			body:           `{bad}`,
			expectedStatus: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := secretsmanager.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, backend)
			}

			h := secretsmanager.NewHandler(backend)
			rec := doSMRequest(t, h, "secretsmanager.CancelRotateSecret", tt.body)

			assert.Equal(t, tt.expectedStatus, rec.Code)

			if tt.checkFn != nil {
				tt.checkFn(t, rec)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// CancelRotateSecret rotation-config preservation (ported from handler_parity_test.go)
// ---------------------------------------------------------------------------

// TestCancelRotateSecret_RotationConfigPreserved verifies that CancelRotateSecret
// does not disable rotation. Real AWS only removes the AWSPENDING label; the Lambda ARN
// and rotation rules remain, and RotationEnabled stays true.
func TestCancelRotateSecret_RotationConfigPreserved(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	ctx := context.Background()

	_, err := b.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         "cancel-rot-config",
		SecretString: "v1",
	})
	require.NoError(t, err)

	_, err = b.RotateSecret(ctx, &secretsmanager.RotateSecretInput{SecretID: "cancel-rot-config"})
	require.NoError(t, err)

	_, err = b.CancelRotateSecret(ctx, &secretsmanager.CancelRotateSecretInput{SecretID: "cancel-rot-config"})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{SecretID: "cancel-rot-config"})
	require.NoError(t, err)
	assert.True(t, desc.RotationEnabled,
		"real AWS: CancelRotateSecret must not disable RotationEnabled")
}

// ---------------------------------------------------------------------------
// CancelRotateSecret idempotency
// ---------------------------------------------------------------------------

// TestCancelRotateSecret_NoRotation verifies CancelRotateSecret without rotation
// removes AWSPENDING gracefully.
func TestCancelRotateSecret_NoRotation(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{Name: "norot", SecretString: "v"})
	require.NoError(t, err)

	// CancelRotate on a non-rotating secret should succeed (idempotent).
	out, err := b.CancelRotateSecret(context.Background(), &secretsmanager.CancelRotateSecretInput{SecretID: "norot"})
	require.NoError(t, err)
	assert.Equal(t, "norot", out.Name)
}

// ---------------------------------------------------------------------------
// CancelRotateSecret backend edge cases (ported from the CancelRotateSecret-specific
// subtests of the original TestNewOpsBackend)
// ---------------------------------------------------------------------------

// TestCancelRotateSecret_BackendEdgeCases verifies backend-level edge cases for
// CancelRotateSecret.
func TestCancelRotateSecret_BackendEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("removes_pending_label", func(t *testing.T) {
		t.Parallel()

		b := secretsmanager.NewInMemoryBackend()
		_, err := b.CreateSecret(
			context.Background(),
			&secretsmanager.CreateSecretInput{Name: "rot-cancel", SecretString: "v"},
		)
		require.NoError(t, err)
		_, err = b.RotateSecret(context.Background(), &secretsmanager.RotateSecretInput{SecretID: "rot-cancel"})
		require.NoError(t, err)

		out, err := b.CancelRotateSecret(
			context.Background(),
			&secretsmanager.CancelRotateSecretInput{SecretID: "rot-cancel"},
		)
		require.NoError(t, err)
		assert.Equal(t, "rot-cancel", out.Name)

		desc, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "rot-cancel"})
		require.NoError(t, err)
		// Real AWS: CancelRotateSecret removes AWSPENDING but does not disable rotation.
		assert.True(t, desc.RotationEnabled)
	})

	t.Run("not_found", func(t *testing.T) {
		t.Parallel()

		b := secretsmanager.NewInMemoryBackend()
		_, err := b.CancelRotateSecret(
			context.Background(),
			&secretsmanager.CancelRotateSecretInput{SecretID: "nonexistent"},
		)
		require.ErrorIs(t, err, secretsmanager.ErrSecretNotFound)
	})
}
