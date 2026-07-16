package secretsmanager_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// ---------------------------------------------------------------------------
// Version pruning
// ---------------------------------------------------------------------------

// TestVersionPruning verifies that old unlabeled versions are pruned
// when the version count exceeds maxVersionsPerSecret (100).
func TestVersionPruning(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		putCount    int
		wantMaxVers int
	}{
		{
			name:        "below_limit_no_pruning",
			putCount:    5,
			wantMaxVers: 6, // 1 initial + 5 puts
		},
		{
			name:        "at_limit_no_pruning",
			putCount:    99,
			wantMaxVers: 100,
		},
		{
			name:        "above_limit_pruned",
			putCount:    150,
			wantMaxVers: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := secretsmanager.NewInMemoryBackend()

			_, err := backend.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
				Name:         "prune-test",
				SecretString: "initial",
			})
			require.NoError(t, err)

			for i := range tt.putCount {
				_, putErr := backend.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
					SecretID:     "prune-test",
					SecretString: fmt.Sprintf("value-%d", i),
				})
				require.NoError(t, putErr)
			}

			out, err := backend.DescribeSecret(
				context.Background(),
				&secretsmanager.DescribeSecretInput{SecretID: "prune-test"},
			)
			require.NoError(t, err)
			assert.LessOrEqual(t, len(out.VersionIDsToStages), tt.wantMaxVers)
		})
	}
}

// TestVersionPruning_LabeledVersionsPreserved verifies labeled versions are never pruned.
func TestVersionPruning_LabeledVersionsPreserved(t *testing.T) {
	t.Parallel()

	backend := secretsmanager.NewInMemoryBackend()

	_, err := backend.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "prune-labeled",
		SecretString: "initial",
	})
	require.NoError(t, err)

	for i := range 150 {
		_, putErr := backend.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
			SecretID:     "prune-labeled",
			SecretString: fmt.Sprintf("value-%d", i),
		})
		require.NoError(t, putErr)
	}

	out, err := backend.DescribeSecret(
		context.Background(),
		&secretsmanager.DescribeSecretInput{SecretID: "prune-labeled"},
	)
	require.NoError(t, err)

	var foundCurrent, foundPrevious bool

	for _, labels := range out.VersionIDsToStages {
		for _, l := range labels {
			if l == secretsmanager.StagingLabelCurrent {
				foundCurrent = true
			}

			if l == secretsmanager.StagingLabelPrevious {
				foundPrevious = true
			}
		}
	}

	assert.True(t, foundCurrent, "AWSCURRENT label must be present after pruning")
	assert.True(t, foundPrevious, "AWSPREVIOUS label must be present after pruning")
	assert.LessOrEqual(t, len(out.VersionIDsToStages), 100)
}

// ---------------------------------------------------------------------------
// Secret size validation
// ---------------------------------------------------------------------------

// TestSecretSizeValidation verifies that oversized secrets are rejected.
func TestSecretSizeValidation(t *testing.T) {
	t.Parallel()

	const maxBytes = 65536

	bigString := strings.Repeat("x", maxBytes+1)
	bigBinary := make([]byte, maxBytes+1)

	tests := []struct {
		op      func(b *secretsmanager.InMemoryBackend) error
		name    string
		wantErr bool
	}{
		{
			name: "create_secret_string_too_large",
			op: func(b *secretsmanager.InMemoryBackend) error {
				_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
					Name:         "big-string",
					SecretString: bigString,
				})

				return err
			},
			wantErr: true,
		},
		{
			name: "create_secret_binary_too_large",
			op: func(b *secretsmanager.InMemoryBackend) error {
				_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
					Name:         "big-binary",
					SecretBinary: bigBinary,
				})

				return err
			},
			wantErr: true,
		},
		{
			name: "put_secret_value_string_too_large",
			op: func(b *secretsmanager.InMemoryBackend) error {
				if _, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "existing", SecretString: "ok"},
				); err != nil {
					return err
				}

				_, err := b.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
					SecretID:     "existing",
					SecretString: bigString,
				})

				return err
			},
			wantErr: true,
		},
		{
			name: "put_secret_value_binary_too_large",
			op: func(b *secretsmanager.InMemoryBackend) error {
				if _, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "existing-bin", SecretString: "ok"},
				); err != nil {
					return err
				}

				_, err := b.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
					SecretID:     "existing-bin",
					SecretBinary: bigBinary,
				})

				return err
			},
			wantErr: true,
		},
		{
			name: "update_secret_string_too_large",
			op: func(b *secretsmanager.InMemoryBackend) error {
				if _, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "update-big", SecretString: "ok"},
				); err != nil {
					return err
				}

				_, err := b.UpdateSecret(context.Background(), &secretsmanager.UpdateSecretInput{
					SecretID:     "update-big",
					SecretString: bigString,
				})

				return err
			},
			wantErr: true,
		},
		{
			name: "create_secret_max_size_accepted",
			op: func(b *secretsmanager.InMemoryBackend) error {
				_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
					Name:         "max-size",
					SecretString: strings.Repeat("x", maxBytes),
				})

				return err
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := secretsmanager.NewInMemoryBackend()
			err := tt.op(b)

			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, secretsmanager.ErrSecretValueTooLarge)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestSecretSizeValidation_Handler verifies size validation returns 400 via HTTP.
func TestSecretSizeValidation_Handler(t *testing.T) {
	t.Parallel()

	e := echo.New()
	backend := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(backend)

	bigValue := strings.Repeat("x", 65537)
	body := fmt.Sprintf(`{"Name":"too-big","SecretString":%q}`, bigValue)

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("X-Amz-Target", "secretsmanager.CreateSecret")
	rec := httptest.NewRecorder()

	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp secretsmanager.ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "InvalidParameterException", errResp.Type)
}

// ---------------------------------------------------------------------------
// Version-by-ID / label / binary / last-changed-date mechanics
// ---------------------------------------------------------------------------

// TestVersionByID verifies retrieving a specific version by ID.
func TestVersionByID(t *testing.T) {
	t.Parallel()

	backend := secretsmanager.NewInMemoryBackend()

	_, _ = backend.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "versioned",
		SecretString: "v1-value",
	})

	// Get the initial version ID
	current, _ := backend.GetSecretValue(
		context.Background(),
		&secretsmanager.GetSecretValueInput{SecretID: "versioned"},
	)
	v1ID := current.VersionID

	// Add v2
	_, _ = backend.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
		SecretID:     "versioned",
		SecretString: "v2-value",
	})

	// Retrieve v1 by ID
	out, err := backend.GetSecretValue(context.Background(), &secretsmanager.GetSecretValueInput{
		SecretID:  "versioned",
		VersionID: v1ID,
	})
	require.NoError(t, err)
	assert.Equal(t, "v1-value", out.SecretString)
}

// TestGetSecretValueVersionLabel tests GetSecretValue with a version label.
func TestGetSecretValueVersionLabel(t *testing.T) {
	t.Parallel()

	backend := secretsmanager.NewInMemoryBackend()
	_, _ = backend.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "labeled-secret",
		SecretString: "v1",
	})
	_, _ = backend.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
		SecretID:     "labeled-secret",
		SecretString: "v2",
	})

	// Retrieve AWSPREVIOUS
	out, err := backend.GetSecretValue(context.Background(), &secretsmanager.GetSecretValueInput{
		SecretID:     "labeled-secret",
		VersionStage: secretsmanager.StagingLabelPrevious,
	})
	require.NoError(t, err)
	assert.Equal(t, "v1", out.SecretString)
}

// TestPutSecretValueLabelRotation tests label rotation in PutSecretValue.
func TestPutSecretValueLabelRotation(t *testing.T) {
	t.Parallel()

	e := echo.New()

	backend := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(backend)

	// Create initial secret
	_, _ = backend.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "rotate-test",
		SecretString: "v1",
	})

	// Put v2 via HTTP
	putBody, _ := json.Marshal(map[string]string{
		"SecretId":     "rotate-test",
		"SecretString": "v2",
	})
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(putBody)))
	req.Header.Set("X-Amz-Target", "secretsmanager.PutSecretValue")
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	var putOut secretsmanager.PutSecretValueOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putOut))
	assert.Contains(t, putOut.VersionStages, secretsmanager.StagingLabelCurrent)

	// Current should be v2
	curr, err := backend.GetSecretValue(
		context.Background(),
		&secretsmanager.GetSecretValueInput{SecretID: "rotate-test"},
	)
	require.NoError(t, err)
	assert.Equal(t, "v2", curr.SecretString)
}

// TestBinarySecret verifies binary secret storage.
func TestBinarySecret(t *testing.T) {
	t.Parallel()

	backend := secretsmanager.NewInMemoryBackend()

	binaryData := []byte{0x01, 0x02, 0x03, 0xFF}

	_, err := backend.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "binary-secret",
		SecretBinary: binaryData,
	})
	require.NoError(t, err)

	out, err := backend.GetSecretValue(
		context.Background(),
		&secretsmanager.GetSecretValueInput{SecretID: "binary-secret"},
	)
	require.NoError(t, err)
	assert.Equal(t, binaryData, out.SecretBinary)
	assert.Empty(t, out.SecretString)
}

// TestLastChangedDate verifies LastChangedDate is set by CreateSecret, PutSecretValue, and UpdateSecret.
func TestLastChangedDate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		after func(t *testing.T, b *secretsmanager.InMemoryBackend)
		name  string
	}{
		{
			name:  "set_by_create_secret",
			after: func(_ *testing.T, _ *secretsmanager.InMemoryBackend) {},
		},
		{
			name: "updated_by_put_secret_value",
			after: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
					SecretID:     "lcd-test",
					SecretString: "v2",
				})
				require.NoError(t, err)
			},
		},
		{
			name: "updated_by_update_secret",
			after: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.UpdateSecret(context.Background(), &secretsmanager.UpdateSecretInput{
					SecretID:     "lcd-test",
					SecretString: "v2-updated",
				})
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := secretsmanager.NewInMemoryBackend()

			_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
				Name:         "lcd-test",
				SecretString: "initial",
			})
			require.NoError(t, err)

			tt.after(t, b)

			desc, err := b.DescribeSecret(
				context.Background(),
				&secretsmanager.DescribeSecretInput{SecretID: "lcd-test"},
			)
			require.NoError(t, err)
			assert.NotNil(t, desc.LastChangedDate)
			assert.Greater(t, *desc.LastChangedDate, float64(0))
		})
	}
}

// ---------------------------------------------------------------------------
// NOTE: equivalent coverage for listing version IDs and for PutSecretValue's
// VersionStages behavior already exists in listsecretversionids_test.go
// (TestListSecretVersionIds_BackendScenarios / TestListSecretVersionIds_ViaHandler /
// TestListSecretVersionIds_HTTPDispatchTable) and putsecretvalue_test.go
// (TestPutSecretValue_VersionStages), so it is not duplicated here.
// ---------------------------------------------------------------------------
