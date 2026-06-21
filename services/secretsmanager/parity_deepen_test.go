package secretsmanager_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sm "github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// TestDeleteSecret_ForceDeleteWithRecoveryWindowConflict verifies that AWS's
// mutual-exclusivity rule between ForceDeleteWithoutRecovery and
// RecoveryWindowInDays is enforced. Real AWS returns InvalidParameterException
// when both are supplied together.
func TestDeleteSecret_ForceDeleteWithRecoveryWindowConflict(t *testing.T) {
	t.Parallel()

	window := int64(7)

	tests := []struct {
		recoveryDays *int64
		name         string
		force        bool
		wantErr      bool
	}{
		{
			name:         "force_plus_recovery_window_rejected",
			recoveryDays: &window,
			force:        true,
			wantErr:      true,
		},
		{
			name:         "force_only_ok",
			recoveryDays: nil,
			force:        true,
			wantErr:      false,
		},
		{
			name:         "recovery_window_only_ok",
			recoveryDays: &window,
			force:        false,
			wantErr:      false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := sm.NewInMemoryBackend()
			_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
				Name:         "del-conflict-" + tc.name,
				SecretString: "v",
			})
			require.NoError(t, err)

			_, err = b.DeleteSecret(context.Background(), &sm.DeleteSecretInput{
				SecretID:                   "del-conflict-" + tc.name,
				ForceDeleteWithoutRecovery: tc.force,
				RecoveryWindowInDays:       tc.recoveryDays,
			})

			if tc.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, sm.ErrInvalidParameter,
					"force-delete + recovery window must be InvalidParameterException")

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestDeleteSecret_ForceWithRecoveryWindowHTTPErrorType verifies the HTTP error
// body and the X-Amzn-Errortype header both carry InvalidParameterException.
func TestDeleteSecret_ForceWithRecoveryWindowHTTPErrorType(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	h := sm.NewHandler(b)

	rec := doR1Request(t, h, "secretsmanager.CreateSecret",
		`{"Name":"del-conflict-http","SecretString":"v"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doR1Request(t, h, "secretsmanager.DeleteSecret",
		`{"SecretId":"del-conflict-http","ForceDeleteWithoutRecovery":true,"RecoveryWindowInDays":7}`)
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp sm.ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "InvalidParameterException", errResp.Type)
	assert.Equal(t, "InvalidParameterException", rec.Header().Get("X-Amzn-Errortype"),
		"error response must echo the error code in X-Amzn-Errortype")
}

// TestErrorResponse_AmznErrortypeHeader verifies that the X-Amzn-Errortype
// response header is set on error responses and matches the body __type, across
// the distinct AWS error categories. Real AWS (awsJson1_1) always emits this
// header and SDKs read it to construct the typed exception.
func TestErrorResponse_AmznErrortypeHeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		seed     func(t *testing.T, b *sm.InMemoryBackend)
		name     string
		action   string
		body     string
		wantType string
	}{
		{
			name:     "not_found",
			action:   "secretsmanager.GetSecretValue",
			body:     `{"SecretId":"does-not-exist"}`,
			wantType: "ResourceNotFoundException",
		},
		{
			name:     "invalid_name",
			action:   "secretsmanager.CreateSecret",
			body:     `{"Name":"bad name with spaces!","SecretString":"v"}`,
			wantType: "InvalidParameterException",
		},
		{
			name:   "already_exists",
			action: "secretsmanager.CreateSecret",
			body:   `{"Name":"dup-secret","SecretString":"v"}`,
			seed: func(t *testing.T, b *sm.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
					Name:         "dup-secret",
					SecretString: "v",
				})
				require.NoError(t, err)
			},
			wantType: "ResourceExistsException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := sm.NewInMemoryBackend()
			if tc.seed != nil {
				tc.seed(t, b)
			}

			h := sm.NewHandler(b)
			rec := doR1Request(t, h, tc.action, tc.body)

			require.GreaterOrEqual(t, rec.Code, http.StatusBadRequest)

			var errResp sm.ErrorResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, tc.wantType, errResp.Type, "body __type")
			assert.Equal(t, tc.wantType, rec.Header().Get("X-Amzn-Errortype"),
				"X-Amzn-Errortype header must equal body __type")
		})
	}
}
