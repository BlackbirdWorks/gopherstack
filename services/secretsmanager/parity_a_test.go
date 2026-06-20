package secretsmanager_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

func newSMHandler() *secretsmanager.Handler {
	return secretsmanager.NewHandler(secretsmanager.NewInMemoryBackend())
}

// TestParity_GetSecretValueRequiresSecretId verifies that GetSecretValue rejects
// requests with a missing or empty SecretId. Real AWS returns InvalidParameterException
// (400) for this case; the emulator previously returned ResourceNotFoundException
// because an empty SecretId resolved to a name lookup that found nothing.
func TestParity_GetSecretValueRequiresSecretId(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     string
		name     string
		wantCode int
	}{
		{
			name:     "absent_secret_id_rejected",
			body:     `{}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty_secret_id_rejected",
			body:     `{"SecretId":""}`,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newSMHandler()
			rec := doSMRequest(t, h, "secretsmanager.GetSecretValue", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code,
				"GetSecretValue status for case %q", tt.name)
		})
	}
}

// TestParity_PutSecretValueRequiresSecretId verifies that PutSecretValue rejects
// requests with a missing or empty SecretId. Real AWS returns InvalidParameterException
// (400); the emulator previously returned ResourceNotFoundException.
func TestParity_PutSecretValueRequiresSecretId(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     string
		name     string
		wantCode int
	}{
		{
			name:     "absent_secret_id_rejected",
			body:     `{"SecretString":"val"}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty_secret_id_rejected",
			body:     `{"SecretId":"","SecretString":"val"}`,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newSMHandler()
			rec := doSMRequest(t, h, "secretsmanager.PutSecretValue", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code,
				"PutSecretValue status for case %q", tt.name)
		})
	}
}
