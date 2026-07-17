package mediaconvert_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediaconvert"
)

func TestMediaConvert_Certificate_TableTests(t *testing.T) {
	t.Parallel()

	const certARN = "arn:aws:acm:us-east-1:123456789012:certificate/abc123"

	tests := []struct {
		body       any
		setup      func(b *mediaconvert.InMemoryBackend)
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "associate_certificate",
			method:     http.MethodPost,
			path:       "/2017-08-29/certificates",
			body:       map[string]any{"arn": certARN},
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "associate_certificate_missing_arn",
			method:     http.MethodPost,
			path:       "/2017-08-29/certificates",
			body:       map[string]any{"description": "no arn"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "associate_certificate_duplicate",
			setup: func(b *mediaconvert.InMemoryBackend) {
				require.NoError(t, b.AssociateCertificate(certARN))
			},
			method:     http.MethodPost,
			path:       "/2017-08-29/certificates",
			body:       map[string]any{"arn": certARN},
			wantStatus: http.StatusConflict,
		},
		{
			name: "disassociate_certificate",
			setup: func(b *mediaconvert.InMemoryBackend) {
				require.NoError(t, b.AssociateCertificate(certARN))
			},
			method:     http.MethodDelete,
			path:       "/2017-08-29/certificates/" + certARN,
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "disassociate_certificate_not_found",
			method:     http.MethodDelete,
			path:       "/2017-08-29/certificates/" + certARN,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
			if tt.setup != nil {
				tt.setup(b)
			}
			h := mediaconvert.NewHandler(b)

			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestAssociateCertificate_EmptyARN verifies empty ARN returns ErrValidation.
func TestAssociateCertificate_EmptyARN(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	err := b.AssociateCertificate("")
	require.ErrorIs(t, err, mediaconvert.ErrValidation)
}

// TestAssociateCertificate_InvalidARN returns ErrValidation for a non-ARN string.
func TestAssociateCertificate_InvalidARN(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	err := b.AssociateCertificate("not-an-arn")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "arn:")
}

// TestDisassociateCertificate_InvalidARN returns an error for a non-ARN string.
func TestDisassociateCertificate_InvalidARN(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	err := b.DisassociateCertificate("not-an-arn")
	require.Error(t, err)
}
