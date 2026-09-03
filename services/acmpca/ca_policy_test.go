package acmpca_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acmpca"
)

func TestInMemoryBackend_Policy(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	ca, err := b.CreateCertificateAuthority(
		context.Background(),
		"ROOT",
		acmpca.CertificateAuthorityConfiguration{
			Subject: acmpca.CertificateAuthoritySubject{CommonName: "Ops CA"},
		},
	)
	require.NoError(t, err)

	policy := `{"Version":"2012-10-17","Statement":[]}`
	require.NoError(t, b.PutPolicy(context.Background(), ca.ARN, policy))

	got, err := b.GetPolicy(context.Background(), ca.ARN)
	require.NoError(t, err)
	assert.Equal(t, policy, got)

	require.NoError(t, b.DeletePolicy(context.Background(), ca.ARN))
	_, err = b.GetPolicy(context.Background(), ca.ARN)
	require.Error(t, err)
}

// TestInMemoryBackend_PolicyValidation covers policy validation edge cases.
func TestInMemoryBackend_PolicyValidation(t *testing.T) {
	t.Parallel()

	_, err := newTestBackend().GetPolicy(context.Background(), "")
	require.ErrorIs(t, err, acmpca.ErrInvalidArn)
}
