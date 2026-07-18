package transfer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/transfer"
)

// TestAgreementCountExport verifies AgreementCount export.
func TestAgreementCountExport(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	assert.Equal(t, 0, transfer.AgreementCount(b))

	_, err = b.CreateAgreement(s.ServerID, "desc", "p-local", "p-partner", "/base", "arn:role", nil)
	require.NoError(t, err)

	assert.Equal(t, 1, transfer.AgreementCount(b))
}

func TestCreateAgreement(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	ag, err := b.CreateAgreement(
		s.ServerID,
		"test agreement",
		"p-local",
		"p-partner",
		"/base",
		"arn:role",
		map[string]string{"env": "test"},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, ag.AgreementID)
	assert.Equal(t, s.ServerID, ag.ServerID)
	assert.Equal(t, "test agreement", ag.Description)
	assert.Equal(t, "ACTIVE", ag.Status)
}

func TestCreateAgreement_ServerNotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	_, err := b.CreateAgreement("s-doesnotexist", "", "", "", "", "", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestDeleteAgreement(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	ag, err := b.CreateAgreement(s.ServerID, "", "", "", "", "", nil)
	require.NoError(t, err)

	require.NoError(t, b.DeleteAgreement(s.ServerID, ag.AgreementID))

	// Double delete should fail
	err = b.DeleteAgreement(s.ServerID, ag.AgreementID)
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestDeleteAgreement_ServerNotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	err := b.DeleteAgreement("s-doesnotexist", "a-123")
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}
