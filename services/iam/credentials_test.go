package iam_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestServiceSpecificCredentials_Backend covers ListServiceSpecificCredentials,
// DeleteServiceSpecificCredential, UpdateServiceSpecificCredential.
func TestServiceSpecificCredentials_Backend(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	_, err := b.CreateUser("ssc-user", "/", "")
	require.NoError(t, err)

	// Create a service specific credential via backend.
	cred, err := b.CreateServiceSpecificCredential("ssc-user", "codecommit.amazonaws.com")
	require.NoError(t, err)

	// ListServiceSpecificCredentials.
	rec := callIAM(t, h, "ListServiceSpecificCredentials", map[string]string{
		"UserName": "ssc-user",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// UpdateServiceSpecificCredential.
	rec = callIAM(t, h, "UpdateServiceSpecificCredential", map[string]string{
		"UserName":                    "ssc-user",
		"ServiceSpecificCredentialId": cred.ServiceSpecificCredentialID,
		"Status":                      "Inactive",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeleteServiceSpecificCredential.
	rec = callIAM(t, h, "DeleteServiceSpecificCredential", map[string]string{
		"UserName":                    "ssc-user",
		"ServiceSpecificCredentialId": cred.ServiceSpecificCredentialID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestServiceSpecificCredential_IDFormat(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("ssc-user", "/", "")

	cred, err := b.CreateServiceSpecificCredential("ssc-user", "codecommit.amazonaws.com")
	require.NoError(t, err)

	// Gopherstack service-specific credential IDs start with "ACCAI" (5 char prefix).
	assert.True(t, strings.HasPrefix(cred.ServiceSpecificCredentialID, "ACCAI"),
		"SSC ID must start with ACCAI, got %q", cred.ServiceSpecificCredentialID)
	assert.Greater(t, len(cred.ServiceSpecificCredentialID), 16,
		"SSC ID must be longer than the prefix alone")
}

func TestServiceSpecificCredential_InitialStatusIsActive(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("ssc-status-user", "/", "")

	cred, err := b.CreateServiceSpecificCredential("ssc-status-user", "codecommit.amazonaws.com")
	require.NoError(t, err)

	assert.Equal(t, "Active", cred.Status, "new SSC must be Active")
}

func TestServiceSpecificCredential_UpdateToInactive(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("ssc-update-user", "/", "")

	cred, err := b.CreateServiceSpecificCredential("ssc-update-user", "codecommit.amazonaws.com")
	require.NoError(t, err)

	require.NoError(t, b.UpdateServiceSpecificCredential(
		"ssc-update-user", cred.ServiceSpecificCredentialID, "Inactive",
	))

	creds, err := b.ListServiceSpecificCredentials("ssc-update-user", "")
	require.NoError(t, err)
	require.Len(t, creds, 1)
	assert.Equal(t, "Inactive", creds[0].Status)
}

func TestServiceSpecificCredential_DeleteRemoves(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("ssc-del-user", "/", "")

	cred, err := b.CreateServiceSpecificCredential("ssc-del-user", "codecommit.amazonaws.com")
	require.NoError(t, err)

	require.NoError(t, b.DeleteServiceSpecificCredential("ssc-del-user", cred.ServiceSpecificCredentialID))

	creds, err := b.ListServiceSpecificCredentials("ssc-del-user", "")
	require.NoError(t, err)
	assert.Empty(t, creds)
}

func TestServiceSpecificCredential_UniqueIDs(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("ssc-unique-user", "/", "")

	cred1, err := b.CreateServiceSpecificCredential("ssc-unique-user", "codecommit.amazonaws.com")
	require.NoError(t, err)

	// Delete first before creating second (no per-user limit beyond testing uniqueness).
	require.NoError(t, b.DeleteServiceSpecificCredential("ssc-unique-user", cred1.ServiceSpecificCredentialID))

	cred2, err := b.CreateServiceSpecificCredential("ssc-unique-user", "codecommit.amazonaws.com")
	require.NoError(t, err)

	assert.NotEqual(t, cred1.ServiceSpecificCredentialID, cred2.ServiceSpecificCredentialID)
}
