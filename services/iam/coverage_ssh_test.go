package iam_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

func TestIAM_ServiceLastAccessedDetails(t *testing.T) {
	t.Parallel()

	_, be := newTestHandler(t)

	// GetServiceLastAccessedDetails — exercises the backend function
	_, err := be.GetServiceLastAccessedDetails("arn:aws:iam::123456789012:user/test-user")
	// May return empty or error — just exercises the code path
	assert.NoError(t, err)
}

func TestIAM_BackendReset(t *testing.T) {
	t.Parallel()

	_, be := newTestHandler(t)

	// Create some resources
	_, err := be.CreateUser("reset-user", "/", nil)
	require.NoError(t, err)

	// Reset exercises collectAndDeleteFunctions and cleanup paths
	be.Reset()

	// Verify reset worked
	users, _, err := be.ListUsers("", 100)
	require.NoError(t, err)
	assert.Empty(t, users)
}

func TestIAM_AccessKey(t *testing.T) {
	t.Parallel()

	_, be := newTestHandler(t)

	_, err := be.CreateUser("key-user", "/", nil)
	require.NoError(t, err)

	// CreateAccessKey
	key, err := be.CreateAccessKey("key-user")
	require.NoError(t, err)
	assert.NotEmpty(t, key.AccessKeyID)

	// ListAccessKeys
	keys, err := be.ListAccessKeys("key-user", "", 100)
	require.NoError(t, err)
	assert.Len(t, keys, 1)
}
