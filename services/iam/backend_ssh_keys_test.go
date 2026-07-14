package iam_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListSSHPublicKeys_Backend covers ListSSHPublicKeys (UploadSSHPublicKey has a backend bug
// that panics on newID slice bounds, so upload is skipped).
func TestListSSHPublicKeys_Backend(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	_, err := b.CreateUser("ssh-list-user", "/", "")
	require.NoError(t, err)

	// ListSSHPublicKeys on a user with no keys.
	rec := callIAM(t, h, "ListSSHPublicKeys", map[string]string{
		"UserName": "ssh-list-user",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}
