package iam_test

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
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

// TestListSSHPublicKeys_MarkerRoundTrip covers the response-side gap found
// while implementing ListSigningCertificates' pagination fix: real
// ListSSHPublicKeysOutput carries a Marker member alongside IsTruncated
// (deserializers.go's awsAwsquery_deserializeOpDocumentListSSHPublicKeysOutput),
// but the response never echoed it, so a real client saw IsTruncated=true
// with no continuation token to page further with.
func TestListSSHPublicKeys_MarkerRoundTrip(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)
	client := newTestIAMClient(t, h)

	_, err := b.CreateUser("ssh-marker-user", "/", "")
	require.NoError(t, err)

	for range 3 {
		_, uploadErr := b.UploadSSHPublicKey("ssh-marker-user", "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC test")
		require.NoError(t, uploadErr)
	}

	page1, err := client.ListSSHPublicKeys(t.Context(), &iamsdk.ListSSHPublicKeysInput{
		UserName: aws.String("ssh-marker-user"),
		MaxItems: aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, page1.SSHPublicKeys, 2)
	assert.True(t, page1.IsTruncated)
	require.NotNil(t, page1.Marker)
	assert.NotEmpty(t, *page1.Marker)

	page2, err := client.ListSSHPublicKeys(t.Context(), &iamsdk.ListSSHPublicKeysInput{
		UserName: aws.String("ssh-marker-user"),
		MaxItems: aws.Int32(2),
		Marker:   page1.Marker,
	})
	require.NoError(t, err)
	require.Len(t, page2.SSHPublicKeys, 1)
	assert.False(t, page2.IsTruncated)
	assert.Nil(t, page2.Marker)
}
