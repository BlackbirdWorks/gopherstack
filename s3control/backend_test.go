package s3control_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/s3control"
)

func TestS3Control_PutGetPublicAccessBlock(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	cfg := s3control.PublicAccessBlock{
		AccountID:             "000000000000",
		BlockPublicAcls:       true,
		IgnorePublicAcls:      true,
		BlockPublicPolicy:     false,
		RestrictPublicBuckets: false,
	}

	b.PutPublicAccessBlock(cfg)

	got, err := b.GetPublicAccessBlock("000000000000")
	require.NoError(t, err)
	assert.True(t, got.BlockPublicAcls)
	assert.True(t, got.IgnorePublicAcls)
	assert.False(t, got.BlockPublicPolicy)
}

func TestS3Control_GetPublicAccessBlock_NotFound(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	_, err := b.GetPublicAccessBlock("000000000000")
	require.Error(t, err)
	assert.ErrorIs(t, err, s3control.ErrNotFound)
}

func TestS3Control_DeletePublicAccessBlock(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	b.PutPublicAccessBlock(s3control.PublicAccessBlock{AccountID: "000000000000"})

	err := b.DeletePublicAccessBlock("000000000000")
	require.NoError(t, err)

	_, err = b.GetPublicAccessBlock("000000000000")
	assert.ErrorIs(t, err, s3control.ErrNotFound)
}

func TestS3Control_DeletePublicAccessBlock_NotFound(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	err := b.DeletePublicAccessBlock("000000000000")
	require.Error(t, err)
	assert.ErrorIs(t, err, s3control.ErrNotFound)
}
