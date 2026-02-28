package s3control_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/s3control"
)

func TestS3ControlBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T, b *s3control.InMemoryBackend)
	}{
		{
			name: "PutGetPublicAccessBlock",
			run: func(t *testing.T, b *s3control.InMemoryBackend) {
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
			},
		},
		{
			name: "GetPublicAccessBlock/NotFound",
			run: func(t *testing.T, b *s3control.InMemoryBackend) {
				_, err := b.GetPublicAccessBlock("000000000000")
				require.Error(t, err)
				assert.ErrorIs(t, err, s3control.ErrNotFound)
			},
		},
		{
			name: "DeletePublicAccessBlock",
			run: func(t *testing.T, b *s3control.InMemoryBackend) {
				b.PutPublicAccessBlock(s3control.PublicAccessBlock{AccountID: "000000000000"})

				err := b.DeletePublicAccessBlock("000000000000")
				require.NoError(t, err)

				_, err = b.GetPublicAccessBlock("000000000000")
				assert.ErrorIs(t, err, s3control.ErrNotFound)
			},
		},
		{
			name: "DeletePublicAccessBlock/NotFound",
			run: func(t *testing.T, b *s3control.InMemoryBackend) {
				err := b.DeletePublicAccessBlock("000000000000")
				require.Error(t, err)
				assert.ErrorIs(t, err, s3control.ErrNotFound)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := s3control.NewInMemoryBackend()
			tt.run(t, b)
		})
	}
}
