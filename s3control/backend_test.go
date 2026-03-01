package s3control_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/s3control"
)

func TestGetPublicAccessBlock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                   string
		seed                   *s3control.PublicAccessBlock
		accountID              string
		wantErr                error
		wantBlockPublicAcls    bool
		wantIgnorePublicAcls   bool
		wantBlockPublicPolicy  bool
	}{
		{
			name: "PutThenGet",
			seed: &s3control.PublicAccessBlock{
				AccountID:             "000000000000",
				BlockPublicAcls:       true,
				IgnorePublicAcls:      true,
				BlockPublicPolicy:     false,
				RestrictPublicBuckets: false,
			},
			accountID:             "000000000000",
			wantBlockPublicAcls:   true,
			wantIgnorePublicAcls:  true,
			wantBlockPublicPolicy: false,
		},
		{
			name:      "NotFound",
			accountID: "000000000000",
			wantErr:   s3control.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			if tt.seed != nil {
				b.PutPublicAccessBlock(*tt.seed)
			}

			got, err := b.GetPublicAccessBlock(tt.accountID)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantBlockPublicAcls, got.BlockPublicAcls)
			assert.Equal(t, tt.wantIgnorePublicAcls, got.IgnorePublicAcls)
			assert.Equal(t, tt.wantBlockPublicPolicy, got.BlockPublicPolicy)
		})
	}
}

func TestDeletePublicAccessBlock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		seed      *s3control.PublicAccessBlock
		accountID string
		wantErr   error
	}{
		{
			name:      "Success",
			seed:      &s3control.PublicAccessBlock{AccountID: "000000000000"},
			accountID: "000000000000",
		},
		{
			name:      "NotFound",
			accountID: "000000000000",
			wantErr:   s3control.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			if tt.seed != nil {
				b.PutPublicAccessBlock(*tt.seed)
			}

			err := b.DeletePublicAccessBlock(tt.accountID)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
			_, err = b.GetPublicAccessBlock(tt.accountID)
			assert.ErrorIs(t, err, s3control.ErrNotFound)
		})
	}
}
