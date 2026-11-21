package azureblob_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azureblob"
)

func TestInMemoryBackend_ReadTail(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, b *azureblob.InMemoryBackend)
		wantErr error
		name    string
		blob    string
		want    string
		n       int64
		wantLen int64
	}{
		{
			name: "block blob", blob: "b", n: 3, want: "def", wantLen: 6,
			setup: func(t *testing.T, b *azureblob.InMemoryBackend) {
				t.Helper()
				_, err := b.PutBlobWith("c", "b", []byte("abcdef"), azureblob.BlobAttrs{})
				require.NoError(t, err)
			},
		},
		{
			name: "shorter than n", blob: "b", n: 100, want: "abcdef", wantLen: 6,
			setup: func(t *testing.T, b *azureblob.InMemoryBackend) {
				t.Helper()
				_, err := b.PutBlobWith("c", "b", []byte("abcdef"), azureblob.BlobAttrs{})
				require.NoError(t, err)
			},
		},
		{
			name: "page blob spans extent and hole", blob: "p", n: 6, want: "\x00\x00\x00\x00XY", wantLen: 1024,
			setup: func(t *testing.T, b *azureblob.InMemoryBackend) {
				t.Helper()
				_, err := b.PutBlobWith("c", "p", nil, azureblob.BlobAttrs{
					BlobType: azureblob.BlobTypePage, PageLength: 1024,
				})
				require.NoError(t, err)

				page := make([]byte, 512)
				copy(page[510:], "XY")
				_, err = b.PutPage("c", "p", 512, 1023, page, false)
				require.NoError(t, err)
			},
		},
		{
			name:    "missing blob",
			blob:    "none",
			n:       1,
			wantErr: azureblob.ErrBlobNotFound,
			setup:   func(*testing.T, *azureblob.InMemoryBackend) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := azureblob.NewInMemoryBackend()
			_, err := b.CreateContainerWith("c", azureblob.ContainerOptions{})
			require.NoError(t, err)
			tt.setup(t, b)

			info, got, err := b.ReadTail("c", tt.blob, tt.n)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, string(got))
			assert.Equal(t, tt.wantLen, info.ContentLength)
		})
	}
}

func TestInMemoryBackend_ReadTailMissingContainer(t *testing.T) {
	t.Parallel()

	_, _, err := azureblob.NewInMemoryBackend().ReadTail("nope", "b", 1)
	require.ErrorIs(t, err, azureblob.ErrContainerNotFound)
}
