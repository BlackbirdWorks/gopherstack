package cloudfrontkeyvaluestore_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

func TestKeyValueStore_DataPlaneWritesAdvanceLastModified(t *testing.T) {
	t.Parallel()

	tests := []struct {
		write func(b *cloudfront.InMemoryBackend, id string) error
		name  string
	}{
		{
			name: "put",
			write: func(b *cloudfront.InMemoryBackend, id string) error {
				_, err := b.PutKVSValue(id, "k", "v", "")

				return err
			},
		},
		{
			name: "update keys",
			write: func(b *cloudfront.InMemoryBackend, id string) error {
				_, err := b.UpdateKVSValues(id, "", []*cloudfront.KVSItem{{Key: "k", Value: "v"}}, nil)

				return err
			},
		},
		{
			name: "delete",
			write: func(b *cloudfront.InMemoryBackend, id string) error {
				_, err := b.DeleteKVSValue(id, "k", "")

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
				defer b.Close()

				kvs, err := b.CreateKeyValueStore("kvs", "", nil)
				require.NoError(t, err)

				time.Sleep(2 * time.Second)
				require.NoError(t, tt.write(b, kvs.ID))

				got, err := b.GetKeyValueStore(kvs.ID)
				require.NoError(t, err)
				assert.NotEqual(t, kvs.LastModifiedTime, got.LastModifiedTime)
			})
		})
	}
}
