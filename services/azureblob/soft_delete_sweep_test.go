package azureblob_test

import (
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azureblob"
)

func TestSoftDelete_ExpiredEntriesAreSwept(t *testing.T) {
	t.Parallel()

	var offset atomic.Int64

	clock := func() time.Time { return time.Unix(1_700_000_000+offset.Load(), 0).UTC() }
	b := azureblob.NewInMemoryBackendWithClock(clock)

	days := 1
	require.NoError(t, b.SetServiceProperties(azureblob.ServiceProperties{
		DeleteRetentionPolicy: &azureblob.RetentionPolicy{Enabled: true, Days: &days},
	}))

	_, err := b.CreateContainerWith("c", azureblob.ContainerOptions{})
	require.NoError(t, err)

	remove := func(name string) {
		_, perr := b.PutBlobWith("c", name, []byte("x"), azureblob.BlobAttrs{})
		require.NoError(t, perr)
		require.NoError(t, b.DeleteBlobAt("c", name, "", ""))
	}

	for i := range 100 {
		remove("old" + strconv.Itoa(i))
	}

	offset.Store(int64(3 * 24 * time.Hour / time.Second))

	for i := range 200 {
		remove("new" + strconv.Itoa(i))
	}

	assert.Equal(t, 200, b.DeletedCount("c"), "the expired entries are purged by the sweep")
}
