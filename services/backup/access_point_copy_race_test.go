package backup_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

// A copy handed out while the access point is CREATING must not alias the metadata
// map the janitor fills in on completion.
func TestBackupAccessPointCopy_NotAliasedToJanitor(t *testing.T) {
	t.Parallel()

	b, _, rpArn := seedBackupAccessPointFixture(t)

	created, err := b.CreateBackupAccessPoint("bap-race", rpArn, "", map[string]string{"k": "v"}, nil)
	require.NoError(t, err)

	held, err := b.DescribeBackupAccessPoint(created.AccessPointArn)
	require.NoError(t, err)

	stop := make(chan struct{})

	var wg sync.WaitGroup

	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				for range held.AccessPointMetadata {
				}
			}
		}
	})

	backup.NewJanitor(b, time.Hour, time.Hour).SweepOnce(t.Context())
	close(stop)
	wg.Wait()

	assert.Equal(t, map[string]string{"k": "v"}, held.AccessPointMetadata)
}
