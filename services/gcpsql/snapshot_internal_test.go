package gcpsql

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshotEncodesBackupsOutsideTheLock(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := NewInMemoryBackend()
		defer b.Close()

		b.projBackups["p/1"] = &projBackup{
			Project: "p", ID: "1", State: backupStateOK, Dump: []byte("dump"), Sum: "x",
		}

		acquired := make(chan struct{})
		b.snapshotHook = func() {
			go func() {
				b.mu.Lock("test")
				b.mu.Unlock()
				close(acquired)
			}()

			select {
			case <-acquired:
			case <-time.After(time.Second):
				t.Error("the backend write lock stayed held while the backups were encoded")
			}
		}

		data := b.Snapshot(t.Context())
		require.NotEmpty(t, data)

		other := NewInMemoryBackend()
		defer other.Close()
		require.NoError(t, other.Restore(t.Context(), data))
		assert.Equal(t, []byte("dump"), other.projBackups["p/1"].Dump)
		assert.Equal(t, "x", other.projBackups["p/1"].Sum)
	})
}
