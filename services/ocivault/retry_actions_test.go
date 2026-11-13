package ocivault_test

import (
	"sync"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/keymanagement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetryTokenOnActions(t *testing.T) {
	t.Parallel()

	t.Run("create key version", func(t *testing.T) {
		t.Parallel()

		se := newSecretEnv(t)
		create := func() string {
			r, err := se.m.CreateKeyVersion(t.Context(), keymanagement.CreateKeyVersionRequest{
				KeyId: se.key.Id, OpcRetryToken: new("ckv"),
			})
			require.NoError(t, err)

			return *r.Id
		}

		first := create()
		assert.Equal(t, first, create())
	})

	t.Run("schedule key deletion", func(t *testing.T) {
		t.Parallel()

		se := newSecretEnv(t)

		for range 2 {
			_, err := se.m.ScheduleKeyDeletion(t.Context(), keymanagement.ScheduleKeyDeletionRequest{
				KeyId: se.key.Id, OpcRetryToken: new("skd"),
				ScheduleKeyDeletionDetails: keymanagement.ScheduleKeyDeletionDetails{},
			})
			require.NoError(t, err)
		}
	})

	t.Run("concurrent duplicates create once", func(t *testing.T) {
		t.Parallel()

		se := newSecretEnv(t)
		ids := make(chan string, 8)

		var wg sync.WaitGroup

		for range cap(ids) {
			wg.Go(func() {
				r, err := se.m.CreateKeyVersion(t.Context(), keymanagement.CreateKeyVersionRequest{
					KeyId: se.key.Id, OpcRetryToken: new("dup"),
				})
				if assert.NoError(t, err) {
					ids <- *r.Id
				}
			})
		}

		wg.Wait()
		close(ids)

		seen := map[string]bool{}
		for id := range ids {
			seen[id] = true
		}

		assert.Len(t, seen, 1)

		list, err := se.m.ListKeyVersions(t.Context(), keymanagement.ListKeyVersionsRequest{KeyId: se.key.Id})
		require.NoError(t, err)
		assert.Len(t, list.Items, 2)
	})
}
