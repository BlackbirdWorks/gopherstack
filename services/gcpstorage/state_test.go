package gcpstorage_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpstorage"
)

type anyTopic struct{}

func (anyTopic) TopicExists(string) bool                         { return true }
func (anyTopic) Publish(string, []byte, map[string]string) error { return nil }

func rawJSON(s string) json.RawMessage { return json.RawMessage(s) }

func TestJanitorRunsLifecycleOnTheClock(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := gcpstorage.NewInMemoryBackend()

		_, err := b.CreateBucket(sdkProject, gcpstorage.Patch{
			"name":      rawJSON(`"bkt"`),
			"lifecycle": rawJSON(`{"rule":[{"action":{"type":"Delete"},"condition":{"age":1}}]}`),
		})
		require.NoError(t, err)

		_, err = b.PutObject("bkt", "a", []byte("x"), gcpstorage.Patch{}, gcpstorage.Preconds{})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})

		go func() {
			defer close(done)

			b.RunJanitor(ctx, time.Minute)
		}()

		<-time.After(12 * time.Hour)
		synctest.Wait()

		_, err = b.GetObject("bkt", "a", 0, gcpstorage.Preconds{})
		require.NoError(t, err, "younger than a day")

		<-time.After(13 * time.Hour)
		synctest.Wait()

		_, err = b.GetObject("bkt", "a", 0, gcpstorage.Preconds{})
		require.Error(t, err, "expired by the janitor")

		cancel()
		<-done
	})
}

func TestNewStateSurvivesSnapshotRestore(t *testing.T) {
	t.Parallel()

	src := gcpstorage.NewInMemoryBackend()
	src.SetNotifier(anyTopic{})

	_, err := src.CreateBucket(sdkProject, gcpstorage.Patch{
		"name":            rawJSON(`"bkt"`),
		"retentionPolicy": rawJSON(`{"retentionPeriod":"3600"}`),
	})
	require.NoError(t, err)

	_, err = src.LockRetentionPolicy("bkt", 1)
	require.NoError(t, err)

	n, err := src.AddNotification("bkt", gcpstorage.Notification{
		Topic: "//pubsub.googleapis.com/projects/p/topics/t", PayloadFormat: "NONE", EventTypes: []string{"OBJECT_DELETE"},
	})
	require.NoError(t, err)

	key, err := src.CreateHMACKey(sdkProject, saA)
	require.NoError(t, err)

	other, err := src.CreateBucket(sdkProject, gcpstorage.Patch{"name": rawJSON(`"soft"`)})
	require.NoError(t, err)

	put, err := src.PutObject(other.Name, "gone", []byte("x"), gcpstorage.Patch{}, gcpstorage.Preconds{})
	require.NoError(t, err)
	require.NoError(t, src.DeleteObject(other.Name, "gone", 0, gcpstorage.Preconds{}))

	dst := gcpstorage.NewInMemoryBackend()
	require.NoError(t, dst.Restore(t.Context(), src.Snapshot(t.Context())))

	got, err := dst.GetNotification("bkt", n.ID)
	require.NoError(t, err)
	assert.Equal(t, n, got)

	meta, err := dst.GetHMACKey(sdkProject, key.AccessID)
	require.NoError(t, err)
	assert.Equal(t, "ACTIVE", meta.State)

	bk, err := dst.GetBucket("bkt", gcpstorage.Preconds{})
	require.NoError(t, err)
	assert.JSONEq(t, `true`, string(mustField(t, bk.Config["retentionPolicy"], "isLocked")))

	_, err = dst.UpdateBucket("bkt", gcpstorage.Patch{"retentionPolicy": rawJSON(`null`)}, false, gcpstorage.Preconds{})

	var apiErr *gcpstorage.APIError

	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, http.StatusBadRequest, apiErr.Status)

	sd, err := dst.GetSoftDeletedObject(other.Name, "gone", put.Generation)
	require.NoError(t, err)
	assert.False(t, sd.HardDeleteTime.IsZero())

	restored, err := dst.RestoreObject(other.Name, "gone", put.Generation, gcpstorage.Preconds{}, false)
	require.NoError(t, err)
	assert.Greater(t, restored.Generation, put.Generation)
}

func TestHMACSecretSurvivesRestoreAndStillVerifies(t *testing.T) {
	t.Parallel()

	s := newSignEnv(t)
	key, err := s.backend.CreateHMACKey(sdkProject, saA)
	require.NoError(t, err)

	snap := s.backend.Snapshot(t.Context())
	s.backend.Reset()

	_, err = s.backend.GetHMACKey(sdkProject, key.AccessID)
	require.Error(t, err)

	require.NoError(t, s.backend.Restore(t.Context(), snap))

	code, body, _ := s.do(t, http.MethodGet, s.signV4HMAC(http.MethodGet, key.AccessID, key.Secret, time.Now().UTC(), 600), nil)
	assert.Equal(t, http.StatusOK, code, body)
}

func mustField(t *testing.T, raw json.RawMessage, field string) json.RawMessage {
	t.Helper()

	var m map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &m))

	return m[field]
}
