package gcpfirestore_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpfirestore"
)

type eventLog struct {
	events []gcpfirestore.DocumentEvent
	mu     sync.Mutex
}

func (l *eventLog) add(ev gcpfirestore.DocumentEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.events = append(l.events, ev)
}

func (l *eventLog) snapshot() []gcpfirestore.DocumentEvent {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]gcpfirestore.DocumentEvent(nil), l.events...)
}

func TestObserveDocuments(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	log := &eventLog{}
	e.handler.Backend.ObserveDocuments(log.add)

	ctx := t.Context()
	ref := e.client.Collection("users").Doc("alice")

	_, err := ref.Create(ctx, map[string]any{"name": "Alice", "age": 30})
	require.NoError(t, err)
	_, err = ref.Update(ctx, []firestore.Update{{Path: "age", Value: 31}})
	require.NoError(t, err)
	_, err = ref.Update(ctx, []firestore.Update{{Path: "age", Value: 31}})
	require.NoError(t, err)
	_, err = ref.Delete(ctx)
	require.NoError(t, err)

	got := log.snapshot()
	require.Len(t, got, 3, "a write that changes nothing is not an event")

	tests := []struct {
		kind       gcpfirestore.DocumentEventKind
		updateMask []string
		hasOld     bool
		hasNew     bool
	}{
		{gcpfirestore.DocumentCreated, nil, false, true},
		{gcpfirestore.DocumentUpdated, []string{"age"}, true, true},
		{gcpfirestore.DocumentDeleted, nil, true, false},
	}

	for i, tt := range tests {
		ev := got[i]
		assert.Equal(t, tt.kind, ev.Kind)
		assert.Equal(t, tt.hasOld, ev.Old != nil)
		assert.Equal(t, tt.hasNew, ev.New != nil)
		assert.Equal(t, tt.updateMask, ev.UpdateMask)
		assert.WithinDuration(t, time.Now(), ev.Time, time.Minute)
	}

	assert.Equal(t, docRoot+"/users/alice", got[0].New.GetName())
	assert.Equal(t, int64(31), got[1].New.GetFields()["age"].GetIntegerValue())
	assert.Equal(t, int64(30), got[1].Old.GetFields()["age"].GetIntegerValue())
}

func TestObserveDocumentsTransaction(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	log := &eventLog{}
	e.handler.Backend.ObserveDocuments(log.add)

	err := e.client.RunTransaction(t.Context(), func(_ context.Context, tx *firestore.Transaction) error {
		if serr := tx.Set(e.client.Collection("a").Doc("1"), map[string]any{"v": 1}); serr != nil {
			return serr
		}

		return tx.Set(e.client.Collection("a").Doc("2"), map[string]any{"v": 2})
	})
	require.NoError(t, err)

	got := log.snapshot()
	require.Len(t, got, 2)

	for _, ev := range got {
		assert.Equal(t, gcpfirestore.DocumentCreated, ev.Kind)
	}
}
