package integration_test

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/sqs"
	"github.com/blackbirdworks/gopherstack/ssm"
)

func TestPersistence_FileStore_RoundTrip_SQS(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store, err := persistence.NewFileStore(dir)
	require.NoError(t, err)

	// Create SQS backend and add a queue.
	backend := sqs.NewInMemoryBackend()
	out, err := backend.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "test-queue",
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.QueueURL)

	// Snapshot and save.
	snap := backend.Snapshot()
	require.NotEmpty(t, snap)
	require.NoError(t, store.Save("sqs", "snapshot", snap))

	// Load snapshot from store and restore into a fresh backend.
	data, err := store.Load("sqs", "snapshot")
	require.NoError(t, err)

	fresh := sqs.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(data))

	// Verify queue exists in fresh backend.
	listOut, err := fresh.ListQueues(&sqs.ListQueuesInput{})
	require.NoError(t, err)
	require.Len(t, listOut.QueueURLs, 1)
	assert.Contains(t, listOut.QueueURLs[0], "test-queue")
}

func TestPersistence_FileStore_RoundTrip_SSM(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store, err := persistence.NewFileStore(dir)
	require.NoError(t, err)

	// Create SSM backend and add a parameter.
	backend := ssm.NewInMemoryBackend()
	_, err = backend.PutParameter(&ssm.PutParameterInput{
		Name:  "/app/config",
		Value: "hello-world",
		Type:  "String",
	})
	require.NoError(t, err)

	// Snapshot and save.
	snap := backend.Snapshot()
	require.NotEmpty(t, snap)
	require.NoError(t, store.Save("ssm", "snapshot", snap))

	// Load snapshot from store and restore into a fresh backend.
	data, err := store.Load("ssm", "snapshot")
	require.NoError(t, err)

	fresh := ssm.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(data))

	// Verify parameter exists in fresh backend.
	getOut, err := fresh.GetParameter(&ssm.GetParameterInput{
		Name: "/app/config",
	})
	require.NoError(t, err)
	assert.Equal(t, "hello-world", getOut.Parameter.Value)
}

func TestPersistence_Manager_SaveRestore(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store, err := persistence.NewFileStore(dir)
	require.NoError(t, err)

	log := slog.Default()
	manager := persistence.NewManager(store, log)
	ctx := t.Context()

	// Set up SQS backend with a queue.
	sqsBackend := sqs.NewInMemoryBackend()
	_, err = sqsBackend.CreateQueue(&sqs.CreateQueueInput{QueueName: "managed-queue"})
	require.NoError(t, err)

	// Set up SSM backend with a parameter.
	ssmBackend := ssm.NewInMemoryBackend()
	_, err = ssmBackend.PutParameter(&ssm.PutParameterInput{
		Name:  "/managed/param",
		Value: "managed-value",
		Type:  "String",
	})
	require.NoError(t, err)

	manager.Register("SQS", sqsBackend)
	manager.Register("SSM", ssmBackend)
	manager.SaveAll(ctx)

	// Create a new manager with the same store; restore into fresh backends.
	freshSQS := sqs.NewInMemoryBackend()
	freshSSM := ssm.NewInMemoryBackend()

	manager2 := persistence.NewManager(store, log)
	manager2.Register("SQS", freshSQS)
	manager2.Register("SSM", freshSSM)
	manager2.RestoreAll(ctx)

	// Verify SQS queue restored.
	listOut, err := freshSQS.ListQueues(&sqs.ListQueuesInput{})
	require.NoError(t, err)
	require.Len(t, listOut.QueueURLs, 1)
	assert.Contains(t, listOut.QueueURLs[0], "managed-queue")

	// Verify SSM parameter restored.
	getOut, err := freshSSM.GetParameter(&ssm.GetParameterInput{Name: "/managed/param"})
	require.NoError(t, err)
	assert.Equal(t, "managed-value", getOut.Parameter.Value)
}

func TestPersistence_FileStore_KeyNotFound(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store, err := persistence.NewFileStore(dir)
	require.NoError(t, err)

	_, err = store.Load("nonexistent", "key")
	require.ErrorIs(t, err, persistence.ErrKeyNotFound)
}

func TestPersistence_FileStore_ListKeys(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store, err := persistence.NewFileStore(dir)
	require.NoError(t, err)

	require.NoError(t, store.Save("svc", "key1", []byte(`{}`)))
	require.NoError(t, store.Save("svc", "key2", []byte(`{}`)))

	keys, err := store.ListKeys("svc")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"key1", "key2"}, keys)
}

func TestPersistence_FileStore_Delete(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store, err := persistence.NewFileStore(dir)
	require.NoError(t, err)

	require.NoError(t, store.Save("svc", "k", []byte(`{}`)))
	require.NoError(t, store.Delete("svc", "k"))

	_, err = store.Load("svc", "k")
	require.ErrorIs(t, err, persistence.ErrKeyNotFound)
}

func TestPersistence_NullStore_NoOp(t *testing.T) {
	t.Parallel()

	var s persistence.Store = persistence.NullStore{}

	require.NoError(t, s.Save("svc", "k", []byte(`{}`)))

	_, err := s.Load("svc", "k")
	require.ErrorIs(t, err, persistence.ErrKeyNotFound)

	require.NoError(t, s.Delete("svc", "k"))

	keys, err := s.ListKeys("svc")
	require.NoError(t, err)
	assert.Empty(t, keys)
}
