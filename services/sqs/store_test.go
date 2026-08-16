package sqs_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sqs"
)

const testEndpoint = "localhost:4566"

func newBackend(t *testing.T) *sqs.InMemoryBackend {
	t.Helper()

	b := sqs.NewInMemoryBackend()
	t.Cleanup(b.Close)

	return b
}

func createTestQueue(t *testing.T, b *sqs.InMemoryBackend, name string) string {
	t.Helper()

	out, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: name,
		Endpoint:  testEndpoint,
	})
	require.NoError(t, err)

	return out.QueueURL
}

func queueURL(name string) string {
	return fmt.Sprintf("http://%s/000000000000/%s", testEndpoint, name)
}

func TestListAll(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	createTestQueue(t, b, "q1")
	createTestQueue(t, b, "q2")

	queues := b.ListAll()
	assert.Len(t, queues, 2)
}

func TestBackendReset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "reset_clears_all_queues"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			createTestQueue(t, b, "q1")
			createTestQueue(t, b, "q2")

			b.Reset()

			out, err := b.ListQueues(&sqs.ListQueuesInput{})
			require.NoError(t, err)
			assert.Empty(t, out.QueueURLs, tt.name)
		})
	}
}

// TestStorageBackendInterface verifies var_ assertion compiles.
func TestStorageBackendInterface(t *testing.T) {
	t.Parallel()

	var _ sqs.StorageBackend = (*sqs.InMemoryBackend)(nil)
}
