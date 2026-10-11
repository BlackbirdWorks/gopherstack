package sqs_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sqs"
)

func TestCreateQueueDefaultEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		def      string
		endpoint string
		want     string
	}{
		{name: "default_used", def: "localhost:8000", want: "http://localhost:8000/000000000000/q"},
		{name: "request_wins", def: "localhost:8000", endpoint: "h:1", want: "http://h:1/000000000000/q"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sqs.NewInMemoryBackend()
			t.Cleanup(b.Close)
			b.SetDefaultEndpoint(tt.def)

			out, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "q", Endpoint: tt.endpoint})
			require.NoError(t, err)
			assert.Equal(t, tt.want, out.QueueURL)
		})
	}
}
