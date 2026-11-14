package sqs_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sqs"
)

func TestQueueOwner(t *testing.T) {
	t.Parallel()

	const (
		def    = "000000000000"
		member = "111111111111"
		other  = "222222222222"
	)

	tests := []struct {
		name, arnAccount, caller, queue, want string
	}{
		{name: "member_queue_by_member_arn", arnAccount: member, caller: other, queue: "mq", want: member},
		{name: "default_arn_resolves_to_caller_queue", arnAccount: def, caller: member, queue: "mq", want: member},
		{name: "default_arn_default_queue", arnAccount: def, caller: member, queue: "dq", want: def},
		{name: "unknown_default_arn_uses_caller", arnAccount: def, caller: member, queue: "new", want: member},
		{name: "unknown_other_arn_keeps_arn_account", arnAccount: other, caller: member, queue: "new", want: other},
		{name: "default_caller_default_arn", arnAccount: def, caller: def, queue: "dq", want: def},
	}

	b := sqs.NewInMemoryBackendWithConfig(def, "us-east-1")
	t.Cleanup(b.Close)

	_, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "mq", Endpoint: "localhost:4566", Region: member + "@us-east-1",
	})
	require.NoError(t, err)
	_, err = b.CreateQueue(&sqs.CreateQueueInput{QueueName: "dq", Endpoint: "localhost:4566", Region: "us-east-1"})
	require.NoError(t, err)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, b.QueueOwner(tt.arnAccount, tt.caller, "us-east-1", tt.queue))
		})
	}
}
