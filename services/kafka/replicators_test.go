package kafka_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

func TestCreateReplicator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*kafka.InMemoryBackend)
		name    string
		repName string
		wantErr bool
	}{
		{
			name:    "success",
			repName: "my-replicator",
			setup:   func(_ *kafka.InMemoryBackend) {},
		},
		{
			name:    "duplicate_name",
			repName: "my-replicator",
			setup: func(b *kafka.InMemoryBackend) {
				_, _ = b.CreateReplicator(
					context.Background(),
					"my-replicator",
					"",
					"arn:aws:iam::000000000000:role/my-role",
					nil,
				)
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			tt.setup(b)

			replicator, err := b.CreateReplicator(context.Background(),
				tt.repName,
				"test replicator",
				"arn:aws:iam::000000000000:role/my-role",
				nil,
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.repName, replicator.ReplicatorName)
			assert.NotEmpty(t, replicator.ReplicatorArn)
			assert.Equal(t, kafka.ReplicatorStateRunning, replicator.ReplicatorState)
		})
	}
}

func TestDeleteReplicator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*kafka.InMemoryBackend) string
		name    string
		wantErr bool
	}{
		{
			name: "success",
			setup: func(b *kafka.InMemoryBackend) string {
				r, _ := b.CreateReplicator(
					context.Background(),
					"my-replicator",
					"",
					"arn:aws:iam::000000000000:role/my-role",
					nil,
				)

				return r.ReplicatorArn
			},
		},
		{
			name: "not_found",
			setup: func(_ *kafka.InMemoryBackend) string {
				return "arn:aws:kafka:us-east-1:000000000000:replicator/nonexistent/uuid"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			replicatorArn := tt.setup(b)

			err := b.DeleteReplicator(context.Background(), replicatorArn)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestCreateReplicator_RequiresName(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateReplicator(context.Background(), "", "", "", nil)

	require.Error(t, err)
	require.ErrorIs(t, err, kafka.ErrValidation)
}

func TestNonNilTags_Replicator(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	rep := b.AddReplicatorInternal("rep1")

	assert.NotNil(t, rep.Tags)
}
