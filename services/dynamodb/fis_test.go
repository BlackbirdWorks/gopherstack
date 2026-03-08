package dynamodb_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

func newFISDynamoDBHandler() *dynamodb.DynamoDBHandler {
	db := dynamodb.NewInMemoryDB()

	return dynamodb.NewHandler(db)
}

func TestDynamoDB_FISActions(t *testing.T) {
	t.Parallel()

	h := newFISDynamoDBHandler()
	actions := h.FISActions()

	ids := make([]string, len(actions))
	for i, a := range actions {
		ids[i] = a.ActionID
	}

	assert.Contains(t, ids, "aws:dynamodb:global-table-pause-replication")
}

func TestDynamoDB_FISActions_TargetType(t *testing.T) {
	t.Parallel()

	h := newFISDynamoDBHandler()

	actions := h.FISActions()
	require.Len(t, actions, 1)
	assert.Equal(t, "aws:dynamodb:global-table", actions[0].TargetType)
}

func TestDynamoDB_ExecuteFISAction_PauseReplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		targets  []string
		duration time.Duration
		wantErr  bool
	}{
		{
			name:    "single_table_no_duration",
			targets: []string{"arn:aws:dynamodb:us-east-1:000000000000:table/MyTable"},
			wantErr: false,
		},
		{
			name:     "single_table_with_duration",
			targets:  []string{"arn:aws:dynamodb:us-east-1:000000000000:table/TimedTable"},
			duration: 100 * time.Millisecond,
			wantErr:  false,
		},
		{
			name:    "no_targets",
			targets: []string{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			h := dynamodb.NewHandler(db)

			err := h.ExecuteFISAction(context.Background(), service.FISActionExecution{
				ActionID: "aws:dynamodb:global-table-pause-replication",
				Targets:  tt.targets,
				Duration: tt.duration,
			})

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			// Verify replication pause state is recorded.
			if len(tt.targets) > 0 {
				assert.True(t, db.IsReplicationPaused(tt.targets[0]),
					"replication should be marked as paused for target %s", tt.targets[0])
			}

			// Verify the pause clears after the duration.
			if tt.duration > 0 && len(tt.targets) > 0 {
				time.Sleep(tt.duration + 50*time.Millisecond)

				assert.False(t, db.IsReplicationPaused(tt.targets[0]),
					"replication pause should have expired after duration")
			}
		})
	}
}

func TestDynamoDB_ExecuteFISAction_Unknown(t *testing.T) {
	t.Parallel()

	h := newFISDynamoDBHandler()

	err := h.ExecuteFISAction(context.Background(), service.FISActionExecution{
		ActionID: "aws:dynamodb:unknown-action",
		Targets:  []string{"some-table"},
	})

	require.NoError(t, err)
}
