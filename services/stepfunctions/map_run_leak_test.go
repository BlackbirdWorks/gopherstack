package stepfunctions_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sfn "github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

const mapDefinitionForLeak = `{"StartAt":"M","States":{"M":{"Type":"Map","ItemsPath":"$.items",
"ItemProcessor":{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}},"End":true}}}`

func TestMapRuns_ReleasedWithOwner(t *testing.T) {
	t.Parallel()

	tests := []struct {
		release func(t *testing.T, bk *sfn.InMemoryBackend, smARN string)
		name    string
		smType  string
	}{
		{
			name:   "express_sync_pruned",
			smType: "EXPRESS",
			release: func(_ *testing.T, bk *sfn.InMemoryBackend, _ string) {
				bk.PruneExecutionsForTest(float64(time.Now().Add(10 * time.Second).Unix()))
			},
		},
		{
			name:   "standard_pruned",
			smType: "STANDARD",
			release: func(_ *testing.T, bk *sfn.InMemoryBackend, _ string) {
				bk.PruneExecutionsForTest(float64(time.Now().Add(10 * time.Second).Unix()))
			},
		},
		{
			name:   "state_machine_deleted",
			smType: "STANDARD",
			release: func(t *testing.T, bk *sfn.InMemoryBackend, smARN string) {
				t.Helper()
				require.NoError(t, bk.DeleteStateMachine(smARN))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				bk := sfn.NewInMemoryBackend()
				sm, err := bk.CreateStateMachine(
					context.Background(), "mr-leak", mapDefinitionForLeak,
					"arn:aws:iam::123456789012:role/r", tt.smType,
				)
				require.NoError(t, err)

				var execARN string
				if tt.smType == "EXPRESS" {
					res, runErr := bk.StartSyncExecution(sm.StateMachineArn, "e1", `{"items":[1,2]}`)
					require.NoError(t, runErr)
					execARN = res.ExecutionArn
				} else {
					exec, runErr := bk.StartExecution(sm.StateMachineArn, "e1", `{"items":[1,2]}`)
					require.NoError(t, runErr)
					execARN = exec.ExecutionArn
				}

				synctest.Wait()

				runs, _, listErr := bk.ListMapRuns(execARN, "", 10)
				require.NoError(t, listErr)
				require.Len(t, runs, 1)

				tt.release(t, bk, sm.StateMachineArn)

				_, descErr := bk.DescribeMapRun(runs[0].MapRunArn)
				require.Error(t, descErr)
				assert.ErrorIs(t, descErr, sfn.ErrMapRunDoesNotExist)
			})
		})
	}
}
