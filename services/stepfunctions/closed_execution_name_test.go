package stepfunctions_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	sfn "github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

func TestStartExecution_ClosedNameReservedAfterPrune(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		smType   string
		wait     time.Duration
		wantFree bool
	}{
		{name: "standard_within_90_days", smType: "STANDARD", wait: 24 * time.Hour, wantFree: false},
		{name: "standard_after_90_days", smType: "STANDARD", wait: 91 * 24 * time.Hour, wantFree: true},
		{name: "express_reusable", smType: "EXPRESS", wait: time.Second, wantFree: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				bk := sfn.NewInMemoryBackend()
				sm, err := bk.CreateStateMachine(
					context.Background(), "reuse", passDefinition, "arn:aws:iam::123456789012:role/r", tt.smType,
				)
				require.NoError(t, err)

				_, err = bk.StartExecution(sm.StateMachineArn, "e1", `{}`)
				require.NoError(t, err)
				synctest.Wait()

				time.Sleep(tt.wait)
				bk.PruneExecutionsForTest(float64(time.Now().Add(10 * time.Second).Unix()))

				_, err = bk.StartExecution(sm.StateMachineArn, "e1", `{}`)
				if tt.wantFree {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, sfn.ErrExecutionAlreadyExists)
				}
				synctest.Wait()
			})
		})
	}
}
