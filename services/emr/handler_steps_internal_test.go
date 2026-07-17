package emr // needs access to unexported effectiveStepStatus/cancelStep; named *_internal_test.go per house convention.

import (
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/stretchr/testify/assert"
)

// Test_EffectiveStepStatus covers the PENDING -> COMPLETED promotion that
// stops a step from being stuck in PENDING forever (which would hang a real
// client's StepComplete waiter).
func Test_EffectiveStepStatus(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		wantState string
		status    StepStatus
		wantEnd   bool
	}{
		{
			name: "freshly created step stays pending",
			status: StepStatus{
				State:    StepStatePending,
				Timeline: StepTimeline{CreationDateTime: awstime.Epoch(time.Now())},
			},
			wantState: StepStatePending,
			wantEnd:   false,
		},
		{
			name: "old pending step promotes to completed",
			status: StepStatus{
				State: StepStatePending,
				Timeline: StepTimeline{
					CreationDateTime: awstime.Epoch(time.Now().Add(-2 * stepCompletionDelay)),
				},
			},
			wantState: StepStateCompleted,
			wantEnd:   true,
		},
		{
			name: "cancelled step is left unchanged regardless of age",
			status: StepStatus{
				State: StepStateCancelled,
				Timeline: StepTimeline{
					CreationDateTime: awstime.Epoch(time.Now().Add(-2 * stepCompletionDelay)),
				},
			},
			wantState: StepStateCancelled,
			wantEnd:   false,
		},
		{
			name: "already completed step is left unchanged",
			status: StepStatus{
				State: StepStateCompleted,
				Timeline: StepTimeline{
					CreationDateTime: awstime.Epoch(time.Now().Add(-2 * stepCompletionDelay)),
					EndDateTime:      awstime.Epoch(time.Now()),
				},
			},
			wantState: StepStateCompleted,
			wantEnd:   true,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := effectiveStepStatus(tt.status)
			assert.Equal(t, tt.wantState, got.State)

			if tt.wantEnd {
				assert.NotZero(t, got.Timeline.EndDateTime)
			}
		})
	}
}

// Test_CancelStep verifies CancelSteps reports the real
// CancelStepsRequestStatus enum (SUBMITTED | FAILED), not the fabricated
// "SUCCESS"/"QUEUED" strings this backend used to return.
func Test_CancelStep(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		wantStatus string
		wantState  string
		step       Step
		wantReason bool
	}{
		{
			name: "pending step is submitted for cancellation",
			step: Step{
				ID: "s-1",
				Status: StepStatus{
					State:    StepStatePending,
					Timeline: StepTimeline{CreationDateTime: awstime.Epoch(time.Now())},
				},
			},
			wantStatus: cancelStepsStatusSubmitted,
			wantState:  StepStateCancelled,
		},
		{
			name: "already cancelled step fails to cancel again",
			step: Step{
				ID:     "s-2",
				Status: StepStatus{State: StepStateCancelled},
			},
			wantStatus: cancelStepsStatusFailed,
			wantState:  StepStateCancelled,
			wantReason: true,
		},
		{
			name: "completed step fails to cancel",
			step: Step{
				ID:     "s-3",
				Status: StepStatus{State: StepStateCompleted},
			},
			wantStatus: cancelStepsStatusFailed,
			wantState:  StepStateCompleted,
			wantReason: true,
		},
		{
			name: "step that already auto-completed fails to cancel",
			step: Step{
				ID: "s-4",
				Status: StepStatus{
					State: StepStatePending,
					Timeline: StepTimeline{
						CreationDateTime: awstime.Epoch(time.Now().Add(-2 * stepCompletionDelay)),
					},
				},
			},
			wantStatus: cancelStepsStatusFailed,
			wantState:  StepStatePending, // stored state is untouched; only the effective read promotes it.
			wantReason: true,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := tt.step
			info := cancelStep(&s)

			assert.Equal(t, tt.step.ID, info.StepID)
			assert.Equal(t, tt.wantStatus, info.Status)
			assert.Equal(t, tt.wantState, s.Status.State)

			if tt.wantReason {
				assert.NotEmpty(t, info.Reason)
			} else {
				assert.Empty(t, info.Reason)
			}
		})
	}
}
