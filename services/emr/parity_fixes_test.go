package emr //nolint:testpackage // needs access to unexported effectiveStepStatus/cancelStep/applyInstanceFleetMod.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
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

// Test_ApplyInstanceFleetMod verifies ModifyInstanceFleet's mutation helper
// actually updates target/provisioned capacities -- the backend previously
// looked up the fleet and returned nil without changing anything.
func Test_ApplyInstanceFleetMod(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		mod          InstanceFleetModification
		fleet        InstanceFleet
		wantOnDemand int
		wantSpot     int
		wantProvOD   int
		wantProvSpot int
	}{
		{
			name:         "sets on-demand capacity only",
			fleet:        InstanceFleet{TargetOnDemandCapacity: 1, TargetSpotCapacity: 2},
			mod:          InstanceFleetModification{TargetOnDemandCapacity: 5},
			wantOnDemand: 5,
			wantSpot:     2,
			wantProvOD:   5,
			wantProvSpot: 0,
		},
		{
			name:         "sets spot capacity only",
			fleet:        InstanceFleet{TargetOnDemandCapacity: 1, TargetSpotCapacity: 2},
			mod:          InstanceFleetModification{TargetSpotCapacity: 7},
			wantOnDemand: 1,
			wantSpot:     7,
			wantProvOD:   0,
			wantProvSpot: 7,
		},
		{
			name:         "sets both capacities",
			fleet:        InstanceFleet{},
			mod:          InstanceFleetModification{TargetOnDemandCapacity: 3, TargetSpotCapacity: 4},
			wantOnDemand: 3,
			wantSpot:     4,
			wantProvOD:   3,
			wantProvSpot: 4,
		},
		{
			name:         "zero mod leaves existing capacities untouched",
			fleet:        InstanceFleet{TargetOnDemandCapacity: 9, TargetSpotCapacity: 9},
			mod:          InstanceFleetModification{},
			wantOnDemand: 9,
			wantSpot:     9,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := tt.fleet
			applyInstanceFleetMod(&f, tt.mod)

			assert.Equal(t, tt.wantOnDemand, f.TargetOnDemandCapacity)
			assert.Equal(t, tt.wantSpot, f.TargetSpotCapacity)

			if tt.mod.TargetOnDemandCapacity > 0 {
				assert.Equal(t, tt.wantProvOD, f.ProvisionedOnDemandCapacity)
			}

			if tt.mod.TargetSpotCapacity > 0 {
				assert.Equal(t, tt.wantProvSpot, f.ProvisionedSpotCapacity)
			}
		})
	}
}
