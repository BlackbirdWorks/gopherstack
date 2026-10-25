package iot_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iotjobsdataplane"
	jdtypes "github.com/aws/aws-sdk-go-v2/service/iotjobsdataplane/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

func TestJobExecutionTimeouts(t *testing.T) {
	t.Parallel()

	type step struct {
		stepTimeout *int64
		advance     time.Duration
	}

	mins := func(m int) time.Duration { return time.Duration(m) * time.Minute }

	tests := []struct {
		start        *int64
		name         string
		wantStatus   iot.JobExecutionStatus
		steps        []step
		inProgress   int64
		finalAdvance time.Duration
		viaStartNext bool
	}{
		{name: "in_progress_timer_expires", inProgress: 10, finalAdvance: mins(10), wantStatus: iot.JobExecTimedOut},
		{
			name:         "in_progress_timer_not_yet",
			inProgress:   10,
			finalAdvance: mins(10) - time.Second,
			wantStatus:   iot.JobExecInProgress,
		},
		{name: "no_timers_never_expire", finalAdvance: 24 * 7 * time.Hour, wantStatus: iot.JobExecInProgress},
		{
			name:         "step_timer_from_start_next",
			start:        aws.Int64(5),
			finalAdvance: mins(5),
			wantStatus:   iot.JobExecTimedOut,
		},
		{
			name:  "step_timer_reset_extends",
			start: aws.Int64(5),
			steps: []step{
				{stepTimeout: aws.Int64(5), advance: mins(4)},
			},
			finalAdvance: mins(4),
			wantStatus:   iot.JobExecInProgress,
		},
		{
			name:  "step_timer_reset_then_expires",
			start: aws.Int64(5),
			steps: []step{
				{stepTimeout: aws.Int64(5), advance: mins(4)},
			},
			finalAdvance: mins(5),
			wantStatus:   iot.JobExecTimedOut,
		},
		{
			name:       "step_timer_capped_by_in_progress",
			inProgress: 10,
			start:      aws.Int64(5),
			steps: []step{
				{stepTimeout: aws.Int64(9), advance: mins(4)},
				{stepTimeout: aws.Int64(9), advance: mins(4)},
			},
			finalAdvance: mins(2),
			wantStatus:   iot.JobExecTimedOut,
		},
		{
			name:  "step_timer_cancelled",
			start: aws.Int64(5),
			steps: []step{
				{stepTimeout: aws.Int64(-1), advance: mins(1)},
			},
			finalAdvance: mins(60),
			wantStatus:   iot.JobExecInProgress,
		},
		{
			name:       "cancelled_step_keeps_in_progress_timer",
			inProgress: 10,
			start:      aws.Int64(5),
			steps: []step{
				{stepTimeout: aws.Int64(-1), advance: mins(1)},
			},
			finalAdvance: mins(9),
			wantStatus:   iot.JobExecTimedOut,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := iot.NewInMemoryBackend()
				_, err := b.CreateThing(&iot.CreateThingInput{ThingName: "dev"})
				require.NoError(t, err)

				in := &iot.CreateJobInput{
					JobID: "j", Document: `{}`, Targets: []string{"arn:aws:iot:us-east-1:000000000000:thing/dev"},
				}
				if tt.inProgress > 0 {
					in.TimeoutConfig = &iot.TimeoutConfig{InProgressTimeoutInMinutes: tt.inProgress}
				}

				_, err = b.CreateJob(in)
				require.NoError(t, err)

				exec, found, err := b.StartNextPendingJobExecution("dev", nil, tt.start)
				require.NoError(t, err)
				require.True(t, found)
				assert.Equal(t, iot.JobExecInProgress, exec.Status)

				if tt.inProgress > 0 || tt.start != nil {
					assert.Positive(t, exec.ApproximateSecondsBeforeTimedOut)
				} else {
					assert.Zero(t, exec.ApproximateSecondsBeforeTimedOut)
				}

				for _, s := range tt.steps {
					time.Sleep(s.advance)

					_, err = b.UpdateDeviceJobExecution(&iot.UpdateDeviceJobExecutionInput{
						JobID: "j", ThingName: "dev", Status: "IN_PROGRESS", StepTimeoutMinutes: s.stepTimeout,
					})
					require.NoError(t, err)
				}

				time.Sleep(tt.finalAdvance)

				got, err := b.DescribeDeviceJobExecution("dev", "j", 0, false)
				require.NoError(t, err)
				assert.Equal(t, tt.wantStatus, got.Status)

				job, err := b.DescribeJob("j")
				require.NoError(t, err)

				if tt.wantStatus == iot.JobExecTimedOut {
					assert.Equal(t, iot.JobStatusCompleted, job.Status)
					assert.EqualValues(t, 1, job.JobProcessDetails.NumberOfTimedOutThings)

					_, err = b.UpdateDeviceJobExecution(&iot.UpdateDeviceJobExecutionInput{
						JobID: "j", ThingName: "dev", Status: "SUCCEEDED",
					})
					require.ErrorIs(t, err, iot.ErrTerminalState)
				}
			})
		})
	}
}

func TestStepTimeoutWire(t *testing.T) {
	t.Parallel()

	tests := []struct {
		step    *int64
		name    string
		wantErr string
		wantPos bool
	}{
		{name: "valid", step: aws.Int64(30), wantPos: true},
		{name: "cancel_allowed_on_update", step: aws.Int64(-1)},
		{name: "zero_rejected", step: aws.Int64(0), wantErr: "InvalidRequestException"},
		{name: "above_max_rejected", step: aws.Int64(10081), wantErr: "InvalidRequestException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newCmdEnv(t)
			_, err := e.backend.CreateThing(&iot.CreateThingInput{ThingName: "dev"})
			require.NoError(t, err)
			_, err = e.backend.CreateJob(&iot.CreateJobInput{
				JobID: "j", Document: `{}`, Targets: []string{"arn:aws:iot:us-east-1:000000000000:thing/dev"},
			})
			require.NoError(t, err)

			_, err = e.data.StartNextPendingJobExecution(
				t.Context(),
				&iotjobsdataplane.StartNextPendingJobExecutionInput{
					ThingName: aws.String("dev"),
				},
			)
			require.NoError(t, err)

			_, err = e.data.UpdateJobExecution(t.Context(), &iotjobsdataplane.UpdateJobExecutionInput{
				JobId: aws.String("j"), ThingName: aws.String("dev"), Status: jdtypes.JobExecutionStatusInProgress,
				StepTimeoutInMinutes: tt.step,
			})
			if tt.wantErr != "" {
				assert.Equal(t, tt.wantErr, apiCode(err))

				return
			}

			require.NoError(t, err)

			desc, err := e.data.DescribeJobExecution(t.Context(), &iotjobsdataplane.DescribeJobExecutionInput{
				JobId: aws.String("j"), ThingName: aws.String("dev"),
			})
			require.NoError(t, err)

			secs := aws.ToInt64(desc.Execution.ApproximateSecondsBeforeTimedOut)
			if tt.wantPos {
				assert.InDelta(t, 30*60, secs, 5)
			} else {
				assert.Zero(t, secs)
			}
		})
	}
}
