package iot_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

func TestJobExecutionRetries(t *testing.T) {
	t.Parallel()

	failedRetries := func(typ string, n int32) *iot.JobExecutionsRetryConfig {
		return &iot.JobExecutionsRetryConfig{CriteriaList: []iot.RetryCriteria{{FailureType: typ, NumberOfRetries: n}}}
	}

	tests := []struct {
		cfg         *iot.JobExecutionsRetryConfig
		name        string
		wantStatus  iot.JobExecutionStatus
		wantJob     iot.JobStatus
		failures    []string
		wantNumber  int64
		wantAttempt int32
	}{
		{
			name: "no config", failures: []string{"FAILED"},
			wantStatus: iot.JobExecFailed, wantJob: iot.JobStatusCompleted, wantNumber: 1,
		},
		{
			name:     "failed retried once",
			cfg:      failedRetries("FAILED", 2),
			failures: []string{"FAILED"}, wantStatus: iot.JobExecQueued, wantJob: iot.JobStatusInProgress,
			wantNumber: 2, wantAttempt: 1,
		},
		{
			name:     "failed exhausted",
			cfg:      failedRetries("FAILED", 2),
			failures: []string{"FAILED", "FAILED", "FAILED"}, wantStatus: iot.JobExecFailed,
			wantJob: iot.JobStatusCompleted, wantNumber: 3, wantAttempt: 2,
		},
		{
			name:     "timed out criterion ignores failed",
			cfg:      failedRetries("TIMED_OUT", 2),
			failures: []string{"FAILED"}, wantStatus: iot.JobExecFailed, wantJob: iot.JobStatusCompleted, wantNumber: 1,
		},
		{
			name:     "all covers both types",
			cfg:      failedRetries("ALL", 2),
			failures: []string{"FAILED", "TIMED_OUT", "FAILED"}, wantStatus: iot.JobExecFailed,
			wantJob: iot.JobStatusCompleted, wantNumber: 3, wantAttempt: 2,
		},
		{
			name:       "rejected is never retried",
			cfg:        failedRetries("ALL", 2),
			failures:   []string{"REJECTED"},
			wantStatus: iot.JobExecRejected, wantJob: iot.JobStatusCompleted, wantNumber: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := iot.NewInMemoryBackend()
				_, err := b.CreateThing(&iot.CreateThingInput{ThingName: "dev"})
				require.NoError(t, err)

				_, err = b.CreateJob(&iot.CreateJobInput{
					JobID: "j", Document: `{}`, Targets: []string{"arn:aws:iot:us-east-1:000000000000:thing/dev"},
					TimeoutConfig:            &iot.TimeoutConfig{InProgressTimeoutInMinutes: 5},
					JobExecutionsRetryConfig: tt.cfg,
				})
				require.NoError(t, err)

				for _, f := range tt.failures {
					_, found, serr := b.StartNextPendingJobExecution("dev", nil, nil)
					require.NoError(t, serr)
					require.True(t, found)

					if f == "TIMED_OUT" {
						time.Sleep(5 * time.Minute)

						continue
					}

					_, err = b.UpdateDeviceJobExecution(&iot.UpdateDeviceJobExecutionInput{
						JobID: "j", ThingName: "dev", Status: f,
					})
					require.NoError(t, err)
				}

				execs := b.ListJobExecutionsForJob("j")
				require.Len(t, execs, 1)
				assert.Equal(t, tt.wantStatus, execs[0].Status)
				assert.Equal(t, tt.wantNumber, execs[0].ExecutionNumber)
				assert.Equal(t, tt.wantAttempt, execs[0].RetryAttempt)

				job, err := b.DescribeJob("j")
				require.NoError(t, err)
				assert.Equal(t, tt.wantJob, job.Status)
			})
		})
	}
}

func TestJobRetryStateSurvivesRestore(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()
	_, err := b.CreateThing(&iot.CreateThingInput{ThingName: "dev"})
	require.NoError(t, err)

	_, err = b.CreateJob(&iot.CreateJobInput{
		JobID: "j", Document: `{}`, Targets: []string{"arn:aws:iot:us-east-1:000000000000:thing/dev"},
		JobExecutionsRetryConfig: &iot.JobExecutionsRetryConfig{
			CriteriaList: []iot.RetryCriteria{{FailureType: "FAILED", NumberOfRetries: 1}},
		},
	})
	require.NoError(t, err)

	_, _, err = b.StartNextPendingJobExecution("dev", nil, nil)
	require.NoError(t, err)

	_, err = b.UpdateDeviceJobExecution(
		&iot.UpdateDeviceJobExecutionInput{JobID: "j", ThingName: "dev", Status: "FAILED"},
	)
	require.NoError(t, err)

	restored := iot.NewInMemoryBackend()
	require.NoError(t, restored.Restore(t.Context(), b.Snapshot(t.Context())))

	execs := restored.ListJobExecutionsForJob("j")
	require.Len(t, execs, 1)
	assert.Equal(t, int32(1), execs[0].RetryAttempt)
	assert.Equal(t, iot.JobExecQueued, execs[0].Status)

	_, _, err = restored.StartNextPendingJobExecution("dev", nil, nil)
	require.NoError(t, err)

	_, err = restored.UpdateDeviceJobExecution(
		&iot.UpdateDeviceJobExecutionInput{JobID: "j", ThingName: "dev", Status: "FAILED"},
	)
	require.NoError(t, err)
	assert.Equal(t, iot.JobExecFailed, restored.ListJobExecutionsForJob("j")[0].Status)
}

func TestRetryConfigValidation(t *testing.T) {
	t.Parallel()

	crit := func(typ string, n int32) iot.RetryCriteria {
		return iot.RetryCriteria{FailureType: typ, NumberOfRetries: n}
	}

	tests := []struct {
		name    string
		list    []iot.RetryCriteria
		wantErr bool
	}{
		{name: "zero retries ok", list: []iot.RetryCriteria{crit("FAILED", 0)}},
		{name: "max retries ok", list: []iot.RetryCriteria{crit("ALL", 10), crit("FAILED", 1)}},
		{name: "eleven", list: []iot.RetryCriteria{crit("FAILED", 11)}, wantErr: true},
		{name: "negative", list: []iot.RetryCriteria{crit("FAILED", -1)}, wantErr: true},
		{name: "bad type", list: []iot.RetryCriteria{crit("REJECTED", 1)}, wantErr: true},
		{name: "empty list", list: []iot.RetryCriteria{}, wantErr: true},
		{
			name:    "three criteria",
			list:    []iot.RetryCriteria{crit("FAILED", 1), crit("TIMED_OUT", 1), crit("ALL", 1)},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iot.NewInMemoryBackend()
			cfg := &iot.JobExecutionsRetryConfig{CriteriaList: tt.list}

			_, err := b.CreateJob(&iot.CreateJobInput{JobID: "j", Document: `{}`, JobExecutionsRetryConfig: cfg})
			upErr := b.UpdateJob("j", &iot.UpdateJobInput{JobExecutionsRetryConfig: cfg})
			_, tErr := b.CreateJobTemplate(&iot.CreateJobTemplateInput{
				JobTemplateID: "t", Document: `{}`, JobExecutionsRetryConfig: cfg,
			})

			if tt.wantErr {
				require.ErrorIs(t, err, iot.ErrValidation)
				require.Error(t, upErr)
				require.ErrorIs(t, tErr, iot.ErrValidation)

				return
			}

			require.NoError(t, err)
			require.NoError(t, upErr)
			require.NoError(t, tErr)
		})
	}
}

func TestRetryNotAfterJobCanceled(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()
	_, err := b.CreateThing(&iot.CreateThingInput{ThingName: "dev"})
	require.NoError(t, err)

	_, err = b.CreateJob(&iot.CreateJobInput{
		JobID: "j", Document: `{}`, Targets: []string{"arn:aws:iot:us-east-1:000000000000:thing/dev"},
		JobExecutionsRetryConfig: &iot.JobExecutionsRetryConfig{
			CriteriaList: []iot.RetryCriteria{{FailureType: "FAILED", NumberOfRetries: 3}},
		},
	})
	require.NoError(t, err)

	_, _, err = b.StartNextPendingJobExecution("dev", nil, nil)
	require.NoError(t, err)

	_, err = b.CancelJob("j", "", "", false)
	require.NoError(t, err)

	_, err = b.UpdateDeviceJobExecution(
		&iot.UpdateDeviceJobExecutionInput{JobID: "j", ThingName: "dev", Status: "FAILED"},
	)
	require.NoError(t, err)

	execs := b.ListJobExecutionsForJob("j")
	require.Len(t, execs, 1)
	assert.Equal(t, iot.JobExecFailed, execs[0].Status)
}
