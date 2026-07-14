package emrserverless_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/emrserverless"
)

// --- CreateApplication client-token idempotency ---

func TestInMemoryBackend_CreateApplication_ClientTokenIdempotency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		firstToken   string
		secondToken  string
		wantSameApp  bool
		wantConflict bool
	}{
		{
			name:        "same_token_replays_same_application",
			firstToken:  "tok-1",
			secondToken: "tok-1",
			wantSameApp: true,
		},
		{
			name:         "different_token_same_name_conflicts",
			firstToken:   "tok-a",
			secondToken:  "tok-b",
			wantConflict: true,
		},
		{
			name:         "no_token_on_retry_conflicts",
			firstToken:   "tok-only-first",
			secondToken:  "",
			wantConflict: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")

			first, err := b.CreateApplication("idempotent-app", "SPARK", "emr-6.6.0", "", nil,
				emrserverless.CreateApplicationOptions{ClientToken: tt.firstToken})
			require.NoError(t, err)

			second, err := b.CreateApplication("idempotent-app", "SPARK", "emr-6.6.0", "", nil,
				emrserverless.CreateApplicationOptions{ClientToken: tt.secondToken})

			switch {
			case tt.wantConflict:
				require.ErrorIs(t, err, emrserverless.ErrAlreadyExists)
			case tt.wantSameApp:
				require.NoError(t, err)
				assert.Equal(t, first.ApplicationID, second.ApplicationID)
				assert.Equal(t, first.Arn, second.Arn)
			}

			assert.Equal(t, 1, emrserverless.ApplicationCount(b))
		})
	}
}

// TestInMemoryBackend_CreateApplication_StaleTokenFallsThrough verifies that a
// clientToken pointing at an application which no longer exists (deleted
// after the original create) is treated as a cache miss rather than
// resurrecting a stale reference or erroring.
func TestInMemoryBackend_CreateApplication_StaleTokenFallsThrough(t *testing.T) {
	t.Parallel()

	b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")

	first, err := b.CreateApplication("stale-token-app", "SPARK", "emr-6.6.0", "", nil,
		emrserverless.CreateApplicationOptions{ClientToken: "reused-token"})
	require.NoError(t, err)
	require.NoError(t, b.DeleteApplication(first.ApplicationID))

	second, err := b.CreateApplication("stale-token-app", "SPARK", "emr-6.6.0", "", nil,
		emrserverless.CreateApplicationOptions{ClientToken: "reused-token"})
	require.NoError(t, err)
	assert.NotEqual(t, first.ApplicationID, second.ApplicationID)
}

// --- CreateApplication ExtraConfig passthrough ---

func TestInMemoryBackend_CreateApplication_ExtraConfigPassthrough(t *testing.T) {
	t.Parallel()

	b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")

	extra := map[string]any{
		"maximumCapacity": map[string]any{"cpu": "400 vCPU", "memory": "3000 GB"},
		"initialCapacity": map[string]any{
			"Driver": map[string]any{"workerCount": float64(2)},
		},
	}

	app, err := b.CreateApplication("extra-config-app", "SPARK", "emr-6.6.0", "", nil,
		emrserverless.CreateApplicationOptions{ExtraConfig: extra})
	require.NoError(t, err)
	require.Equal(t, extra["maximumCapacity"], app.ExtraConfig["maximumCapacity"])
	require.Equal(t, extra["initialCapacity"], app.ExtraConfig["initialCapacity"])

	got, err := b.GetApplication(app.ApplicationID)
	require.NoError(t, err)
	assert.Equal(t, extra["maximumCapacity"], got.ExtraConfig["maximumCapacity"])

	// Mutating the caller's map after the call, and mutating the returned
	// clone, must not affect the stored backend state (clone independence).
	extra["maximumCapacity"].(map[string]any)["cpu"] = "mutated"
	got.ExtraConfig["maximumCapacity"].(map[string]any)["cpu"] = "also-mutated"

	again, err := b.GetApplication(app.ApplicationID)
	require.NoError(t, err)
	assert.Equal(t, "400 vCPU", again.ExtraConfig["maximumCapacity"].(map[string]any)["cpu"])
}

// --- StartJobRun client-token idempotency ---

func TestInMemoryBackend_StartJobRun_ClientTokenIdempotency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		firstToken  string
		secondToken string
		wantSameRun bool
	}{
		{
			name:        "same_token_replays_same_job_run",
			firstToken:  "jr-tok-1",
			secondToken: "jr-tok-1",
			wantSameRun: true,
		},
		{
			name:        "different_token_creates_new_run",
			firstToken:  "jr-tok-a",
			secondToken: "jr-tok-b",
			wantSameRun: false,
		},
		{
			name:        "no_token_creates_new_run_each_time",
			firstToken:  "",
			secondToken: "",
			wantSameRun: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
			app, err := b.CreateApplication("jr-token-app", "SPARK", "emr-6.6.0", "", nil)
			require.NoError(t, err)

			first, err := b.StartJobRun(app.ApplicationID, "arn:aws:iam::000000000000:role/r", "run", "", nil,
				emrserverless.StartJobRunOptions{ClientToken: tt.firstToken})
			require.NoError(t, err)

			second, err := b.StartJobRun(app.ApplicationID, "arn:aws:iam::000000000000:role/r", "run", "", nil,
				emrserverless.StartJobRunOptions{ClientToken: tt.secondToken})
			require.NoError(t, err)

			if tt.wantSameRun {
				assert.Equal(t, first.JobRunID, second.JobRunID)
				assert.Equal(t, 1, emrserverless.JobRunCount(b))
			} else {
				assert.NotEqual(t, first.JobRunID, second.JobRunID)
				assert.Equal(t, 2, emrserverless.JobRunCount(b))
			}
		})
	}
}

// --- StartJobRun JobDriver / ConfigurationOverrides passthrough ---

func TestInMemoryBackend_StartJobRun_JobDriverPassthrough(t *testing.T) {
	t.Parallel()

	b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	app, err := b.CreateApplication("jr-driver-app", "SPARK", "emr-6.6.0", "", nil)
	require.NoError(t, err)

	jobDriver := map[string]any{
		"sparkSubmit": map[string]any{
			"entryPoint":            "s3://bucket/job.py",
			"entryPointArguments":   []any{"--input", "s3://bucket/in"},
			"sparkSubmitParameters": "--conf spark.executor.cores=4",
		},
	}
	configOverrides := map[string]any{
		"applicationConfiguration": []any{
			map[string]any{"classification": "spark-defaults", "properties": map[string]any{"k": "v"}},
		},
	}

	jr, err := b.StartJobRun(app.ApplicationID, "arn:aws:iam::000000000000:role/r", "driver-run", "", nil,
		emrserverless.StartJobRunOptions{JobDriver: jobDriver, ConfigurationOverrides: configOverrides})
	require.NoError(t, err)
	assert.Equal(t, jobDriver, jr.JobDriver)
	assert.Equal(t, configOverrides, jr.ConfigurationOverrides)

	got, err := b.GetJobRun(app.ApplicationID, jr.JobRunID)
	require.NoError(t, err)
	assert.Equal(t, jobDriver, got.JobDriver)
	assert.Equal(t, configOverrides, got.ConfigurationOverrides)
}

// TestInMemoryBackend_StartJobRun_NoJobDriverOmitsField verifies that
// StartJobRun without a jobDriver leaves JobRun.JobDriver nil rather than
// fabricating a value, so jobRunToMap omits the key entirely.
func TestInMemoryBackend_StartJobRun_NoJobDriverOmitsField(t *testing.T) {
	t.Parallel()

	b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	app, err := b.CreateApplication("jr-no-driver-app", "SPARK", "emr-6.6.0", "", nil)
	require.NoError(t, err)

	jr, err := b.StartJobRun(app.ApplicationID, "arn:aws:iam::000000000000:role/r", "no-driver-run", "", nil)
	require.NoError(t, err)
	assert.Nil(t, jr.JobDriver)
	assert.Nil(t, jr.ConfigurationOverrides)
}
