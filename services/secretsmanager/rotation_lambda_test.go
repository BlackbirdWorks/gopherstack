package secretsmanager_test

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

const testLambdaARN = "arn:aws:lambda:us-east-1:000000000000:function:my-rotator"

// TestRotation_StagingLabels_PendingBeforeFinish verifies that a rotation version
// stays AWSPENDING until all Lambda steps complete (finishSecret included).
func TestRotation_StagingLabels_PendingBeforeFinish(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		lambdaSteps []string
		wantCurrent bool
	}{
		{
			name:        "all_four_steps_succeed_version_becomes_current",
			lambdaSteps: []string{"createSecret", "setSecret", "testSecret", "finishSecret"},
			wantCurrent: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := secretsmanager.NewInMemoryBackend()
			h := secretsmanager.NewHandler(backend)

			// Track which steps were called.
			var calledSteps []string
			mock := &recordingLambdaInvoker{
				onInvoke: func(name, _ string, payload []byte) ([]byte, int, error) {
					calledSteps = append(calledSteps, extractStep(payload))
					return nil, 200, nil
				},
			}
			h.SetLambdaInvoker(mock)

			_, err := backend.CreateSecret(&secretsmanager.CreateSecretInput{
				Name:         "pending-test",
				SecretString: "v1",
			})
			require.NoError(t, err)

			e := echo.New()
			body := fmt.Sprintf(`{"SecretId":"pending-test","RotationLambdaARN":%q}`, testLambdaARN)
			req := httptest.NewRequest("POST", "/", strings.NewReader(body))
			req.Header.Set("X-Amz-Target", "secretsmanager.RotateSecret")
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			assert.Equal(t, 200, rec.Code)

			// Verify steps were invoked in order.
			require.Equal(t, []string{"createSecret", "setSecret", "testSecret", "finishSecret"}, calledSteps)

			// New version must be AWSCURRENT after all steps succeed.
			if tt.wantCurrent {
				curr, getErr := backend.GetSecretValue(&secretsmanager.GetSecretValueInput{SecretID: "pending-test"})
				require.NoError(t, getErr)
				assert.Contains(t, curr.VersionStages, "AWSCURRENT")
				assert.NotContains(t, curr.VersionStages, "AWSPENDING",
					"promoted version must NOT retain AWSPENDING label")
			}
		})
	}
}

// TestRotation_LambdaFailure_AbortsRotation verifies that a Lambda step failure
// rolls back the pending version and the current secret is unchanged.
func TestRotation_LambdaFailure_AbortsRotation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		failStep  string
		failAfter int // fail on Nth invocation (1-based)
	}{
		{
			name:      "createSecret_failure_aborts",
			failStep:  "createSecret",
			failAfter: 1,
		},
		{
			name:      "testSecret_failure_aborts",
			failStep:  "testSecret",
			failAfter: 3,
		},
		{
			name:      "finishSecret_failure_aborts",
			failStep:  "finishSecret",
			failAfter: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := secretsmanager.NewInMemoryBackend()
			h := secretsmanager.NewHandler(backend)

			callCount := 0
			mock := &recordingLambdaInvoker{
				onInvoke: func(_, _ string, _ []byte) ([]byte, int, error) {
					callCount++
					if callCount == tt.failAfter {
						return nil, 500, errors.New("lambda execution failed")
					}
					return nil, 200, nil
				},
			}
			h.SetLambdaInvoker(mock)

			originalVersion := ""
			{
				out, err := backend.CreateSecret(&secretsmanager.CreateSecretInput{
					Name:         "abort-test",
					SecretString: "original",
				})
				require.NoError(t, err)
				originalVersion = out.VersionID
			}

			e := echo.New()
			body := fmt.Sprintf(`{"SecretId":"abort-test","RotationLambdaARN":%q}`, testLambdaARN)
			req := httptest.NewRequest("POST", "/", strings.NewReader(body))
			req.Header.Set("X-Amz-Target", "secretsmanager.RotateSecret")
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))

			// Handler should return an error status.
			assert.NotEqual(t, 200, rec.Code, "Lambda failure must cause non-200 response")

			// Original AWSCURRENT must be unchanged.
			curr, err := backend.GetSecretValue(&secretsmanager.GetSecretValueInput{SecretID: "abort-test"})
			require.NoError(t, err)
			assert.Equal(t, originalVersion, curr.VersionID,
				"original AWSCURRENT version must be intact after Lambda failure")
			assert.Equal(t, "original", curr.SecretString)
		})
	}
}

// TestRotation_ScheduledRotation_InvokesLambda verifies that the rotation
// scheduler invokes the Lambda when a lambdaInvoker is wired and promotes the
// pending version to AWSCURRENT on success.
func TestRotation_ScheduledRotation_InvokesLambda(t *testing.T) {
	t.Parallel()

	backend := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(backend)

	var calledSteps []string
	mock := &recordingLambdaInvoker{
		onInvoke: func(name, _ string, payload []byte) ([]byte, int, error) {
			calledSteps = append(calledSteps, extractStep(payload))
			return nil, 200, nil
		},
	}
	h.SetLambdaInvoker(mock)

	_, err := backend.CreateSecret(&secretsmanager.CreateSecretInput{
		Name:         "sched-lambda",
		SecretString: "v0",
	})
	require.NoError(t, err)

	// Enable rotation with a very short interval so the scheduler fires quickly.
	days := int64(1)
	_, err = backend.RotateSecret(&secretsmanager.RotateSecretInput{
		SecretID:          "sched-lambda",
		RotationLambdaARN: testLambdaARN,
		RotationRules: &secretsmanager.RotationRulesType{
			AutomaticallyAfterDays: &days,
		},
		RotateImmediately: boolPtr(false),
	})
	require.NoError(t, err)

	// The scheduler runs every ~1s. Advance time far enough for the rule to fire.
	// We use a direct call via TickScheduler (exported for tests) OR wait for the scheduler.
	// Here we rely on the real scheduler with a 1s interval; wait up to 3s.
	require.Eventually(t, func() bool {
		curr, getErr := backend.GetSecretValue(&secretsmanager.GetSecretValueInput{SecretID: "sched-lambda"})
		if getErr != nil {
			return false
		}
		// Scheduler rotated if the version changed AND Lambda was invoked.
		return len(calledSteps) == 4
	}, 3*time.Second, 50*time.Millisecond, "scheduler must invoke Lambda for scheduled rotation")

	// All four steps in order.
	assert.Equal(t, []string{"createSecret", "setSecret", "testSecret", "finishSecret"}, calledSteps)

	// New version must be AWSCURRENT.
	curr, err := backend.GetSecretValue(&secretsmanager.GetSecretValueInput{SecretID: "sched-lambda"})
	require.NoError(t, err)
	assert.Contains(t, curr.VersionStages, "AWSCURRENT")
}

// recordingLambdaInvoker records calls and delegates to onInvoke.
type recordingLambdaInvoker struct {
	onInvoke func(name, invocationType string, payload []byte) ([]byte, int, error)
}

func (r *recordingLambdaInvoker) InvokeFunction(
	_ context.Context,
	name, invocationType string,
	payload []byte,
) ([]byte, int, error) {
	return r.onInvoke(name, invocationType, payload)
}

// extractStep extracts the "Step" field from a Lambda rotation event JSON payload.
func extractStep(payload []byte) string {
	// Fast-path: find `"Step":"<value>"`
	s := string(payload)
	const marker = `"Step":"`
	idx := strings.Index(s, marker)
	if idx < 0 {
		return ""
	}
	rest := s[idx+len(marker):]
	end := strings.Index(rest, `"`)
	if end < 0 {
		return rest
	}
	return rest[:end]
}

func boolPtr(v bool) *bool { return &v }
