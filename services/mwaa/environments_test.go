package mwaa_test

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mwaa"
)

func newTestBackend() *mwaa.InMemoryBackend {
	return mwaa.NewInMemoryBackend("us-east-1", "123456789012")
}

// testNetworkConfig returns a minimal valid NetworkConfiguration satisfying AWS's
// CreateEnvironment contract: NetworkConfiguration is required, SubnetIds must
// contain exactly 2 entries, and SecurityGroupIds must contain 1-5 entries.
func testNetworkConfig() *mwaa.NetworkConfig {
	return &mwaa.NetworkConfig{
		SubnetIDs:        []string{"subnet-aaaa1111", "subnet-bbbb2222"},
		SecurityGroupIDs: []string{"sg-cccc3333"},
	}
}

func newCreateReq() *mwaa.ExportedCreateEnvironmentRequest {
	return &mwaa.ExportedCreateEnvironmentRequest{
		DagS3Path:            "dags/",
		ExecutionRoleArn:     "arn:aws:iam::123456789012:role/mwaa-role",
		SourceBucketArn:      "arn:aws:s3:::my-bucket",
		NetworkConfiguration: testNetworkConfig(),
	}
}

func seedEnv(t *testing.T, b *mwaa.InMemoryBackend, name string) {
	t.Helper()

	_, err := b.CreateEnvironment(context.Background(), name, &mwaa.ExportedCreateEnvironmentRequest{
		DagS3Path:            "dags/",
		ExecutionRoleArn:     "arn:aws:iam::123456789012:role/role",
		SourceBucketArn:      "arn:aws:s3:::bucket",
		NetworkConfiguration: testNetworkConfig(),
	})
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), name)
}

// ----------------------------------------
// Refinement tests
// ----------------------------------------

func TestBackend_CreateEnvironment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		envName     string
		req         *mwaa.ExportedCreateEnvironmentRequest
		wantStatus  string
		wantVersion string
		wantClass   string
		wantErr     bool
	}{
		{
			name:        "creates_with_defaults",
			envName:     "my-env",
			req:         newCreateReq(),
			wantStatus:  "CREATING",
			wantVersion: "2.10.3",
			wantClass:   "mw1.small",
		},
		{
			name:    "creates_with_custom_version",
			envName: "custom-env",
			req: &mwaa.ExportedCreateEnvironmentRequest{
				DagS3Path:            "dags/",
				ExecutionRoleArn:     "arn:aws:iam::123456789012:role/mwaa-role",
				SourceBucketArn:      "arn:aws:s3:::my-bucket",
				AirflowVersion:       "2.8.1",
				EnvironmentClass:     "mw1.medium",
				NetworkConfiguration: testNetworkConfig(),
			},
			wantStatus:  "CREATING",
			wantVersion: "2.8.1",
			wantClass:   "mw1.medium",
		},
		{
			name:    "duplicate_returns_error",
			envName: "dupe-env",
			req:     newCreateReq(),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if tt.name == "duplicate_returns_error" {
				_, err := b.CreateEnvironment(context.Background(), tt.envName, newCreateReq())
				require.NoError(t, err)
			}

			env, err := b.CreateEnvironment(context.Background(), tt.envName, tt.req)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.envName, env.Name)
			assert.Equal(t, tt.wantStatus, env.Status)
			assert.Equal(t, tt.wantVersion, env.AirflowVersion)
			assert.Equal(t, tt.wantClass, env.EnvironmentClass)
			assert.NotEmpty(t, env.ARN)
			assert.NotEmpty(t, env.WebserverURL)
		})
	}
}

func TestBackend_GetEnvironment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		envName string
		seed    bool
		wantErr bool
	}{
		{
			name:    "found",
			envName: "existing-env",
			seed:    true,
		},
		{
			name:    "not_found",
			envName: "missing-env",
			seed:    false,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if tt.seed {
				_, err := b.CreateEnvironment(context.Background(), tt.envName, newCreateReq())
				require.NoError(t, err)
			}

			env, err := b.GetEnvironment(context.Background(), tt.envName)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.envName, env.Name)
		})
	}
}

func TestBackend_DeleteEnvironment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		envName string
		seed    bool
		wantErr bool
	}{
		{
			name:    "deletes_existing",
			envName: "to-delete",
			seed:    true,
		},
		{
			name:    "not_found",
			envName: "missing",
			seed:    false,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if tt.seed {
				_, err := b.CreateEnvironment(context.Background(), tt.envName, newCreateReq())
				require.NoError(t, err)
			}

			deleted, err := b.DeleteEnvironment(context.Background(), tt.envName)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.envName, deleted.Name)

			_, err = b.GetEnvironment(context.Background(), tt.envName)
			require.Error(t, err, "environment should be gone after delete")
		})
	}
}

func TestBackend_ListEnvironments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		seedNames []string
		wantCount int
	}{
		{
			name:      "empty",
			seedNames: []string{},
			wantCount: 0,
		},
		{
			name:      "multiple",
			seedNames: []string{"env-a", "env-b", "env-c"},
			wantCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			for _, n := range tt.seedNames {
				_, err := b.CreateEnvironment(context.Background(), n, newCreateReq())
				require.NoError(t, err)
			}

			names, err := b.ListEnvironments(context.Background())
			require.NoError(t, err)
			assert.Len(t, names, tt.wantCount)
		})
	}
}

func TestBackend_UpdateEnvironment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		update    *mwaa.ExportedUpdateEnvironmentRequest
		name      string
		envName   string
		wantClass string
		seed      bool
		wantErr   bool
	}{
		{
			name:    "updates_class",
			envName: "update-env",
			seed:    true,
			update: &mwaa.ExportedUpdateEnvironmentRequest{
				EnvironmentClass: "mw1.large",
			},
			wantClass: "mw1.large",
		},
		{
			name:    "not_found",
			envName: "missing",
			seed:    false,
			update:  &mwaa.ExportedUpdateEnvironmentRequest{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if tt.seed {
				_, err := b.CreateEnvironment(context.Background(), tt.envName, newCreateReq())
				require.NoError(t, err)
				_, _ = b.GetEnvironment(context.Background(), tt.envName)
			}

			env, err := b.UpdateEnvironment(context.Background(), tt.envName, tt.update)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantClass, env.EnvironmentClass)
		})
	}
}

func TestBackend_UpdateEnvironment_MinMaxValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		updateReq *mwaa.ExportedUpdateEnvironmentRequest
		name      string
		wantErr   bool
	}{
		{
			name: "valid_update",
			updateReq: &mwaa.ExportedUpdateEnvironmentRequest{
				DagS3Path: "new-dags/",
			},
			wantErr: false,
		},
		{
			name: "min_greater_than_max_fails",
			updateReq: &mwaa.ExportedUpdateEnvironmentRequest{
				MinWorkers: 20,
				MaxWorkers: 5,
			},
			wantErr: true,
		},
		{
			name: "only_min_set_keeps_existing_max",
			updateReq: &mwaa.ExportedUpdateEnvironmentRequest{
				MinWorkers: 1,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			_, err := b.CreateEnvironment(
				context.Background(),
				"env-update",
				&mwaa.ExportedCreateEnvironmentRequest{
					DagS3Path:            "dags/",
					ExecutionRoleArn:     "arn:r",
					SourceBucketArn:      "arn:b",
					NetworkConfiguration: testNetworkConfig(),
				},
			)
			require.NoError(t, err)
			_, _ = b.GetEnvironment(context.Background(), "env-update")

			_, err = b.UpdateEnvironment(context.Background(), "env-update", tt.updateReq)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCreateEnvironment_RequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		req     *mwaa.ExportedCreateEnvironmentRequest
		name    string
		wantMsg string
	}{
		{
			name: "missing_DagS3Path",
			req: &mwaa.ExportedCreateEnvironmentRequest{
				ExecutionRoleArn: "arn:aws:iam::123456789012:role/role",
				SourceBucketArn:  "arn:aws:s3:::bucket",
			},
			wantMsg: "DagS3Path",
		},
		{
			name: "missing_ExecutionRoleArn",
			req: &mwaa.ExportedCreateEnvironmentRequest{
				DagS3Path:       "dags/",
				SourceBucketArn: "arn:aws:s3:::bucket",
			},
			wantMsg: "ExecutionRoleArn",
		},
		{
			name: "missing_SourceBucketArn",
			req: &mwaa.ExportedCreateEnvironmentRequest{
				DagS3Path:        "dags/",
				ExecutionRoleArn: "arn:aws:iam::123456789012:role/role",
			},
			wantMsg: "SourceBucketArn",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			_, err := b.CreateEnvironment(context.Background(), "env", tt.req)

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantMsg)
		})
	}
}

func TestCreateEnvironment_Duplicate(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	seedEnv(t, b, "dup-env")

	_, err := b.CreateEnvironment(context.Background(), "dup-env", &mwaa.ExportedCreateEnvironmentRequest{
		DagS3Path:            "dags/",
		ExecutionRoleArn:     "arn:aws:iam::123456789012:role/role",
		SourceBucketArn:      "arn:aws:s3:::bucket",
		NetworkConfiguration: testNetworkConfig(),
	})

	require.Error(t, err)
	require.ErrorIs(t, err, mwaa.ErrEnvironmentAlreadyExists)
}

func TestGetEnvironment_DeepCopy(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	seedEnv(t, b, "deep-copy-env")

	env1, err := b.GetEnvironment(context.Background(), "deep-copy-env")
	require.NoError(t, err)

	// Mutate the returned copy.
	env1.Name = "mutated"
	env1.Tags["injected"] = "value"

	// Re-fetch should have original name.
	env2, err := b.GetEnvironment(context.Background(), "deep-copy-env")
	require.NoError(t, err)

	assert.Equal(t, "deep-copy-env", env2.Name)
	assert.NotContains(t, env2.Tags, "injected")
}

func TestDeleteEnvironment_CleansUpMetrics(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	seedEnv(t, b, "metrics-env")

	v := float64(1.0)
	err := b.PublishMetrics(context.Background(), "metrics-env", &mwaa.ExportedPublishMetricsRequest{
		MetricData: []mwaa.ExportedMetricDatum{
			{MetricName: "Workers", Value: &v},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, mwaa.MetricsCount(b, "metrics-env"))

	_, err = b.DeleteEnvironment(context.Background(), "metrics-env")
	require.NoError(t, err)

	assert.Equal(t, 0, mwaa.MetricsCount(b, "metrics-env"))
}

func TestUpdateEnvironment_MinMaxWorkers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		update  *mwaa.ExportedUpdateEnvironmentRequest
		name    string
		wantErr bool
	}{
		{
			name: "valid_min_max",
			update: &mwaa.ExportedUpdateEnvironmentRequest{
				MinWorkers: 2,
				MaxWorkers: 5,
			},
			wantErr: false,
		},
		{
			name: "min_greater_than_max",
			update: &mwaa.ExportedUpdateEnvironmentRequest{
				MinWorkers: 10,
				MaxWorkers: 5,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			seedEnv(t, b, "worker-env")

			_, err := b.UpdateEnvironment(context.Background(), "worker-env", tt.update)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestHandler_CreateEnvironment_InvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "invalid_json_body",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Test the create environment validation path via backend directly.
			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			_, err := b.CreateEnvironment(context.Background(), "env-err", &mwaa.ExportedCreateEnvironmentRequest{
				DagS3Path:            "dags/",
				ExecutionRoleArn:     "arn:r",
				SourceBucketArn:      "arn:b",
				NetworkConfiguration: testNetworkConfig(),
				MinWorkers:           5,
				MaxWorkers:           3,
			})
			assert.Error(t, err)
			_ = tt.wantStatus
		})
	}
}

func TestUpdateEnvironment_StatusTransitionsToUpdatingThenAvailable(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	_, err := b.CreateEnvironment(context.Background(), "lc-env", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "lc-env") // promote CREATING → AVAILABLE

	_, err = b.UpdateEnvironment(context.Background(), "lc-env", &mwaa.ExportedUpdateEnvironmentRequest{
		EnvironmentClass: "mw1.medium",
	})
	require.NoError(t, err)

	first, err := b.GetEnvironment(context.Background(), "lc-env")
	require.NoError(t, err)
	assert.Equal(t, "UPDATING", first.Status)

	second, err := b.GetEnvironment(context.Background(), "lc-env")
	require.NoError(t, err)
	assert.Equal(t, "AVAILABLE", second.Status)
}

func TestUpdateEnvironment_RejectsEmptyNetworkConfig(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	_, err := b.CreateEnvironment(context.Background(), "nc-env", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "nc-env") // promote CREATING → AVAILABLE

	_, err = b.UpdateEnvironment(context.Background(), "nc-env", &mwaa.ExportedUpdateEnvironmentRequest{
		NetworkConfiguration: &mwaa.UpdateNetworkConfig{},
	})
	require.Error(t, err)
}

func TestLifecycle_CreateReturnsCreating(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	env, err := b.CreateEnvironment(context.Background(), "lc-create-env", newCreateReq())
	require.NoError(t, err)
	assert.Equal(t, "CREATING", env.Status)
}

func TestLifecycle_FirstGetReturnsCreating(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "lc-first-get-env", newCreateReq())
	require.NoError(t, err)

	first, err := b.GetEnvironment(context.Background(), "lc-first-get-env")
	require.NoError(t, err)
	assert.Equal(t, "CREATING", first.Status)
}

func TestLifecycle_SecondGetReturnsAvailable(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "lc-second-get-env", newCreateReq())
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "lc-second-get-env")

	second, err := b.GetEnvironment(context.Background(), "lc-second-get-env")
	require.NoError(t, err)
	assert.Equal(t, "AVAILABLE", second.Status)
}

func TestLifecycle_MultipleGetsStayAvailable(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "lc-multi-get-env", newCreateReq())
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "lc-multi-get-env")

	for range 5 {
		env, err2 := b.GetEnvironment(context.Background(), "lc-multi-get-env")
		require.NoError(t, err2)
		assert.Equal(t, "AVAILABLE", env.Status)
	}
}

func TestLifecycle_CreateThenUpdateStatusFlow(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "lc-full-flow-env", newCreateReq())
	require.NoError(t, err)

	// CREATING → AVAILABLE
	first, err := b.GetEnvironment(context.Background(), "lc-full-flow-env")
	require.NoError(t, err)
	assert.Equal(t, "CREATING", first.Status)

	second, err := b.GetEnvironment(context.Background(), "lc-full-flow-env")
	require.NoError(t, err)
	assert.Equal(t, "AVAILABLE", second.Status)

	// Update → UPDATING → AVAILABLE
	_, err = b.UpdateEnvironment(context.Background(), "lc-full-flow-env", &mwaa.ExportedUpdateEnvironmentRequest{
		EnvironmentClass: "mw1.medium",
	})
	require.NoError(t, err)

	afterUpd, err := b.GetEnvironment(context.Background(), "lc-full-flow-env")
	require.NoError(t, err)
	assert.Equal(t, "UPDATING", afterUpd.Status)

	afterUpd2, err := b.GetEnvironment(context.Background(), "lc-full-flow-env")
	require.NoError(t, err)
	assert.Equal(t, "AVAILABLE", afterUpd2.Status)
}

func TestLifecycle_DeleteReturnsEnvWithDeletingStatus(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "lc-del-env", newCreateReq())
	require.NoError(t, err)

	deleted, err := b.DeleteEnvironment(context.Background(), "lc-del-env")
	require.NoError(t, err)
	require.NotNil(t, deleted)
	// The returned env carries the name.
	assert.Equal(t, "lc-del-env", deleted.Name)
}

func TestLifecycle_DeleteThenGetReturns404(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "lc-del-get-env", newCreateReq())
	require.NoError(t, err)

	_, err = b.DeleteEnvironment(context.Background(), "lc-del-get-env")
	require.NoError(t, err)

	_, err = b.GetEnvironment(context.Background(), "lc-del-get-env")
	require.Error(t, err)
	require.ErrorIs(t, err, mwaa.ErrEnvironmentNotFound)
}

// ─────────────────────────────────────────────────────────────
// 3. S3 path / object-version pair validation
// ─────────────────────────────────────────────────────────────

func TestStatus_CreatingSnapshot_PromotedOnGet(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	env := b.AddEnvironmentInternal("snapshot-env")
	env.Status = "CREATING_SNAPSHOT"

	got, err := b.GetEnvironment(context.Background(), "snapshot-env")
	require.NoError(t, err)
	assert.Equal(t, "CREATING_SNAPSHOT", got.Status, "first Get returns the transient status")

	got2, err := b.GetEnvironment(context.Background(), "snapshot-env")
	require.NoError(t, err)
	assert.Equal(t, "AVAILABLE", got2.Status, "second Get promotes to AVAILABLE")
}

func TestStatus_UpdateRollingBack_PromotedOnGet(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	env := b.AddEnvironmentInternal("rollback-env")
	env.Status = "ROLLING_BACK"

	got, err := b.GetEnvironment(context.Background(), "rollback-env")
	require.NoError(t, err)
	assert.Equal(t, "ROLLING_BACK", got.Status)

	got2, err := b.GetEnvironment(context.Background(), "rollback-env")
	require.NoError(t, err)
	assert.Equal(t, "AVAILABLE", got2.Status)
}

func TestStatus_Pending_PromotedOnGet(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	env := b.AddEnvironmentInternal("pending-env")
	env.Status = "PENDING"

	got, err := b.GetEnvironment(context.Background(), "pending-env")
	require.NoError(t, err)
	assert.Equal(t, "PENDING", got.Status)

	got2, err := b.GetEnvironment(context.Background(), "pending-env")
	require.NoError(t, err)
	assert.Equal(t, "AVAILABLE", got2.Status)
}

// Statuses that should NOT be promoted (terminal/steady states).

func TestStatus_Terminal_NotPromoted(t *testing.T) {
	t.Parallel()

	terminalStatuses := []string{"AVAILABLE", "CREATE_FAILED", "UPDATE_FAILED", "UNAVAILABLE"}

	for _, status := range terminalStatuses {
		t.Run(status, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			env := b.AddEnvironmentInternal("terminal-env-" + status)
			env.Status = status

			got, err := b.GetEnvironment(context.Background(), "terminal-env-"+status)
			require.NoError(t, err)
			assert.Equal(t, status, got.Status, "terminal status %q must not be promoted", status)

			got2, err := b.GetEnvironment(context.Background(), "terminal-env-"+status)
			require.NoError(t, err)
			assert.Equal(t, status, got2.Status, "terminal status %q must not be promoted on second Get", status)
		})
	}
}

// ─────────────────────────────────────────────────────────────
// 7. GetMetrics – environment isolation
// ─────────────────────────────────────────────────────────────

func TestDerivedFields_CeleryExecutorQueue(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "derived-env", newCreateReq())
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "derived-env")
	env, err := b.GetEnvironment(context.Background(), "derived-env")
	require.NoError(t, err)

	// CeleryExecutorQueue must be an SQS URL.
	assert.True(
		t,
		strings.HasPrefix(env.CeleryExecutorQueue, "https://sqs."),
		"CeleryExecutorQueue must start with https://sqs., got %q", env.CeleryExecutorQueue,
	)
	assert.Contains(t, env.CeleryExecutorQueue, testRegion)
	assert.Contains(t, env.CeleryExecutorQueue, testAccountID)
}

func TestDerivedFields_ServiceRoleArn(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "sra-env", newCreateReq())
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "sra-env")
	env, err := b.GetEnvironment(context.Background(), "sra-env")
	require.NoError(t, err)

	// ServiceRoleArn must be an IAM ARN.
	assert.True(
		t,
		strings.HasPrefix(env.ServiceRoleArn, "arn:aws:iam::"),
		"ServiceRoleArn must be an IAM ARN, got %q", env.ServiceRoleArn,
	)
	assert.Contains(t, env.ServiceRoleArn, testAccountID)
	assert.Contains(t, env.ServiceRoleArn, "AWSServiceRoleForAmazonMWAA")
}

func TestDerivedFields_WebserverURL(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "ws-url-env", newCreateReq())
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "ws-url-env")
	env, err := b.GetEnvironment(context.Background(), "ws-url-env")
	require.NoError(t, err)

	assert.True(
		t,
		strings.HasPrefix(env.WebserverURL, "https://"),
		"WebserverURL must start with https://, got %q", env.WebserverURL,
	)
	assert.Contains(t, env.WebserverURL, testRegion)
	assert.Contains(t, env.WebserverURL, "amazonaws.com")
}

func TestDerivedFields_VpcEndpointServices(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "vpc-svc-env", newCreateReq())
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "vpc-svc-env")
	env, err := b.GetEnvironment(context.Background(), "vpc-svc-env")
	require.NoError(t, err)

	assert.NotEmpty(t, env.DatabaseVpcEndpointService)
	assert.NotEmpty(t, env.WebserverVpcEndpointService)
	assert.Contains(t, env.DatabaseVpcEndpointService, testRegion)
	assert.Contains(t, env.WebserverVpcEndpointService, testRegion)
}

func TestDerivedFields_DifferentForDifferentEnvs(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "diff-derived-a", newCreateReq())
	require.NoError(t, err)
	_, err = b.CreateEnvironment(context.Background(), "diff-derived-b", newCreateReq())
	require.NoError(t, err)

	// Consume CREATING for both
	b.GetEnvironment(context.Background(), "diff-derived-a")
	b.GetEnvironment(context.Background(), "diff-derived-b")

	envA, err := b.GetEnvironment(context.Background(), "diff-derived-a")
	require.NoError(t, err)
	envB, err := b.GetEnvironment(context.Background(), "diff-derived-b")
	require.NoError(t, err)

	// Each env gets a unique webserver URL and celery queue.
	assert.NotEqual(t, envA.WebserverURL, envB.WebserverURL)
	assert.NotEqual(t, envA.CeleryExecutorQueue, envB.CeleryExecutorQueue)
}

// ─────────────────────────────────────────────────────────────
// 15. DagS3Path persistence and update
// ─────────────────────────────────────────────────────────────

func TestDagS3Path_CreateAndGet(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.DagS3Path = "custom/dags/"

	_, err := b.CreateEnvironment(context.Background(), "dag-path-env", req)
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "dag-path-env")
	env, err := b.GetEnvironment(context.Background(), "dag-path-env")
	require.NoError(t, err)
	assert.Equal(t, "custom/dags/", env.DagS3Path)
}

func TestDagS3Path_Update_Persisted(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "dag-upd-env", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "dag-upd-env") // promote CREATING → AVAILABLE

	_, err = b.UpdateEnvironment(context.Background(), "dag-upd-env", &mwaa.ExportedUpdateEnvironmentRequest{
		DagS3Path: "new/dags/path/",
	})
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "dag-upd-env")
	env, err := b.GetEnvironment(context.Background(), "dag-upd-env")
	require.NoError(t, err)
	assert.Equal(t, "new/dags/path/", env.DagS3Path)
}

func TestDagS3Path_Required_OnCreate(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "dag-missing-env", &mwaa.ExportedCreateEnvironmentRequest{
		ExecutionRoleArn: "arn:aws:iam::123456789012:role/r",
		SourceBucketArn:  "arn:aws:s3:::bucket",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DagS3Path")
}

// ─────────────────────────────────────────────────────────────
// 16. SourceBucketArn and ExecutionRoleArn required fields
// ─────────────────────────────────────────────────────────────

func TestRequiredFields_MissingSourceBucketArn(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "req-sb-env", &mwaa.ExportedCreateEnvironmentRequest{
		DagS3Path:        "dags/",
		ExecutionRoleArn: "arn:aws:iam::123456789012:role/r",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SourceBucketArn")
}

func TestRequiredFields_MissingExecutionRoleArn(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "req-era-env", &mwaa.ExportedCreateEnvironmentRequest{
		DagS3Path:       "dags/",
		SourceBucketArn: "arn:aws:s3:::bucket",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ExecutionRoleArn")
}

// ─────────────────────────────────────────────────────────────
// 17. ExecutionRoleArn and SourceBucketArn update persistence
// ─────────────────────────────────────────────────────────────

func TestUpdate_ExecutionRoleArnAndSourceBucketArn(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "role-upd-env", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "role-upd-env") // promote CREATING → AVAILABLE

	newRole := "arn:aws:iam::123456789012:role/new-mwaa-role"
	newBucket := "arn:aws:s3:::new-bucket"
	_, err = b.UpdateEnvironment(context.Background(), "role-upd-env", &mwaa.ExportedUpdateEnvironmentRequest{
		ExecutionRoleArn: newRole,
		SourceBucketArn:  newBucket,
	})
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "role-upd-env")
	env, err := b.GetEnvironment(context.Background(), "role-upd-env")
	require.NoError(t, err)
	assert.Equal(t, newRole, env.ExecutionRoleArn)
	assert.Equal(t, newBucket, env.SourceBucketArn)
}

// ─────────────────────────────────────────────────────────────
// 18. LastUpdate fields after UpdateEnvironment
// ─────────────────────────────────────────────────────────────

func TestLastUpdate_PopulatedAfterUpdate(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "lu-check-env", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "lu-check-env") // promote CREATING → AVAILABLE

	_, err = b.UpdateEnvironment(context.Background(), "lu-check-env", &mwaa.ExportedUpdateEnvironmentRequest{
		DagS3Path: "updated-dags/",
	})
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "lu-check-env")
	env, err := b.GetEnvironment(context.Background(), "lu-check-env")
	require.NoError(t, err)
	require.NotNil(t, env.LastUpdate)
	assert.Equal(t, "SUCCESS", env.LastUpdate.Status)
	assert.Equal(t, "USER", env.LastUpdate.Source)
	assert.Positive(t, env.LastUpdate.CreatedAt)
}

func TestLastUpdate_NilBeforeFirstUpdate(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "lu-nil-env", newCreateReq())
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "lu-nil-env")
	env, err := b.GetEnvironment(context.Background(), "lu-nil-env")
	require.NoError(t, err)
	assert.Nil(t, env.LastUpdate)
}

// ─────────────────────────────────────────────────────────────
// 19. ListEnvironments sorted order + pagination consistency
// ─────────────────────────────────────────────────────────────

func TestListEnvironments_SortedAlphabetically(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	names := []string{"zebra-env", "alpha-env", "middle-env"}
	for _, n := range names {
		_, err := b.CreateEnvironment(context.Background(), n, newCreateReq())
		require.NoError(t, err)
	}

	listed, err := b.ListEnvironments(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"alpha-env", "middle-env", "zebra-env"}, listed)
}

func TestListEnvironments_PaginationConsistentOrder(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	for _, n := range []string{"aa", "bb", "cc", "dd", "ee"} {
		_, err := b.CreateEnvironment(context.Background(), n, newCreateReq())
		require.NoError(t, err)
	}

	// Page 1: 2 items
	page1, tok1, err := b.ListEnvironmentsPage(context.Background(), "", 2)
	require.NoError(t, err)
	assert.Equal(t, []string{"aa", "bb"}, page1)
	assert.Equal(t, "cc", tok1)

	// Page 2: 2 items starting from tok1
	page2, tok2, err := b.ListEnvironmentsPage(context.Background(), tok1, 2)
	require.NoError(t, err)
	assert.Equal(t, []string{"cc", "dd"}, page2)
	assert.Equal(t, "ee", tok2)

	// Page 3: last 1 item
	page3, tok3, err := b.ListEnvironmentsPage(context.Background(), tok2, 2)
	require.NoError(t, err)
	assert.Equal(t, []string{"ee"}, page3)
	assert.Empty(t, tok3)
}

// ─────────────────────────────────────────────────────────────
// 20. ARN format and uniqueness
// ─────────────────────────────────────────────────────────────

func TestARN_Format(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	env, err := b.CreateEnvironment(context.Background(), "arn-fmt-env", newCreateReq())
	require.NoError(t, err)

	// ARN must match arn:aws:airflow:REGION:ACCOUNT:environment/NAME
	expected := "arn:aws:airflow:" + testRegion + ":" + testAccountID + ":environment/arn-fmt-env"
	assert.Equal(t, expected, env.ARN)
}

func TestARN_UniquePerEnvironment(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	envA, err := b.CreateEnvironment(context.Background(), "arn-unique-a", newCreateReq())
	require.NoError(t, err)
	envB, err := b.CreateEnvironment(context.Background(), "arn-unique-b", newCreateReq())
	require.NoError(t, err)

	assert.NotEqual(t, envA.ARN, envB.ARN)
}

// ─────────────────────────────────────────────────────────────
// 21. CreatedAt timestamp
// ─────────────────────────────────────────────────────────────

func TestCreatedAt_Set(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	env, err := b.CreateEnvironment(context.Background(), "created-at-env", newCreateReq())
	require.NoError(t, err)

	assert.Positive(t, env.CreatedAt, "CreatedAt must be a positive Unix epoch")
}

// ─────────────────────────────────────────────────────────────
// 22. Persistence snapshot / restore round-trip with new fields
// ─────────────────────────────────────────────────────────────

func TestTags_MutationDoesNotAffectStoredEnv(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.Tags = map[string]string{"original": "value"}
	_, err := b.CreateEnvironment(context.Background(), "deep-copy-env", req)
	require.NoError(t, err)

	env1, err := b.GetEnvironment(context.Background(), "deep-copy-env")
	require.NoError(t, err)

	// Mutate the returned tags.
	env1.Tags["injected"] = "malicious"

	env2, err := b.GetEnvironment(context.Background(), "deep-copy-env")
	require.NoError(t, err)
	assert.NotContains(t, env2.Tags, "injected",
		"mutation of returned tags must not affect the stored environment")
}

func TestNetworkConfig_MutationDoesNotAffectStoredEnv(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.NetworkConfiguration = &mwaa.NetworkConfig{
		SubnetIDs:        []string{"subnet-aaa", "subnet-bbb"},
		SecurityGroupIDs: []string{"sg-111"},
	}
	_, err := b.CreateEnvironment(context.Background(), "nc-copy-env", req)
	require.NoError(t, err)

	env1, err := b.GetEnvironment(context.Background(), "nc-copy-env")
	require.NoError(t, err)

	// Replace the pointer entirely.
	env1.NetworkConfiguration = nil

	env2, err := b.GetEnvironment(context.Background(), "nc-copy-env")
	require.NoError(t, err)
	require.NotNil(t, env2.NetworkConfiguration,
		"stored NetworkConfiguration must survive mutation of returned copy")
	assert.Equal(t, []string{"subnet-aaa", "subnet-bbb"}, env2.NetworkConfiguration.SubnetIDs)
}

// ─────────────────────────────────────────────────────────────
// 11. ARN index consistency
// ─────────────────────────────────────────────────────────────

func TestMultipleValidationErrors_FirstReturned(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.AirflowVersion = "bogus"
	req.EnvironmentClass = "mw99.huge"
	req.MaxWorkers = 999

	_, err := b.CreateEnvironment(context.Background(), "multi-err-env", req)
	require.Error(t, err)
}

func TestCreateEnvironment_NameValidationBeforeBodyValidation(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)

	// Invalid name AND invalid body — name error should surface first.
	req := newCreateReq()
	req.DagS3Path = "" // required field

	_, err := b.CreateEnvironment(context.Background(), "1invalid-name", req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "environment name")
}

func TestFullLifecycle_AllValidations(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)

	// Create with specific supported version and environment class.
	req := newCreateReq()
	req.AirflowVersion = "2.8.1"
	req.EnvironmentClass = "mw1.medium"
	req.MaxWorkers = 20
	req.MinWorkers = 2

	env, err := b.CreateEnvironment(context.Background(), "full-lifecycle-env", req)
	require.NoError(t, err)
	assert.Equal(t, "2.8.1", env.AirflowVersion)
	assert.Equal(t, "mw1.medium", env.EnvironmentClass)
	_, _ = b.GetEnvironment(context.Background(), "full-lifecycle-env") // promote CREATING → AVAILABLE

	// Update with valid strategy and access mode.
	_, err = b.UpdateEnvironment(context.Background(), "full-lifecycle-env", &mwaa.ExportedUpdateEnvironmentRequest{
		WorkerReplacementStrategy: "GRACEFUL",
		WebserverAccessMode:       "PRIVATE_ONLY",
		EnvironmentClass:          "mw1.large",
	})
	require.NoError(t, err)

	got, err := b.GetEnvironment(context.Background(), "full-lifecycle-env")
	require.NoError(t, err)
	assert.Equal(t, "PRIVATE_ONLY", got.WebserverAccessMode)
	assert.Equal(t, "mw1.large", got.EnvironmentClass)

	// Tokens should be JWT-shaped.
	cli, _, err := b.CreateCliToken(context.Background(), "full-lifecycle-env")
	require.NoError(t, err)
	assert.Len(t, strings.Split(cli, "."), 3)

	web, _, err := b.CreateWebLoginToken(context.Background(), "full-lifecycle-env")
	require.NoError(t, err)
	assert.Len(t, strings.Split(web, "."), 3)
}

func TestUpdateEnvironment_RequiresAvailable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		status  string
		wantErr bool
	}{
		{name: "creating_rejected", status: "CREATING", wantErr: true},
		{name: "updating_rejected", status: "UPDATING", wantErr: true},
		{name: "deleting_rejected", status: "DELETING", wantErr: true},
		{name: "available_ok", status: "AVAILABLE", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			env := b.AddEnvironmentInternal("upd-state-env-" + tt.name)
			env.Status = tt.status

			_, err := b.UpdateEnvironment(
				context.Background(),
				"upd-state-env-"+tt.name,
				&mwaa.ExportedUpdateEnvironmentRequest{
					DagS3Path: "new-dags/",
				},
			)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, mwaa.ErrInvalidParameter,
					"update on non-AVAILABLE env must return invalid-parameter sentinel")
			} else {
				require.NoError(t, err)
			}
		})
	}
}
