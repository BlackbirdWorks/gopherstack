package secretsmanager_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// doSMRequest is a helper that sends a POST with the given X-Amz-Target and JSON body.
func doSMRequest(t *testing.T, h *secretsmanager.Handler, target, body string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("X-Amz-Target", target)
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))

	return rec
}

// TestBatchGetSecretValue verifies BatchGetSecretValue.
func TestBatchGetSecretValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup          func(*testing.T, *secretsmanager.InMemoryBackend)
		checkFn        func(*testing.T, *httptest.ResponseRecorder)
		name           string
		body           string
		expectedStatus int
	}{
		{
			name: "by_secret_id_list",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "batch-s1", SecretString: "val1"},
				)
				require.NoError(t, err)
				_, err = b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "batch-s2", SecretString: "val2"},
				)
				require.NoError(t, err)
			},
			body:           `{"SecretIdList":["batch-s1","batch-s2"]}`,
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out secretsmanager.BatchGetSecretValueOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Len(t, out.SecretValues, 2)
				assert.Empty(t, out.Errors)
			},
		},
		{
			name: "partial_errors",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "batch-ok", SecretString: "v"},
				)
				require.NoError(t, err)
			},
			body:           `{"SecretIdList":["batch-ok","batch-missing"]}`,
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out secretsmanager.BatchGetSecretValueOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Len(t, out.SecretValues, 1)
				assert.Len(t, out.Errors, 1)
				assert.Equal(t, "ResourceNotFoundException", out.Errors[0].ErrorCode)
				assert.Equal(t, "batch-missing", out.Errors[0].SecretID)
			},
		},
		{
			name: "all_secrets_when_no_id_list",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "batch-all-1", SecretString: "a"},
				)
				require.NoError(t, err)
				_, err = b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "batch-all-2", SecretString: "b"},
				)
				require.NoError(t, err)
			},
			body:           `{}`,
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out secretsmanager.BatchGetSecretValueOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Len(t, out.SecretValues, 2)
			},
		},
		{
			name:           "bad_json",
			body:           `{bad}`,
			expectedStatus: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := secretsmanager.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, backend)
			}

			h := secretsmanager.NewHandler(backend)
			rec := doSMRequest(t, h, "secretsmanager.BatchGetSecretValue", tt.body)

			assert.Equal(t, tt.expectedStatus, rec.Code)

			if tt.checkFn != nil {
				tt.checkFn(t, rec)
			}
		})
	}
}

// TestCancelRotateSecret verifies CancelRotateSecret.
func TestCancelRotateSecret(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup          func(*testing.T, *secretsmanager.InMemoryBackend)
		checkFn        func(*testing.T, *httptest.ResponseRecorder)
		name           string
		body           string
		expectedStatus int
	}{
		{
			name: "cancels_rotation",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "cancel-rot", SecretString: "v1"},
				)
				require.NoError(t, err)
				// Rotate to create a pending version.
				_, err = b.RotateSecret(context.Background(), &secretsmanager.RotateSecretInput{SecretID: "cancel-rot"})
				require.NoError(t, err)
			},
			body:           `{"SecretId":"cancel-rot"}`,
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out secretsmanager.CancelRotateSecretOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, "cancel-rot", out.Name)
				assert.NotEmpty(t, out.ARN)
			},
		},
		{
			name:           "not_found",
			body:           `{"SecretId":"no-such-secret"}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name: "deleted_secret",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "cancel-del", SecretString: "v"},
				)
				require.NoError(t, err)
				_, err = b.DeleteSecret(context.Background(), &secretsmanager.DeleteSecretInput{SecretID: "cancel-del"})
				require.NoError(t, err)
			},
			body:           `{"SecretId":"cancel-del"}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "bad_json",
			body:           `{bad}`,
			expectedStatus: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := secretsmanager.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, backend)
			}

			h := secretsmanager.NewHandler(backend)
			rec := doSMRequest(t, h, "secretsmanager.CancelRotateSecret", tt.body)

			assert.Equal(t, tt.expectedStatus, rec.Code)

			if tt.checkFn != nil {
				tt.checkFn(t, rec)
			}
		})
	}
}

// TestResourcePolicyCycle verifies PutResourcePolicy, GetResourcePolicy, and DeleteResourcePolicy.
func TestResourcePolicyCycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup          func(*testing.T, *secretsmanager.InMemoryBackend)
		checkFn        func(*testing.T, *httptest.ResponseRecorder)
		name           string
		target         string
		body           string
		expectedStatus int
	}{
		{
			name: "put_resource_policy",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "policy-secret", SecretString: "v"},
				)
				require.NoError(t, err)
			},
			target:         "secretsmanager.PutResourcePolicy",
			body:           `{"SecretId":"policy-secret","ResourcePolicy":"{\"Version\":\"2012-10-17\",\"Statement\":[]}"}`,
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out secretsmanager.PutResourcePolicyOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, "policy-secret", out.Name)
				assert.NotEmpty(t, out.ARN)
			},
		},
		{
			name: "get_resource_policy_after_put",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "get-policy", SecretString: "v"},
				)
				require.NoError(t, err)
				_, err = b.PutResourcePolicy(context.Background(), &secretsmanager.PutResourcePolicyInput{
					SecretID:       "get-policy",
					ResourcePolicy: `{"Version":"2012-10-17","Statement":[]}`,
				})
				require.NoError(t, err)
			},
			target:         "secretsmanager.GetResourcePolicy",
			body:           `{"SecretId":"get-policy"}`,
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out secretsmanager.GetResourcePolicyOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, "get-policy", out.Name)
				assert.Contains(t, out.ResourcePolicy, "2012-10-17")
			},
		},
		{
			name: "delete_resource_policy",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "del-policy", SecretString: "v"},
				)
				require.NoError(t, err)
				_, err = b.PutResourcePolicy(context.Background(), &secretsmanager.PutResourcePolicyInput{
					SecretID:       "del-policy",
					ResourcePolicy: `{"Version":"2012-10-17","Statement":[]}`,
				})
				require.NoError(t, err)
			},
			target:         "secretsmanager.DeleteResourcePolicy",
			body:           `{"SecretId":"del-policy"}`,
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out secretsmanager.DeleteResourcePolicyOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, "del-policy", out.Name)
			},
		},
		{
			name:           "get_policy_not_found",
			target:         "secretsmanager.GetResourcePolicy",
			body:           `{"SecretId":"nonexistent"}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "put_policy_not_found",
			target:         "secretsmanager.PutResourcePolicy",
			body:           `{"SecretId":"nonexistent","ResourcePolicy":"{}"}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "delete_policy_not_found",
			target:         "secretsmanager.DeleteResourcePolicy",
			body:           `{"SecretId":"nonexistent"}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "put_bad_json",
			target:         "secretsmanager.PutResourcePolicy",
			body:           `{bad}`,
			expectedStatus: http.StatusInternalServerError,
		},
		{
			name:           "get_bad_json",
			target:         "secretsmanager.GetResourcePolicy",
			body:           `{bad}`,
			expectedStatus: http.StatusInternalServerError,
		},
		{
			name:           "delete_bad_json",
			target:         "secretsmanager.DeleteResourcePolicy",
			body:           `{bad}`,
			expectedStatus: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := secretsmanager.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, backend)
			}

			h := secretsmanager.NewHandler(backend)
			rec := doSMRequest(t, h, tt.target, tt.body)

			assert.Equal(t, tt.expectedStatus, rec.Code)

			if tt.checkFn != nil {
				tt.checkFn(t, rec)
			}
		})
	}
}

// TestReplicationOperations verifies ReplicateSecretToRegions, RemoveRegionsFromReplication,
// and StopReplicationToReplica.
func TestReplicationOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup          func(*testing.T, *secretsmanager.InMemoryBackend)
		checkFn        func(*testing.T, *httptest.ResponseRecorder)
		name           string
		target         string
		body           string
		expectedStatus int
	}{
		{
			name: "replicate_to_regions",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "rep-secret", SecretString: "v"},
				)
				require.NoError(t, err)
			},
			target:         "secretsmanager.ReplicateSecretToRegions",
			body:           `{"SecretId":"rep-secret","AddReplicaRegions":[{"Region":"us-west-2"}]}`,
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out secretsmanager.ReplicateSecretToRegionsOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out.ARN)
				require.Len(t, out.ReplicationStatus, 1)
				assert.Equal(t, "us-west-2", out.ReplicationStatus[0].Region)
				assert.Equal(t, "InSync", out.ReplicationStatus[0].Status)
			},
		},
		{
			name: "remove_regions_from_replication",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "rem-rep", SecretString: "v"},
				)
				require.NoError(t, err)
				_, err = b.ReplicateSecretToRegions(context.Background(), &secretsmanager.ReplicateSecretToRegionsInput{
					SecretID:          "rem-rep",
					AddReplicaRegions: []secretsmanager.ReplicaRegion{{Region: "eu-west-1"}, {Region: "ap-east-1"}},
				})
				require.NoError(t, err)
			},
			target:         "secretsmanager.RemoveRegionsFromReplication",
			body:           `{"SecretId":"rem-rep","RemoveReplicaRegions":["eu-west-1"]}`,
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out secretsmanager.RemoveRegionsFromReplicationOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				require.Len(t, out.ReplicationStatus, 1)
				assert.Equal(t, "ap-east-1", out.ReplicationStatus[0].Region)
			},
		},
		{
			name: "stop_replication_to_replica",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "stop-rep", SecretString: "v"},
				)
				require.NoError(t, err)
				_, err = b.ReplicateSecretToRegions(context.Background(), &secretsmanager.ReplicateSecretToRegionsInput{
					SecretID:          "stop-rep",
					AddReplicaRegions: []secretsmanager.ReplicaRegion{{Region: "eu-west-1"}},
				})
				require.NoError(t, err)
			},
			target:         "secretsmanager.StopReplicationToReplica",
			body:           `{"SecretId":"stop-rep"}`,
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out secretsmanager.StopReplicationToReplicaOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out.ARN)
			},
		},
		{
			name:           "replicate_not_found",
			target:         "secretsmanager.ReplicateSecretToRegions",
			body:           `{"SecretId":"nonexistent","AddReplicaRegions":[{"Region":"us-west-2"}]}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "remove_regions_not_found",
			target:         "secretsmanager.RemoveRegionsFromReplication",
			body:           `{"SecretId":"nonexistent","RemoveReplicaRegions":["us-west-2"]}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "stop_replication_not_found",
			target:         "secretsmanager.StopReplicationToReplica",
			body:           `{"SecretId":"nonexistent"}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "replicate_bad_json",
			target:         "secretsmanager.ReplicateSecretToRegions",
			body:           `{bad}`,
			expectedStatus: http.StatusInternalServerError,
		},
		{
			name:           "remove_regions_bad_json",
			target:         "secretsmanager.RemoveRegionsFromReplication",
			body:           `{bad}`,
			expectedStatus: http.StatusInternalServerError,
		},
		{
			name:           "stop_replication_bad_json",
			target:         "secretsmanager.StopReplicationToReplica",
			body:           `{bad}`,
			expectedStatus: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := secretsmanager.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, backend)
			}

			h := secretsmanager.NewHandler(backend)
			rec := doSMRequest(t, h, tt.target, tt.body)

			assert.Equal(t, tt.expectedStatus, rec.Code)

			if tt.checkFn != nil {
				tt.checkFn(t, rec)
			}
		})
	}
}

// TestUpdateSecretVersionStage verifies UpdateSecretVersionStage.
func TestUpdateSecretVersionStage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup          func(*testing.T, *secretsmanager.InMemoryBackend) string
		checkFn        func(*testing.T, *httptest.ResponseRecorder)
		bodyFn         func(versionID string) string
		name           string
		expectedStatus int
	}{
		{
			name: "move_label_to_new_version",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) string {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "stage-secret", SecretString: "v1"},
				)
				require.NoError(t, err)
				out, err := b.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
					SecretID:     "stage-secret",
					SecretString: "v2",
				})
				require.NoError(t, err)

				return out.VersionID
			},
			bodyFn: func(versionID string) string {
				return `{"SecretId":"stage-secret","VersionStage":"AWSCUSTOM","MoveToVersionId":"` + versionID + `"}`
			},
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out secretsmanager.UpdateSecretVersionStageOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, "stage-secret", out.Name)
				assert.NotEmpty(t, out.ARN)
			},
		},
		{
			name: "remove_label_from_version",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) string {
				t.Helper()
				out, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "stage-remove", SecretString: "v1"},
				)
				require.NoError(t, err)
				// Add a custom label first so we can remove it.
				// AWSCURRENT cannot be removed without MoveToVersionId (AWS constraint).
				_, err = b.UpdateSecretVersionStage(context.Background(), &secretsmanager.UpdateSecretVersionStageInput{
					SecretID:        "stage-remove",
					VersionStage:    "AWSCUSTOM",
					MoveToVersionID: out.VersionID,
				})
				require.NoError(t, err)

				return out.VersionID
			},
			bodyFn: func(versionID string) string {
				return `{"SecretId":"stage-remove","VersionStage":"AWSCUSTOM","RemoveFromVersionId":"` + versionID + `"}`
			},
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out secretsmanager.UpdateSecretVersionStageOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, "stage-remove", out.Name)
			},
		},
		{
			name: "not_found_secret",
			setup: func(t *testing.T, _ *secretsmanager.InMemoryBackend) string {
				t.Helper()

				return "irrelevant"
			},
			bodyFn: func(_ string) string {
				return `{"SecretId":"nonexistent","VersionStage":"AWSCUSTOM"}`
			},
			expectedStatus: http.StatusBadRequest,
		},
		{
			name: "bad_json",
			setup: func(t *testing.T, _ *secretsmanager.InMemoryBackend) string {
				t.Helper()

				return ""
			},
			bodyFn: func(_ string) string {
				return `{bad}`
			},
			expectedStatus: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := secretsmanager.NewInMemoryBackend()
			var versionID string

			if tt.setup != nil {
				versionID = tt.setup(t, backend)
			}

			h := secretsmanager.NewHandler(backend)
			rec := doSMRequest(t, h, "secretsmanager.UpdateSecretVersionStage", tt.bodyFn(versionID))

			assert.Equal(t, tt.expectedStatus, rec.Code)

			if tt.checkFn != nil {
				tt.checkFn(t, rec)
			}
		})
	}
}

// TestListSecretVersionIds verifies the renamed ListSecretVersionIds operation works via HTTP.
func TestListSecretVersionIds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup          func(*testing.T, *secretsmanager.InMemoryBackend)
		checkFn        func(*testing.T, *httptest.ResponseRecorder)
		name           string
		body           string
		expectedStatus int
	}{
		{
			name: "lists_versions",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "lsvi-secret", SecretString: "v1"},
				)
				require.NoError(t, err)
				_, err = b.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
					SecretID:     "lsvi-secret",
					SecretString: "v2",
				})
				require.NoError(t, err)
			},
			body:           `{"SecretId":"lsvi-secret"}`,
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out secretsmanager.ListSecretVersionIDsOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, "lsvi-secret", out.Name)
				assert.NotEmpty(t, out.Versions)
			},
		},
		{
			name:           "not_found",
			body:           `{"SecretId":"nonexistent"}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "bad_json",
			body:           `{bad}`,
			expectedStatus: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := secretsmanager.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, backend)
			}

			h := secretsmanager.NewHandler(backend)
			rec := doSMRequest(t, h, "secretsmanager.ListSecretVersionIds", tt.body)

			assert.Equal(t, tt.expectedStatus, rec.Code)

			if tt.checkFn != nil {
				tt.checkFn(t, rec)
			}
		})
	}
}

// TestNewOpsBackend verifies backend methods directly for new operations.
func TestNewOpsBackend(t *testing.T) {
	t.Parallel()

	t.Run("BatchGetSecretValue_deleted_secret_error", func(t *testing.T) {
		t.Parallel()

		b := secretsmanager.NewInMemoryBackend()
		_, err := b.CreateSecret(
			context.Background(),
			&secretsmanager.CreateSecretInput{Name: "del-batch", SecretString: "v"},
		)
		require.NoError(t, err)
		_, err = b.DeleteSecret(context.Background(), &secretsmanager.DeleteSecretInput{SecretID: "del-batch"})
		require.NoError(t, err)

		out, err := b.BatchGetSecretValue(context.Background(), &secretsmanager.BatchGetSecretValueInput{
			SecretIDList: []string{"del-batch"},
		})
		require.NoError(t, err)
		assert.Empty(t, out.SecretValues)
		require.Len(t, out.Errors, 1)
		assert.Equal(t, "InvalidRequestException", out.Errors[0].ErrorCode)
	})

	t.Run("CancelRotateSecret_removes_pending_label", func(t *testing.T) {
		t.Parallel()

		b := secretsmanager.NewInMemoryBackend()
		_, err := b.CreateSecret(
			context.Background(),
			&secretsmanager.CreateSecretInput{Name: "rot-cancel", SecretString: "v"},
		)
		require.NoError(t, err)
		_, err = b.RotateSecret(context.Background(), &secretsmanager.RotateSecretInput{SecretID: "rot-cancel"})
		require.NoError(t, err)

		out, err := b.CancelRotateSecret(
			context.Background(),
			&secretsmanager.CancelRotateSecretInput{SecretID: "rot-cancel"},
		)
		require.NoError(t, err)
		assert.Equal(t, "rot-cancel", out.Name)

		desc, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "rot-cancel"})
		require.NoError(t, err)
		// Real AWS: CancelRotateSecret removes AWSPENDING but does not disable rotation.
		assert.True(t, desc.RotationEnabled)
	})

	t.Run("CancelRotateSecret_not_found", func(t *testing.T) {
		t.Parallel()

		b := secretsmanager.NewInMemoryBackend()
		_, err := b.CancelRotateSecret(
			context.Background(),
			&secretsmanager.CancelRotateSecretInput{SecretID: "nonexistent"},
		)
		require.ErrorIs(t, err, secretsmanager.ErrSecretNotFound)
	})

	t.Run("GetResourcePolicy_empty_when_not_set", func(t *testing.T) {
		t.Parallel()

		b := secretsmanager.NewInMemoryBackend()
		_, err := b.CreateSecret(
			context.Background(),
			&secretsmanager.CreateSecretInput{Name: "no-policy", SecretString: "v"},
		)
		require.NoError(t, err)

		out, err := b.GetResourcePolicy(
			context.Background(),
			&secretsmanager.GetResourcePolicyInput{SecretID: "no-policy"},
		)
		require.NoError(t, err)
		assert.Empty(t, out.ResourcePolicy)
		assert.Equal(t, "no-policy", out.Name)
	})

	t.Run("PutResourcePolicy_deleted_returns_error", func(t *testing.T) {
		t.Parallel()

		b := secretsmanager.NewInMemoryBackend()
		_, err := b.CreateSecret(
			context.Background(),
			&secretsmanager.CreateSecretInput{Name: "put-del-policy", SecretString: "v"},
		)
		require.NoError(t, err)
		_, err = b.DeleteSecret(context.Background(), &secretsmanager.DeleteSecretInput{SecretID: "put-del-policy"})
		require.NoError(t, err)

		_, err = b.PutResourcePolicy(context.Background(), &secretsmanager.PutResourcePolicyInput{
			SecretID:       "put-del-policy",
			ResourcePolicy: "{}",
		})
		require.ErrorIs(t, err, secretsmanager.ErrSecretDeleted)
	})

	t.Run("DeleteResourcePolicy_deleted_returns_error", func(t *testing.T) {
		t.Parallel()

		b := secretsmanager.NewInMemoryBackend()
		_, err := b.CreateSecret(
			context.Background(),
			&secretsmanager.CreateSecretInput{Name: "del-del-policy", SecretString: "v"},
		)
		require.NoError(t, err)
		_, err = b.DeleteSecret(context.Background(), &secretsmanager.DeleteSecretInput{SecretID: "del-del-policy"})
		require.NoError(t, err)

		_, err = b.DeleteResourcePolicy(
			context.Background(),
			&secretsmanager.DeleteResourcePolicyInput{SecretID: "del-del-policy"},
		)
		require.ErrorIs(t, err, secretsmanager.ErrSecretDeleted)
	})

	t.Run("ReplicateSecretToRegions_idempotent_update", func(t *testing.T) {
		t.Parallel()

		b := secretsmanager.NewInMemoryBackend()
		_, err := b.CreateSecret(
			context.Background(),
			&secretsmanager.CreateSecretInput{Name: "rep-idem", SecretString: "v"},
		)
		require.NoError(t, err)

		// Add us-east-2.
		_, err = b.ReplicateSecretToRegions(context.Background(), &secretsmanager.ReplicateSecretToRegionsInput{
			SecretID:          "rep-idem",
			AddReplicaRegions: []secretsmanager.ReplicaRegion{{Region: "us-east-2"}},
		})
		require.NoError(t, err)

		// Add us-east-2 again with ForceOverwriteReplicaSecret=true (required to update).
		out, err := b.ReplicateSecretToRegions(context.Background(), &secretsmanager.ReplicateSecretToRegionsInput{
			SecretID:                    "rep-idem",
			AddReplicaRegions:           []secretsmanager.ReplicaRegion{{Region: "us-east-2", KmsKeyID: "key-123"}},
			ForceOverwriteReplicaSecret: true,
		})
		require.NoError(t, err)
		assert.Len(t, out.ReplicationStatus, 1)
		assert.Equal(t, "key-123", out.ReplicationStatus[0].KmsKeyID)
	})

	t.Run("UpdateSecretVersionStage_version_not_found", func(t *testing.T) {
		t.Parallel()

		b := secretsmanager.NewInMemoryBackend()
		_, err := b.CreateSecret(
			context.Background(),
			&secretsmanager.CreateSecretInput{Name: "stage-ver-nf", SecretString: "v"},
		)
		require.NoError(t, err)

		_, err = b.UpdateSecretVersionStage(context.Background(), &secretsmanager.UpdateSecretVersionStageInput{
			SecretID:            "stage-ver-nf",
			VersionStage:        "AWSCUSTOM",
			RemoveFromVersionID: "no-such-version",
		})
		require.ErrorIs(t, err, secretsmanager.ErrVersionNotFound)
	})

	t.Run("UpdateSecretVersionStage_move_to_version_not_found", func(t *testing.T) {
		t.Parallel()

		b := secretsmanager.NewInMemoryBackend()
		_, err := b.CreateSecret(
			context.Background(),
			&secretsmanager.CreateSecretInput{Name: "stage-mov-nf", SecretString: "v"},
		)
		require.NoError(t, err)

		_, err = b.UpdateSecretVersionStage(context.Background(), &secretsmanager.UpdateSecretVersionStageInput{
			SecretID:        "stage-mov-nf",
			VersionStage:    "AWSCUSTOM",
			MoveToVersionID: "no-such-version",
		})
		require.ErrorIs(t, err, secretsmanager.ErrVersionNotFound)
	})
}
