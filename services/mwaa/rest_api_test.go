package mwaa_test

import (
	"context"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/mwaa"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend_InvokeRestApi(t *testing.T) {
	t.Parallel()

	tests := []struct {
		req     *mwaa.ExportedInvokeRestAPIRequest
		name    string
		envName string
		seed    bool
		wantErr bool
	}{
		{
			name:    "success",
			envName: "invoke-env",
			seed:    true,
			req:     &mwaa.ExportedInvokeRestAPIRequest{Method: "GET", Path: "/dags"},
		},
		{
			name:    "env_not_found",
			envName: "nonexistent",
			seed:    false,
			req:     &mwaa.ExportedInvokeRestAPIRequest{Method: "GET", Path: "/dags"},
			wantErr: true,
		},
		{
			name:    "missing_method",
			envName: "invoke-env-missing-method",
			seed:    true,
			req:     &mwaa.ExportedInvokeRestAPIRequest{Path: "/dags"},
			wantErr: true,
		},
		{
			name:    "missing_path",
			envName: "invoke-env-missing-path",
			seed:    true,
			req:     &mwaa.ExportedInvokeRestAPIRequest{Method: "GET"},
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
				_, _ = b.GetEnvironment(context.Background(), tt.envName) // promote CREATING → AVAILABLE
			}

			resp, err := b.InvokeRestAPI(context.Background(), tt.envName, tt.req)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotNil(t, resp)
			assert.Equal(t, int32(200), resp.RestAPIStatusCode)
		})
	}
}

// TestInvokeRestApi_RequiresAvailable verifies InvokeRestApi is rejected for
// any non-AVAILABLE environment status, mirroring CreateCliToken/
// CreateWebLoginToken: the Airflow webserver InvokeRestApi calls into doesn't
// exist yet while the environment is CREATING/UPDATING/etc.
func TestInvokeRestApi_RequiresAvailable(t *testing.T) {
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
			env := b.AddEnvironmentInternal("restapi-state-env-" + tt.name)
			env.Status = tt.status

			resp, err := b.InvokeRestAPI(
				context.Background(),
				"restapi-state-env-"+tt.name,
				&mwaa.ExportedInvokeRestAPIRequest{Method: "GET", Path: "/dags"},
			)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, mwaa.ErrEnvironmentNotFound,
					"non-AVAILABLE env must return not-found sentinel")

				return
			}

			require.NoError(t, err)
			assert.NotNil(t, resp)
		})
	}
}

func TestInvokeRestApi_Variations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		req     *mwaa.ExportedInvokeRestAPIRequest
		name    string
		wantErr bool
	}{
		{
			name: "list_dags_get",
			req:  &mwaa.ExportedInvokeRestAPIRequest{Method: "GET", Path: "/dags"},
		},
		{
			name: "trigger_dag_run_post",
			req: &mwaa.ExportedInvokeRestAPIRequest{
				Method: "POST",
				Path:   "/dags/my_dag/dagRuns",
				Body:   map[string]any{"dag_run_id": "run-001", "conf": map[string]any{"key": "val"}},
			},
		},
		{
			name: "get_dag_run",
			req: &mwaa.ExportedInvokeRestAPIRequest{
				Method: "GET",
				Path:   "/dags/my_dag/dagRuns/run-001",
			},
		},
		{
			name: "poke_task_instance",
			req: &mwaa.ExportedInvokeRestAPIRequest{
				Method: "POST",
				Path:   "/dags/my_dag/dagRuns/run-001/taskInstances/my_task/notes",
				Body:   map[string]any{"note": "manual poke"},
			},
		},
		{
			name: "get_with_query_params",
			req: &mwaa.ExportedInvokeRestAPIRequest{
				Method:          "GET",
				Path:            "/dags",
				QueryParameters: map[string]any{"limit": "10", "offset": "0"},
			},
		},
		{
			name: "delete_dag_run",
			req: &mwaa.ExportedInvokeRestAPIRequest{
				Method: "DELETE",
				Path:   "/dags/my_dag/dagRuns/run-001",
			},
		},
		{
			name: "patch_dag",
			req: &mwaa.ExportedInvokeRestAPIRequest{
				Method: "PATCH",
				Path:   "/dags/my_dag",
				Body:   map[string]any{"is_paused": false},
			},
		},
		{
			name:    "missing_method_rejected",
			req:     &mwaa.ExportedInvokeRestAPIRequest{Path: "/dags"},
			wantErr: true,
		},
		{
			name:    "missing_path_rejected",
			req:     &mwaa.ExportedInvokeRestAPIRequest{Method: "GET"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			_, err := b.CreateEnvironment(context.Background(), "restapi-env", newCreateReq())
			require.NoError(t, err)
			_, _ = b.GetEnvironment(context.Background(), "restapi-env") // promote CREATING → AVAILABLE

			resp, err := b.InvokeRestAPI(context.Background(), "restapi-env", tt.req)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotNil(t, resp)
			assert.Equal(t, int32(200), resp.RestAPIStatusCode)
		})
	}
}
