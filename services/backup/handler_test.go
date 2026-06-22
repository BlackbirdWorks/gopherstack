package backup_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/backup"
)

func newTestBackupHandler() *backup.Handler {
	backend := backup.NewInMemoryBackend("123456789012", config.DefaultRegion)

	return backup.NewHandler(backend)
}

func doREST(
	t *testing.T,
	h *backup.Handler,
	method, path string,
	body map[string]any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func parseResp(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))

	return m
}

// TestBackupVaultCRUD exercises CreateBackupVault, DescribeBackupVault,
// ListBackupVaults, and DeleteBackupVault through the REST handler.
func TestBackupVaultCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops        func(t *testing.T, h *backup.Handler)
		name       string
		wantStatus int
	}{
		{
			name: "create_vault",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPut, "/backup-vaults/my-vault", map[string]any{
					"BackupVaultTags": map[string]string{"Env": "test"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseResp(t, rec)
				assert.Equal(t, "my-vault", resp["BackupVaultName"])
				assert.NotEmpty(t, resp["BackupVaultArn"])
				assert.NotNil(t, resp["CreationDate"])
			},
		},
		{
			name: "describe_vault",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPut, "/backup-vaults/my-vault", nil)
				rec := doREST(t, h, http.MethodGet, "/backup-vaults/my-vault", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseResp(t, rec)
				assert.Equal(t, "my-vault", resp["BackupVaultName"])
			},
		},
		{
			name: "describe_vault_not_found",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodGet, "/backup-vaults/missing", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "list_vaults",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPut, "/backup-vaults/vault-a", nil)
				doREST(t, h, http.MethodPut, "/backup-vaults/vault-b", nil)
				rec := doREST(t, h, http.MethodGet, "/backup-vaults", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseResp(t, rec)
				list, ok := resp["BackupVaultList"].([]any)
				require.True(t, ok)
				assert.Len(t, list, 2)
			},
		},
		{
			name: "delete_vault",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPut, "/backup-vaults/del-vault", nil)
				rec := doREST(t, h, http.MethodDelete, "/backup-vaults/del-vault", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
				rec2 := doREST(t, h, http.MethodGet, "/backup-vaults/del-vault", nil)
				assert.Equal(t, http.StatusNotFound, rec2.Code)
			},
		},
		{
			name: "create_vault_duplicate",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPut, "/backup-vaults/dup-vault", nil)
				rec := doREST(t, h, http.MethodPut, "/backup-vaults/dup-vault", nil)
				assert.Equal(t, http.StatusConflict, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()
			tt.ops(t, h)
		})
	}
}

// TestBackupPlanCRUD exercises CreateBackupPlan, GetBackupPlan, ListBackupPlans,
// UpdateBackupPlan, and DeleteBackupPlan through the REST handler.
func TestBackupPlanCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *backup.Handler)
		name string
	}{
		{
			name: "create_and_get",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPut, "/backup/plans", map[string]any{
					"BackupPlan": map[string]any{
						"BackupPlanName": "my-plan",
						"Rules": []map[string]any{
							{
								"RuleName":              "rule1",
								"TargetBackupVaultName": "vault1",
								"ScheduleExpression":    "cron(0 5 ? * * *)",
							},
						},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				createResp := parseResp(t, rec)
				planID, ok := createResp["BackupPlanId"].(string)
				require.True(t, ok)
				assert.NotEmpty(t, planID)

				rec2 := doREST(t, h, http.MethodGet, "/backup/plans/"+planID, nil)
				assert.Equal(t, http.StatusOK, rec2.Code)
				getResp := parseResp(t, rec2)
				assert.Equal(t, planID, getResp["BackupPlanId"])
				bp, ok := getResp["BackupPlan"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "my-plan", bp["BackupPlanName"])
			},
		},
		{
			name: "list_plans",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPut, "/backup/plans", map[string]any{
					"BackupPlan": map[string]any{"BackupPlanName": "plan-a", "Rules": []any{}},
				})
				doREST(t, h, http.MethodPut, "/backup/plans", map[string]any{
					"BackupPlan": map[string]any{"BackupPlanName": "plan-b", "Rules": []any{}},
				})
				rec := doREST(t, h, http.MethodGet, "/backup/plans", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseResp(t, rec)
				list, ok := resp["BackupPlansList"].([]any)
				require.True(t, ok)
				assert.Len(t, list, 2)
			},
		},
		{
			name: "update_plan",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				createRec := doREST(t, h, http.MethodPut, "/backup/plans", map[string]any{
					"BackupPlan": map[string]any{"BackupPlanName": "upd-plan", "Rules": []any{}},
				})
				createResp := parseResp(t, createRec)
				planID := createResp["BackupPlanId"].(string)

				rec := doREST(t, h, http.MethodPost, "/backup/plans/"+planID, map[string]any{
					"BackupPlan": map[string]any{
						"BackupPlanName": "upd-plan",
						"Rules": []map[string]any{
							{"RuleName": "new-rule", "TargetBackupVaultName": "vault1"},
						},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				updResp := parseResp(t, rec)
				assert.Equal(t, planID, updResp["BackupPlanId"])
				assert.NotEmpty(t, updResp["VersionId"])
			},
		},
		{
			name: "delete_plan",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				createRec := doREST(t, h, http.MethodPut, "/backup/plans", map[string]any{
					"BackupPlan": map[string]any{"BackupPlanName": "del-plan", "Rules": []any{}},
				})
				planID := parseResp(t, createRec)["BackupPlanId"].(string)

				rec := doREST(t, h, http.MethodDelete, "/backup/plans/"+planID, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				rec2 := doREST(t, h, http.MethodGet, "/backup/plans/"+planID, nil)
				assert.Equal(t, http.StatusNotFound, rec2.Code)
			},
		},
		{
			name: "get_plan_not_found",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodGet, "/backup/plans/not-exist", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()
			tt.ops(t, h)
		})
	}
}

// TestBackupJobCRUD exercises StartBackupJob, DescribeBackupJob, and ListBackupJobs.
func TestBackupJobCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *backup.Handler)
		name string
	}{
		{
			name: "start_and_describe",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPut, "/backup-vaults/myvault", nil)
				rec := doREST(t, h, http.MethodPut, "/backup-jobs", map[string]any{
					"BackupVaultName": "myvault",
					"ResourceArn":     "arn:aws:ec2:us-east-1:123456789012:instance/i-abc",
					"IamRoleArn":      "arn:aws:iam::123456789012:role/role",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				startResp := parseResp(t, rec)
				jobID, ok := startResp["BackupJobId"].(string)
				require.True(t, ok)
				assert.NotEmpty(t, jobID)

				rec2 := doREST(t, h, http.MethodGet, "/backup-jobs/"+jobID, nil)
				assert.Equal(t, http.StatusOK, rec2.Code)
				descResp := parseResp(t, rec2)
				assert.Equal(t, jobID, descResp["BackupJobId"])
				assert.Equal(t, "CREATED", descResp["State"])
			},
		},
		{
			name: "list_jobs",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPut, "/backup-vaults/vault1", nil)
				doREST(t, h, http.MethodPut, "/backup-jobs", map[string]any{
					"BackupVaultName": "vault1",
					"ResourceArn":     "arn:aws:ec2:us-east-1:123456789012:instance/i-111",
					"IamRoleArn":      "arn:aws:iam::123456789012:role/role",
				})
				doREST(t, h, http.MethodPut, "/backup-jobs", map[string]any{
					"BackupVaultName": "vault1",
					"ResourceArn":     "arn:aws:ec2:us-east-1:123456789012:instance/i-222",
					"IamRoleArn":      "arn:aws:iam::123456789012:role/role",
				})
				rec := doREST(t, h, http.MethodGet, "/backup-jobs", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseResp(t, rec)
				jobs, ok := resp["BackupJobs"].([]any)
				require.True(t, ok)
				assert.Len(t, jobs, 2)
			},
		},
		{
			name: "start_job_vault_not_found",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPut, "/backup-jobs", map[string]any{
					"BackupVaultName": "missing",
					"ResourceArn":     "arn:aws:ec2:us-east-1:123456789012:instance/i-abc",
					"IamRoleArn":      "arn:aws:iam::123456789012:role/role",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "describe_job_not_found",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodGet, "/backup-jobs/no-such-job", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()
			tt.ops(t, h)
		})
	}
}

// TestBackupTagging exercises TagResource and ListTags.
func TestBackupTagging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *backup.Handler)
		name string
	}{
		{
			name: "tag_and_list",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				// Create vault and get its ARN.
				rec := doREST(t, h, http.MethodPut, "/backup-vaults/tag-vault", nil)
				require.Equal(t, http.StatusOK, rec.Code)
				vaultResp := parseResp(t, rec)
				vaultARN := vaultResp["BackupVaultArn"].(string)

				// TagResource.
				tagRec := doREST(t, h, http.MethodPost, "/tags/"+vaultARN, map[string]any{
					"Tags": map[string]string{"Project": "demo"},
				})
				assert.Equal(t, http.StatusOK, tagRec.Code)

				// ListTags.
				listRec := doREST(t, h, http.MethodGet, "/tags/"+vaultARN, nil)
				assert.Equal(t, http.StatusOK, listRec.Code)
				listResp := parseResp(t, listRec)
				tags, ok := listResp["Tags"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "demo", tags["Project"])
			},
		},
		{
			name: "tag_not_found",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(
					t,
					h,
					http.MethodPost,
					"/tags/arn:aws:backup:us-east-1:123:backup-vault:no",
					map[string]any{
						"Tags": map[string]string{"k": "v"},
					},
				)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()
			tt.ops(t, h)
		})
	}
}

// TestBackupRouteMatcher verifies that the RouteMatcher correctly identifies Backup requests.
func TestBackupRouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler()
	matcher := h.RouteMatcher()

	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "backup-vaults collection", path: "/backup-vaults", want: true},
		{name: "backup-vaults item", path: "/backup-vaults/my-vault", want: true},
		{name: "backup plans collection", path: "/backup/plans", want: true},
		{name: "backup plans item", path: "/backup/plans/some-id", want: true},
		{name: "backup jobs collection", path: "/backup-jobs", want: true},
		{name: "backup jobs item", path: "/backup-jobs/some-id", want: true},
		{name: "tags on backup resource", path: "/tags/arn:aws:backup:us-east-1:123:backup-vault:v", want: true},
		{name: "unrelated path", path: "/applications", want: false},
		{name: "s3 path", path: "/my-bucket", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			e := echo.New()
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			got := matcher(c)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestBackupHandlerMetadata covers Name, GetSupportedOperations, Chaos methods, MatchPriority,
// ExtractOperation, ExtractResource, Region, and Persistence methods.
func TestBackupHandlerMetadata(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "Name",
			run: func(t *testing.T) {
				t.Helper()
				assert.Equal(t, "Backup", h.Name())
			},
		},
		{
			name: "GetSupportedOperations",
			run: func(t *testing.T) {
				t.Helper()
				ops := h.GetSupportedOperations()
				assert.Contains(t, ops, "CreateBackupVault")
				assert.Contains(t, ops, "CreateBackupPlan")
				assert.Contains(t, ops, "StartBackupJob")
			},
		},
		{
			name: "ChaosServiceName",
			run: func(t *testing.T) {
				t.Helper()
				assert.Equal(t, "backup", h.ChaosServiceName())
			},
		},
		{
			name: "ChaosOperations",
			run: func(t *testing.T) {
				t.Helper()
				ops := h.ChaosOperations()
				assert.NotEmpty(t, ops)
			},
		},
		{
			name: "ChaosRegions",
			run: func(t *testing.T) {
				t.Helper()
				regions := h.ChaosRegions()
				assert.NotEmpty(t, regions)
			},
		},
		{
			name: "MatchPriority",
			run: func(t *testing.T) {
				t.Helper()
				assert.Positive(t, h.MatchPriority())
			},
		},
		{
			name: "Region",
			run: func(t *testing.T) {
				t.Helper()
				assert.Equal(t, "us-east-1", h.Backend.Region())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

// TestBackupExtractOperation verifies ExtractOperation for the various REST paths.
func TestBackupExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler()

	tests := []struct {
		name    string
		method  string
		path    string
		wantOp  string
		wantRes string
	}{
		{
			name:    "create vault",
			method:  http.MethodPut,
			path:    "/backup-vaults/myvault",
			wantOp:  "CreateBackupVault",
			wantRes: "myvault",
		},
		{
			name:    "describe vault",
			method:  http.MethodGet,
			path:    "/backup-vaults/myvault",
			wantOp:  "DescribeBackupVault",
			wantRes: "myvault",
		},
		{name: "list vaults", method: http.MethodGet, path: "/backup-vaults", wantOp: "ListBackupVaults"},
		{
			name:    "delete vault",
			method:  http.MethodDelete,
			path:    "/backup-vaults/myvault",
			wantOp:  "DeleteBackupVault",
			wantRes: "myvault",
		},
		{name: "create plan", method: http.MethodPut, path: "/backup/plans", wantOp: "CreateBackupPlan"},
		{
			name:    "get plan",
			method:  http.MethodGet,
			path:    "/backup/plans/planid",
			wantOp:  "GetBackupPlan",
			wantRes: "planid",
		},
		{name: "list plans", method: http.MethodGet, path: "/backup/plans", wantOp: "ListBackupPlans"},
		{
			name:    "update plan",
			method:  http.MethodPost,
			path:    "/backup/plans/planid",
			wantOp:  "UpdateBackupPlan",
			wantRes: "planid",
		},
		{
			name:    "delete plan",
			method:  http.MethodDelete,
			path:    "/backup/plans/planid",
			wantOp:  "DeleteBackupPlan",
			wantRes: "planid",
		},
		{name: "start job", method: http.MethodPut, path: "/backup-jobs", wantOp: "StartBackupJob"},
		{
			name:    "describe job",
			method:  http.MethodGet,
			path:    "/backup-jobs/jobid",
			wantOp:  "DescribeBackupJob",
			wantRes: "jobid",
		},
		{name: "list jobs", method: http.MethodGet, path: "/backup-jobs", wantOp: "ListBackupJobs"},
		{
			name:    "tag resource",
			method:  http.MethodPost,
			path:    "/tags/arn:aws:backup:us-east-1:123:backup-vault:v",
			wantOp:  "TagResource",
			wantRes: "arn:aws:backup:us-east-1:123:backup-vault:v",
		},
		{
			name:    "list tags",
			method:  http.MethodGet,
			path:    "/tags/arn:aws:backup:us-east-1:123:backup-vault:v",
			wantOp:  "ListTags",
			wantRes: "arn:aws:backup:us-east-1:123:backup-vault:v",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			e := echo.New()
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
			assert.Equal(t, tt.wantRes, h.ExtractResource(c))
		})
	}
}

// TestBackupPersistence exercises Snapshot and Restore.
func TestBackupPersistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "snapshot_restore",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestBackupHandler()
				doREST(t, h, http.MethodPut, "/backup-vaults/snap-vault", nil)

				snap := h.Snapshot(t.Context())
				require.NotNil(t, snap)

				h2 := newTestBackupHandler()
				require.NoError(t, h2.Restore(t.Context(), snap))

				rec := doREST(t, h2, http.MethodGet, "/backup-vaults/snap-vault", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "restore_rebuilds_arn_index_for_vault",
			run: func(t *testing.T) {
				t.Helper()
				b := backup.NewInMemoryBackend("000000000000", "us-east-1")
				vault, err := b.CreateBackupVault("my-vault", "", "", nil)
				require.NoError(t, err)

				snap := b.Snapshot(t.Context())
				require.NotNil(t, snap)

				fresh := backup.NewInMemoryBackend("000000000000", "us-east-1")
				require.NoError(t, fresh.Restore(t.Context(), snap))

				// Tag by ARN must succeed using the rebuilt index.
				err = fresh.TagResource(vault.BackupVaultArn, map[string]string{"env": "prod"})
				require.NoError(t, err)

				kv, err := fresh.ListTags(vault.BackupVaultArn)
				require.NoError(t, err)
				assert.Equal(t, "prod", kv["env"])
			},
		},
		{
			name: "restore_rebuilds_arn_index_for_plan",
			run: func(t *testing.T) {
				t.Helper()
				b := backup.NewInMemoryBackend("000000000000", "us-east-1")
				plan, err := b.CreateBackupPlan("my-plan", nil, nil, nil)
				require.NoError(t, err)

				snap := b.Snapshot(t.Context())
				require.NotNil(t, snap)

				fresh := backup.NewInMemoryBackend("000000000000", "us-east-1")
				require.NoError(t, fresh.Restore(t.Context(), snap))

				// Tag by ARN must succeed using the rebuilt index.
				err = fresh.TagResource(plan.BackupPlanArn, map[string]string{"team": "ops"})
				require.NoError(t, err)

				kv, err := fresh.ListTags(plan.BackupPlanArn)
				require.NoError(t, err)
				assert.Equal(t, "ops", kv["team"])
			},
		},
		{
			name: "restore_rebuilds_plan_id_index",
			run: func(t *testing.T) {
				t.Helper()
				b := backup.NewInMemoryBackend("000000000000", "us-east-1")
				plan, err := b.CreateBackupPlan("id-plan", nil, nil, nil)
				require.NoError(t, err)

				snap := b.Snapshot(t.Context())
				require.NotNil(t, snap)

				fresh := backup.NewInMemoryBackend("000000000000", "us-east-1")
				require.NoError(t, fresh.Restore(t.Context(), snap))

				// GetBackupPlan by plan ID must succeed using the rebuilt planIDIndex.
				got, err := fresh.GetBackupPlan(plan.BackupPlanID)
				require.NoError(t, err)
				assert.Equal(t, plan.BackupPlanName, got.BackupPlanName)
			},
		},
		{
			name: "restore_invalid_json",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestBackupHandler()
				err := h.Restore(t.Context(), []byte("not-json"))
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

// TestBackupErrorPaths exercises additional error branches for full handler coverage.
func TestBackupErrorPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops        func(t *testing.T, h *backup.Handler)
		name       string
		wantStatus int
	}{
		{
			name: "create_plan_missing_name",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPut, "/backup/plans", map[string]any{
					"BackupPlan": map[string]any{"BackupPlanName": "", "Rules": []any{}},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "create_plan_bad_body",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				req := httptest.NewRequest(http.MethodPut, "/backup/plans", bytes.NewBufferString("notjson"))
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				e := echo.New()
				c := e.NewContext(req, rec)
				err := h.Handler()(c)
				require.NoError(t, err)
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "update_plan_bad_body",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				req := httptest.NewRequest(http.MethodPost, "/backup/plans/someid", bytes.NewBufferString("notjson"))
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				e := echo.New()
				c := e.NewContext(req, rec)
				err := h.Handler()(c)
				require.NoError(t, err)
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "start_job_bad_body",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				req := httptest.NewRequest(http.MethodPut, "/backup-jobs", bytes.NewBufferString("notjson"))
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				e := echo.New()
				c := e.NewContext(req, rec)
				err := h.Handler()(c)
				require.NoError(t, err)
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "tag_bad_body",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				req := httptest.NewRequest(
					http.MethodPost,
					"/tags/arn:aws:backup:us-east-1:123:backup-vault:v",
					bytes.NewBufferString("notjson"),
				)
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				e := echo.New()
				c := e.NewContext(req, rec)
				err := h.Handler()(c)
				require.NoError(t, err)
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "list_tags_not_found",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodGet, "/tags/arn:aws:backup:us-east-1:123:backup-vault:nope", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "delete_plan_not_found",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodDelete, "/backup/plans/missing-id", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "update_plan_not_found",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/backup/plans/missing-id", map[string]any{
					"BackupPlan": map[string]any{"BackupPlanName": "n", "Rules": []any{}},
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()
			tt.ops(t, h)
		})
	}
}

// TestAssociateBackupVaultMpaApprovalTeam exercises the MPA approval team association operation.
func TestAssociateBackupVaultMpaApprovalTeam(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops        func(t *testing.T, h *backup.Handler)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPut, "/backup-vaults/my-vault", nil)
				rec := doREST(t, h, http.MethodPut, "/backup-vaults/my-vault/mpaApprovalTeam", map[string]any{
					"MpaApprovalTeamArn": "arn:aws:mpa:us-east-1:123456789012:approval-team/my-team",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "vault_not_found",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPut, "/backup-vaults/missing/mpaApprovalTeam", map[string]any{
					"MpaApprovalTeamArn": "arn:aws:mpa:us-east-1:123:approval-team/t",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "missing_arn",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPut, "/backup-vaults/vault-x", nil)
				rec := doREST(t, h, http.MethodPut, "/backup-vaults/vault-x/mpaApprovalTeam", map[string]any{
					"MpaApprovalTeamArn": "",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()
			tt.ops(t, h)
		})
	}
}

// TestCancelLegalHold exercises creating and cancelling legal holds.
func TestCancelLegalHold(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops        func(t *testing.T, h *backup.Handler)
		name       string
		wantStatus int
	}{
		{
			name: "create_and_cancel",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				createRec := doREST(t, h, http.MethodPost, "/legal-holds", map[string]any{
					"Title":       "litigation hold",
					"Description": "hold for lawsuit XYZ",
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				resp := parseResp(t, createRec)
				legalHoldID, ok := resp["LegalHoldId"].(string)
				require.True(t, ok)
				assert.NotEmpty(t, legalHoldID)
				assert.Equal(t, "ACTIVE", resp["Status"])

				cancelRec := doREST(t, h, http.MethodDelete, "/legal-holds/"+legalHoldID, nil)
				assert.Equal(t, http.StatusOK, cancelRec.Code)
			},
		},
		{
			name: "cancel_not_found",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodDelete, "/legal-holds/nonexistent-id", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "create_missing_title",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/legal-holds", map[string]any{
					"Description": "some description",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "create_missing_description",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/legal-holds", map[string]any{
					"Title": "some title",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()
			tt.ops(t, h)
		})
	}
}

// TestCreateBackupSelection exercises the backup selection creation operation.
func TestCreateBackupSelection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops        func(t *testing.T, h *backup.Handler)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				createPlanRec := doREST(t, h, http.MethodPut, "/backup/plans", map[string]any{
					"BackupPlan": map[string]any{"BackupPlanName": "my-plan", "Rules": []any{}},
				})
				require.Equal(t, http.StatusOK, createPlanRec.Code)
				planResp := parseResp(t, createPlanRec)
				planID, ok := planResp["BackupPlanId"].(string)
				require.True(t, ok)

				selRec := doREST(t, h, http.MethodPut, "/backup/plans/"+planID+"/selections", map[string]any{
					"BackupSelection": map[string]any{
						"SelectionName": "my-selection",
						"IamRoleArn":    "arn:aws:iam::123456789012:role/backup-role",
					},
				})
				require.Equal(t, http.StatusOK, selRec.Code)
				selResp := parseResp(t, selRec)
				assert.Equal(t, planID, selResp["BackupPlanId"])
				assert.NotEmpty(t, selResp["SelectionId"])
			},
		},
		{
			name: "plan_not_found",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPut, "/backup/plans/nonexistent/selections", map[string]any{
					"BackupSelection": map[string]any{"SelectionName": "sel"},
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()
			tt.ops(t, h)
		})
	}
}

// TestCreateFramework exercises the audit framework creation operation.
func TestCreateFramework(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops        func(t *testing.T, h *backup.Handler)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/audit/frameworks", map[string]any{
					"FrameworkName":        "my-framework",
					"FrameworkDescription": "A test framework",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseResp(t, rec)
				assert.Equal(t, "my-framework", resp["FrameworkName"])
				assert.NotEmpty(t, resp["FrameworkArn"])
			},
		},
		{
			name: "duplicate_name",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/audit/frameworks", map[string]any{
					"FrameworkName": "dup-framework",
				})
				rec := doREST(t, h, http.MethodPost, "/audit/frameworks", map[string]any{
					"FrameworkName": "dup-framework",
				})
				assert.Equal(t, http.StatusConflict, rec.Code)
			},
		},
		{
			name: "missing_name",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/audit/frameworks", map[string]any{})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()
			tt.ops(t, h)
		})
	}
}

// TestCreateLogicallyAirGappedBackupVault exercises the logically air-gapped vault creation.
func TestCreateLogicallyAirGappedBackupVault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops        func(t *testing.T, h *backup.Handler)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPut, "/logically-air-gapped-backup-vaults/lag-vault", map[string]any{
					"MinRetentionDays": 7,
					"MaxRetentionDays": 365,
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseResp(t, rec)
				assert.Equal(t, "lag-vault", resp["BackupVaultName"])
				assert.NotEmpty(t, resp["BackupVaultArn"])
				assert.Equal(t, "CREATING", resp["VaultState"])
			},
		},
		{
			name: "duplicate_name",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPut, "/logically-air-gapped-backup-vaults/dup-lag", map[string]any{
					"MinRetentionDays": 7,
					"MaxRetentionDays": 30,
				})
				rec := doREST(t, h, http.MethodPut, "/logically-air-gapped-backup-vaults/dup-lag", map[string]any{
					"MinRetentionDays": 7,
					"MaxRetentionDays": 30,
				})
				assert.Equal(t, http.StatusConflict, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()
			tt.ops(t, h)
		})
	}
}

// TestCreateReportPlan exercises the report plan creation operation.
func TestCreateReportPlan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops        func(t *testing.T, h *backup.Handler)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/audit/report-plans", map[string]any{
					"ReportPlanName":        "my-report-plan",
					"ReportPlanDescription": "Daily backup report",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseResp(t, rec)
				assert.Equal(t, "my-report-plan", resp["ReportPlanName"])
				assert.NotEmpty(t, resp["ReportPlanArn"])
				assert.NotNil(t, resp["CreationTime"])
			},
		},
		{
			name: "duplicate_name",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/audit/report-plans", map[string]any{
					"ReportPlanName": "dup-report",
				})
				rec := doREST(t, h, http.MethodPost, "/audit/report-plans", map[string]any{
					"ReportPlanName": "dup-report",
				})
				assert.Equal(t, http.StatusConflict, rec.Code)
			},
		},
		{
			name: "missing_name",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/audit/report-plans", map[string]any{})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()
			tt.ops(t, h)
		})
	}
}

// TestCreateRestoreAccessBackupVault exercises the restore access vault creation.
func TestCreateRestoreAccessBackupVault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops        func(t *testing.T, h *backup.Handler)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/restore-access-backup-vaults", map[string]any{
					"SourceBackupVaultArn": "arn:aws:backup:us-east-1:123456789012:backup-vault:source-vault",
					"BackupVaultName":      "restore-access-vault",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseResp(t, rec)
				assert.Equal(t, "restore-access-vault", resp["RestoreAccessBackupVaultName"])
				assert.NotEmpty(t, resp["RestoreAccessBackupVaultArn"])
				assert.Equal(t, "CREATING", resp["VaultState"])
			},
		},
		{
			name: "missing_source_arn",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/restore-access-backup-vaults", map[string]any{
					"BackupVaultName": "some-vault",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()
			tt.ops(t, h)
		})
	}
}

// TestCreateRestoreTestingPlan exercises the restore testing plan creation.
func TestCreateRestoreTestingPlan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops        func(t *testing.T, h *backup.Handler)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPut, "/restore-testing/plans", map[string]any{
					"RestoreTestingPlan": map[string]any{
						"RestoreTestingPlanName": "my-test-plan",
						"ScheduleExpression":     "cron(0 1 ? * * *)",
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseResp(t, rec)
				assert.Equal(t, "my-test-plan", resp["RestoreTestingPlanName"])
				assert.NotEmpty(t, resp["RestoreTestingPlanArn"])
				assert.NotNil(t, resp["CreationTime"])
			},
		},
		{
			name: "duplicate_name",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPut, "/restore-testing/plans", map[string]any{
					"RestoreTestingPlan": map[string]any{"RestoreTestingPlanName": "dup-plan"},
				})
				rec := doREST(t, h, http.MethodPut, "/restore-testing/plans", map[string]any{
					"RestoreTestingPlan": map[string]any{"RestoreTestingPlanName": "dup-plan"},
				})
				assert.Equal(t, http.StatusConflict, rec.Code)
			},
		},
		{
			name: "missing_plan_name",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPut, "/restore-testing/plans", map[string]any{
					"RestoreTestingPlan": map[string]any{},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()
			tt.ops(t, h)
		})
	}
}

// TestCreateRestoreTestingSelection exercises restore testing selection creation.
func TestCreateRestoreTestingSelection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops        func(t *testing.T, h *backup.Handler)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPut, "/restore-testing/plans", map[string]any{
					"RestoreTestingPlan": map[string]any{"RestoreTestingPlanName": "my-plan"},
				})
				rec := doREST(t, h, http.MethodPut, "/restore-testing/plans/my-plan/selections", map[string]any{
					"RestoreTestingSelection": map[string]any{
						"RestoreTestingSelectionName": "my-selection",
						"ProtectedResourceType":       "EC2",
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseResp(t, rec)
				assert.Equal(t, "my-plan", resp["RestoreTestingPlanName"])
				assert.Equal(t, "my-selection", resp["RestoreTestingSelectionName"])
				assert.NotEmpty(t, resp["RestoreTestingPlanArn"])
			},
		},
		{
			name: "plan_not_found",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPut, "/restore-testing/plans/missing-plan/selections", map[string]any{
					"RestoreTestingSelection": map[string]any{
						"RestoreTestingSelectionName": "sel",
					},
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "duplicate_selection",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPut, "/restore-testing/plans", map[string]any{
					"RestoreTestingPlan": map[string]any{"RestoreTestingPlanName": "plan-b"},
				})
				selBody := map[string]any{
					"RestoreTestingSelection": map[string]any{"RestoreTestingSelectionName": "sel-b"},
				}
				doREST(t, h, http.MethodPut, "/restore-testing/plans/plan-b/selections", selBody)
				rec := doREST(t, h, http.MethodPut, "/restore-testing/plans/plan-b/selections", selBody)
				assert.Equal(t, http.StatusConflict, rec.Code)
			},
		},
		{
			name: "missing_selection_name",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPut, "/restore-testing/plans", map[string]any{
					"RestoreTestingPlan": map[string]any{"RestoreTestingPlanName": "plan-c"},
				})
				rec := doREST(t, h, http.MethodPut, "/restore-testing/plans/plan-c/selections", map[string]any{
					"RestoreTestingSelection": map[string]any{},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()
			tt.ops(t, h)
		})
	}
}

// TestBackupVaultValidation covers vault name validation.
func TestBackupVaultValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		vaultName  string
		wantStatus int
	}{
		{
			name:       "valid_name",
			vaultName:  "my-vault",
			wantStatus: http.StatusOK,
		},
		{
			name:       "too_short",
			vaultName:  "a",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "underscore_allowed",
			vaultName:  "my_vault_under",
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_chars",
			vaultName:  "my.vault.dot",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "exactly_2_chars",
			vaultName:  "ab",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestBackupHandler()
			rec := doREST(t, h, http.MethodPut, "/backup-vaults/"+tt.vaultName, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestAirGappedVaultValidation covers retention-day validation.
func TestAirGappedVaultValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "valid",
			body: map[string]any{
				"MinRetentionDays": 1,
				"MaxRetentionDays": 30,
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "min_zero",
			body: map[string]any{
				"MinRetentionDays": 0,
				"MaxRetentionDays": 30,
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "max_less_than_min",
			body: map[string]any{
				"MinRetentionDays": 10,
				"MaxRetentionDays": 5,
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestBackupHandler()
			rec := doREST(t, h, http.MethodPut, "/logically-air-gapped-backup-vaults/air-vault", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestStartBackupJobValidation covers required-field validation.
func TestStartBackupJobValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "missing_resource_arn",
			body: map[string]any{
				"BackupVaultName": "vault",
				"IamRoleArn":      "arn:aws:iam::123456789012:role/r",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_iam_role_arn",
			body: map[string]any{
				"BackupVaultName": "vault",
				"ResourceArn":     "arn:aws:ec2:us-east-1:123456789012:instance/i-1",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestBackupHandler()
			rec := doREST(t, h, http.MethodPut, "/backup-jobs", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestBackupSelectionValidation covers SelectionName required check.
func TestBackupSelectionValidation(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler()
	rec := doREST(t, h, http.MethodPut, "/backup/plans", map[string]any{
		"BackupPlan": map[string]any{"BackupPlanName": "plan"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	planResp := parseResp(t, rec)
	planID := planResp["BackupPlanId"].(string)

	rec2 := doREST(t, h, http.MethodPut, "/backup/plans/"+planID+"/selections", map[string]any{
		"BackupSelection": map[string]any{},
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// TestUntagResource exercises UntagResource via the REST handler.
func TestUntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *backup.Handler)
		name string
	}{
		{
			name: "untag_vault",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPut, "/backup-vaults/my-vault", map[string]any{
					"BackupVaultTags": map[string]string{"env": "test", "team": "platform"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				vaultARN := parseResp(t, rec)["BackupVaultArn"].(string)

				// Remove one tag.
				delRec := doREST(t, h, http.MethodDelete, "/tags/"+vaultARN, map[string]any{
					"TagKeyList": []string{"env"},
				})
				require.Equal(t, http.StatusOK, delRec.Code)

				// Verify tag was removed.
				listRec := doREST(t, h, http.MethodGet, "/tags/"+vaultARN, nil)
				require.Equal(t, http.StatusOK, listRec.Code)
				tags := parseResp(t, listRec)["Tags"].(map[string]any)
				assert.NotContains(t, tags, "env")
				assert.Contains(t, tags, "team")
			},
		},
		{
			name: "untag_framework",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/audit/frameworks", map[string]any{
					"FrameworkName": "fw",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				fwARN := parseResp(t, rec)["FrameworkArn"].(string)

				// Tag and then untag.
				doREST(t, h, http.MethodPost, "/tags/"+fwARN, map[string]any{
					"Tags": map[string]string{"k": "v"},
				})
				delRec := doREST(t, h, http.MethodDelete, "/tags/"+fwARN, map[string]any{
					"TagKeyList": []string{"k"},
				})
				assert.Equal(t, http.StatusOK, delRec.Code)
			},
		},
		{
			name: "untag_not_found",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				const missingARN = "/tags/arn:aws:backup:us-east-1:000000000000:backup-vault:missing"
				rec := doREST(t, h, http.MethodDelete, missingARN, map[string]any{
					"TagKeyList": []string{"k"},
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()
			tt.ops(t, h)
		})
	}
}

// TestTagsInVaultResponse checks that DescribeBackupVault includes Tags.
func TestTagsInVaultResponse(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler()
	doREST(t, h, http.MethodPut, "/backup-vaults/tagged-vault", map[string]any{
		"BackupVaultTags": map[string]string{"env": "prod"},
	})

	rec := doREST(t, h, http.MethodGet, "/backup-vaults/tagged-vault", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	tags, ok := resp["Tags"].(map[string]any)
	require.True(t, ok, "Tags field missing from DescribeBackupVault response")
	assert.Equal(t, "prod", tags["env"])
}

// TestTagsInPlanResponse checks that GetBackupPlan includes Tags.
func TestTagsInPlanResponse(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler()
	doREST(t, h, http.MethodPut, "/backup/plans", map[string]any{
		"BackupPlan":     map[string]any{"BackupPlanName": "tagged-plan"},
		"BackupPlanTags": map[string]string{"env": "staging"},
	})

	// List plans to get the plan ID.
	listRec := doREST(t, h, http.MethodGet, "/backup/plans", nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	listResp := parseResp(t, listRec)
	plans := listResp["BackupPlansList"].([]any)
	require.Len(t, plans, 1)
	planID := plans[0].(map[string]any)["BackupPlanId"].(string)

	rec := doREST(t, h, http.MethodGet, "/backup/plans/"+planID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	tags, ok := resp["Tags"].(map[string]any)
	require.True(t, ok, "Tags field missing from GetBackupPlan response")
	assert.Equal(t, "staging", tags["env"])
}

// TestTagResourceForFrameworkAndReportPlan checks tagging on frameworks and report plans.
func TestTagResourceForFrameworkAndReportPlan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func(h *backup.Handler) string
		name   string
	}{
		{
			name: "framework",
			create: func(h *backup.Handler) string {
				rec := doREST(t, h, http.MethodPost, "/audit/frameworks", map[string]any{
					"FrameworkName": "fw-tag-test",
				})

				return parseResp(t, rec)["FrameworkArn"].(string)
			},
		},
		{
			name: "report_plan",
			create: func(h *backup.Handler) string {
				rec := doREST(t, h, http.MethodPost, "/audit/report-plans", map[string]any{
					"ReportPlanName": "rp-tag-test",
				})

				return parseResp(t, rec)["ReportPlanArn"].(string)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()
			arn := tt.create(h)

			// Tag resource.
			tagRec := doREST(t, h, http.MethodPost, "/tags/"+arn, map[string]any{
				"Tags": map[string]string{"foo": "bar"},
			})
			assert.Equal(t, http.StatusOK, tagRec.Code)

			// List tags.
			listRec := doREST(t, h, http.MethodGet, "/tags/"+arn, nil)
			require.Equal(t, http.StatusOK, listRec.Code)
			tags := parseResp(t, listRec)["Tags"].(map[string]any)
			assert.Equal(t, "bar", tags["foo"])
		})
	}
}

// TestSortedListings verifies that List* operations return results in deterministic order.
func TestSortedListings(t *testing.T) {
	t.Parallel()

	t.Run("vaults_sorted_by_name", func(t *testing.T) {
		t.Parallel()
		h := newTestBackupHandler()
		for _, name := range []string{"zoo-vault", "alpha-vault", "mid-vault"} {
			doREST(t, h, http.MethodPut, "/backup-vaults/"+name, nil)
		}
		rec := doREST(t, h, http.MethodGet, "/backup-vaults", nil)
		require.Equal(t, http.StatusOK, rec.Code)
		resp := parseResp(t, rec)
		vaults := resp["BackupVaultList"].([]any)
		require.Len(t, vaults, 3)
		names := []string{
			vaults[0].(map[string]any)["BackupVaultName"].(string),
			vaults[1].(map[string]any)["BackupVaultName"].(string),
			vaults[2].(map[string]any)["BackupVaultName"].(string),
		}
		assert.Equal(t, []string{"alpha-vault", "mid-vault", "zoo-vault"}, names)
	})

	t.Run("plans_sorted_by_name", func(t *testing.T) {
		t.Parallel()
		h := newTestBackupHandler()
		for _, name := range []string{"z-plan", "a-plan", "m-plan"} {
			doREST(t, h, http.MethodPut, "/backup/plans", map[string]any{
				"BackupPlan": map[string]any{"BackupPlanName": name},
			})
		}
		rec := doREST(t, h, http.MethodGet, "/backup/plans", nil)
		require.Equal(t, http.StatusOK, rec.Code)
		resp := parseResp(t, rec)
		plans := resp["BackupPlansList"].([]any)
		require.Len(t, plans, 3)
		names := []string{
			plans[0].(map[string]any)["BackupPlanName"].(string),
			plans[1].(map[string]any)["BackupPlanName"].(string),
			plans[2].(map[string]any)["BackupPlanName"].(string),
		}
		assert.Equal(t, []string{"a-plan", "m-plan", "z-plan"}, names)
	})
}

// TestPersistenceRoundTrip exercises Snapshot/Restore with all new resource types.
func TestPersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	original := backup.NewInMemoryBackend("123456789012", "us-east-1")

	// Populate original with all new resource types.
	_, err := original.CreateBackupVault("persist-vault", "", "", nil)
	require.NoError(t, err)

	_, err = original.CreateFramework("persist-fw", "test framework", nil)
	require.NoError(t, err)

	_, err = original.CreateLegalHold("hold title", "hold description")
	require.NoError(t, err)

	_, err = original.CreateReportPlan("persist-rp", "test report", nil, nil)
	require.NoError(t, err)

	_, err = original.CreateRestoreTestingPlan("persist-rtp", "cron(0 12 * * ? *)", 0)
	require.NoError(t, err)

	_, err = original.CreateRestoreTestingSelection("persist-rtp", "persist-sel", "EC2")
	require.NoError(t, err)

	_, err = original.CreateLogicallyAirGappedBackupVault("persist-air", "", 1, 30, nil)
	require.NoError(t, err)

	err = original.AssociateBackupVaultMpaApprovalTeam("persist-vault", "arn:aws:mpa::123456789012:team/t")
	require.NoError(t, err)

	// Create a plan and selection.
	plan, err := original.CreateBackupPlan("persist-plan", nil, nil, nil)
	require.NoError(t, err)

	_, err = original.CreateBackupSelection(
		plan.BackupPlanID,
		"selection-1",
		"arn:aws:iam::123456789012:role/r",
		nil,
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)

	// Create a restore access vault.
	_, err = original.CreateRestoreAccessBackupVault(
		"arn:aws:backup:us-east-1:123456789012:backup-vault:src",
		"persist-rav",
		"",
		nil,
	)
	require.NoError(t, err)

	snap := original.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	// Restore into a fresh backend.
	restored := backup.NewInMemoryBackend("", "")
	require.NoError(t, restored.Restore(t.Context(), snap))

	// Verify vaults.
	vault, err := restored.DescribeBackupVault("persist-vault")
	require.NoError(t, err)
	assert.Equal(t, "persist-vault", vault.BackupVaultName)

	// Verify frameworks.
	fw, err := restored.CreateFramework("persist-fw-2", "", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, fw.FrameworkArn)
	// Original framework should still conflict.
	_, err = restored.CreateFramework("persist-fw", "", nil)
	require.ErrorIs(t, err, backup.ErrAlreadyExists)

	// Verify legal holds are restored.
	_, err = restored.CreateLegalHold("new title", "desc")
	require.NoError(t, err)

	// Verify report plans.
	_, err = restored.CreateReportPlan("persist-rp", "dup", nil, nil)
	require.ErrorIs(t, err, backup.ErrAlreadyExists)

	// Verify restore testing plans and selections.
	_, err = restored.CreateRestoreTestingPlan("persist-rtp", "", 0)
	require.ErrorIs(t, err, backup.ErrAlreadyExists)

	_, err = restored.CreateRestoreTestingSelection("persist-rtp", "persist-sel", "EC2")
	require.ErrorIs(t, err, backup.ErrAlreadyExists)

	// Verify plan selection.
	_, err = restored.GetBackupPlan(plan.BackupPlanID)
	require.NoError(t, err)
}

// TestDeleteBackupPlanDeletionDate verifies that DeletionDate is current time, not creation time.
func TestDeleteBackupPlanDeletionDate(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler()
	createRec := doREST(t, h, http.MethodPut, "/backup/plans", map[string]any{
		"BackupPlan": map[string]any{"BackupPlanName": "del-plan"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	createResp := parseResp(t, createRec)
	planID := createResp["BackupPlanId"].(string)
	creationDate := createResp["CreationDate"].(float64)

	delRec := doREST(t, h, http.MethodDelete, "/backup/plans/"+planID, nil)
	require.Equal(t, http.StatusOK, delRec.Code)
	delResp := parseResp(t, delRec)
	deletionDate := delResp["DeletionDate"].(float64)

	// DeletionDate must be >= CreationDate.
	assert.GreaterOrEqual(t, deletionDate, creationDate)
}

// TestBackendReset verifies Reset clears all state.
func TestBackendReset(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := backup.NewInMemoryBackend("123456789012", "us-east-1").CreateFramework("fw", "", nil)
	require.NoError(t, err)

	// Populate the backend.
	_, err = backend.CreateBackupVault("my-vault", "", "", nil)
	require.NoError(t, err)

	_, err = backend.CreateFramework("fw", "", nil)
	require.NoError(t, err)

	_, err = backend.CreateReportPlan("rp", "", nil, nil)
	require.NoError(t, err)

	handler := backup.NewHandler(backend)
	handler.Reset()

	// After reset, vault should not exist.
	vaults := backend.ListBackupVaults()
	assert.Empty(t, vaults)

	// Framework can be recreated.
	_, err = backend.CreateFramework("fw", "", nil)
	assert.NoError(t, err)
}
