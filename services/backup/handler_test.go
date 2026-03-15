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
				doREST(t, h, http.MethodPut, "/backup-jobs", map[string]any{"BackupVaultName": "vault1"})
				doREST(t, h, http.MethodPut, "/backup-jobs", map[string]any{"BackupVaultName": "vault1"})
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
				rec := doREST(t, h, http.MethodPut, "/backup-jobs", map[string]any{"BackupVaultName": "missing"})
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

				snap := h.Snapshot()
				require.NotNil(t, snap)

				h2 := newTestBackupHandler()
				require.NoError(t, h2.Restore(snap))

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

				snap := b.Snapshot()
				require.NotNil(t, snap)

				fresh := backup.NewInMemoryBackend("000000000000", "us-east-1")
				require.NoError(t, fresh.Restore(snap))

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
				plan, err := b.CreateBackupPlan("my-plan", nil, nil)
				require.NoError(t, err)

				snap := b.Snapshot()
				require.NotNil(t, snap)

				fresh := backup.NewInMemoryBackend("000000000000", "us-east-1")
				require.NoError(t, fresh.Restore(snap))

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
				plan, err := b.CreateBackupPlan("id-plan", nil, nil)
				require.NoError(t, err)

				snap := b.Snapshot()
				require.NotNil(t, snap)

				fresh := backup.NewInMemoryBackend("000000000000", "us-east-1")
				require.NoError(t, fresh.Restore(snap))

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
				err := h.Restore([]byte("not-json"))
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
