package backup_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

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
