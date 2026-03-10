package backup_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/backup"
)

func newTestBackupHandler(t *testing.T) *backup.Handler {
	t.Helper()

	return backup.NewHandler(backup.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doBackupRequest(t *testing.T, h *backup.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	} else {
		bodyBytes = []byte("{}")
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AmazonBackupService."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func doInvalidBackupRequest(t *testing.T, h *backup.Handler, action string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("not-json"))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AmazonBackupService."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestBackupHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler(t)
	assert.Equal(t, "Backup", h.Name())
}

func TestBackupHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateBackupVault")
	assert.Contains(t, ops, "DescribeBackupVault")
	assert.Contains(t, ops, "ListBackupVaults")
	assert.Contains(t, ops, "DeleteBackupVault")
	assert.Contains(t, ops, "CreateBackupPlan")
	assert.Contains(t, ops, "GetBackupPlan")
	assert.Contains(t, ops, "ListBackupPlans")
	assert.Contains(t, ops, "UpdateBackupPlan")
	assert.Contains(t, ops, "DeleteBackupPlan")
	assert.Contains(t, ops, "StartBackupJob")
	assert.Contains(t, ops, "DescribeBackupJob")
	assert.Contains(t, ops, "ListBackupJobs")
	assert.Contains(t, ops, "TagResource")
	assert.Contains(t, ops, "ListTags")
}

func TestBackupHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler(t)
	assert.Equal(t, service.PriorityHeaderExact, h.MatchPriority())
}

func TestBackupHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		target    string
		wantMatch bool
	}{
		{name: "Match", target: "AmazonBackupService.CreateBackupVault", wantMatch: true},
		{name: "NoMatch", target: "AWSScheduler.CreateSchedule", wantMatch: false},
		{name: "Empty", target: "", wantMatch: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestBackupHandler(t)
			matcher := h.RouteMatcher()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantMatch, matcher(c))
		})
	}
}

func TestBackupHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler(t)
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "AmazonBackupService.CreateBackupVault")
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "CreateBackupVault", h.ExtractOperation(c))
}

func TestBackupHandler_CreateBackupVault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
		wantArn  bool
	}{
		{
			name:     "success",
			body:     map[string]any{"BackupVaultName": "my-vault"},
			wantCode: http.StatusOK,
			wantArn:  true,
		},
		{
			name:     "missing_name",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestBackupHandler(t)
			rec := doBackupRequest(t, h, "CreateBackupVault", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantArn {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp["BackupVaultArn"], "arn:aws:backup:")
			}
		})
	}
}

func TestBackupHandler_CreateBackupVaultAlreadyExists(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler(t)
	body := map[string]any{"BackupVaultName": "my-vault"}
	doBackupRequest(t, h, "CreateBackupVault", body)

	rec := doBackupRequest(t, h, "CreateBackupVault", body)
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestBackupHandler_DescribeBackupVault(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler(t)
	doBackupRequest(t, h, "CreateBackupVault", map[string]any{"BackupVaultName": "my-vault"})

	rec := doBackupRequest(t, h, "DescribeBackupVault", map[string]any{"BackupVaultName": "my-vault"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "my-vault", resp["BackupVaultName"])
	assert.Contains(t, resp["BackupVaultArn"], "arn:aws:backup:")
}

func TestBackupHandler_ListBackupVaults(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler(t)
	doBackupRequest(t, h, "CreateBackupVault", map[string]any{"BackupVaultName": "vault-1"})
	doBackupRequest(t, h, "CreateBackupVault", map[string]any{"BackupVaultName": "vault-2"})

	rec := doBackupRequest(t, h, "ListBackupVaults", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	list, ok := resp["BackupVaultList"].([]any)
	require.True(t, ok)
	assert.Len(t, list, 2)
}

func TestBackupHandler_DeleteBackupVault(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler(t)
	doBackupRequest(t, h, "CreateBackupVault", map[string]any{"BackupVaultName": "my-vault"})

	rec := doBackupRequest(t, h, "DeleteBackupVault", map[string]any{"BackupVaultName": "my-vault"})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify deleted
	rec2 := doBackupRequest(t, h, "DescribeBackupVault", map[string]any{"BackupVaultName": "my-vault"})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

func TestBackupHandler_CreateBackupPlan(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler(t)
	doBackupRequest(t, h, "CreateBackupVault", map[string]any{"BackupVaultName": "my-vault"})

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
		wantID   bool
	}{
		{
			name: "success",
			body: map[string]any{
				"BackupPlan": map[string]any{
					"BackupPlanName": "my-plan",
					"Rules": []map[string]any{
						{
							"RuleName":              "daily",
							"TargetBackupVaultName": "my-vault",
							"ScheduleExpression":    "cron(0 5 ? * * *)",
						},
					},
				},
			},
			wantCode: http.StatusOK,
			wantID:   true,
		},
		{
			name: "missing_name",
			body: map[string]any{
				"BackupPlan": map[string]any{},
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler := newTestBackupHandler(t)
			rec := doBackupRequest(t, handler, "CreateBackupPlan", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantID {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["BackupPlanId"])
				assert.Contains(t, resp["BackupPlanArn"], "arn:aws:backup:")
			}
		})
	}
}

func TestBackupHandler_GetBackupPlan(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler(t)
	createRec := doBackupRequest(t, h, "CreateBackupPlan", map[string]any{
		"BackupPlan": map[string]any{
			"BackupPlanName": "my-plan",
			"Rules": []map[string]any{
				{"RuleName": "daily", "TargetBackupVaultName": "my-vault"},
			},
		},
	})
	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	planID := createResp["BackupPlanId"]

	rec := doBackupRequest(t, h, "GetBackupPlan", map[string]any{"BackupPlanId": planID})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, planID, resp["BackupPlanId"])
	assert.Contains(t, resp, "BackupPlan")
}

func TestBackupHandler_ListBackupPlans(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler(t)
	doBackupRequest(t, h, "CreateBackupPlan", map[string]any{
		"BackupPlan": map[string]any{
			"BackupPlanName": "plan-1",
			"Rules":          []map[string]any{},
		},
	})
	doBackupRequest(t, h, "CreateBackupPlan", map[string]any{
		"BackupPlan": map[string]any{
			"BackupPlanName": "plan-2",
			"Rules":          []map[string]any{},
		},
	})

	rec := doBackupRequest(t, h, "ListBackupPlans", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	list, ok := resp["BackupPlansList"].([]any)
	require.True(t, ok)
	assert.Len(t, list, 2)
}

func TestBackupHandler_UpdateBackupPlan(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler(t)
	createRec := doBackupRequest(t, h, "CreateBackupPlan", map[string]any{
		"BackupPlan": map[string]any{
			"BackupPlanName": "my-plan",
			"Rules": []map[string]any{
				{"RuleName": "daily", "TargetBackupVaultName": "my-vault"},
			},
		},
	})
	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	planID := createResp["BackupPlanId"]

	rec := doBackupRequest(t, h, "UpdateBackupPlan", map[string]any{
		"BackupPlanId": planID,
		"BackupPlan": map[string]any{
			"BackupPlanName": "my-plan",
			"Rules": []map[string]any{
				{"RuleName": "weekly", "TargetBackupVaultName": "my-vault", "ScheduleExpression": "cron(0 5 ? * 1 *)"},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["VersionId"])
}

func TestBackupHandler_DeleteBackupPlan(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler(t)
	createRec := doBackupRequest(t, h, "CreateBackupPlan", map[string]any{
		"BackupPlan": map[string]any{
			"BackupPlanName": "my-plan",
			"Rules":          []map[string]any{},
		},
	})
	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	planID := createResp["BackupPlanId"]

	rec := doBackupRequest(t, h, "DeleteBackupPlan", map[string]any{"BackupPlanId": planID})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify deleted
	rec2 := doBackupRequest(t, h, "GetBackupPlan", map[string]any{"BackupPlanId": planID})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

func TestBackupHandler_StartBackupJob(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler(t)
	doBackupRequest(t, h, "CreateBackupVault", map[string]any{"BackupVaultName": "my-vault"})

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
		wantID   bool
	}{
		{
			name: "success",
			body: map[string]any{
				"BackupVaultName": "my-vault",
				"ResourceArn":     "arn:aws:ec2:us-east-1:000000000000:instance/i-12345",
				"IamRoleArn":      "arn:aws:iam::000000000000:role/backup-role",
			},
			wantCode: http.StatusOK,
			wantID:   true,
		},
		{
			name:     "missing_vault",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "vault_not_found",
			body:     map[string]any{"BackupVaultName": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler := newTestBackupHandler(t)
			doBackupRequest(t, handler, "CreateBackupVault", map[string]any{"BackupVaultName": "my-vault"})
			rec := doBackupRequest(t, handler, "StartBackupJob", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantID {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["BackupJobId"])
			}
		})
	}
}

func TestBackupHandler_DescribeBackupJob(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler(t)
	doBackupRequest(t, h, "CreateBackupVault", map[string]any{"BackupVaultName": "my-vault"})
	startRec := doBackupRequest(t, h, "StartBackupJob", map[string]any{
		"BackupVaultName": "my-vault",
		"ResourceArn":     "arn:aws:ec2:::instance/i-12345",
		"IamRoleArn":      "arn:aws:iam:::role/backup-role",
	})
	var startResp map[string]string
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))

	rec := doBackupRequest(t, h, "DescribeBackupJob", map[string]any{"BackupJobId": startResp["BackupJobId"]})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, startResp["BackupJobId"], resp["BackupJobId"])
	assert.Equal(t, "CREATED", resp["State"])
}

func TestBackupHandler_ListBackupJobs(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler(t)
	doBackupRequest(t, h, "CreateBackupVault", map[string]any{"BackupVaultName": "my-vault"})
	doBackupRequest(t, h, "StartBackupJob", map[string]any{
		"BackupVaultName": "my-vault",
		"ResourceArn":     "arn:1",
	})
	doBackupRequest(t, h, "StartBackupJob", map[string]any{
		"BackupVaultName": "my-vault",
		"ResourceArn":     "arn:2",
	})

	rec := doBackupRequest(t, h, "ListBackupJobs", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	jobs, ok := resp["BackupJobs"].([]any)
	require.True(t, ok)
	assert.Len(t, jobs, 2)
}

func TestBackupHandler_TagResource(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler(t)
	createRec := doBackupRequest(t, h, "CreateBackupVault", map[string]any{"BackupVaultName": "my-vault"})
	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	vaultARN := createResp["BackupVaultArn"]

	rec := doBackupRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": vaultARN,
		"Tags":        map[string]string{"env": "test", "team": "platform"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestBackupHandler_ListTags(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler(t)
	createRec := doBackupRequest(t, h, "CreateBackupVault", map[string]any{"BackupVaultName": "my-vault"})
	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	vaultARN := createResp["BackupVaultArn"]

	doBackupRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": vaultARN,
		"Tags":        map[string]string{"env": "prod"},
	})

	rec := doBackupRequest(t, h, "ListTags", map[string]any{"ResourceArn": vaultARN})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	tags, ok := resp["Tags"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "prod", tags["env"])
}

func TestBackupHandler_ErrorStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "DescribeBackupVault_NotFound",
			action:   "DescribeBackupVault",
			body:     map[string]any{"BackupVaultName": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "DeleteBackupVault_NotFound",
			action:   "DeleteBackupVault",
			body:     map[string]any{"BackupVaultName": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "GetBackupPlan_NotFound",
			action:   "GetBackupPlan",
			body:     map[string]any{"BackupPlanId": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "DeleteBackupPlan_NotFound",
			action:   "DeleteBackupPlan",
			body:     map[string]any{"BackupPlanId": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "DescribeBackupJob_NotFound",
			action:   "DescribeBackupJob",
			body:     map[string]any{"BackupJobId": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "UnknownAction",
			action:   "UnknownAction",
			body:     nil,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestBackupHandler(t)
			rec := doBackupRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestBackupHandler_InvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		action   string
		wantCode int
	}{
		{name: "CreateBackupVault", action: "CreateBackupVault", wantCode: http.StatusBadRequest},
		{name: "DescribeBackupVault", action: "DescribeBackupVault", wantCode: http.StatusBadRequest},
		{name: "DeleteBackupVault", action: "DeleteBackupVault", wantCode: http.StatusBadRequest},
		{name: "CreateBackupPlan", action: "CreateBackupPlan", wantCode: http.StatusBadRequest},
		{name: "GetBackupPlan", action: "GetBackupPlan", wantCode: http.StatusBadRequest},
		{name: "DeleteBackupPlan", action: "DeleteBackupPlan", wantCode: http.StatusBadRequest},
		{name: "StartBackupJob", action: "StartBackupJob", wantCode: http.StatusBadRequest},
		{name: "DescribeBackupJob", action: "DescribeBackupJob", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestBackupHandler(t)
			rec := doInvalidBackupRequest(t, h, tt.action)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestBackupProvider(t *testing.T) {
	t.Parallel()

	p := &backup.Provider{}
	assert.Equal(t, "Backup", p.Name())
}

func TestBackupProviderInit(t *testing.T) {
	t.Parallel()

	p := &backup.Provider{}
	ctx := &service.AppContext{Logger: slog.Default()}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	assert.NotNil(t, svc)
	assert.Equal(t, "Backup", svc.Name())
}

func TestBackupPersistence(t *testing.T) {
	t.Parallel()

	b := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(b)

	// Create some data.
	_, err := b.CreateBackupVault("my-vault", "", "", nil)
	require.NoError(t, err)
	_, err = b.CreateBackupPlan("my-plan", []backup.Rule{}, nil)
	require.NoError(t, err)
	job, err := b.StartBackupJob("my-vault", "arn:aws:ec2:::instance/i-1", "", "")
	require.NoError(t, err)
	require.NotNil(t, job)

	// Snapshot.
	snap := h.Snapshot()
	require.NotNil(t, snap)

	// Restore into fresh backend.
	b2 := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h2 := backup.NewHandler(b2)
	require.NoError(t, h2.Restore(snap))

	vaults := b2.ListBackupVaults()
	require.Len(t, vaults, 1)
	assert.Equal(t, "my-vault", vaults[0].BackupVaultName)

	plans := b2.ListBackupPlans()
	require.Len(t, plans, 1)
	assert.Equal(t, "my-plan", plans[0].BackupPlanName)

	jobs := b2.ListBackupJobs("")
	require.Len(t, jobs, 1)
}

func TestBackupBackend_Region(t *testing.T) {
	t.Parallel()

	b := backup.NewInMemoryBackend("000000000000", "eu-west-1")
	assert.Equal(t, "eu-west-1", b.Region())
}

func TestBackupHandler_ChaosInterface(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler(t)
	assert.Equal(t, "backup", h.ChaosServiceName())
	assert.Contains(t, h.ChaosOperations(), "CreateBackupVault")
	assert.Equal(t, []string{"us-east-1"}, h.ChaosRegions())
}

func TestBackupHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body    string
		name    string
		wantRes string
	}{
		{
			name:    "BackupVaultName",
			body:    `{"BackupVaultName":"my-vault"}`,
			wantRes: "my-vault",
		},
		{
			name:    "BackupPlanId",
			body:    `{"BackupPlanId":"my-plan-id"}`,
			wantRes: "my-plan-id",
		},
		{
			name:    "BackupJobId",
			body:    `{"BackupJobId":"my-job-id"}`,
			wantRes: "my-job-id",
		},
		{
			name:    "ResourceArn",
			body:    `{"ResourceArn":"arn:aws:ec2:::instance/i-12345"}`,
			wantRes: "arn:aws:ec2:::instance/i-12345",
		},
		{
			name:    "empty_body",
			body:    `{}`,
			wantRes: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestBackupHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantRes, h.ExtractResource(c))
		})
	}
}

func TestBackupHandler_TagPlan(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler(t)
	createRec := doBackupRequest(t, h, "CreateBackupPlan", map[string]any{
		"BackupPlan": map[string]any{
			"BackupPlanName": "my-plan",
			"Rules":          []map[string]any{},
		},
	})
	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	planARN := createResp["BackupPlanArn"]

	rec := doBackupRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": planARN,
		"Tags":        map[string]string{"env": "test"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	listRec := doBackupRequest(t, h, "ListTags", map[string]any{"ResourceArn": planARN})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	tags, ok := listResp["Tags"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "test", tags["env"])
}

func TestBackupHandler_TagResource_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler(t)
	rec := doBackupRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": "arn:aws:backup:us-east-1:000000000000:backup-vault:nonexistent",
		"Tags":        map[string]string{"env": "test"},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestBackupHandler_ListTags_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler(t)
	rec := doBackupRequest(t, h, "ListTags", map[string]any{
		"ResourceArn": "arn:aws:backup:us-east-1:000000000000:backup-vault:nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestBackupHandler_GetBackupPlan_ByName(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler(t)
	createRec := doBackupRequest(t, h, "CreateBackupPlan", map[string]any{
		"BackupPlan": map[string]any{
			"BackupPlanName": "my-plan",
			"Rules":          []map[string]any{},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	// GetBackupPlan by name (name = plan ID or plan name)
	rec := doBackupRequest(t, h, "GetBackupPlan", map[string]any{"BackupPlanId": "my-plan"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	plan, ok := resp["BackupPlan"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-plan", plan["BackupPlanName"])
}

func TestBackupHandler_DeleteBackupPlan_ByID(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler(t)
	createRec := doBackupRequest(t, h, "CreateBackupPlan", map[string]any{
		"BackupPlan": map[string]any{
			"BackupPlanName": "my-plan",
			"Rules":          []map[string]any{},
		},
	})
	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	planID := createResp["BackupPlanId"]

	rec := doBackupRequest(t, h, "DeleteBackupPlan", map[string]any{"BackupPlanId": planID})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, planID, resp["BackupPlanId"])
}

func TestBackupHandler_ListBackupJobs_FilterByVault(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler(t)
	doBackupRequest(t, h, "CreateBackupVault", map[string]any{"BackupVaultName": "vault-a"})
	doBackupRequest(t, h, "CreateBackupVault", map[string]any{"BackupVaultName": "vault-b"})
	doBackupRequest(t, h, "StartBackupJob", map[string]any{"BackupVaultName": "vault-a", "ResourceArn": "arn:1"})
	doBackupRequest(t, h, "StartBackupJob", map[string]any{"BackupVaultName": "vault-b", "ResourceArn": "arn:2"})

	rec := doBackupRequest(t, h, "ListBackupJobs", map[string]any{"ByBackupVaultName": "vault-a"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	jobs, ok := resp["BackupJobs"].([]any)
	require.True(t, ok)
	assert.Len(t, jobs, 1)
}
