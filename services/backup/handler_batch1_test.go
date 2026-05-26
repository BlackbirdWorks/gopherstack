package backup_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

func newBatch1Handler(
	t *testing.T,
) (*backup.Handler, *backup.InMemoryBackend) { //nolint:unparam // b used in some callers
	t.Helper()
	b := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(b)

	return h, b
}

func doBatch1Request(
	t *testing.T,
	h *backup.Handler,
	method, path, body string,
) *httptest.ResponseRecorder {
	t.Helper()
	e := echo.New()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// ---- Global/Region Settings ----

func TestBatch1_GlobalSettings(t *testing.T) {
	t.Parallel()
	h, _ := newBatch1Handler(t)

	t.Run("describe returns empty initially", func(t *testing.T) {
		t.Parallel()
		h2, _ := newBatch1Handler(t)
		resp := doBatch1Request(t, h2, http.MethodGet, "/global-settings", "")
		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Contains(t, resp.Body.String(), "GlobalSettings")
	})

	t.Run("update and describe", func(t *testing.T) {
		t.Parallel()
		h2, _ := newBatch1Handler(t)
		_ = doBatch1Request(t, h2, http.MethodPut, "/global-settings",
			`{"GlobalSettings":{"isCrossAccountBackupEnabled":"true"}}`)
		resp := doBatch1Request(t, h2, http.MethodGet, "/global-settings", "")
		assert.Contains(t, resp.Body.String(), "isCrossAccountBackupEnabled")
	})
	_ = h
}

func TestBatch1_RegionSettings(t *testing.T) {
	t.Parallel()
	h, _ := newBatch1Handler(t)

	t.Run("describe returns defaults", func(t *testing.T) {
		t.Parallel()
		h2, _ := newBatch1Handler(t)
		resp := doBatch1Request(t, h2, http.MethodGet, "/region-settings", "")
		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Contains(t, resp.Body.String(), "ResourceTypeOptInPreference")
	})
	_ = h
}

// ---- Backend unit tests ----

func TestBatch1_RestoreJob(t *testing.T) {
	t.Parallel()
	b := backup.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("start and describe", func(t *testing.T) {
		t.Parallel()
		b2 := backup.NewInMemoryBackend("000000000000", "us-east-1")
		job := b2.StartRestoreJob(
			"arn:aws:backup:us-east-1:000000000000:recovery-point:test",
			"arn:aws:iam::000000000000:role/backup",
			"EBS",
			nil,
		)
		assert.NotEmpty(t, job.RestoreJobID)
		assert.Equal(t, "COMPLETED", job.Status)

		found, err := b2.DescribeRestoreJob(job.RestoreJobID)
		require.NoError(t, err)
		assert.Equal(t, job.RestoreJobID, found.RestoreJobID)
	})

	t.Run("list jobs", func(t *testing.T) {
		t.Parallel()
		b2 := backup.NewInMemoryBackend("000000000000", "us-east-1")
		b2.StartRestoreJob("arn:rp-1", "arn:role", "EBS", nil)
		b2.StartRestoreJob("arn:rp-2", "arn:role", "RDS", nil)
		jobs := b2.ListRestoreJobs()
		require.Len(t, jobs, 2)
	})

	t.Run("not found error", func(t *testing.T) {
		t.Parallel()
		_, err := b.DescribeRestoreJob("missing")
		require.Error(t, err)
	})
}

func TestBatch1_ReportJob(t *testing.T) {
	t.Parallel()
	b := backup.NewInMemoryBackend("000000000000", "us-east-1")

	job := b.StartReportJob("my-report-plan")
	assert.NotEmpty(t, job.ReportJobID)

	found, err := b.DescribeReportJob(job.ReportJobID)
	require.NoError(t, err)
	assert.Equal(t, "COMPLETED", found.Status)

	jobs := b.ListReportJobs("")
	assert.NotEmpty(t, jobs)
}

func TestBatch1_ScanJob(t *testing.T) {
	t.Parallel()
	b := backup.NewInMemoryBackend("000000000000", "us-east-1")

	job := b.StartScanJob("arn:aws:backup:us-east-1:000000000000:backup-vault:test")
	assert.NotEmpty(t, job.ScanJobID)

	found, err := b.DescribeScanJob(job.ScanJobID)
	require.NoError(t, err)
	assert.Equal(t, "COMPLETED", found.Status)

	jobs := b.ListScanJobs()
	assert.NotEmpty(t, jobs)
}

func TestBatch1_LegalHold(t *testing.T) {
	t.Parallel()
	b := backup.NewInMemoryBackend("000000000000", "us-east-1")

	lh, _ := b.CreateLegalHold("test hold", "test description")
	assert.NotEmpty(t, lh.LegalHoldID)

	found, err := b.GetLegalHold(lh.LegalHoldID)
	require.NoError(t, err)
	assert.Equal(t, "test hold", found.Title)

	holds := b.ListLegalHolds()
	assert.NotEmpty(t, holds)

	rps := b.ListRecoveryPointsByLegalHold(lh.LegalHoldID)
	assert.Empty(t, rps)
}

func TestBatch1_ProtectedResources(t *testing.T) {
	t.Parallel()
	b := backup.NewInMemoryBackend("000000000000", "us-east-1")

	b.PutProtectedResource("arn:aws:ec2:us-east-1:000000000000:instance/i-test", "EC2", "my-vault")

	pr, err := b.DescribeProtectedResource("arn:aws:ec2:us-east-1:000000000000:instance/i-test")
	require.NoError(t, err)
	assert.Equal(t, "EC2", pr.ResourceType)

	all := b.ListProtectedResources()
	require.Len(t, all, 1)

	byVault := b.ListProtectedResourcesByBackupVault("my-vault")
	require.Len(t, byVault, 1)
}

func TestBatch1_RecoveryPointOps(t *testing.T) {
	t.Parallel()
	b := backup.NewInMemoryBackend("000000000000", "us-east-1")

	// Create vault and recovery point
	vault, _ := b.CreateBackupVault("test-vault", "", "", nil)
	b.AddRecoveryPoint(
		vault.BackupVaultName,
		&backup.RecoveryPoint{ //nolint:exhaustruct // test fixture only needs specific fields
			RecoveryPointArn: "arn:rp-test",
			BackupVaultName:  vault.BackupVaultName,
			ResourceArn:      "arn:aws:ec2:us-east-1:000000000000:volume/vol-test",
			Status:           "COMPLETED",
		},
	)

	t.Run("list by resource ARN", func(t *testing.T) {
		t.Parallel()
		rps := b.ListRecoveryPointsByResource("arn:aws:ec2:us-east-1:000000000000:volume/vol-test")
		assert.NotEmpty(t, rps)
	})

	t.Run("update lifecycle", func(t *testing.T) {
		t.Parallel()
		err := b.UpdateRecoveryPointLifecycle(vault.BackupVaultName, "arn:rp-test", 30, 365)
		require.NoError(t, err)
	})

	t.Run("index details", func(t *testing.T) {
		t.Parallel()
		status, err := b.GetRecoveryPointIndexDetails(vault.BackupVaultName, "arn:rp-test")
		require.NoError(t, err)
		assert.NotEmpty(t, status)
	})
}

func TestBatch1_TieringConfig(t *testing.T) {
	t.Parallel()
	b := backup.NewInMemoryBackend("000000000000", "us-east-1")
	b.CreateBackupVault("tier-vault", "", "", nil)

	t.Run("create and get", func(t *testing.T) {
		t.Parallel()
		b2 := backup.NewInMemoryBackend("000000000000", "us-east-1")
		b2.CreateBackupVault("tc-vault", "", "", nil)
		require.NoError(t, b2.CreateTieringConfiguration("tc-vault"))
		tc, err := b2.GetTieringConfiguration("tc-vault")
		require.NoError(t, err)
		assert.Equal(t, "tc-vault", tc.BackupVaultName)
	})

	t.Run("list configurations", func(t *testing.T) {
		t.Parallel()
		b2 := backup.NewInMemoryBackend("000000000000", "us-east-1")
		b2.CreateBackupVault("v1", "", "", nil)
		b2.CreateBackupVault("v2", "", "", nil)
		_ = b2.CreateTieringConfiguration("v1")
		_ = b2.CreateTieringConfiguration("v2")
		tcs := b2.ListTieringConfigurations()
		require.Len(t, tcs, 2)
	})

	t.Run("delete removes config", func(t *testing.T) {
		t.Parallel()
		b2 := backup.NewInMemoryBackend("000000000000", "us-east-1")
		b2.CreateBackupVault("del-vault", "", "", nil)
		_ = b2.CreateTieringConfiguration("del-vault")
		require.NoError(t, b2.DeleteTieringConfiguration("del-vault"))
		tcs := b2.ListTieringConfigurations()
		assert.Empty(t, tcs)
	})
	_ = b
}

func TestBatch1_StopBackupJob(t *testing.T) {
	t.Parallel()
	b := backup.NewInMemoryBackend("000000000000", "us-east-1")
	b.CreateBackupVault("vault-stop", "", "", nil)
	job, err := b.StartBackupJob(
		"vault-stop",
		"arn:aws:ec2:us-east-1:000000000000:volume/vol-1",
		"arn:aws:iam::000000000000:role/r",
		"EBS",
	)
	require.NoError(t, err)

	require.NoError(t, b.StopBackupJob(job.BackupJobID))
}

func TestBatch1_StartCopyJob(t *testing.T) {
	t.Parallel()
	b := backup.NewInMemoryBackend("000000000000", "us-east-1")

	job := b.StartCopyJob("arn:rp", "arn:src-vault", "arn:dst-vault", "arn:role")
	assert.NotEmpty(t, job.CopyJobID)
	assert.Equal(t, "COMPLETED", job.State)

	summaries := b.ListCopyJobSummaries()
	assert.NotEmpty(t, summaries)
}

func TestBatch1_ExportBackupPlanTemplate(t *testing.T) {
	t.Parallel()
	b := backup.NewInMemoryBackend("000000000000", "us-east-1")

	plan, err := b.CreateBackupPlan(
		"my-plan",
		[]backup.Rule{{RuleName: "r1", TargetVaultName: "v"}},
		nil,
		nil,
	)
	require.NoError(t, err)

	tmpl, err := b.ExportBackupPlanTemplate(plan.BackupPlanID)
	require.NoError(t, err)
	assert.Contains(t, tmpl, "my-plan")
}

func TestBatch1_GlobalSettingsBackend(t *testing.T) {
	t.Parallel()
	b := backup.NewInMemoryBackend("000000000000", "us-east-1")

	b.UpdateGlobalSettings(map[string]string{"isCrossAccountBackupEnabled": "true"})
	settings, _ := b.DescribeGlobalSettings()
	assert.Equal(t, "true", settings["isCrossAccountBackupEnabled"])
}
