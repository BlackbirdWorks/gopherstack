package backup_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	backupsdk "github.com/aws/aws-sdk-go-v2/service/backup"
	"github.com/aws/aws-sdk-go-v2/service/backup/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/backup"
)

// newTestBackupClient stands up the real aws-sdk-go-v2 Backup client against
// an httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production.
func newTestBackupClient(t *testing.T, h *backup.Handler) *backupsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return backupsdk.NewFromConfig(cfg, func(o *backupsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// Test_SDKRoundTrip_StartScanJob proves that all six required members of the
// real StartScanJob request (api_op_StartScanJob.go:29-75, backup@v1.59.4:
// BackupVaultName, IamRoleArn, MalwareScanner, RecoveryPointArn, ScanMode,
// ScannerRoleArn) are actually read, not just BackupVaultName. Before the
// fix, a real client's request 200'd (masking the drop) with the other five
// fields silently discarded -- this test asserts on backend state, not just
// a 2xx, so it fails against the unfixed handler even though the HTTP call
// itself would have succeeded.
func Test_SDKRoundTrip_StartScanJob(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(backend)
	client := newTestBackupClient(t, h)

	_, err := client.CreateBackupVault(t.Context(), &backupsdk.CreateBackupVaultInput{
		BackupVaultName: aws.String("scan-vault"),
	})
	require.NoError(t, err)

	continuousScanEndTime := time.Now().Add(time.Hour).UTC().Truncate(time.Second)

	out, err := client.StartScanJob(t.Context(), &backupsdk.StartScanJobInput{
		BackupVaultName:          aws.String("scan-vault"),
		IamRoleArn:               aws.String("arn:aws:iam::000000000000:role/ScanRole"),
		MalwareScanner:           types.MalwareScannerGuardduty,
		RecoveryPointArn:         aws.String("arn:aws:backup:us-east-1:000000000000:recovery-point:rp-1"),
		ScanMode:                 types.ScanModeIncrementalScan,
		ScannerRoleArn:           aws.String("arn:aws:iam::000000000000:role/ScannerRole"),
		ScanBaseRecoveryPointArn: aws.String("arn:aws:backup:us-east-1:000000000000:recovery-point:rp-0"),
		ContinuousScanEndTime:    &continuousScanEndTime,
		IdempotencyToken:         aws.String("token-1"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.ScanJobId)
	require.NotNil(t, out.CreationDate)

	job, err := backend.DescribeScanJob(*out.ScanJobId)
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::000000000000:role/ScanRole", job.IamRoleArn)
	assert.Equal(t, "GUARDDUTY", job.MalwareScanner)
	assert.Equal(t, "arn:aws:backup:us-east-1:000000000000:recovery-point:rp-1", job.RecoveryPointArn)
	assert.Equal(t, "INCREMENTAL_SCAN", job.ScanMode)
	assert.Equal(t, "arn:aws:iam::000000000000:role/ScannerRole", job.ScannerRoleArn)
	assert.Equal(t, "arn:aws:backup:us-east-1:000000000000:recovery-point:rp-0", job.ScanBaseRecoveryPointArn)
	assert.Equal(t, "token-1", job.IdempotencyToken)
	require.NotNil(t, job.ContinuousScanEndTime)
	assert.True(t, continuousScanEndTime.Equal(*job.ContinuousScanEndTime))
}

// TestStartScanJob_MissingRequiredFields verifies each of the five newly
// wired required fields is actually enforced, not merely accepted.
func TestStartScanJob_MissingRequiredFields(t *testing.T) {
	t.Parallel()

	valid := `"BackupVaultName":"scan-vault","IamRoleArn":"arn:aws:iam::000000000000:role/r",` +
		`"MalwareScanner":"GUARDDUTY","RecoveryPointArn":"arn:rp","ScanMode":"FULL_SCAN",` +
		`"ScannerRoleArn":"arn:aws:iam::000000000000:role/scanner"`

	tests := []struct {
		name string
		body string
	}{
		{name: "missing_backup_vault_name", body: `{"IamRoleArn":"r","MalwareScanner":"GUARDDUTY",` +
			`"RecoveryPointArn":"arn:rp","ScanMode":"FULL_SCAN","ScannerRoleArn":"r"}`},
		{name: "missing_iam_role_arn", body: `{"BackupVaultName":"scan-vault","MalwareScanner":"GUARDDUTY",` +
			`"RecoveryPointArn":"arn:rp","ScanMode":"FULL_SCAN","ScannerRoleArn":"r"}`},
		{name: "missing_malware_scanner", body: `{"BackupVaultName":"scan-vault","IamRoleArn":"r",` +
			`"RecoveryPointArn":"arn:rp","ScanMode":"FULL_SCAN","ScannerRoleArn":"r"}`},
		{name: "missing_recovery_point_arn", body: `{"BackupVaultName":"scan-vault","IamRoleArn":"r",` +
			`"MalwareScanner":"GUARDDUTY","ScanMode":"FULL_SCAN","ScannerRoleArn":"r"}`},
		{name: "missing_scan_mode", body: `{"BackupVaultName":"scan-vault","IamRoleArn":"r",` +
			`"MalwareScanner":"GUARDDUTY","RecoveryPointArn":"arn:rp","ScannerRoleArn":"r"}`},
		{name: "missing_scanner_role_arn", body: `{"BackupVaultName":"scan-vault","IamRoleArn":"r",` +
			`"MalwareScanner":"GUARDDUTY","RecoveryPointArn":"arn:rp","ScanMode":"FULL_SCAN"}`},
		{name: "all_present", body: `{` + valid + `}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
			h := backup.NewHandler(backend)

			e := echo.New()
			req := httptest.NewRequest(http.MethodPut, "/scan/job", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			require.NoError(t, h.Handler()(c))

			if tt.name == "all_present" {
				assert.Equal(t, 201, rec.Code)
			} else {
				assert.Equal(t, 400, rec.Code)
				assert.Contains(t, rec.Body.String(), "MissingParameterValueException")
			}
		})
	}
}
