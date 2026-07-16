package backup_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/backup"
)

func newTestBackend(t *testing.T) *backup.InMemoryBackend {
	t.Helper()

	return backup.NewInMemoryBackend("123456789012", "us-east-1")
}

// mustVault creates a vault or fatals.
func mustVault(t *testing.T, b *backup.InMemoryBackend, name string) *backup.Vault {
	t.Helper()
	v, err := b.CreateBackupVault(name, "", "", nil)
	if err != nil {
		t.Fatalf("CreateBackupVault(%q): %v", name, err)
	}

	return v
}

// mustPlan creates a plan with one valid rule or fatals.
func mustPlan(t *testing.T, b *backup.InMemoryBackend, name, vaultName string) *backup.Plan {
	t.Helper()
	rules := []backup.Rule{{RuleName: "daily", TargetVaultName: vaultName}}
	p, err := b.CreateBackupPlanValidated(name, rules, nil, nil)
	if err != nil {
		t.Fatalf("CreateBackupPlanValidated(%q): %v", name, err)
	}

	return p
}

// mustJob creates a backup job or fatals.
func mustJob(
	t *testing.T,
	b *backup.InMemoryBackend,
	vaultName, resourceArn, resourceType string,
) *backup.Job {
	t.Helper()
	j, err := b.StartBackupJob(vaultName, resourceArn, "arn:aws:iam::123:role/r", resourceType)
	if err != nil {
		t.Fatalf("StartBackupJob: %v", err)
	}

	return j
}

// mustRP adds a recovery point to a vault or fatals.
func mustRP(
	t *testing.T,
	b *backup.InMemoryBackend,
	vaultName, rpArn, resourceArn, resourceType string,
) {
	t.Helper()
	now := time.Now().UTC()
	rp := &backup.RecoveryPoint{
		RecoveryPointArn: rpArn,
		BackupVaultName:  vaultName,
		ResourceArn:      resourceArn,
		ResourceType:     resourceType,
		Status:           "COMPLETED",
		CreationDate:     now,
	}
	if err := b.AddRecoveryPoint(vaultName, rp); err != nil {
		t.Fatalf("AddRecoveryPoint: %v", err)
	}
}

// ---- Rule validation ----

func newBatch1Handler(
	t *testing.T,
) (*backup.Handler, *backup.InMemoryBackend) {
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

// Helper: create a backup plan and return its ID.
func createBackupPlan(t *testing.T, h *backup.Handler, name string) string {
	t.Helper()

	rec := doREST(t, h, http.MethodPut, "/backup/plans", map[string]any{
		"BackupPlan": map[string]any{
			"BackupPlanName": name,
			"Rules": []map[string]any{{
				"RuleName":              "daily",
				"TargetBackupVaultName": name + "-vault",
				"ScheduleExpression":    "cron(0 12 * * ? *)",
			}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	id, ok := resp["BackupPlanId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, id)

	return id
}

func newHandlerAndBackend() (*backup.Handler, *backup.InMemoryBackend) {
	be := backup.NewInMemoryBackend("123456789012", config.DefaultRegion)

	return backup.NewHandler(be), be
}

func seedVaultAndRP(t *testing.T, h *backup.Handler, be *backup.InMemoryBackend, vaultName string) string {
	t.Helper()

	// Create vault via HTTP
	rec := doREST(t, h, http.MethodPut, "/backup-vaults/"+vaultName, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// Add recovery point directly via backend
	rpArn := fmt.Sprintf("arn:aws:backup:us-east-1:123456789012:recovery-point:rp-%s-001", vaultName)
	now := time.Now().UTC()
	rp := &backup.RecoveryPoint{
		RecoveryPointArn:  rpArn,
		BackupVaultName:   vaultName,
		BackupVaultArn:    fmt.Sprintf("arn:aws:backup:us-east-1:123456789012:backup-vault:%s", vaultName),
		ResourceArn:       "arn:aws:ec2:us-east-1:123456789012:instance/i-test",
		ResourceType:      "EC2",
		IAMRoleArn:        "arn:aws:iam::123456789012:role/AWSBackupDefaultServiceRole",
		Status:            "COMPLETED",
		CreationDate:      now,
		BackupSizeInBytes: 1024,
	}
	err := be.AddRecoveryPoint(vaultName, rp)
	require.NoError(t, err)

	return rpArn
}
