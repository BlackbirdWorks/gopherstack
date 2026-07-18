package backup_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

func TestExportBackupPlanTemplate(t *testing.T) {
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

func TestExportBackupPlanTemplate_RealRules(t *testing.T) {
	t.Parallel()
	h, _ := newHandler(t)

	doRequest(t, h, http.MethodPut, "/backup-vaults/my-vault", "{}")

	createBody := `{"BackupPlan":{"BackupPlanName":"export-plan","Rules":[
		{"RuleName":"r1","TargetBackupVaultName":"my-vault","ScheduleExpression":"cron(0 5 * * ? *)"}
	]}}`
	createResp := doRequest(t, h, http.MethodPut, "/backup/plans", createBody)
	require.Equal(t, http.StatusOK, createResp.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createResp.Body.Bytes(), &created))
	planID := created["BackupPlanId"].(string)

	resp := doRequest(t, h, http.MethodGet, "/backup/plans/"+planID+"/toTemplate", "")
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	tmplJSON, ok := out["BackupPlanTemplateJson"].(string)
	require.True(t, ok)

	var tmpl map[string]any
	require.NoError(t, json.Unmarshal([]byte(tmplJSON), &tmpl))
	assert.Equal(t, "export-plan", tmpl["BackupPlanName"])
	rules, ok := tmpl["Rules"].([]any)
	require.True(t, ok)
	require.Len(t, rules, 1)
	rule := rules[0].(map[string]any)
	assert.Equal(t, "r1", rule["RuleName"])
	assert.Equal(t, "cron(0 5 * * ? *)", rule["ScheduleExpression"])
}

// ---- GetBackupPlanFromJSON: build the plan from the parsed JSON, not a hardcoded stub ----

func TestGetBackupPlanFromJSON_BuildsFromParsedBody(t *testing.T) {
	t.Parallel()
	h, _ := newHandler(t)

	body := `{"BackupPlanTemplateJson":"{\"BackupPlanName\":\"json-plan\",` +
		`\"Rules\":[{\"RuleName\":\"jr1\",\"TargetBackupVaultName\":\"vault-x\"}]}"}`
	resp := doRequest(t, h, http.MethodPost, "/backup/template/json/toPlan", body)
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	plan, ok := out["BackupPlan"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "json-plan", plan["BackupPlanName"])
	rules, ok := plan["Rules"].([]any)
	require.True(t, ok)
	require.Len(t, rules, 1)
	rule := rules[0].(map[string]any)
	assert.Equal(t, "jr1", rule["RuleName"])
	assert.Equal(t, "vault-x", rule["TargetBackupVaultName"])
}

func TestGetBackupPlanFromJSON_InvalidJSON(t *testing.T) {
	t.Parallel()
	h, _ := newHandler(t)

	body := `{"BackupPlanTemplateJson":"not-json"}`
	resp := doRequest(t, h, http.MethodPost, "/backup/template/json/toPlan", body)
	assert.Equal(t, http.StatusBadRequest, resp.Code)
	assert.Contains(t, resp.Body.String(), "ValidationException")
}

// ---- GetBackupPlanFromTemplate: resolve the real builtin template by ID ----

func TestGetBackupPlanFromTemplate_ResolvesBuiltinTemplate(t *testing.T) {
	t.Parallel()
	h, _ := newHandler(t)

	listResp := doRequest(t, h, http.MethodGet, "/backup/template/plans", "")
	require.Equal(t, http.StatusOK, listResp.Code)

	var list map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &list))
	tmpls, ok := list["BackupPlanTemplatesList"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, tmpls)

	first := tmpls[0].(map[string]any)
	tmplID := first["BackupPlanTemplateId"].(string)
	tmplName := first["BackupPlanTemplateName"].(string)

	getResp := doRequest(t, h, http.MethodGet, "/backup/template/plans/"+tmplID+"/toPlan", "")
	require.Equal(t, http.StatusOK, getResp.Code)

	var doc map[string]any
	require.NoError(t, json.Unmarshal(getResp.Body.Bytes(), &doc))
	planDoc, ok := doc["BackupPlanDocument"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, tmplName, planDoc["BackupPlanName"])
	rules, ok := planDoc["Rules"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, rules)
}

func TestGetBackupPlanFromTemplate_UnknownIDNotFound(t *testing.T) {
	t.Parallel()
	h, _ := newHandler(t)

	resp := doRequest(t, h, http.MethodGet, "/backup/template/plans/does-not-exist/toPlan", "")
	assert.Equal(t, http.StatusNotFound, resp.Code)
	assert.Contains(t, resp.Body.String(), "ResourceNotFoundException")
}

// ---- ListBackupPlanTemplates: real AWS built-in template catalog, not empty ----

func TestListBackupPlanTemplates_ReturnsBuiltinCatalog(t *testing.T) {
	t.Parallel()
	h, _ := newHandler(t)

	resp := doRequest(t, h, http.MethodGet, "/backup/template/plans", "")
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	items, ok := out["BackupPlanTemplatesList"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, items)

	for _, it := range items {
		m := it.(map[string]any)
		assert.NotEmpty(t, m["BackupPlanTemplateId"])
		assert.NotEmpty(t, m["BackupPlanTemplateName"])
	}
}

// ---- GetTieringConfiguration: real TieringConfiguration, and 404 when absent ----

func TestGetTieringConfiguration_ReturnsRealConfig(t *testing.T) {
	t.Parallel()
	h, _ := newHandler(t)

	doRequest(t, h, http.MethodPut, "/backup-vaults/tier-vault", "{}")
	createResp := doRequest(t, h, http.MethodPost, "/backup-vault-tiering/tier-vault", "")
	require.Equal(t, http.StatusOK, createResp.Code)

	resp := doRequest(t, h, http.MethodGet, "/backup-vault-tiering/tier-vault", "")
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	tc, ok := out["TieringConfiguration"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "tier-vault", tc["BackupVaultName"])
	assert.NotEmpty(t, tc["BackupVaultArn"])
}

func TestGetTieringConfiguration_NotFound(t *testing.T) {
	t.Parallel()
	h, _ := newHandler(t)

	resp := doRequest(t, h, http.MethodGet, "/backup-vault-tiering/no-such-vault", "")
	assert.Equal(t, http.StatusNotFound, resp.Code)
	assert.Contains(t, resp.Body.String(), "ResourceNotFoundException")
}
