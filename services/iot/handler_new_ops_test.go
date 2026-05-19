package iot_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

func newIoTHandlerBatch1(t *testing.T) *iot.Handler {
	t.Helper()

	return iot.NewHandler(iot.NewInMemoryBackend(), nil)
}

func iotRequest(
	t *testing.T,
	h *iot.Handler,
	method, path string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()
	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
	}
	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	if err := h.Handler()(c); err != nil {
		t.Fatalf("handler error for %s %s: %v", method, path, err)
	}

	return rec
}

func iotOK(t *testing.T, h *iot.Handler, method, path string, body any) map[string]any {
	t.Helper()
	rec := iotRequest(t, h, method, path, body)
	if rec.Code != http.StatusOK && rec.Code != http.StatusNoContent {
		t.Fatalf("%s %s: want 200/204, got %d body: %s", method, path, rec.Code, rec.Body.String())
	}
	if rec.Body.Len() == 0 {
		return map[string]any{}
	}
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	return out
}

func iotExpectError(t *testing.T, h *iot.Handler, path string) {
	t.Helper()
	rec := iotRequest(t, h, http.MethodGet, path, nil)
	if rec.Code == http.StatusOK {
		t.Fatalf("%s %s: expected error got 200, body: %s", http.MethodGet, path, rec.Body.String())
	}
}

// TestNewOps_Job tests Job lifecycle.
func TestNewOps_Job(t *testing.T) {
	t.Parallel()
	h := newIoTHandlerBatch1(t)

	// CreateJob
	out := iotOK(t, h, http.MethodPost, "/jobs/my-job", map[string]any{
		"targets":         []string{"arn:aws:iot:us-east-1:000000000000:thing/my-thing"},
		"document":        `{"version":"1.0"}`,
		"targetSelection": "SNAPSHOT",
	})
	if out["jobId"] != "my-job" {
		t.Errorf("jobId mismatch: %v", out)
	}

	// DescribeJob
	out2 := iotOK(t, h, http.MethodGet, "/jobs/my-job", nil)
	job := out2["job"].(map[string]any)
	if job["jobId"] != "my-job" {
		t.Errorf("describe job mismatch: %v", job)
	}

	// ListJobs
	out3 := iotOK(t, h, http.MethodGet, "/jobs", nil)
	jobs, _ := out3["jobs"].([]any)
	if len(jobs) != 1 {
		t.Errorf("expected 1 job, got %d", len(jobs))
	}

	// UpdateJob
	iotOK(t, h, http.MethodPatch, "/jobs/my-job", map[string]any{
		"description": "updated",
	})

	// GetJobDocument
	out4 := iotOK(t, h, http.MethodGet, "/jobs/my-job/document", nil)
	if out4["document"] != `{"version":"1.0"}` {
		t.Errorf("document mismatch: %v", out4["document"])
	}

	// CancelJob
	iotOK(t, h, http.MethodPut, "/jobs/my-job/cancel", map[string]any{
		"comment": "test cancel",
	})

	// DeleteJob
	iotOK(t, h, http.MethodDelete, "/jobs/my-job", nil)

	// Verify deletion
	iotExpectError(t, h, "/jobs/my-job")
}

// TestNewOps_JobExecution tests job execution lifecycle.
func TestNewOps_JobExecution(t *testing.T) {
	t.Parallel()
	h := newIoTHandlerBatch1(t)

	// Create job first.
	iotOK(t, h, http.MethodPost, "/jobs/exec-job", map[string]any{
		"targets":  []string{"arn:aws:iot:us-east-1:000000000000:thing/my-thing"},
		"document": `{}`,
	})

	// CancelJobExecution (creates execution if not exists).
	iotOK(t, h, http.MethodPut, "/jobs/exec-job/things/my-thing/cancel", nil)

	// DescribeJobExecution
	out := iotOK(t, h, http.MethodGet, "/jobs/exec-job/things/my-thing", nil)
	exec, ok := out["execution"].(map[string]any)
	if !ok {
		t.Fatalf("expected execution in response: %v", out)
	}
	if exec["jobId"] != "exec-job" {
		t.Errorf("exec jobId mismatch: %v", exec)
	}

	// DeleteJobExecution
	iotOK(t, h, http.MethodDelete, "/jobs/exec-job/things/my-thing", nil)
}

// TestNewOps_JobTemplate tests JobTemplate CRUD.
func TestNewOps_JobTemplate(t *testing.T) {
	t.Parallel()
	h := newIoTHandlerBatch1(t)

	// CreateJobTemplate
	out := iotOK(t, h, http.MethodPost, "/job-templates/my-template", map[string]any{
		"description": "test template",
		"document":    `{"version":"1.0"}`,
	})
	if out["jobTemplateId"] != "my-template" {
		t.Errorf("jobTemplateId mismatch: %v", out)
	}

	// DescribeJobTemplate
	out2 := iotOK(t, h, http.MethodGet, "/job-templates/my-template", nil)
	if out2["jobTemplateId"] != "my-template" {
		t.Errorf("describe template mismatch: %v", out2)
	}

	// ListJobTemplates
	out3 := iotOK(t, h, http.MethodGet, "/job-templates", nil)
	templates, _ := out3["jobTemplates"].([]any)
	if len(templates) != 1 {
		t.Errorf("expected 1 template, got %d", len(templates))
	}

	// DeleteJobTemplate
	iotOK(t, h, http.MethodDelete, "/job-templates/my-template", nil)

	// Verify deletion
	iotExpectError(t, h, "/job-templates/my-template")
}

// TestNewOps_RoleAlias tests RoleAlias CRUD.
func TestNewOps_RoleAlias(t *testing.T) {
	t.Parallel()
	h := newIoTHandlerBatch1(t)

	// CreateRoleAlias
	out := iotOK(t, h, http.MethodPost, "/role-aliases/my-alias", map[string]any{
		"roleArn":                   "arn:aws:iam::000000000000:role/MyRole",
		"credentialDurationSeconds": 3600,
	})
	if out["roleAlias"] != "my-alias" {
		t.Errorf("roleAlias mismatch: %v", out)
	}

	// DescribeRoleAlias
	out2 := iotOK(t, h, http.MethodGet, "/role-aliases/my-alias", nil)
	desc, _ := out2["roleAliasDescription"].(map[string]any)
	if desc["roleAlias"] != "my-alias" {
		t.Errorf("describe role alias mismatch: %v", out2)
	}

	// ListRoleAliases
	out3 := iotOK(t, h, http.MethodGet, "/role-aliases", nil)
	aliases, _ := out3["roleAliases"].([]any)
	if len(aliases) != 1 {
		t.Errorf("expected 1 alias, got %d", len(aliases))
	}

	// UpdateRoleAlias
	iotOK(t, h, http.MethodPut, "/role-aliases/my-alias", map[string]any{
		"roleArn": "arn:aws:iam::000000000000:role/UpdatedRole",
	})

	// DeleteRoleAlias
	iotOK(t, h, http.MethodDelete, "/role-aliases/my-alias", nil)

	iotExpectError(t, h, "/role-aliases/my-alias")
}

// TestNewOps_DomainConfiguration tests DomainConfiguration CRUD.
func TestNewOps_DomainConfiguration(t *testing.T) {
	t.Parallel()
	h := newIoTHandlerBatch1(t)

	// CreateDomainConfiguration
	out := iotOK(t, h, http.MethodPost, "/domainConfigurations/my-config", map[string]any{
		"serviceType": "DATA",
	})
	if out["domainConfigurationName"] != "my-config" {
		t.Errorf("name mismatch: %v", out)
	}

	// DescribeDomainConfiguration
	out2 := iotOK(t, h, http.MethodGet, "/domainConfigurations/my-config", nil)
	if out2["domainConfigurationName"] != "my-config" {
		t.Errorf("describe mismatch: %v", out2)
	}

	// ListDomainConfigurations
	out3 := iotOK(t, h, http.MethodGet, "/domainConfigurations", nil)
	configs, _ := out3["domainConfigurations"].([]any)
	if len(configs) != 1 {
		t.Errorf("expected 1 config, got %d", len(configs))
	}

	// UpdateDomainConfiguration
	iotOK(t, h, http.MethodPut, "/domainConfigurations/my-config", map[string]any{
		"domainConfigurationStatus": "DISABLED",
	})

	// DeleteDomainConfiguration
	iotOK(t, h, http.MethodDelete, "/domainConfigurations/my-config", nil)

	iotExpectError(t, h, "/domainConfigurations/my-config")
}

// TestNewOps_ProvisioningTemplate tests ProvisioningTemplate CRUD.
func TestNewOps_ProvisioningTemplate(t *testing.T) {
	t.Parallel()
	h := newIoTHandlerBatch1(t)

	// CreateProvisioningTemplate
	out := iotOK(t, h, http.MethodPost, "/provisioning-templates", map[string]any{
		"templateName": "my-template",
		"templateBody": `{"Parameters":{}}`,
		"enabled":      true,
	})
	if out["templateName"] != "my-template" {
		t.Errorf("templateName mismatch: %v", out)
	}

	// DescribeProvisioningTemplate
	out2 := iotOK(t, h, http.MethodGet, "/provisioning-templates/my-template", nil)
	if out2["templateName"] != "my-template" {
		t.Errorf("describe mismatch: %v", out2)
	}

	// ListProvisioningTemplates
	out3 := iotOK(t, h, http.MethodGet, "/provisioning-templates", nil)
	templates, _ := out3["templates"].([]any)
	if len(templates) != 1 {
		t.Errorf("expected 1 template, got %d", len(templates))
	}

	// CreateProvisioningTemplateVersion
	out4 := iotOK(
		t,
		h,
		http.MethodPost,
		"/provisioning-templates/my-template/versions",
		map[string]any{
			"templateBody": `{"Parameters":{"v2":{}}}`,
		},
	)
	if out4["templateName"] != "my-template" {
		t.Errorf("version templateName mismatch: %v", out4)
	}

	// ListProvisioningTemplateVersions
	out5 := iotOK(t, h, http.MethodGet, "/provisioning-templates/my-template/versions", nil)
	versions, _ := out5["versions"].([]any)
	if len(versions) != 2 {
		t.Errorf("expected 2 versions, got %d", len(versions))
	}

	// UpdateProvisioningTemplate
	iotOK(t, h, http.MethodPatch, "/provisioning-templates/my-template", map[string]any{
		"description": "updated",
	})

	// DeleteProvisioningTemplateVersion
	iotOK(t, h, http.MethodDelete, "/provisioning-templates/my-template/versions/2", nil)

	// DeleteProvisioningTemplate
	iotOK(t, h, http.MethodDelete, "/provisioning-templates/my-template", nil)

	iotExpectError(t, h, "/provisioning-templates/my-template")
}

// TestNewOps_Authorizer tests Authorizer CRUD.
func TestNewOps_Authorizer(t *testing.T) {
	t.Parallel()
	h := newIoTHandlerBatch1(t)

	// CreateAuthorizer
	out := iotOK(t, h, http.MethodPost, "/authorizer/my-auth", map[string]any{
		"authorizerFunctionArn": "arn:aws:lambda:us-east-1:000000000000:function:MyAuthFn",
		"status":                "ACTIVE",
	})
	if out["authorizerName"] != "my-auth" {
		t.Errorf("authorizerName mismatch: %v", out)
	}

	// DescribeAuthorizer
	out2 := iotOK(t, h, http.MethodGet, "/authorizer/my-auth", nil)
	desc, _ := out2["authorizerDescription"].(map[string]any)
	if desc["authorizerName"] != "my-auth" {
		t.Errorf("describe mismatch: %v", out2)
	}

	// ListAuthorizers
	out3 := iotOK(t, h, http.MethodGet, "/authorizers", nil)
	authorizers, _ := out3["authorizers"].([]any)
	if len(authorizers) != 1 {
		t.Errorf("expected 1 authorizer, got %d", len(authorizers))
	}

	// UpdateAuthorizer
	iotOK(t, h, http.MethodPut, "/authorizer/my-auth", map[string]any{
		"status": "INACTIVE",
	})

	// DeleteAuthorizer
	iotOK(t, h, http.MethodDelete, "/authorizer/my-auth", nil)

	iotExpectError(t, h, "/authorizer/my-auth")
}

// TestNewOps_BillingGroup tests BillingGroup CRUD.
func TestNewOps_BillingGroup(t *testing.T) {
	t.Parallel()
	h := newIoTHandlerBatch1(t)

	// CreateBillingGroup
	out := iotOK(t, h, http.MethodPost, "/billing-groups/my-group", map[string]any{
		"billingGroupProperties": map[string]any{
			"billingGroupDescription": "test group",
		},
	})
	if out["billingGroupName"] != "my-group" {
		t.Errorf("billingGroupName mismatch: %v", out)
	}

	// DescribeBillingGroup
	out2 := iotOK(t, h, http.MethodGet, "/billing-groups/my-group", nil)
	if out2["billingGroupName"] != "my-group" {
		t.Errorf("describe mismatch: %v", out2)
	}

	// ListBillingGroups
	out3 := iotOK(t, h, http.MethodGet, "/billing-groups", nil)
	groups, _ := out3["billingGroups"].([]any)
	if len(groups) != 1 {
		t.Errorf("expected 1 group, got %d", len(groups))
	}

	// UpdateBillingGroup
	out4 := iotOK(t, h, http.MethodPatch, "/billing-groups/my-group", map[string]any{
		"billingGroupProperties": map[string]any{
			"billingGroupDescription": "updated",
		},
	})
	if out4["version"] == nil {
		t.Error("expected version in update response")
	}

	// DeleteBillingGroup
	iotOK(t, h, http.MethodDelete, "/billing-groups/my-group", nil)

	iotExpectError(t, h, "/billing-groups/my-group")
}

// TestNewOps_ScheduledAudit tests ScheduledAudit CRUD.
func TestNewOps_ScheduledAudit(t *testing.T) {
	t.Parallel()
	h := newIoTHandlerBatch1(t)

	// CreateScheduledAudit
	out := iotOK(t, h, http.MethodPost, "/audit/scheduledaudits/my-audit", map[string]any{
		"frequency":        "WEEKLY",
		"dayOfWeek":        "MON",
		"targetCheckNames": []string{"DEVICE_CERTIFICATE_EXPIRING_CHECK"},
	})
	if out["scheduledAuditArn"] == "" {
		t.Errorf("expected scheduledAuditArn: %v", out)
	}

	// DescribeScheduledAudit
	out2 := iotOK(t, h, http.MethodGet, "/audit/scheduledaudits/my-audit", nil)
	if out2["scheduledAuditName"] != "my-audit" {
		t.Errorf("describe mismatch: %v", out2)
	}

	// ListScheduledAudits
	out3 := iotOK(t, h, http.MethodGet, "/audit/scheduledaudits", nil)
	audits, _ := out3["scheduledAudits"].([]any)
	if len(audits) != 1 {
		t.Errorf("expected 1 audit, got %d", len(audits))
	}

	// UpdateScheduledAudit
	iotOK(t, h, http.MethodPatch, "/audit/scheduledaudits/my-audit", map[string]any{
		"frequency": "DAILY",
	})

	// DeleteScheduledAudit
	iotOK(t, h, http.MethodDelete, "/audit/scheduledaudits/my-audit", nil)

	iotExpectError(t, h, "/audit/scheduledaudits/my-audit")
}

// TestNewOps_MitigationAction tests MitigationAction CRUD.
func TestNewOps_MitigationAction(t *testing.T) {
	t.Parallel()
	h := newIoTHandlerBatch1(t)

	// CreateMitigationAction
	out := iotOK(t, h, http.MethodPost, "/mitigationactions/actions/my-action", map[string]any{
		"roleArn": "arn:aws:iam::000000000000:role/MitigationRole",
		"actionParams": map[string]any{
			"updateDeviceCertificateParams": map[string]any{"action": "DEACTIVATE"},
		},
	})
	if out["actionArn"] == "" {
		t.Errorf("expected actionArn: %v", out)
	}

	// DescribeMitigationAction
	out2 := iotOK(t, h, http.MethodGet, "/mitigationactions/actions/my-action", nil)
	if out2["actionName"] != "my-action" {
		t.Errorf("describe mismatch: %v", out2)
	}

	// ListMitigationActions
	out3 := iotOK(t, h, http.MethodGet, "/mitigationactions/actions", nil)
	actions, _ := out3["actionIdentifiers"].([]any)
	if len(actions) != 1 {
		t.Errorf("expected 1 action, got %d", len(actions))
	}

	// UpdateMitigationAction
	iotOK(t, h, http.MethodPatch, "/mitigationactions/actions/my-action", map[string]any{
		"roleArn": "arn:aws:iam::000000000000:role/UpdatedRole",
	})

	// DeleteMitigationAction
	iotOK(t, h, http.MethodDelete, "/mitigationactions/actions/my-action", nil)

	iotExpectError(t, h, "/mitigationactions/actions/my-action")
}

// TestNewOps_SecurityProfile tests SecurityProfile CRUD.
func TestNewOps_SecurityProfile(t *testing.T) {
	t.Parallel()
	h := newIoTHandlerBatch1(t)

	// CreateSecurityProfile
	out := iotOK(t, h, http.MethodPost, "/security-profiles/my-profile", map[string]any{
		"securityProfileDescription": "test profile",
	})
	if out["securityProfileName"] != "my-profile" {
		t.Errorf("name mismatch: %v", out)
	}

	// DescribeSecurityProfile
	out2 := iotOK(t, h, http.MethodGet, "/security-profiles/my-profile", nil)
	if out2["securityProfileName"] != "my-profile" {
		t.Errorf("describe mismatch: %v", out2)
	}

	// ListSecurityProfiles
	out3 := iotOK(t, h, http.MethodGet, "/security-profiles", nil)
	profiles, _ := out3["securityProfileIdentifiers"].([]any)
	if len(profiles) != 1 {
		t.Errorf("expected 1 profile, got %d", len(profiles))
	}

	// UpdateSecurityProfile
	out4 := iotOK(t, h, http.MethodPatch, "/security-profiles/my-profile", map[string]any{
		"securityProfileDescription": "updated",
	})
	if out4["version"] == nil {
		t.Error("expected version in update response")
	}

	// DeleteSecurityProfile
	iotOK(t, h, http.MethodDelete, "/security-profiles/my-profile", nil)

	iotExpectError(t, h, "/security-profiles/my-profile")
}
