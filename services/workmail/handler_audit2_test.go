package workmail_test

// Audit batch-2: coverage for the 36 Appendix A ops (go-0gtx).
//
// Covers:
//   - Availability configurations: CreateAvailabilityConfiguration/DeleteAvailabilityConfiguration/
//     UpdateAvailabilityConfiguration/ListAvailabilityConfigurations/TestAvailabilityConfiguration
//   - Mobile device access rules: CreateMobileDeviceAccessRule/DeleteMobileDeviceAccessRule/
//     UpdateMobileDeviceAccessRule/ListMobileDeviceAccessRules/GetMobileDeviceAccessEffect
//   - Mobile device access overrides: PutMobileDeviceAccessOverride/DeleteMobileDeviceAccessOverride/
//     GetMobileDeviceAccessOverride/ListMobileDeviceAccessOverrides
//   - Email monitoring: PutEmailMonitoringConfiguration/DeleteEmailMonitoringConfiguration/
//     DescribeEmailMonitoringConfiguration
//   - Inbound DMARC: PutInboundDmarcSettings/DescribeInboundDmarcSettings
//   - Retention policies: PutRetentionPolicy/DeleteRetentionPolicy/GetDefaultRetentionPolicy
//   - Mailbox export jobs: StartMailboxExportJob/CancelMailboxExportJob/DescribeMailboxExportJob/
//     ListMailboxExportJobs
//   - Identity center apps: CreateIdentityCenterApplication/DeleteIdentityCenterApplication
//   - Identity provider config: PutIdentityProviderConfiguration/DeleteIdentityProviderConfiguration/
//     DescribeIdentityProviderConfiguration
//   - Personal access tokens: DeletePersonalAccessToken/GetPersonalAccessTokenMetadata/
//     ListPersonalAccessTokens
//   - Impersonation role effect: GetImpersonationRoleEffect
//   - Assume impersonation role: AssumeImpersonationRole

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/workmail"
)

func a2Handler(t *testing.T) *workmail.Handler {
	t.Helper()

	return workmail.NewHandler(workmail.NewInMemoryBackend("000000000000", "us-east-1"))
}

// ---- Availability Configurations ----

func TestAvailabilityConfigurationLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		providerType string
		provider     string
	}{
		{
			name:         "EWS provider",
			providerType: "EWS",
			provider:     `"EwsProvider":{"EwsEndpoint":"https://ews.example.com","EwsUsername":"user","EwsPassword":"pass"}`,
		},
		{
			name:         "Lambda provider",
			providerType: "LAMBDA",
			provider:     `"LambdaProvider":{"LambdaArn":"arn:aws:lambda:us-east-1:000000000000:function:avail"}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := a2Handler(t)
			orgID := createTestOrg(t, h, "avail-test-org")

			// Create
			rec := a1Do(t, h, "CreateAvailabilityConfiguration", fmt.Sprintf(
				`{"OrganizationId":%q,"DomainName":"example.com",%s}`, orgID, tc.provider,
			))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			// List
			rec = a1Do(t, h, "ListAvailabilityConfigurations", fmt.Sprintf(
				`{"OrganizationId":%q}`, orgID,
			))
			require.Equal(t, http.StatusOK, rec.Code)
			m := a1JSON(t, rec)
			cfgs, ok := m["AvailabilityConfigurations"].([]any)
			require.True(t, ok)
			require.Len(t, cfgs, 1)
			cfg := cfgs[0].(map[string]any)
			assert.Equal(t, "example.com", cfg["DomainName"])
			assert.Equal(t, tc.providerType, cfg["ProviderType"])

			// Test
			rec = a1Do(t, h, "TestAvailabilityConfiguration", fmt.Sprintf(
				`{"OrganizationId":%q,"DomainName":"example.com"}`, orgID,
			))
			require.Equal(t, http.StatusOK, rec.Code)
			m = a1JSON(t, rec)
			assert.Equal(t, true, m["TestPassed"])

			// Update
			updateProvider := `"LambdaProvider":{"LambdaArn":"arn:aws:lambda:us-east-1:000000000000:function:updated"}`
			if tc.providerType == "LAMBDA" {
				updateProvider = `"EwsProvider":{"EwsEndpoint":"https://ews2.example.com","EwsUsername":"user2","EwsPassword":"pass2"}` //nolint:lll // existing issue.
			}
			rec = a1Do(t, h, "UpdateAvailabilityConfiguration", fmt.Sprintf(
				`{"OrganizationId":%q,"DomainName":"example.com",%s}`, orgID, updateProvider,
			))
			require.Equal(t, http.StatusOK, rec.Code)

			// Verify update
			rec = a1Do(t, h, "ListAvailabilityConfigurations", fmt.Sprintf(
				`{"OrganizationId":%q}`, orgID,
			))
			m = a1JSON(t, rec)
			cfgs = m["AvailabilityConfigurations"].([]any)
			updated := cfgs[0].(map[string]any)
			if tc.providerType == "LAMBDA" {
				assert.Equal(t, "EWS", updated["ProviderType"])
			} else {
				assert.Equal(t, "LAMBDA", updated["ProviderType"])
			}

			// Delete
			rec = a1Do(t, h, "DeleteAvailabilityConfiguration", fmt.Sprintf(
				`{"OrganizationId":%q,"DomainName":"example.com"}`, orgID,
			))
			require.Equal(t, http.StatusOK, rec.Code)

			// Verify gone
			rec = a1Do(t, h, "ListAvailabilityConfigurations", fmt.Sprintf(
				`{"OrganizationId":%q}`, orgID,
			))
			m = a1JSON(t, rec)
			cfgs = m["AvailabilityConfigurations"].([]any)
			assert.Empty(t, cfgs)
		})
	}
}

func TestAvailabilityConfigurationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		action    string
		body      string
		wantError string
	}{
		{
			name:      "create duplicate",
			action:    "sequence",
			wantError: "EntityAlreadyExistsException",
		},
		{
			name:      "delete nonexistent",
			action:    "DeleteAvailabilityConfiguration",
			body:      `{"OrganizationId":"org-123456789012","DomainName":"nope.com"}`,
			wantError: "EntityNotFoundException",
		},
		{
			name:      "update nonexistent",
			action:    "UpdateAvailabilityConfiguration",
			body:      `{"OrganizationId":"org-123456789012","DomainName":"nope.com","LambdaProvider":{"LambdaArn":"arn:aws:lambda:us-east-1:000:function:f"}}`, //nolint:lll // existing issue.
			wantError: "EntityNotFoundException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := a2Handler(t)

			if tc.name == "create duplicate" {
				orgID := createTestOrg(t, h, "dup-avail-org")
				body := fmt.Sprintf(
					`{"OrganizationId":%q,"DomainName":"example.com","LambdaProvider":{"LambdaArn":"arn:x"}}`, orgID,
				)
				a1Do(t, h, "CreateAvailabilityConfiguration", body)
				rec := a1Do(t, h, "CreateAvailabilityConfiguration", body)
				require.Equal(t, http.StatusBadRequest, rec.Code)
				m := a1JSON(t, rec)
				assert.Contains(t, m["__type"], tc.wantError)

				return
			}

			rec := a1Do(t, h, tc.action, tc.body)
			require.Equal(t, http.StatusBadRequest, rec.Code)
			m := a1JSON(t, rec)
			assert.Contains(t, m["__type"], tc.wantError)
		})
	}
}

// ---- Mobile Device Access Rules ----

func TestMobileDeviceAccessRuleLifecycle(t *testing.T) {
	t.Parallel()

	h := a2Handler(t)
	orgID := createTestOrg(t, h, "mdar-org")

	type createCase struct {
		name         string
		effect       string
		description  string
		deviceModels []string
		deviceTypes  []string
	}
	cases := []createCase{
		{name: "rule-allow", effect: "ALLOW", description: "Allow iPhones", deviceModels: []string{"iPhone"}},
		{name: "rule-deny", effect: "DENY", description: "Deny Androids", deviceTypes: []string{"Android"}},
	}

	ruleIDs := map[string]string{}
	for _, tc := range cases {
		body := fmt.Sprintf(`{"OrganizationId":%q,"Name":%q,"Effect":%q,"Description":%q`,
			orgID, tc.name, tc.effect, tc.description)
		if len(tc.deviceModels) > 0 {
			modelsJSON, _ := json.Marshal(tc.deviceModels)
			body += fmt.Sprintf(`,"DeviceModels":%s`, string(modelsJSON))
		}
		if len(tc.deviceTypes) > 0 {
			typesJSON, _ := json.Marshal(tc.deviceTypes)
			body += fmt.Sprintf(`,"DeviceTypes":%s`, string(typesJSON))
		}
		body += "}"
		rec := a1Do(t, h, "CreateMobileDeviceAccessRule", body)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		m := a1JSON(t, rec)
		ruleID, ok := m["MobileDeviceAccessRuleId"].(string)
		require.True(t, ok)
		require.NotEmpty(t, ruleID)
		ruleIDs[tc.name] = ruleID
	}

	// List
	rec := a1Do(t, h, "ListMobileDeviceAccessRules", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
	require.Equal(t, http.StatusOK, rec.Code)
	m := a1JSON(t, rec)
	rules, ok := m["Rules"].([]any)
	require.True(t, ok)
	assert.Len(t, rules, 2)

	// Update
	ruleID := ruleIDs["rule-allow"]
	rec = a1Do(t, h, "UpdateMobileDeviceAccessRule", fmt.Sprintf(
		`{"OrganizationId":%q,"MobileDeviceAccessRuleId":%q,"Name":"updated-rule","Effect":"DENY","Description":"Updated"}`,
		orgID,
		ruleID,
	))
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify update in list
	rec = a1Do(t, h, "ListMobileDeviceAccessRules", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
	m = a1JSON(t, rec)
	rules = m["Rules"].([]any)
	found := false
	for _, r := range rules {
		rule := r.(map[string]any)
		if rule["MobileDeviceAccessRuleId"] == ruleID {
			assert.Equal(t, "updated-rule", rule["Name"])
			assert.Equal(t, "DENY", rule["Effect"])
			found = true
		}
	}
	assert.True(t, found)

	// Delete
	rec = a1Do(t, h, "DeleteMobileDeviceAccessRule", fmt.Sprintf(
		`{"OrganizationId":%q,"MobileDeviceAccessRuleId":%q}`, orgID, ruleID,
	))
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify gone
	rec = a1Do(t, h, "ListMobileDeviceAccessRules", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
	m = a1JSON(t, rec)
	rules = m["Rules"].([]any)
	assert.Len(t, rules, 1)
}

func TestGetMobileDeviceAccessEffect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		ruleEffect   string
		queryModel   string
		wantEffect   string
		deviceModels []string
		wantMatchLen int
	}{
		{
			name:         "matches ALLOW rule",
			ruleEffect:   "ALLOW",
			deviceModels: []string{"iPhone"},
			queryModel:   "iPhone",
			wantEffect:   "ALLOW",
			wantMatchLen: 1,
		},
		{
			name:         "no match returns default ALLOW",
			ruleEffect:   "DENY",
			deviceModels: []string{"Android"},
			queryModel:   "iPhone",
			wantEffect:   "ALLOW",
			wantMatchLen: 0,
		},
		{
			name:         "matches DENY rule",
			ruleEffect:   "DENY",
			deviceModels: []string{"BlackBerry"},
			queryModel:   "BlackBerry",
			wantEffect:   "DENY",
			wantMatchLen: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := a2Handler(t)
			orgID := createTestOrg(t, h, "effect-org-"+tc.name)

			modelsJSON, _ := json.Marshal(tc.deviceModels)
			a1Do(t, h, "CreateMobileDeviceAccessRule", fmt.Sprintf(
				`{"OrganizationId":%q,"Name":"test-rule","Effect":%q,"DeviceModels":%s}`,
				orgID, tc.ruleEffect, string(modelsJSON),
			))

			rec := a1Do(t, h, "GetMobileDeviceAccessEffect", fmt.Sprintf(
				`{"OrganizationId":%q,"DeviceModel":%q}`, orgID, tc.queryModel,
			))
			require.Equal(t, http.StatusOK, rec.Code)
			m := a1JSON(t, rec)
			assert.Equal(t, tc.wantEffect, m["Effect"])
			matched := m["MatchedRules"].([]any)
			assert.Len(t, matched, tc.wantMatchLen)
		})
	}
}

// ---- Mobile Device Access Overrides ----

func TestMobileDeviceAccessOverrideLifecycle(t *testing.T) {
	t.Parallel()

	h := a2Handler(t)
	orgID := createTestOrg(t, h, "override-org")

	tests := []struct {
		name        string
		userID      string
		deviceID    string
		effect      string
		description string
	}{
		{
			name:        "allow override",
			userID:      "user-abc",
			deviceID:    "device-001",
			effect:      "ALLOW",
			description: "Allow specific device",
		},
		{
			name:        "deny override",
			userID:      "user-xyz",
			deviceID:    "device-002",
			effect:      "DENY",
			description: "Deny specific device",
		},
	}

	for _, tc := range tests {
		// Put
		rec := a1Do(t, h, "PutMobileDeviceAccessOverride", fmt.Sprintf(
			`{"OrganizationId":%q,"UserId":%q,"DeviceId":%q,"Effect":%q,"Description":%q}`,
			orgID, tc.userID, tc.deviceID, tc.effect, tc.description,
		))
		require.Equal(t, http.StatusOK, rec.Code, tc.name)

		// Get
		rec = a1Do(t, h, "GetMobileDeviceAccessOverride", fmt.Sprintf(
			`{"OrganizationId":%q,"UserId":%q,"DeviceId":%q}`, orgID, tc.userID, tc.deviceID,
		))
		require.Equal(t, http.StatusOK, rec.Code)
		m := a1JSON(t, rec)
		assert.Equal(t, tc.userID, m["UserId"])
		assert.Equal(t, tc.effect, m["Effect"])
		assert.Equal(t, tc.description, m["Description"])
	}

	// List all
	rec := a1Do(t, h, "ListMobileDeviceAccessOverrides", fmt.Sprintf(
		`{"OrganizationId":%q}`, orgID,
	))
	require.Equal(t, http.StatusOK, rec.Code)
	m := a1JSON(t, rec)
	overrides := m["Overrides"].([]any)
	assert.Len(t, overrides, 2)

	// List filtered by user
	rec = a1Do(t, h, "ListMobileDeviceAccessOverrides", fmt.Sprintf(
		`{"OrganizationId":%q,"UserId":"user-abc"}`, orgID,
	))
	m = a1JSON(t, rec)
	overrides = m["Overrides"].([]any)
	assert.Len(t, overrides, 1)
	assert.Equal(t, "user-abc", overrides[0].(map[string]any)["UserId"])

	// Delete
	rec = a1Do(t, h, "DeleteMobileDeviceAccessOverride", fmt.Sprintf(
		`{"OrganizationId":%q,"UserId":"user-abc","DeviceId":"device-001"}`, orgID,
	))
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify gone
	rec = a1Do(t, h, "ListMobileDeviceAccessOverrides", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
	m = a1JSON(t, rec)
	overrides = m["Overrides"].([]any)
	assert.Len(t, overrides, 1)
}

func TestMobileDeviceAccessOverrideErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		action    string
		body      string
		wantError string
		wantCode  int
	}{
		{
			name:      "get nonexistent override",
			action:    "GetMobileDeviceAccessOverride",
			body:      `{"OrganizationId":"org-123456789012","UserId":"nope","DeviceId":"dev-nope"}`,
			wantCode:  http.StatusBadRequest,
			wantError: "EntityNotFoundException",
		},
		{
			name:      "delete nonexistent override",
			action:    "DeleteMobileDeviceAccessOverride",
			body:      `{"OrganizationId":"org-123456789012","UserId":"nope","DeviceId":"dev-nope"}`,
			wantCode:  http.StatusBadRequest,
			wantError: "EntityNotFoundException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := a2Handler(t)
			rec := a1Do(t, h, tc.action, tc.body)
			require.Equal(t, tc.wantCode, rec.Code)
			m := a1JSON(t, rec)
			assert.Contains(t, m["__type"], tc.wantError)
		})
	}
}

// ---- Email Monitoring Configuration ----

func TestEmailMonitoringConfigurationLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		roleArn     string
		logGroupArn string
	}{
		{
			name:        "full config",
			roleArn:     "arn:aws:iam::000000000000:role/WorkMailMonitoringRole",
			logGroupArn: "arn:aws:logs:us-east-1:000000000000:log-group:/workmail/monitoring",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := a2Handler(t)
			orgID := createTestOrg(t, h, "monitoring-org")

			// Describe empty
			rec := a1Do(t, h, "DescribeEmailMonitoringConfiguration", fmt.Sprintf(
				`{"OrganizationId":%q}`, orgID,
			))
			require.Equal(t, http.StatusOK, rec.Code)
			m := a1JSON(t, rec)
			assert.Empty(t, m["RoleArn"])

			// Put
			rec = a1Do(t, h, "PutEmailMonitoringConfiguration", fmt.Sprintf(
				`{"OrganizationId":%q,"RoleArn":%q,"LogGroupArn":%q}`,
				orgID, tc.roleArn, tc.logGroupArn,
			))
			require.Equal(t, http.StatusOK, rec.Code)

			// Describe
			rec = a1Do(t, h, "DescribeEmailMonitoringConfiguration", fmt.Sprintf(
				`{"OrganizationId":%q}`, orgID,
			))
			require.Equal(t, http.StatusOK, rec.Code)
			m = a1JSON(t, rec)
			assert.Equal(t, tc.roleArn, m["RoleArn"])
			assert.Equal(t, tc.logGroupArn, m["LogGroupArn"])

			// Delete
			rec = a1Do(t, h, "DeleteEmailMonitoringConfiguration", fmt.Sprintf(
				`{"OrganizationId":%q}`, orgID,
			))
			require.Equal(t, http.StatusOK, rec.Code)

			// Describe empty again
			rec = a1Do(t, h, "DescribeEmailMonitoringConfiguration", fmt.Sprintf(
				`{"OrganizationId":%q}`, orgID,
			))
			require.Equal(t, http.StatusOK, rec.Code)
			m = a1JSON(t, rec)
			assert.Empty(t, m["RoleArn"])
		})
	}
}

// ---- Inbound DMARC Settings ----

func TestInboundDmarcSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		enforced bool
	}{
		{name: "enable enforcement", enforced: true},
		{name: "disable enforcement", enforced: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := a2Handler(t)
			orgID := createTestOrg(t, h, "dmarc-org")

			// Describe default (false)
			rec := a1Do(t, h, "DescribeInboundDmarcSettings", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
			require.Equal(t, http.StatusOK, rec.Code)
			m := a1JSON(t, rec)
			assert.Equal(t, false, m["Enforced"])

			// Put
			rec = a1Do(t, h, "PutInboundDmarcSettings", fmt.Sprintf(
				`{"OrganizationId":%q,"Enforced":%v}`, orgID, tc.enforced,
			))
			require.Equal(t, http.StatusOK, rec.Code)

			// Describe
			rec = a1Do(t, h, "DescribeInboundDmarcSettings", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
			m = a1JSON(t, rec)
			assert.Equal(t, tc.enforced, m["Enforced"])
		})
	}
}

// ---- Retention Policies ----

func TestRetentionPolicyLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		policyName   string
		description  string
		folderName   string
		folderAction string
		folderPeriod int
	}{
		{
			name:         "inbox delete after 365 days",
			policyName:   "default-retention",
			description:  "Default 365 day retention",
			folderName:   "INBOX",
			folderAction: "DELETE",
			folderPeriod: 365,
		},
		{
			name:         "sent items permanently delete",
			policyName:   "sent-retention",
			description:  "Sent items policy",
			folderName:   "SENT_ITEMS",
			folderAction: "PERMANENTLY_DELETE",
			folderPeriod: 730,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := a2Handler(t)
			orgID := createTestOrg(t, h, "retention-org")

			// Get before put → not found
			rec := a1Do(t, h, "GetDefaultRetentionPolicy", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
			require.Equal(t, http.StatusBadRequest, rec.Code)

			// Put
			rec = a1Do(t, h, "PutRetentionPolicy", fmt.Sprintf(
				`{"OrganizationId":%q,"Name":%q,"Description":%q,"FolderConfigurations":[{"Name":%q,"Action":%q,"Period":%d}]}`,
				orgID,
				tc.policyName,
				tc.description,
				tc.folderName,
				tc.folderAction,
				tc.folderPeriod,
			))
			require.Equal(t, http.StatusOK, rec.Code)

			// Get
			rec = a1Do(t, h, "GetDefaultRetentionPolicy", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
			require.Equal(t, http.StatusOK, rec.Code)
			m := a1JSON(t, rec)
			assert.Equal(t, tc.policyName, m["Name"])
			assert.Equal(t, tc.description, m["Description"])
			folderCfgs := m["FolderConfigurations"].([]any)
			require.Len(t, folderCfgs, 1)
			fc := folderCfgs[0].(map[string]any)
			assert.Equal(t, tc.folderName, fc["Name"])
			assert.Equal(t, tc.folderAction, fc["Action"])

			policyID := m["Id"].(string)

			// Delete
			rec = a1Do(t, h, "DeleteRetentionPolicy", fmt.Sprintf(
				`{"OrganizationId":%q,"Id":%q}`, orgID, policyID,
			))
			require.Equal(t, http.StatusOK, rec.Code)

			// Verify gone
			rec = a1Do(t, h, "GetDefaultRetentionPolicy", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
			require.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// ---- Mailbox Export Jobs ----

func TestMailboxExportJobLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		description  string
		s3BucketName string
		s3Prefix     string
	}{
		{
			name:         "basic export",
			description:  "Full mailbox export",
			s3BucketName: "my-export-bucket",
			s3Prefix:     "exports/2024",
		},
		{
			name:         "no description",
			description:  "",
			s3BucketName: "another-bucket",
			s3Prefix:     "prefix/subdir",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := a2Handler(t)
			orgID := createTestOrg(t, h, "export-org")
			userID := createTestUser(t, h, orgID, "exportuser", "Export User")

			// Start
			rec := a1Do(t, h, "StartMailboxExportJob", fmt.Sprintf(
				`{"OrganizationId":%q,"EntityId":%q,"Description":%q,"RoleArn":"arn:aws:iam::000:role/ExportRole","KmsKeyArn":"arn:aws:kms:us-east-1:000:key/abc","S3BucketName":%q,"S3Prefix":%q}`, //nolint:lll // existing issue.
				orgID,
				userID,
				tc.description,
				tc.s3BucketName,
				tc.s3Prefix,
			))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			m := a1JSON(t, rec)
			jobID, ok := m["JobId"].(string)
			require.True(t, ok)
			require.NotEmpty(t, jobID)

			// Describe
			rec = a1Do(t, h, "DescribeMailboxExportJob", fmt.Sprintf(
				`{"OrganizationId":%q,"JobId":%q}`, orgID, jobID,
			))
			require.Equal(t, http.StatusOK, rec.Code)
			m = a1JSON(t, rec)
			assert.Equal(t, "RUNNING", m["State"])
			assert.Equal(t, tc.s3BucketName, m["S3BucketName"])
			assert.Equal(t, tc.s3Prefix, m["S3Prefix"])

			// List
			rec = a1Do(t, h, "ListMailboxExportJobs", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
			require.Equal(t, http.StatusOK, rec.Code)
			m = a1JSON(t, rec)
			jobs := m["Jobs"].([]any)
			assert.Len(t, jobs, 1)
			assert.Equal(t, jobID, jobs[0].(map[string]any)["JobId"])

			// Cancel
			rec = a1Do(t, h, "CancelMailboxExportJob", fmt.Sprintf(
				`{"OrganizationId":%q,"JobId":%q,"ClientToken":"tok-123"}`, orgID, jobID,
			))
			require.Equal(t, http.StatusOK, rec.Code)

			// Describe after cancel
			rec = a1Do(t, h, "DescribeMailboxExportJob", fmt.Sprintf(
				`{"OrganizationId":%q,"JobId":%q}`, orgID, jobID,
			))
			m = a1JSON(t, rec)
			assert.Equal(t, "CANCELLED", m["State"])
		})
	}
}

// ---- Identity Center Applications ----

func TestIdentityCenterApplicationLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		appName     string
		instanceARN string
	}{
		{
			name:        "create and delete",
			appName:     "WorkMailApp",
			instanceARN: "arn:aws:sso:::instance/ssoins-000000000000",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := a2Handler(t)

			// Create
			rec := a1Do(t, h, "CreateIdentityCenterApplication", fmt.Sprintf(
				`{"InstanceArn":%q,"Name":%q}`, tc.instanceARN, tc.appName,
			))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			m := a1JSON(t, rec)
			appARN, ok := m["ApplicationArn"].(string)
			require.True(t, ok)
			require.NotEmpty(t, appARN)

			// Delete
			rec = a1Do(t, h, "DeleteIdentityCenterApplication", fmt.Sprintf(
				`{"ApplicationArn":%q}`, appARN,
			))
			require.Equal(t, http.StatusOK, rec.Code)

			// Delete again → not found
			rec = a1Do(t, h, "DeleteIdentityCenterApplication", fmt.Sprintf(
				`{"ApplicationArn":%q}`, appARN,
			))
			require.Equal(t, http.StatusBadRequest, rec.Code)
			m = a1JSON(t, rec)
			assert.Contains(t, m["__type"], "EntityNotFoundException")
		})
	}
}

// ---- Identity Provider Configuration ----

func TestIdentityProviderConfigurationLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		authMode  string
		patStatus string
		lifetime  int
	}{
		{
			name:      "identity-provider-only",
			authMode:  "IDENTITY_PROVIDER_ONLY",
			patStatus: "ACTIVE",
			lifetime:  90,
		},
		{
			name:      "identity-provider-and-directory",
			authMode:  "IDENTITY_PROVIDER_AND_DIRECTORY",
			patStatus: "INACTIVE",
			lifetime:  0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := a2Handler(t)
			orgID := createTestOrg(t, h, "idp-org")

			// Describe before put → not found
			rec := a1Do(t, h, "DescribeIdentityProviderConfiguration", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
			require.Equal(t, http.StatusBadRequest, rec.Code)

			appARN := "arn:aws:sso:::application/ssoins-abc/apl-123"
			instanceARN := "arn:aws:sso:::instance/ssoins-abc"
			body := fmt.Sprintf(
				`{"OrganizationId":%q,"AuthenticationMode":%q,"IdentityCenterConfiguration":{"ApplicationArn":%q,"InstanceArn":%q},"PersonalAccessTokenConfiguration":{"Status":%q}}`, //nolint:lll // existing issue.
				orgID,
				tc.authMode,
				appARN,
				instanceARN,
				tc.patStatus,
			)
			if tc.lifetime > 0 {
				body = fmt.Sprintf(
					`{"OrganizationId":%q,"AuthenticationMode":%q,"IdentityCenterConfiguration":{"ApplicationArn":%q,"InstanceArn":%q},"PersonalAccessTokenConfiguration":{"Status":%q,"LifetimeInDays":%d}}`, //nolint:lll // existing issue.
					orgID,
					tc.authMode,
					appARN,
					instanceARN,
					tc.patStatus,
					tc.lifetime,
				)
			}

			// Put
			rec = a1Do(t, h, "PutIdentityProviderConfiguration", body)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			// Describe
			rec = a1Do(t, h, "DescribeIdentityProviderConfiguration", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
			require.Equal(t, http.StatusOK, rec.Code)
			m := a1JSON(t, rec)
			assert.Equal(t, tc.authMode, m["AuthenticationMode"])
			icc, ok := m["IdentityCenterConfiguration"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, appARN, icc["ApplicationArn"])
			patCfg, ok := m["PersonalAccessTokenConfiguration"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tc.patStatus, patCfg["Status"])

			// Delete
			rec = a1Do(t, h, "DeleteIdentityProviderConfiguration", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
			require.Equal(t, http.StatusOK, rec.Code)

			// Describe after delete → not found
			rec = a1Do(t, h, "DescribeIdentityProviderConfiguration", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
			require.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// ---- Personal Access Tokens ----

func TestPersonalAccessTokenLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		scopes []string
	}{
		{name: "read-only scopes", scopes: []string{"ReadMail", "ReadCalendar"}},
		{name: "full scopes", scopes: []string{"ReadMail", "WriteMail", "ReadCalendar", "WriteCalendar"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := a2Handler(t) //nolint:ineffassign,staticcheck,wastedassign // existing issue.
			backend := workmail.NewInMemoryBackend("000000000000", "us-east-1")
			h = workmail.NewHandler(backend)
			orgID := createTestOrg(t, h, "pat-org")
			userID := createTestUser(t, h, orgID, "patuser", "PAT User")

			// Create a PAT via backend directly (no CreatePersonalAccessToken op in the 36)
			tok, err := backend.CreatePersonalAccessToken(orgID, userID, "my-token", tc.scopes)
			require.NoError(t, err)

			// GetPersonalAccessTokenMetadata
			rec := a1Do(t, h, "GetPersonalAccessTokenMetadata", fmt.Sprintf(
				`{"OrganizationId":%q,"PersonalAccessTokenId":%q}`, orgID, tok.TokenID,
			))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			m := a1JSON(t, rec)
			assert.Equal(t, tok.TokenID, m["PersonalAccessTokenId"])
			assert.Equal(t, userID, m["UserId"])
			assert.Equal(t, "my-token", m["Name"])

			// ListPersonalAccessTokens all
			rec = a1Do(t, h, "ListPersonalAccessTokens", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
			require.Equal(t, http.StatusOK, rec.Code)
			m = a1JSON(t, rec)
			summaries := m["PersonalAccessTokenSummaries"].([]any)
			assert.Len(t, summaries, 1)

			// ListPersonalAccessTokens filtered by user
			rec = a1Do(t, h, "ListPersonalAccessTokens", fmt.Sprintf(
				`{"OrganizationId":%q,"UserId":%q}`, orgID, userID,
			))
			m = a1JSON(t, rec)
			summaries = m["PersonalAccessTokenSummaries"].([]any)
			assert.Len(t, summaries, 1)

			// Delete
			rec = a1Do(t, h, "DeletePersonalAccessToken", fmt.Sprintf(
				`{"OrganizationId":%q,"PersonalAccessTokenId":%q}`, orgID, tok.TokenID,
			))
			require.Equal(t, http.StatusOK, rec.Code)

			// Get after delete → not found
			rec = a1Do(t, h, "GetPersonalAccessTokenMetadata", fmt.Sprintf(
				`{"OrganizationId":%q,"PersonalAccessTokenId":%q}`, orgID, tok.TokenID,
			))
			require.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// ---- Impersonation Role Effect ----

func TestGetImpersonationRoleEffect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		ruleEffect  string
		queryUser   string
		wantEffect  string
		targetUsers []string
	}{
		{
			name:        "allow specific user",
			ruleEffect:  "ALLOW",
			targetUsers: []string{"user-alpha"},
			queryUser:   "user-alpha",
			wantEffect:  "ALLOW",
		},
		{
			name:        "deny unmatched user",
			ruleEffect:  "ALLOW",
			targetUsers: []string{"user-alpha"},
			queryUser:   "user-beta",
			wantEffect:  "DENY",
		},
		{
			name:        "deny rule matched",
			ruleEffect:  "DENY",
			targetUsers: []string{"user-gamma"},
			queryUser:   "user-gamma",
			wantEffect:  "DENY",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := a2Handler(t)
			orgID := createTestOrg(t, h, "imp-effect-org")

			targetJSON, _ := json.Marshal(tc.targetUsers)
			roleBody := fmt.Sprintf(
				`{"OrganizationId":%q,"Name":"test-role","Type":"FULL_ACCESS","Rules":[{"ImpersonationRuleId":"rule-1","Name":"rule-one","Effect":%q,"TargetUsers":%s}]}`, //nolint:lll // existing issue.
				orgID,
				tc.ruleEffect,
				string(targetJSON),
			)
			rec := a1Do(t, h, "CreateImpersonationRole", roleBody)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			m := a1JSON(t, rec)
			roleID := m["ImpersonationRoleId"].(string)

			rec = a1Do(t, h, "GetImpersonationRoleEffect", fmt.Sprintf(
				`{"OrganizationId":%q,"ImpersonationRoleId":%q,"TargetUser":%q}`,
				orgID, roleID, tc.queryUser,
			))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			m = a1JSON(t, rec)
			assert.Equal(t, tc.wantEffect, m["Effect"])
			assert.Equal(t, "FULL_ACCESS", m["Type"])
		})
	}
}

// ---- Assume Impersonation Role ----

func TestAssumeImpersonationRole(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		roleType  string
		wantToken bool
	}{
		{name: "full access role", roleType: "FULL_ACCESS", wantToken: true},
		{name: "read only role", roleType: "READ_ONLY", wantToken: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := a2Handler(t)
			orgID := createTestOrg(t, h, "assume-imp-org")

			rec := a1Do(t, h, "CreateImpersonationRole", fmt.Sprintf(
				`{"OrganizationId":%q,"Name":"test-role","Type":%q,"Rules":[]}`, orgID, tc.roleType,
			))
			require.Equal(t, http.StatusOK, rec.Code)
			m := a1JSON(t, rec)
			roleID := m["ImpersonationRoleId"].(string)

			rec = a1Do(t, h, "AssumeImpersonationRole", fmt.Sprintf(
				`{"OrganizationId":%q,"ImpersonationRoleId":%q}`, orgID, roleID,
			))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			m = a1JSON(t, rec)
			if tc.wantToken {
				token, ok := m["Token"].(string)
				require.True(t, ok)
				assert.NotEmpty(t, token)
				expiresIn, ok := m["ExpiresIn"].(float64)
				require.True(t, ok)
				assert.Greater(t, expiresIn, float64(0))
			}
		})
	}
}

func TestAssumeImpersonationRoleErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		orgID     string
		roleID    string
		wantError string
	}{
		{
			name:      "org not found",
			orgID:     "org-nonexistent0001",
			roleID:    "role-abc",
			wantError: "EntityNotFoundException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := a2Handler(t)
			rec := a1Do(t, h, "AssumeImpersonationRole", fmt.Sprintf(
				`{"OrganizationId":%q,"ImpersonationRoleId":%q}`, tc.orgID, tc.roleID,
			))
			require.Equal(t, http.StatusBadRequest, rec.Code)
			m := a1JSON(t, rec)
			assert.Contains(t, m["__type"], tc.wantError)
		})
	}
}
