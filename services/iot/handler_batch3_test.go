package iot_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

func newHandlerForBatch3Test(t *testing.T) (*iot.Handler, *iot.InMemoryBackend) {
	t.Helper()
	b := iot.NewInMemoryBackend()
	h := iot.NewHandler(b, nil)

	return h, b
}

// TestBatch3_OTAUpdateCRUD tests OTAUpdate create/get/list/delete.
func TestBatch3_OTAUpdateCRUD(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForBatch3Test(t)

	// Create
	out := iotOK(t, h, http.MethodPost, "/otaUpdates/my-ota", map[string]any{
		"targets":     []string{"arn:aws:iot:us-east-1:000000000000:thing/device1"},
		"roleArn":     "arn:aws:iam::000000000000:role/IoTRole",
		"description": "test OTA",
	})
	if out["otaUpdateId"] != "my-ota" {
		t.Errorf("expected otaUpdateId=my-ota, got %v", out)
	}

	// Get
	out2 := iotOK(t, h, http.MethodGet, "/otaUpdates/my-ota", nil)
	info := out2["otaUpdateInfo"].(map[string]any)
	if info["otaUpdateId"] != "my-ota" {
		t.Errorf("get mismatch: %v", info)
	}

	// List
	out3 := iotOK(t, h, http.MethodGet, "/otaUpdates", nil)
	updates, _ := out3["otaUpdates"].([]any)
	if len(updates) != 1 {
		t.Errorf("expected 1 OTA update, got %d", len(updates))
	}

	// Delete
	iotOK(t, h, http.MethodDelete, "/otaUpdates/my-ota", nil)
	iotExpectError(t, h, "/otaUpdates/my-ota")
}

// TestBatch3_PackageCRUD tests Package create/get/update/list/delete.
func TestBatch3_PackageCRUD(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForBatch3Test(t)

	// Create
	out := iotOK(t, h, http.MethodPut, "/packages/my-pkg", map[string]any{
		"description": "test package",
	})
	if out["packageName"] != "my-pkg" {
		t.Errorf("expected packageName=my-pkg, got %v", out)
	}

	// Get
	out2 := iotOK(t, h, http.MethodGet, "/packages/my-pkg", nil)
	if out2["packageName"] != "my-pkg" {
		t.Errorf("get mismatch: %v", out2)
	}

	// Update
	iotOK(t, h, http.MethodPatch, "/packages/my-pkg", map[string]any{
		"description": "updated",
	})

	// List
	out3 := iotOK(t, h, http.MethodGet, "/packages", nil)
	pkgs, _ := out3["packageList"].([]any)
	if len(pkgs) != 1 {
		t.Errorf("expected 1 package, got %d", len(pkgs))
	}

	// Delete
	iotOK(t, h, http.MethodDelete, "/packages/my-pkg", nil)
	iotExpectError(t, h, "/packages/my-pkg")
}

// TestBatch3_PackageVersionCRUD tests PackageVersion create/get/update/list/delete.
func TestBatch3_PackageVersionCRUD(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForBatch3Test(t)

	// Create version
	out := iotOK(t, h, http.MethodPut, "/packages/my-pkg/versions/1.0.0", map[string]any{
		"description": "version 1",
	})
	if out["versionName"] != "1.0.0" {
		t.Errorf("expected versionName=1.0.0, got %v", out)
	}

	// Get version
	out2 := iotOK(t, h, http.MethodGet, "/packages/my-pkg/versions/1.0.0", nil)
	if out2["versionName"] != "1.0.0" {
		t.Errorf("get mismatch: %v", out2)
	}

	// Update version
	iotOK(t, h, http.MethodPatch, "/packages/my-pkg/versions/1.0.0", map[string]any{
		"status": "PUBLISHED",
	})

	// List versions
	out3 := iotOK(t, h, http.MethodGet, "/packages/my-pkg/versions", nil)
	versions, _ := out3["packageVersionList"].([]any)
	if len(versions) != 1 {
		t.Errorf("expected 1 version, got %d", len(versions))
	}

	// Delete version
	iotOK(t, h, http.MethodDelete, "/packages/my-pkg/versions/1.0.0", nil)
}

// TestBatch3_PackageConfiguration tests GetPackageConfiguration and UpdatePackageConfiguration.
func TestBatch3_PackageConfiguration(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForBatch3Test(t)

	// Get (empty)
	out := iotOK(t, h, http.MethodGet, "/package-configuration", nil)
	if out == nil {
		t.Fatal("expected non-nil response")
	}

	// Update
	iotOK(t, h, http.MethodPatch, "/package-configuration", map[string]any{
		"versionUpdateByJobsConfig": map[string]any{"enabled": true},
	})

	// Get again to verify
	out2 := iotOK(t, h, http.MethodGet, "/package-configuration", nil)
	cfg, _ := out2["versionUpdateByJobsConfig"].(map[string]any)
	if cfg == nil {
		t.Errorf("expected versionUpdateByJobsConfig, got %v", out2)
	}
}

// TestBatch3_AuditSuppression tests audit suppression CRUD.
func TestBatch3_AuditSuppression(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForBatch3Test(t)

	body := map[string]any{
		"checkName":            "CA_CERTIFICATE_EXPIRING_CHECK",
		"resourceIdentifier":   map[string]any{"caCertificateId": "abc123"},
		"suppressIndefinitely": true,
	}

	// Create
	iotOK(t, h, http.MethodPost, "/audit/suppressions", body)

	// Describe
	out := iotOK(t, h, http.MethodGet, "/audit/suppressions/describe", body)
	if out["checkName"] != "CA_CERTIFICATE_EXPIRING_CHECK" {
		t.Errorf("describe mismatch: %v", out)
	}

	// Update
	updateBody := map[string]any{
		"checkName":            "CA_CERTIFICATE_EXPIRING_CHECK",
		"resourceIdentifier":   map[string]any{"caCertificateId": "abc123"},
		"suppressIndefinitely": false,
		"description":          "updated",
	}
	iotOK(t, h, http.MethodPatch, "/audit/suppressions/update", updateBody)

	// List
	out2 := iotOK(t, h, http.MethodGet, "/audit/suppressions/list", nil)
	suppressions, _ := out2["suppressions"].([]any)
	if len(suppressions) != 1 {
		t.Errorf("expected 1 suppression, got %d", len(suppressions))
	}

	// Delete
	iotOK(t, h, http.MethodPost, "/audit/suppressions/delete", body)

	// List again - should be empty
	out3 := iotOK(t, h, http.MethodGet, "/audit/suppressions/list", nil)
	suppressions2, _ := out3["suppressions"].([]any)
	if len(suppressions2) != 0 {
		t.Errorf("expected 0 suppressions after delete, got %d", len(suppressions2))
	}
}

// TestBatch3_V2Logging tests V2 logging options and levels.
func TestBatch3_V2Logging(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForBatch3Test(t)

	// Get default
	out := iotOK(t, h, http.MethodGet, "/v2LoggingOptions", nil)
	if out["defaultLogLevel"] != "DISABLED" {
		t.Errorf("expected DISABLED default, got %v", out)
	}

	// Set options
	iotOK(t, h, http.MethodPost, "/v2LoggingOptions", map[string]any{
		"roleArn":         "arn:aws:iam::000000000000:role/IoTRole",
		"defaultLogLevel": "INFO",
	})

	// Get updated
	out2 := iotOK(t, h, http.MethodGet, "/v2LoggingOptions", nil)
	if out2["defaultLogLevel"] != "INFO" {
		t.Errorf("expected INFO, got %v", out2)
	}

	// Set level
	iotOK(t, h, http.MethodPost, "/v2LoggingLevel", map[string]any{
		"logTarget": map[string]any{"targetType": "THING_GROUP", "targetName": "my-group"},
		"logLevel":  "DEBUG",
	})

	// List levels
	out3 := iotOK(t, h, http.MethodGet, "/v2LoggingLevel", nil)
	levels, _ := out3["logTargetConfigurations"].([]any)
	if len(levels) != 1 {
		t.Errorf("expected 1 level, got %d", len(levels))
	}

	// Delete level
	iotOK(t, h, http.MethodDelete, "/v2LoggingLevel?targetType=THING_GROUP&targetName=my-group", nil)

	// List after delete
	out4 := iotOK(t, h, http.MethodGet, "/v2LoggingLevel", nil)
	levels2, _ := out4["logTargetConfigurations"].([]any)
	if len(levels2) != 0 {
		t.Errorf("expected 0 levels after delete, got %d", len(levels2))
	}
}

// TestBatch3_LoggingOptions tests GetLoggingOptions and SetLoggingOptions.
func TestBatch3_LoggingOptions(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForBatch3Test(t)

	// Get default
	out := iotOK(t, h, http.MethodGet, "/loggingOptions", nil)
	if out["logLevel"] != "DISABLED" {
		t.Errorf("expected DISABLED, got %v", out)
	}

	// Set
	iotOK(t, h, http.MethodPost, "/loggingOptions", map[string]any{
		"roleArn":  "arn:aws:iam::000000000000:role/IoTLoggingRole",
		"logLevel": "WARN",
	})

	// Get updated
	out2 := iotOK(t, h, http.MethodGet, "/loggingOptions", nil)
	if out2["logLevel"] != "WARN" {
		t.Errorf("expected WARN, got %v", out2)
	}
}

// TestBatch3_CreateKeysAndCertificate tests CreateKeysAndCertificate.
func TestBatch3_CreateKeysAndCertificate(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForBatch3Test(t)

	out := iotOK(t, h, http.MethodPost, "/keys-and-certificate?setAsActive=true", nil)
	if out["certificateId"] == "" {
		t.Errorf("expected certificateId, got %v", out)
	}
	kp, ok := out["keyPair"].(map[string]any)
	if !ok || kp["PublicKey"] == "" {
		t.Errorf("expected keyPair, got %v", out)
	}
}

// TestBatch3_TransferCertificate tests TransferCertificate and RejectCertificateTransfer.
func TestBatch3_TransferCertificate(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForBatch3Test(t)

	// Create a cert first
	out := iotOK(t, h, http.MethodPost, "/keys-and-certificate?setAsActive=true", nil)
	certID, _ := out["certificateId"].(string)
	if certID == "" {
		t.Fatal("expected certificateId")
	}

	// Transfer
	iotOK(t, h, http.MethodPatch, "/certificates/"+certID+"/transfer?targetAwsAccount=111111111111", nil)

	// Reject
	iotOK(t, h, http.MethodPatch, "/reject-certificate-transfer/"+certID, nil)
}

// TestBatch3_EventConfigurations tests DescribeEventConfigurations and UpdateEventConfigurations.
func TestBatch3_EventConfigurations(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForBatch3Test(t)

	// Describe (empty)
	out := iotOK(t, h, http.MethodGet, "/event-configurations", nil)
	evts, _ := out["eventConfigurations"].(map[string]any)
	if evts == nil {
		t.Errorf("expected eventConfigurations map, got %v", out)
	}

	// Update
	iotOK(t, h, http.MethodPatch, "/event-configurations", map[string]any{
		"eventConfigurations": map[string]any{
			"THING": map[string]any{"Enabled": true},
		},
	})

	// Describe again
	out2 := iotOK(t, h, http.MethodGet, "/event-configurations", nil)
	evts2, _ := out2["eventConfigurations"].(map[string]any)
	thing, _ := evts2["THING"].(map[string]any)
	if thing["Enabled"] != true {
		t.Errorf("expected THING enabled=true, got %v", evts2)
	}
}

// TestBatch3_SecurityProfileTargets tests DetachSecurityProfile,
// ListTargetsForSecurityProfile, ListSecurityProfilesForTarget.
func TestBatch3_SecurityProfileTargets(t *testing.T) {
	t.Parallel()
	h, b := newHandlerForBatch3Test(t)

	profileName := "test-profile"
	targetARN := "arn:aws:iot:us-east-1:000000000000:thinggroup/my-group"

	// Attach via backend directly (AttachSecurityProfile already implemented)
	if err := b.AttachSecurityProfile(&iot.AttachSecurityProfileInput{
		SecurityProfileName:      profileName,
		SecurityProfileTargetArn: targetARN,
	}); err != nil {
		t.Fatal(err)
	}

	// List targets for profile
	out := iotOK(t, h, http.MethodGet, "/security-profiles/"+profileName+"/targets", nil)
	targets, _ := out["securityProfileTargets"].([]any)
	if len(targets) != 1 {
		t.Errorf("expected 1 target, got %d", len(targets))
	}

	// List profiles for target
	out2 := iotOK(t, h, http.MethodGet, "/security-profiles-for-target?securityProfileTargetArn="+targetARN, nil)
	mappings, _ := out2["securityProfileTargetMappings"].([]any)
	if len(mappings) != 1 {
		t.Errorf("expected 1 mapping, got %d", len(mappings))
	}

	// Detach
	iotOK(
		t,
		h,
		http.MethodDelete,
		"/security-profiles/"+profileName+"/targets?securityProfileTargetArn="+targetARN,
		nil,
	)

	// Verify detached
	out3 := iotOK(t, h, http.MethodGet, "/security-profiles/"+profileName+"/targets", nil)
	targets2, _ := out3["securityProfileTargets"].([]any)
	if len(targets2) != 0 {
		t.Errorf("expected 0 targets after detach, got %d", len(targets2))
	}
}

// TestBatch3_DynamicThingGroup tests CreateDynamicThingGroup, UpdateDynamicThingGroup, DeleteDynamicThingGroup.
func TestBatch3_DynamicThingGroup(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForBatch3Test(t)

	// Create
	out := iotOK(t, h, http.MethodPost, "/dynamic-thing-groups/my-dynamic-group", map[string]any{
		"queryString": "thingName:*",
		"description": "test dynamic group",
	})
	if out["thingGroupName"] != "my-dynamic-group" {
		t.Errorf("expected thingGroupName=my-dynamic-group, got %v", out)
	}

	// Update
	out2 := iotOK(t, h, http.MethodPatch, "/dynamic-thing-groups/my-dynamic-group", map[string]any{
		"queryString": "thingName:device*",
	})
	if out2["version"] == nil {
		t.Errorf("expected version in response, got %v", out2)
	}

	// Delete
	iotOK(t, h, http.MethodDelete, "/dynamic-thing-groups/my-dynamic-group", nil)
}

// TestBatch3_CommandCRUD tests Command create/get/update/list/delete.
func TestBatch3_CommandCRUD(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForBatch3Test(t)

	// Create
	out := iotOK(t, h, http.MethodPut, "/commands/my-cmd", map[string]any{
		"displayName": "My Command",
		"description": "test command",
		"namespace":   "AWS-IoT",
	})
	if out["commandId"] != "my-cmd" {
		t.Errorf("expected commandId=my-cmd, got %v", out)
	}

	// Get
	out2 := iotOK(t, h, http.MethodGet, "/commands/my-cmd", nil)
	if out2["commandId"] != "my-cmd" {
		t.Errorf("get mismatch: %v", out2)
	}

	// Update
	iotOK(t, h, http.MethodPatch, "/commands/my-cmd", map[string]any{
		"description": "updated",
	})

	// List
	out3 := iotOK(t, h, http.MethodGet, "/commands", nil)
	cmds, _ := out3["commands"].([]any)
	if len(cmds) != 1 {
		t.Errorf("expected 1 command, got %d", len(cmds))
	}

	// Delete
	iotOK(t, h, http.MethodDelete, "/commands/my-cmd", nil)
	iotExpectError(t, h, "/commands/my-cmd")
}

// TestBatch3_ProvisioningClaim tests CreateProvisioningClaim (needs a provisioning template).
func TestBatch3_ProvisioningClaim(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForBatch3Test(t)

	// Create provisioning template first
	iotOK(t, h, http.MethodPost, "/provisioning-templates", map[string]any{
		"templateName":        "my-template",
		"templateBody":        `{"Parameters":{},"Resources":{}}`,
		"provisioningRoleArn": "arn:aws:iam::000000000000:role/ProvisioningRole",
	})

	// Create provisioning claim
	out := iotOK(t, h, http.MethodPost, "/provisioning-templates/my-template/provisioning-claim", nil)
	if out["certificatePem"] == "" {
		t.Errorf("expected certificatePem, got %v", out)
	}
	kp, _ := out["keyPair"].(map[string]any)
	if kp == nil {
		t.Errorf("expected keyPair, got %v", out)
	}
}

// TestBatch3_AuditFinding tests ListAuditFindings (empty list since we have no direct create endpoint).
func TestBatch3_AuditFinding(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForBatch3Test(t)

	// List (empty)
	out := iotOK(t, h, http.MethodGet, "/audit/findings", nil)
	findings, _ := out["findings"].([]any)
	if findings == nil {
		t.Errorf("expected findings array, got %v", out)
	}
}
