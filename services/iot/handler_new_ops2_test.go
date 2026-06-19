package iot_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

func newHandlerForNewOps2Test(t *testing.T) (*iot.Handler, *iot.InMemoryBackend) {
	t.Helper()
	b := iot.NewInMemoryBackend()
	h := iot.NewHandler(b, nil)

	return h, b
}

// TestNewOps2_DetectMitigationActions tests the detect mitigation action ops.
func TestNewOps2_DetectMitigationActions(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForNewOps2Test(t)

	tests := []struct {
		method string
		path   string
		body   map[string]any
	}{
		{http.MethodPost, "/detect/mitigationactions/tasks", map[string]any{
			"target": map[string]any{"violationIds": []string{"v1"}},
		}},
		{http.MethodGet, "/detect/mitigationactions/tasks", nil},
		{http.MethodGet, "/detect/mitigationactions/tasks/task-123", nil},
		{http.MethodPut, "/detect/mitigationactions/tasks/task-123/cancel", nil},
		{http.MethodGet, "/detect/mitigationactions/executions", nil},
	}

	for _, tc := range tests {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			t.Parallel()
			iotOK(t, h, tc.method, tc.path, tc.body)
		})
	}
}

// TestNewOps2_AuditMitigationActions tests audit mitigation action ops.
func TestNewOps2_AuditMitigationActions(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForNewOps2Test(t)

	tests := []struct {
		method string
		path   string
		body   map[string]any
	}{
		{http.MethodPost, "/audit/mitigationactions/tasks", map[string]any{
			"target": map[string]any{"auditTaskId": "t1"},
		}},
		{http.MethodGet, "/audit/mitigationactions/tasks", nil},
		{http.MethodGet, "/audit/mitigationactions/tasks/task-123", nil},
		{http.MethodGet, "/audit/mitigationactions/executions", nil},
	}

	for _, tc := range tests {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			t.Parallel()
			iotOK(t, h, tc.method, tc.path, tc.body)
		})
	}
}

// TestNewOps2_ViolationsAndBehavior tests violation and behavior ops.
func TestNewOps2_ViolationsAndBehavior(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForNewOps2Test(t)

	tests := []struct {
		method string
		path   string
		body   map[string]any
	}{
		{http.MethodGet, "/active-violations", nil},
		{http.MethodGet, "/violation-events", nil},
		{http.MethodPatch, "/violations/verification-state/v-abc", map[string]any{
			"verificationState": "TRUE_POSITIVE",
		}},
		{http.MethodGet, "/behavior-model-training/summaries", nil},
		{http.MethodPost, "/security-profile-behaviors/validate", map[string]any{
			"behaviors": []any{},
		}},
	}

	for _, tc := range tests {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			t.Parallel()
			iotOK(t, h, tc.method, tc.path, tc.body)
		})
	}
}

// TestNewOps2_Indexing tests indexing configuration and search ops.
func TestNewOps2_Indexing(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForNewOps2Test(t)

	tests := []struct {
		method string
		path   string
		body   map[string]any
		check  func(t *testing.T, out map[string]any)
	}{
		{http.MethodGet, "/indexing/config", nil, nil},
		{http.MethodPost, "/indexing/config", map[string]any{
			"thingIndexingConfiguration": map[string]any{"thingIndexingMode": "OFF"},
		}, nil},
		{http.MethodGet, "/indices", nil, func(t *testing.T, out map[string]any) {
			t.Helper()
			names, _ := out["indexNames"].([]any)
			if len(names) == 0 {
				t.Error("expected at least one index name")
			}
		}},
		{http.MethodGet, "/indices/AWS_Things", nil, func(t *testing.T, out map[string]any) {
			t.Helper()
			if out["indexName"] != "AWS_Things" {
				t.Errorf("expected indexName=AWS_Things, got %v", out["indexName"])
			}
		}},
		{http.MethodPost, "/indices/search", map[string]any{"queryString": "thingName:foo"}, nil},
		{http.MethodPost, "/indices/cardinality", map[string]any{"queryString": "*"}, func(t *testing.T, out map[string]any) {
			t.Helper()
			if _, ok := out["cardinality"]; !ok {
				t.Error("missing cardinality field")
			}
		}},
		{http.MethodPost, "/indices/statistics", map[string]any{"queryString": "*", "aggregationField": "connectivity.connected"}, func(t *testing.T, out map[string]any) {
			t.Helper()
			if _, ok := out["statistics"]; !ok {
				t.Error("missing statistics field")
			}
		}},
		{http.MethodPost, "/indices/percentiles", map[string]any{"queryString": "*"}, nil},
		{http.MethodPost, "/indices/buckets-aggregation", map[string]any{"queryString": "*", "aggregationField": "shadow.hasDelta"}, nil},
		{http.MethodGet, "/metric-values", nil, nil},
	}

	for _, tc := range tests {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			t.Parallel()
			out := iotOK(t, h, tc.method, tc.path, tc.body)
			if tc.check != nil {
				tc.check(t, out)
			}
		})
	}
}

// TestNewOps2_CertificatesAndAuth tests certificates / auth ops.
func TestNewOps2_CertificatesAndAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		body   map[string]any
	}{
		{"ListOutgoing", http.MethodGet, "/certificates-out-going", nil},
		{"DescribeEncryption", http.MethodGet, "/encryption-configuration", nil},
		{"UpdateEncryption", http.MethodPatch, "/encryption-configuration", map[string]any{"keyType": "NONE"}},
		{"TestAuth", http.MethodPost, "/test-authorization", map[string]any{
			"authInfos": []any{},
		}},
		{"ConfirmDestination", http.MethodGet, "/confirmdestination/some-token", nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newHandlerForNewOps2Test(t)
			iotOK(t, h, tc.method, tc.path, tc.body)
		})
	}
}

// TestNewOps2_TestInvokeAuthorizer tests TestInvokeAuthorizer.
func TestNewOps2_TestInvokeAuthorizer(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForNewOps2Test(t)

	out := iotOK(t, h, http.MethodPost, "/authorizer/my-auth/test", map[string]any{
		"token": "some-token",
	})
	if _, ok := out["isAuthenticated"]; !ok {
		t.Error("missing isAuthenticated")
	}
}

// TestNewOps2_DetachPrincipalPolicy tests DetachPrincipalPolicy.
func TestNewOps2_DetachPrincipalPolicy(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForNewOps2Test(t)

	// Create policy and attach via the legacy /target-policies path (AttachPrincipalPolicy).
	iotOK(t, h, http.MethodPost, "/policies/my-policy", map[string]any{
		"policyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"iot:*","Resource":"*"}]}`,
	})
	principal := "arn:aws:iot:us-east-1:000000000000:cert/abc123"
	iotOK(t, h, http.MethodPost, "/target-policies/my-policy", map[string]any{
		"principal": principal,
	})

	// Detach via /principal-policies/{name}?principal=xxx (DetachPrincipalPolicy legacy path).
	iotOK(t, h, http.MethodDelete, fmt.Sprintf("/principal-policies/my-policy?principal=%s", principal), nil)
}

// TestNewOps2_ThingPrincipalsV2 tests ListThingPrincipalsV2.
func TestNewOps2_ThingPrincipalsV2(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForNewOps2Test(t)

	iotOK(t, h, http.MethodPost, "/things/my-thing", nil)

	out := iotOK(t, h, http.MethodGet, "/things/my-thing/principals/v2", nil)
	if _, ok := out["principals"]; !ok {
		t.Error("missing principals field")
	}
}

// TestNewOps2_ThingConnectivityData tests GetThingConnectivityData.
func TestNewOps2_ThingConnectivityData(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForNewOps2Test(t)

	iotOK(t, h, http.MethodPost, "/things/my-thing", nil)
	iotOK(t, h, http.MethodGet, "/things/my-thing/connectivity-data", nil)
}

// TestNewOps2_UpdateThingGroupsForThing tests UpdateThingGroupsForThing.
func TestNewOps2_UpdateThingGroupsForThing(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForNewOps2Test(t)

	iotOK(t, h, http.MethodPost, "/things/my-thing", nil)
	iotOK(t, h, http.MethodPost, "/thing-groups/my-group", nil)

	out := iotOK(t, h, http.MethodPut, "/thing-groups/updateThingGroupsForThing", map[string]any{
		"thingName":           "my-thing",
		"thingGroupsToAdd":    []string{"my-group"},
		"thingGroupsToRemove": []string{},
	})
	_ = out

	// Verify thing is in group.
	members := iotOK(t, h, http.MethodGet, "/thing-groups/my-group/things", nil)
	things, _ := members["things"].([]any)
	found := false
	for _, th := range things {
		if th == "my-thing" {
			found = true
		}
	}
	if !found {
		t.Error("thing not found in group after UpdateThingGroupsForThing")
	}

	// Remove.
	iotOK(t, h, http.MethodPut, "/thing-groups/updateThingGroupsForThing", map[string]any{
		"thingName":           "my-thing",
		"thingGroupsToAdd":    []string{},
		"thingGroupsToRemove": []string{"my-group"},
	})

	// Verify removed.
	members2 := iotOK(t, h, http.MethodGet, "/thing-groups/my-group/things", nil)
	things2, _ := members2["things"].([]any)
	for _, th := range things2 {
		if th == "my-thing" {
			t.Error("thing still in group after remove")
		}
	}
}

// TestNewOps2_RegisterThing tests RegisterThing.
func TestNewOps2_RegisterThing(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForNewOps2Test(t)

	out := iotOK(t, h, http.MethodPost, "/things/register", map[string]any{
		"templateName": "my-template",
		"parameters": map[string]any{
			"ThingName": "registered-thing",
		},
	})
	if out["thingName"] != "registered-thing" {
		t.Errorf("expected thingName=registered-thing, got %v", out["thingName"])
	}
}

// TestNewOps2_ThingRegistrationTasks tests ThingRegistrationTask ops.
func TestNewOps2_ThingRegistrationTasks(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForNewOps2Test(t)

	tests := []struct {
		name   string
		method string
		path   string
		body   map[string]any
	}{
		{"Start", http.MethodPost, "/thing-registration-tasks", map[string]any{
			"templateBody": `{}`, "inputFileBucket": "bucket", "inputFileKey": "key", "roleArn": "arn:aws:iam::000:role/r",
		}},
		{"List", http.MethodGet, "/thing-registration-tasks", nil},
		{"Reports", http.MethodGet, "/thing-registration-tasks/task-123/reports", nil},
		{"Stop", http.MethodPut, "/thing-registration-tasks/task-123", nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			iotOK(t, h, tc.method, tc.path, tc.body)
		})
	}

	// DescribeThingRegistrationTask returns 404 (no real state backing tasks).
	t.Run("Describe", func(t *testing.T) {
		t.Parallel()
		rec := iotRequest(t, h, http.MethodGet, "/thing-registration-tasks/task-123", nil)
		if rec.Code == http.StatusOK {
			t.Errorf("expected non-200 for describe, got %d", rec.Code)
		}
	})
}

// TestNewOps2_ManagedJobTemplates tests ManagedJobTemplate ops.
func TestNewOps2_ManagedJobTemplates(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForNewOps2Test(t)

	out := iotOK(t, h, http.MethodGet, "/managed-job-templates", nil)
	if _, ok := out["managedJobTemplates"]; !ok {
		t.Error("missing managedJobTemplates")
	}

	rec := iotRequest(t, h, http.MethodGet, "/managed-job-templates/no-such", nil)
	if rec.Code == http.StatusOK {
		t.Errorf("expected 404 for unknown managed template, got %d", rec.Code)
	}
}

// TestNewOps2_DeleteAccountAuditConfiguration tests DeleteAccountAuditConfiguration.
func TestNewOps2_DeleteAccountAuditConfiguration(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForNewOps2Test(t)

	iotOK(t, h, http.MethodDelete, "/audit/configuration", nil)
}

// TestNewOps2_ListRelatedResourcesForAuditFinding tests ListRelatedResourcesForAuditFinding.
func TestNewOps2_ListRelatedResourcesForAuditFinding(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForNewOps2Test(t)

	out := iotOK(t, h, http.MethodGet, "/audit/relatedResources", nil)
	if _, ok := out["relatedResources"]; !ok {
		t.Error("missing relatedResources")
	}
}

// TestNewOps2_SbomOps tests SBOM-related ops.
func TestNewOps2_SbomOps(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForNewOps2Test(t)

	// Create package and version first.
	iotOK(t, h, http.MethodPut, "/packages/my-pkg", map[string]any{"description": "p"})
	iotOK(t, h, http.MethodPut, "/packages/my-pkg/versions/1.0.0", map[string]any{"description": "v"})

	// Associate SBOM.
	iotOK(t, h, http.MethodPut, "/packages/my-pkg/versions/1.0.0/sbom", map[string]any{
		"s3Version": "1", "s3Bucket": "b", "s3Key": "k",
	})

	// List validation results.
	out := iotOK(t, h, http.MethodGet, "/packages/my-pkg/versions/1.0.0/sbom-validation-results", nil)
	if _, ok := out["validationResultSummaries"]; !ok {
		t.Error("missing validationResultSummaries")
	}

	// Disassociate.
	iotOK(t, h, http.MethodDelete, "/packages/my-pkg/versions/1.0.0/sbom", nil)
}

// TestNewOps2_UpdateThingType tests UpdateThingType.
func TestNewOps2_UpdateThingType(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForNewOps2Test(t)

	iotOK(t, h, http.MethodPost, "/thing-types/my-type", map[string]any{
		"thingTypeProperties": map[string]any{"thingTypeDescription": "original"},
	})
	iotOK(t, h, http.MethodPatch, "/thing-types/my-type", map[string]any{
		"thingTypeProperties": map[string]any{"thingTypeDescription": "updated"},
	})

	// Verify description updated.
	out := iotOK(t, h, http.MethodGet, "/thing-types/my-type", nil)
	props, _ := out["thingTypeProperties"].(map[string]any)
	if props["thingTypeDescription"] != "updated" {
		t.Errorf("expected description=updated, got %v", props["thingTypeDescription"])
	}
}

// TestNewOps2_DeleteCommandExecution tests DeleteCommandExecution returns 404 for unknown execution.
// Command executions are created by devices (not the control plane), so we test the not-found path.
func TestNewOps2_DeleteCommandExecution(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForNewOps2Test(t)

	rec := iotRequest(t, h, http.MethodDelete, "/commands/cmd-1/executions/exec-1", nil)
	if rec.Code == http.StatusOK {
		t.Errorf("expected error for unknown execution, got 200")
	}
}

// TestNewOps2_DescribeProvisioningTemplateVersion tests DescribeProvisioningTemplateVersion.
func TestNewOps2_DescribeProvisioningTemplateVersion(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForNewOps2Test(t)

	// Create template and a version.
	iotOK(t, h, http.MethodPost, "/provisioning-templates", map[string]any{
		"templateName":        "my-template",
		"templateBody":        `{"Parameters":{},"Resources":{}}`,
		"enabled":             true,
		"provisioningRoleArn": "arn:aws:iam::000:role/r",
	})
	iotOK(t, h, http.MethodPost, "/provisioning-templates/my-template/versions", map[string]any{
		"templateBody": `{"Parameters":{},"Resources":{"thing":{}}}`,
	})

	// List versions to find the version ID.
	versOut := iotOK(t, h, http.MethodGet, "/provisioning-templates/my-template/versions", nil)
	versions, _ := versOut["versions"].([]any)
	if len(versions) == 0 {
		t.Fatal("no versions found")
	}

	firstVer, _ := versions[0].(map[string]any)
	versionID := fmt.Sprintf("%.0f", firstVer["versionId"])

	out := iotOK(t, h, http.MethodGet, fmt.Sprintf("/provisioning-templates/my-template/versions/%s", versionID), nil)
	if out["templateName"] != "my-template" {
		t.Errorf("expected templateName=my-template, got %v", out["templateName"])
	}
}
