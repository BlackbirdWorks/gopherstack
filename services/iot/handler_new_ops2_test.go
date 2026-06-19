package iot_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

func newHandlerForNewOps2Test(t *testing.T) *iot.Handler {
	t.Helper()
	b := iot.NewInMemoryBackend()

	return iot.NewHandler(b, nil)
}

// TestNewOps2_DetectMitigationActions tests the detect mitigation action ops.
func TestNewOps2_DetectMitigationActions(t *testing.T) {
	t.Parallel()
	h := newHandlerForNewOps2Test(t)

	tests := []struct {
		body   map[string]any
		method string
		path   string
	}{
		{
			method: http.MethodPost,
			path:   "/detect/mitigationactions/tasks",
			body:   map[string]any{"target": map[string]any{"violationIds": []string{"v1"}}},
		},
		{method: http.MethodGet, path: "/detect/mitigationactions/tasks"},
		{method: http.MethodGet, path: "/detect/mitigationactions/tasks/task-123"},
		{method: http.MethodPut, path: "/detect/mitigationactions/tasks/task-123/cancel"},
		{method: http.MethodGet, path: "/detect/mitigationactions/executions"},
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
	h := newHandlerForNewOps2Test(t)

	tests := []struct {
		body   map[string]any
		method string
		path   string
	}{
		{
			method: http.MethodPost,
			path:   "/audit/mitigationactions/tasks",
			body:   map[string]any{"target": map[string]any{"auditTaskId": "t1"}},
		},
		{method: http.MethodGet, path: "/audit/mitigationactions/tasks"},
		{method: http.MethodGet, path: "/audit/mitigationactions/tasks/task-123"},
		{method: http.MethodGet, path: "/audit/mitigationactions/executions"},
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
	h := newHandlerForNewOps2Test(t)

	tests := []struct {
		body   map[string]any
		method string
		path   string
	}{
		{method: http.MethodGet, path: "/active-violations"},
		{method: http.MethodGet, path: "/violation-events"},
		{
			method: http.MethodPatch,
			path:   "/violations/verification-state/v-abc",
			body:   map[string]any{"verificationState": "TRUE_POSITIVE"},
		},
		{method: http.MethodGet, path: "/behavior-model-training/summaries"},
		{
			method: http.MethodPost,
			path:   "/security-profile-behaviors/validate",
			body:   map[string]any{"behaviors": []any{}},
		},
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
	h := newHandlerForNewOps2Test(t)

	tests := []struct {
		body   map[string]any
		check  func(t *testing.T, out map[string]any)
		method string
		path   string
	}{
		{method: http.MethodGet, path: "/indexing/config"},
		{
			method: http.MethodPost,
			path:   "/indexing/config",
			body:   map[string]any{"thingIndexingConfiguration": map[string]any{"thingIndexingMode": "OFF"}},
		},
		{
			method: http.MethodGet,
			path:   "/indices",
			check: func(t *testing.T, out map[string]any) {
				t.Helper()
				names, _ := out["indexNames"].([]any)
				if len(names) == 0 {
					t.Error("expected at least one index name")
				}
			},
		},
		{
			method: http.MethodGet,
			path:   "/indices/AWS_Things",
			check: func(t *testing.T, out map[string]any) {
				t.Helper()
				if out["indexName"] != "AWS_Things" {
					t.Errorf("expected indexName=AWS_Things, got %v", out["indexName"])
				}
			},
		},
		{
			method: http.MethodPost,
			path:   "/indices/search",
			body:   map[string]any{"queryString": "thingName:foo"},
		},
		{
			method: http.MethodPost,
			path:   "/indices/cardinality",
			body:   map[string]any{"queryString": "*"},
			check: func(t *testing.T, out map[string]any) {
				t.Helper()
				if _, ok := out["cardinality"]; !ok {
					t.Error("missing cardinality field")
				}
			},
		},
		{
			method: http.MethodPost,
			path:   "/indices/statistics",
			body:   map[string]any{"queryString": "*", "aggregationField": "connectivity.connected"},
			check: func(t *testing.T, out map[string]any) {
				t.Helper()
				if _, ok := out["statistics"]; !ok {
					t.Error("missing statistics field")
				}
			},
		},
		{method: http.MethodPost, path: "/indices/percentiles", body: map[string]any{"queryString": "*"}},
		{
			method: http.MethodPost,
			path:   "/indices/buckets-aggregation",
			body:   map[string]any{"queryString": "*", "aggregationField": "shadow.hasDelta"},
		},
		{method: http.MethodGet, path: "/metric-values"},
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
		body   map[string]any
		name   string
		method string
		path   string
	}{
		{name: "ListOutgoing", method: http.MethodGet, path: "/certificates-out-going"},
		{name: "DescribeEncryption", method: http.MethodGet, path: "/encryption-configuration"},
		{
			name: "UpdateEncryption", method: http.MethodPatch,
			path: "/encryption-configuration", body: map[string]any{"keyType": "NONE"},
		},
		{
			name: "TestAuth", method: http.MethodPost,
			path: "/test-authorization", body: map[string]any{"authInfos": []any{}},
		},
		{name: "ConfirmDestination", method: http.MethodGet, path: "/confirmdestination/some-token"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newHandlerForNewOps2Test(t)
			iotOK(t, h, tc.method, tc.path, tc.body)
		})
	}
}

// TestNewOps2_TestInvokeAuthorizer tests TestInvokeAuthorizer.
func TestNewOps2_TestInvokeAuthorizer(t *testing.T) {
	t.Parallel()
	h := newHandlerForNewOps2Test(t)

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
	h := newHandlerForNewOps2Test(t)

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
	h := newHandlerForNewOps2Test(t)

	iotOK(t, h, http.MethodPost, "/things/my-thing", nil)

	out := iotOK(t, h, http.MethodGet, "/things/my-thing/principals/v2", nil)
	if _, ok := out["principals"]; !ok {
		t.Error("missing principals field")
	}
}

// TestNewOps2_ThingConnectivityData tests GetThingConnectivityData.
func TestNewOps2_ThingConnectivityData(t *testing.T) {
	t.Parallel()
	h := newHandlerForNewOps2Test(t)

	iotOK(t, h, http.MethodPost, "/things/my-thing", nil)
	iotOK(t, h, http.MethodGet, "/things/my-thing/connectivity-data", nil)
}

// TestNewOps2_UpdateThingGroupsForThing tests UpdateThingGroupsForThing.
func TestNewOps2_UpdateThingGroupsForThing(t *testing.T) {
	t.Parallel()
	h := newHandlerForNewOps2Test(t)

	iotOK(t, h, http.MethodPost, "/things/my-thing", nil)
	iotOK(t, h, http.MethodPost, "/thing-groups/my-group", nil)

	iotOK(t, h, http.MethodPut, "/thing-groups/updateThingGroupsForThing", map[string]any{
		"thingName":           "my-thing",
		"thingGroupsToAdd":    []string{"my-group"},
		"thingGroupsToRemove": []string{},
	})

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
	h := newHandlerForNewOps2Test(t)

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
	h := newHandlerForNewOps2Test(t)

	tests := []struct {
		body   map[string]any
		name   string
		method string
		path   string
	}{
		{
			name:   "Start",
			method: http.MethodPost,
			path:   "/thing-registration-tasks",
			body: map[string]any{
				"templateBody": `{}`, "inputFileBucket": "bucket",
				"inputFileKey": "key", "roleArn": "arn:aws:iam::000:role/r",
			},
		},
		{name: "List", method: http.MethodGet, path: "/thing-registration-tasks"},
		{name: "Reports", method: http.MethodGet, path: "/thing-registration-tasks/task-123/reports"},
		{name: "Stop", method: http.MethodPut, path: "/thing-registration-tasks/task-123"},
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
	h := newHandlerForNewOps2Test(t)

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
	h := newHandlerForNewOps2Test(t)

	iotOK(t, h, http.MethodDelete, "/audit/configuration", nil)
}

// TestNewOps2_ListRelatedResourcesForAuditFinding tests ListRelatedResourcesForAuditFinding.
func TestNewOps2_ListRelatedResourcesForAuditFinding(t *testing.T) {
	t.Parallel()
	h := newHandlerForNewOps2Test(t)

	out := iotOK(t, h, http.MethodGet, "/audit/relatedResources", nil)
	if _, ok := out["relatedResources"]; !ok {
		t.Error("missing relatedResources")
	}
}

// TestNewOps2_SbomOps tests SBOM-related ops.
func TestNewOps2_SbomOps(t *testing.T) {
	t.Parallel()
	h := newHandlerForNewOps2Test(t)

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
	h := newHandlerForNewOps2Test(t)

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
	h := newHandlerForNewOps2Test(t)

	rec := iotRequest(t, h, http.MethodDelete, "/commands/cmd-1/executions/exec-1", nil)
	if rec.Code == http.StatusOK {
		t.Errorf("expected error for unknown execution, got 200")
	}
}

// TestNewOps2_DescribeProvisioningTemplateVersion tests DescribeProvisioningTemplateVersion.
func TestNewOps2_DescribeProvisioningTemplateVersion(t *testing.T) {
	t.Parallel()
	h := newHandlerForNewOps2Test(t)

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

	out := iotOK(
		t, h, http.MethodGet,
		fmt.Sprintf("/provisioning-templates/my-template/versions/%s", versionID),
		nil,
	)
	if out["templateName"] != "my-template" {
		t.Errorf("expected templateName=my-template, got %v", out["templateName"])
	}
}
