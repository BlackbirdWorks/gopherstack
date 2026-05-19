package iot_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

// TestBatch2_CACertificate tests CA certificate lifecycle.
func TestBatch2_CACertificate(t *testing.T) {
	t.Parallel()
	h := newIoTHandler(t)

	// Register
	out := iotOK(t, h, http.MethodPost, "/cacertificate/register", map[string]any{
		"caCertificate": "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----",
	})
	certID, _ := out["certificateId"].(string)
	if certID == "" {
		t.Fatalf("expected certificateId: %v", out)
	}

	// Describe
	out2 := iotOK(t, h, http.MethodGet, "/cacertificates/"+certID, nil)
	desc := out2["certificateDescription"].(map[string]any)
	if desc["certificateId"] != certID {
		t.Errorf("describe mismatch: %v", desc)
	}

	// List
	out3 := iotOK(t, h, http.MethodGet, "/cacertificates", nil)
	certs, _ := out3["certificates"].([]any)
	if len(certs) != 1 {
		t.Errorf("expected 1 CA cert, got %d", len(certs))
	}

	// Update
	iotOK(t, h, http.MethodPut, "/cacertificates/"+certID, map[string]any{
		"newStatus": "INACTIVE",
	})

	// Delete
	iotOK(t, h, http.MethodDelete, "/cacertificates/"+certID, nil)

	iotExpectError(t, h, "/cacertificates/"+certID)
}

// TestBatch2_Stream tests stream lifecycle.
func TestBatch2_Stream(t *testing.T) {
	t.Parallel()
	h := newIoTHandler(t)

	// Create
	out := iotOK(t, h, http.MethodPost, "/streams/my-stream", map[string]any{
		"roleArn":     "arn:aws:iam::000000000000:role/IoTRole",
		"description": "test stream",
		"files": []any{
			map[string]any{"fileId": 0, "s3Bucket": "my-bucket", "s3Key": "firmware.bin"},
		},
	})
	if out["streamId"] != "my-stream" {
		t.Errorf("streamId mismatch: %v", out)
	}

	// Describe
	out2 := iotOK(t, h, http.MethodGet, "/streams/my-stream", nil)
	info := out2["streamInfo"].(map[string]any)
	if info["streamId"] != "my-stream" {
		t.Errorf("describe mismatch: %v", info)
	}

	// List
	out3 := iotOK(t, h, http.MethodGet, "/streams", nil)
	streams, _ := out3["streams"].([]any)
	if len(streams) != 1 {
		t.Errorf("expected 1 stream, got %d", len(streams))
	}

	// Update
	iotOK(t, h, http.MethodPut, "/streams/my-stream", map[string]any{
		"description": "updated",
	})

	// Delete
	iotOK(t, h, http.MethodDelete, "/streams/my-stream", nil)

	iotExpectError(t, h, "/streams/my-stream")
}

// TestBatch2_FleetMetric tests fleet metric lifecycle.
func TestBatch2_FleetMetric(t *testing.T) {
	t.Parallel()
	h := newIoTHandler(t)

	// Create
	out := iotOK(t, h, http.MethodPut, "/fleet-metric/my-metric", map[string]any{
		"queryString": "SELECT * FROM 'iot/+/data'",
		"period":      300,
	})
	if out["metricName"] != "my-metric" {
		t.Errorf("metricName mismatch: %v", out)
	}

	// Describe
	out2 := iotOK(t, h, http.MethodGet, "/fleet-metric/my-metric", nil)
	if out2["metricName"] != "my-metric" {
		t.Errorf("describe mismatch: %v", out2)
	}

	// List
	out3 := iotOK(t, h, http.MethodGet, "/fleet-metrics", nil)
	metrics, _ := out3["fleetMetrics"].([]any)
	if len(metrics) != 1 {
		t.Errorf("expected 1 metric, got %d", len(metrics))
	}

	// Update
	iotOK(t, h, http.MethodPatch, "/fleet-metric/my-metric", map[string]any{
		"description": "updated",
	})

	// Delete
	iotOK(t, h, http.MethodDelete, "/fleet-metric/my-metric", nil)

	iotExpectError(t, h, "/fleet-metric/my-metric")
}

// TestBatch2_CustomMetric tests custom metric lifecycle.
func TestBatch2_CustomMetric(t *testing.T) {
	t.Parallel()
	h := newIoTHandler(t)

	// Create
	out := iotOK(t, h, http.MethodPost, "/custom-metric/my-custom-metric", map[string]any{
		"metricType":  "number",
		"displayName": "My Metric",
	})
	if out["metricName"] != "my-custom-metric" {
		t.Errorf("metricName mismatch: %v", out)
	}

	// Describe
	out2 := iotOK(t, h, http.MethodGet, "/custom-metric/my-custom-metric", nil)
	if out2["metricName"] != "my-custom-metric" {
		t.Errorf("describe mismatch: %v", out2)
	}

	// List
	out3 := iotOK(t, h, http.MethodGet, "/custom-metric", nil)
	names, _ := out3["metricNames"].([]any)
	if len(names) != 1 {
		t.Errorf("expected 1 custom metric, got %d", len(names))
	}

	// Update
	iotOK(t, h, http.MethodPatch, "/custom-metric/my-custom-metric", map[string]any{
		"displayName": "Updated",
	})

	// Delete
	iotOK(t, h, http.MethodDelete, "/custom-metric/my-custom-metric", nil)

	iotExpectError(t, h, "/custom-metric/my-custom-metric")
}

// TestBatch2_Dimension tests dimension lifecycle.
func TestBatch2_Dimension(t *testing.T) {
	t.Parallel()
	h := newIoTHandler(t)

	// Create
	out := iotOK(t, h, http.MethodPost, "/dimensions/my-dim", map[string]any{
		"type":         "TOPIC_FILTER",
		"stringValues": []any{"iot/sensors/+"},
	})
	if out["name"] != "my-dim" {
		t.Errorf("name mismatch: %v", out)
	}

	// Describe
	out2 := iotOK(t, h, http.MethodGet, "/dimensions/my-dim", nil)
	if out2["name"] != "my-dim" {
		t.Errorf("describe mismatch: %v", out2)
	}

	// List
	out3 := iotOK(t, h, http.MethodGet, "/dimensions", nil)
	names, _ := out3["dimensionNames"].([]any)
	if len(names) != 1 {
		t.Errorf("expected 1 dimension, got %d", len(names))
	}

	// Update
	iotOK(t, h, http.MethodPatch, "/dimensions/my-dim", map[string]any{
		"stringValues": []any{"iot/updated/+"},
	})

	// Delete
	iotOK(t, h, http.MethodDelete, "/dimensions/my-dim", nil)

	iotExpectError(t, h, "/dimensions/my-dim")
}

// TestBatch2_Tags tests tag operations.
func TestBatch2_Tags(t *testing.T) {
	t.Parallel()
	h := newIoTHandler(t)

	arn := "arn:aws:iot:us-east-1:000000000000:thing/my-thing"

	// Tag
	iotOK(t, h, http.MethodPost, "/tags", map[string]any{
		"resourceArn": arn,
		"tags":        map[string]any{"env": "test", "owner": "alice"},
	})

	// List
	out := iotOK(t, h, http.MethodGet, "/tags?resourceArn="+arn, nil)
	tags, _ := out["tags"].([]any)
	if len(tags) != 2 {
		t.Errorf("expected 2 tags, got %d", len(tags))
	}

	// Untag
	iotOK(t, h, http.MethodDelete, "/tags?resourceArn="+arn+"&tagKeys=env", nil)

	// Verify
	out2 := iotOK(t, h, http.MethodGet, "/tags?resourceArn="+arn, nil)
	tags2, _ := out2["tags"].([]any)
	if len(tags2) != 1 {
		t.Errorf("expected 1 tag after untag, got %d", len(tags2))
	}
}

// TestBatch2_AuditConfiguration tests audit configuration.
func TestBatch2_AuditConfiguration(t *testing.T) {
	t.Parallel()
	h := newIoTHandler(t)

	// Describe empty
	out := iotOK(t, h, http.MethodGet, "/audit/configuration", nil)
	if out == nil {
		t.Error("expected audit configuration response")
	}

	// Update
	iotOK(t, h, http.MethodPatch, "/audit/configuration", map[string]any{
		"roleArn": "arn:aws:iam::000000000000:role/AuditRole",
		"auditCheckConfigurations": map[string]any{
			"DEVICE_CERTIFICATE_EXPIRING_CHECK": map[string]any{"enabled": true},
		},
	})

	// Start on-demand task
	out2 := iotOK(t, h, http.MethodPost, "/audit/tasks", map[string]any{
		"targetCheckNames": []any{"DEVICE_CERTIFICATE_EXPIRING_CHECK"},
	})
	taskID, _ := out2["taskId"].(string)
	if taskID == "" {
		t.Errorf("expected taskId: %v", out2)
	}

	// Describe task
	out3 := iotOK(t, h, http.MethodGet, "/audit/tasks/"+taskID, nil)
	if out3["taskId"] != taskID {
		t.Errorf("task describe mismatch: %v", out3)
	}

	// List tasks
	out4 := iotOK(t, h, http.MethodGet, "/audit/tasks", nil)
	tasks, _ := out4["tasks"].([]any)
	if len(tasks) != 1 {
		t.Errorf("expected 1 task, got %d", len(tasks))
	}
}

// TestBatch2_PolicyPrincipalOps tests policy/principal listing.
func TestBatch2_PolicyPrincipalOps(t *testing.T) {
	t.Parallel()
	b := iot.NewInMemoryBackend()
	h := iot.NewHandler(b, nil)

	// Seed: policy + attachment
	b.AddPolicyInternal(iot.Policy{
		PolicyName:     "my-policy",
		PolicyDocument: `{"Version":"2012-10-17"}`,
		ARN:            "arn:aws:iot:us-east-1:000000000000:policy/my-policy",
	})
	_ = b.AttachPolicy(&iot.AttachPolicyInput{
		PolicyName: "my-policy",
		Target:     "arn:aws:iot:us-east-1:000000000000:cert/abc123",
	})

	// ListPolicyPrincipals (via header)
	out := iotOK(t, h, http.MethodGet, "/policy-principals", nil)
	// No header set so principals is empty — just verify response structure
	if out["principals"] == nil {
		t.Error("expected principals key in response")
	}

	// ListTargetsForPolicy
	out2 := iotOK(t, h, http.MethodGet, "/policy-targets/my-policy", nil)
	targets, _ := out2["targets"].([]any)
	if len(targets) != 1 {
		t.Errorf("expected 1 target, got %d", len(targets))
	}
}

// TestBatch2_DefaultAuthorizer tests default authorizer operations.
func TestBatch2_DefaultAuthorizer(t *testing.T) {
	t.Parallel()
	h := newIoTHandler(t)

	// Describe (none set)
	iotExpectError(t, h, "/default-authorizer")

	// Set
	iotOK(t, h, http.MethodPost, "/default-authorizer", map[string]any{
		"authorizerName": "my-auth",
	})

	// Describe
	out := iotOK(t, h, http.MethodGet, "/default-authorizer", nil)
	if out["authorizerDescription"] == nil {
		t.Error("expected authorizerDescription")
	}

	// Clear
	iotOK(t, h, http.MethodDelete, "/default-authorizer", nil)

	// Describe again (none set)
	iotExpectError(t, h, "/default-authorizer")
}

// TestBatch2_JobExecutions tests listing job executions.
func TestBatch2_JobExecutions(t *testing.T) {
	t.Parallel()
	h := newIoTHandler(t)

	// Create a job first
	iotOK(t, h, http.MethodPost, "/jobs/test-job", map[string]any{
		"targets":  []any{"arn:aws:iot:us-east-1:000000000000:thing/my-thing"},
		"document": `{"action":"update"}`,
	})

	// Cancel an execution (creates it)
	iotOK(t, h, http.MethodPut, "/jobs/test-job/things/my-thing/cancel", nil)

	// ListJobExecutionsForJob
	out := iotOK(t, h, http.MethodGet, "/jobs/test-job/things", nil)
	execs, _ := out["executionSummaries"].([]any)
	if len(execs) != 1 {
		t.Errorf("expected 1 execution for job, got %d", len(execs))
	}

	// ListJobExecutionsForThing
	out2 := iotOK(t, h, http.MethodGet, "/things/my-thing/jobs", nil)
	execs2, _ := out2["executionSummaries"].([]any)
	if len(execs2) != 1 {
		t.Errorf("expected 1 execution for thing, got %d", len(execs2))
	}
}

// TestBatch2_RegistrationCode tests registration code ops.
func TestBatch2_RegistrationCode(t *testing.T) {
	t.Parallel()
	h := newIoTHandler(t)

	// Get (generates on first call)
	out := iotOK(t, h, http.MethodGet, "/registrationcode", nil)
	code, _ := out["registrationCode"].(string)
	if code == "" {
		t.Error("expected non-empty registrationCode")
	}

	// Get again (same code)
	out2 := iotOK(t, h, http.MethodGet, "/registrationcode", nil)
	if out2["registrationCode"] != code {
		t.Errorf("code changed between calls: %v != %v", out2["registrationCode"], code)
	}

	// Delete
	iotOK(t, h, http.MethodDelete, "/registrationcode", nil)
}

// TestBatch2_ThingGroupAndBillingOps tests thing-group/billing helpers.
func TestBatch2_ThingGroupAndBillingOps(t *testing.T) {
	t.Parallel()
	b := iot.NewInMemoryBackend()
	h := iot.NewHandler(b, nil)

	// Seed: thing, group, billing group
	_, _ = b.CreateThing(&iot.CreateThingInput{ThingName: "my-thing"})
	_, _ = b.CreateThingGroup(&iot.CreateThingGroupInput{ThingGroupName: "my-group"})
	_ = b.AddThingToThingGroup(&iot.AddThingToThingGroupInput{
		ThingName:      "my-thing",
		ThingGroupName: "my-group",
	})
	_ = b.AddThingToBillingGroup(&iot.AddThingToBillingGroupInput{
		ThingName:        "my-thing",
		BillingGroupName: "my-billing-group",
	})

	// ListThingGroupsForThing
	out := iotOK(t, h, http.MethodGet, "/things/my-thing/thing-groups", nil)
	groups, _ := out["thingGroups"].([]any)
	if len(groups) != 1 {
		t.Errorf("expected 1 group, got %d", len(groups))
	}

	// ListThingsInBillingGroup
	out2 := iotOK(t, h, http.MethodGet, "/billing-groups/my-billing-group/things", nil)
	things, _ := out2["things"].([]any)
	if len(things) != 1 {
		t.Errorf("expected 1 thing in billing group, got %d", len(things))
	}

	// RemoveThingFromBillingGroup
	iotOK(t, h, http.MethodPut, "/billing-groups/removeThingFromBillingGroup", map[string]any{
		"thingName":        "my-thing",
		"billingGroupName": "my-billing-group",
	})

	out3 := iotOK(t, h, http.MethodGet, "/billing-groups/my-billing-group/things", nil)
	things3, _ := out3["things"].([]any)
	if len(things3) != 0 {
		t.Errorf("expected 0 things after removal, got %d", len(things3))
	}
}
