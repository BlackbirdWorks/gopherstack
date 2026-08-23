package iot_test

import (
	"net/http"
	"testing"
)

// TestBatch3_OTAUpdateCRUD tests OTAUpdate create/get/list/delete.
func TestOTAUpdateCRUD(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForBatch3Test(t)

	// Create
	out := iotOK(t, h, http.MethodPost, "/otaUpdates/my-ota", map[string]any{
		"targets":     []string{"arn:aws:iot:us-east-1:000000000000:thing/device1"},
		"roleArn":     "arn:aws:iam::000000000000:role/IoTRole",
		"description": "test OTA",
		"otaUpdateFiles": []any{
			map[string]any{"fileName": "fw.bin"},
		},
	})
	if out["otaUpdateId"] != "my-ota" {
		t.Errorf("expected otaUpdateId=my-ota, got %v", out)
	}
	if out["awsIotJobId"] == "" || out["awsIotJobId"] == nil {
		t.Errorf("expected awsIotJobId, got %v", out)
	}
	if out["awsIotJobArn"] == "" || out["awsIotJobArn"] == nil {
		t.Errorf("expected awsIotJobArn, got %v", out)
	}

	// Get
	out2 := iotOK(t, h, http.MethodGet, "/otaUpdates/my-ota", nil)
	info := out2["otaUpdateInfo"].(map[string]any)
	if info["otaUpdateId"] != "my-ota" {
		t.Errorf("get mismatch: %v", info)
	}
	if info["awsIotJobId"] == nil {
		t.Errorf("expected awsIotJobId on otaUpdateInfo, got %v", info)
	}
	files, _ := info["otaUpdateFiles"].([]any)
	if len(files) != 1 {
		t.Errorf("expected 1 otaUpdateFiles entry, got %v", info)
	}

	// List
	out3 := iotOK(t, h, http.MethodGet, "/otaUpdates", nil)
	updates, _ := out3["otaUpdates"].([]any)
	if len(updates) != 1 {
		t.Errorf("expected 1 OTA update, got %d", len(updates))
	}
	first, _ := updates[0].(map[string]any)
	if first["creationDate"] == nil {
		t.Errorf("expected creationDate on ListOTAUpdates entry, got %v", first)
	}

	// Delete
	iotOK(t, h, http.MethodDelete, "/otaUpdates/my-ota", nil)
	iotExpectError(t, h, "/otaUpdates/my-ota")
}
