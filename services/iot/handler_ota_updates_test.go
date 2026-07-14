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
