package iot_test

import (
	"net/http"
	"testing"
)

// TestBatch2_Stream tests stream lifecycle.
func TestStream(t *testing.T) {
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
	first, _ := streams[0].(map[string]any)
	if first["description"] != "test stream" {
		t.Errorf("expected description on ListStreams entry, got %v", first)
	}
	if first["streamVersion"] == nil {
		t.Errorf("expected streamVersion on ListStreams entry, got %v", first)
	}

	// Update
	updateOut := iotOK(t, h, http.MethodPut, "/streams/my-stream", map[string]any{
		"description": "updated",
	})
	if updateOut["description"] != "updated" {
		t.Errorf("expected description on UpdateStream response, got %v", updateOut)
	}

	// Delete
	iotOK(t, h, http.MethodDelete, "/streams/my-stream", nil)

	iotExpectError(t, h, "/streams/my-stream")
}
