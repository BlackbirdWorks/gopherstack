package iot_test

import (
	"net/http"
	"testing"
)

// TestBatch3_V2Logging tests V2 logging options and levels.
func TestV2Logging(t *testing.T) {
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
func TestLoggingOptions(t *testing.T) {
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
