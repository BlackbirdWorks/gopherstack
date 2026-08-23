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

// TestV2LoggingOptions_EventConfigurationsSurvive guards
// SetV2LoggingOptionsInput/GetV2LoggingOptionsOutput's real
// eventConfigurations member ([]types.LogEventConfiguration, iot@v1.77.4),
// previously entirely dropped on Set and never surfaced on Get.
func TestV2LoggingOptions_EventConfigurationsSurvive(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForBatch3Test(t)

	iotOK(t, h, http.MethodPost, "/v2LoggingOptions", map[string]any{
		"roleArn":         "arn:aws:iam::000000000000:role/IoTRole",
		"defaultLogLevel": "INFO",
		"eventConfigurations": []map[string]any{
			{"eventType": "THING", "logLevel": "DEBUG"},
		},
	})

	out := iotOK(t, h, http.MethodGet, "/v2LoggingOptions", nil)
	events, ok := out["eventConfigurations"].([]any)
	if !ok || len(events) != 1 {
		t.Fatalf("expected 1 eventConfiguration, got %v", out["eventConfigurations"])
	}
	entry, _ := events[0].(map[string]any)
	if entry["eventType"] != "THING" || entry["logLevel"] != "DEBUG" {
		t.Errorf("eventConfiguration mismatch: %v", entry)
	}
}

// TestListV2LoggingLevels_TargetTypeFilterAndPagination guards
// ListV2LoggingLevelsInput's real targetType/maxResults/nextToken query
// params (iot@v1.77.4 serializers.go), previously all three ignored.
func TestListV2LoggingLevels_TargetTypeFilterAndPagination(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForBatch3Test(t)

	iotOK(t, h, http.MethodPost, "/v2LoggingLevel", map[string]any{
		"logTarget": map[string]any{"targetType": "THING_GROUP", "targetName": "group-a"},
		"logLevel":  "DEBUG",
	})
	iotOK(t, h, http.MethodPost, "/v2LoggingLevel", map[string]any{
		"logTarget": map[string]any{"targetType": "DEFAULT", "targetName": "DEFAULT"},
		"logLevel":  "ERROR",
	})

	t.Run("target_type_filter", func(t *testing.T) {
		t.Parallel()

		out := iotOK(t, h, http.MethodGet, "/v2LoggingLevel?targetType=DEFAULT", nil)
		levels, _ := out["logTargetConfigurations"].([]any)
		if len(levels) != 1 {
			t.Fatalf("expected 1 level, got %d: %v", len(levels), levels)
		}
	})

	t.Run("pagination", func(t *testing.T) {
		t.Parallel()

		out := iotOK(t, h, http.MethodGet, "/v2LoggingLevel?maxResults=1", nil)
		levels, _ := out["logTargetConfigurations"].([]any)
		if len(levels) != 1 {
			t.Errorf("expected 1 level, got %d", len(levels))
		}
		if out["nextToken"] == nil || out["nextToken"] == "" {
			t.Error("expected non-empty nextToken")
		}
	})
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
