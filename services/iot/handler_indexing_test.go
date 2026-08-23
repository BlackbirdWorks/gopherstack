package iot_test

import (
	"net/http"
	"slices"
	"testing"
)

// TestIndexing_GetSetIndexingConfiguration exercises GetIndexingConfiguration
// and UpdateIndexingConfiguration over HTTP.
func TestIndexing_GetSetIndexingConfiguration(t *testing.T) {
	t.Parallel()
	h := newIoTHandlerBatch1(t)

	// Default configuration is OFF.
	out := iotOK(t, h, http.MethodGet, "/indices/config", nil)
	tic := out["thingIndexingConfiguration"].(map[string]any)
	if tic["thingIndexingMode"] != "OFF" {
		t.Errorf("expected default thingIndexingMode OFF, got %v", tic["thingIndexingMode"])
	}

	// Update it.
	iotOK(t, h, http.MethodPost, "/indices/config", map[string]any{
		"thingIndexingConfiguration": map[string]any{
			"thingIndexingMode":             "REGISTRY_AND_SHADOW",
			"thingConnectivityIndexingMode": "STATUS",
		},
		"thingGroupIndexingConfiguration": map[string]any{
			"thingGroupIndexingMode": "ON",
		},
	})

	out2 := iotOK(t, h, http.MethodGet, "/indices/config", nil)
	tic2 := out2["thingIndexingConfiguration"].(map[string]any)
	if tic2["thingIndexingMode"] != "REGISTRY_AND_SHADOW" {
		t.Errorf("thingIndexingMode not updated: %v", tic2)
	}

	tgic2 := out2["thingGroupIndexingConfiguration"].(map[string]any)
	if tgic2["thingGroupIndexingMode"] != "ON" {
		t.Errorf("thingGroupIndexingMode not updated: %v", tgic2)
	}

	// Invalid mode is rejected.
	rec := iotRequest(t, h, http.MethodPost, "/indices/config", map[string]any{
		"thingIndexingConfiguration": map[string]any{"thingIndexingMode": "BOGUS"},
	})
	if rec.Code != http.StatusBadRequest {
		t.Errorf("want 400 for invalid mode, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestIndexing_DeviceDefenderIndexingModeSurvives guards
// ThingIndexingConfiguration's real deviceDefenderIndexingMode member
// (types.ThingIndexingConfiguration, iot@v1.77.4), previously entirely
// unmodeled -- an UpdateIndexingConfiguration call setting it had no effect,
// and GetIndexingConfiguration could never surface it.
func TestIndexing_DeviceDefenderIndexingModeSurvives(t *testing.T) {
	t.Parallel()
	h := newIoTHandlerBatch1(t)

	iotOK(t, h, http.MethodPost, "/indices/config", map[string]any{
		"thingIndexingConfiguration": map[string]any{
			"thingIndexingMode":          "REGISTRY",
			"deviceDefenderIndexingMode": "VIOLATIONS",
		},
	})

	out := iotOK(t, h, http.MethodGet, "/indices/config", nil)
	tic := out["thingIndexingConfiguration"].(map[string]any)
	if tic["deviceDefenderIndexingMode"] != "VIOLATIONS" {
		t.Errorf("deviceDefenderIndexingMode not surfaced: %v", tic)
	}
}

// TestIndexing_ListAndDescribeIndex exercises ListIndices and DescribeIndex over HTTP.
func TestIndexing_ListAndDescribeIndex(t *testing.T) {
	t.Parallel()
	h := newIoTHandlerBatch1(t)

	// No indices before enabling.
	out := iotOK(t, h, http.MethodGet, "/indices", nil)
	if names, _ := out["indexNames"].([]any); len(names) != 0 {
		t.Errorf("expected no indices by default, got %v", names)
	}

	iotOK(t, h, http.MethodPost, "/indices/config", map[string]any{
		"thingIndexingConfiguration": map[string]any{"thingIndexingMode": "REGISTRY"},
	})

	out2 := iotOK(t, h, http.MethodGet, "/indices", nil)
	names, _ := out2["indexNames"].([]any)
	if len(names) != 1 || names[0] != "AWS_Things" {
		t.Errorf("expected [AWS_Things], got %v", names)
	}

	// DescribeIndex for a known index.
	out3 := iotOK(t, h, http.MethodGet, "/indices/AWS_Things", nil)
	if out3["indexStatus"] != "ACTIVE" {
		t.Errorf("expected ACTIVE status, got %v", out3)
	}

	// DescribeIndex for an unknown index returns an error.
	iotExpectError(t, h, "/indices/bogus")
}

// TestIndexing_SearchIndex exercises SearchIndex over HTTP against seeded Things.
func TestIndexing_SearchIndex(t *testing.T) {
	t.Parallel()
	h := newIoTHandlerBatch1(t)

	iotOK(t, h, http.MethodPost, "/things/sensor-1", map[string]any{
		"thingTypeName": "TemperatureSensor",
		"attributePayload": map[string]any{
			"attributes": map[string]string{"location": "lab-a", "temperature": "21.5"},
		},
	})
	iotOK(t, h, http.MethodPost, "/things/sensor-2", map[string]any{
		"thingTypeName": "TemperatureSensor",
		"attributePayload": map[string]any{
			"attributes": map[string]string{"location": "lab-b", "temperature": "30.0"},
		},
	})

	tests := []struct {
		name        string
		queryString string
		wantCount   int
	}{
		{name: "match_all", queryString: "", wantCount: 2},
		{name: "substring", queryString: "sensor-1", wantCount: 1},
		{name: "attribute_key_value", queryString: "attributes.location:lab-b", wantCount: 1},
		{name: "no_match", queryString: "attributes.location:nowhere", wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out := iotOK(t, h, http.MethodPost, "/indices/search", map[string]any{
				"queryString": tt.queryString,
			})
			things, _ := out["things"].([]any)
			if len(things) != tt.wantCount {
				t.Errorf("%s: want %d things, got %d (%v)", tt.name, tt.wantCount, len(things), things)
			}
		})
	}
}

// TestIndexing_Aggregations exercises GetCardinality, GetStatistics,
// GetPercentiles and GetBucketsAggregation over HTTP.
func TestIndexing_Aggregations(t *testing.T) {
	t.Parallel()
	h := newIoTHandlerBatch1(t)

	temps := []string{"10", "20", "30"}
	for i, v := range temps {
		iotOK(t, h, http.MethodPost, "/things/device-"+string(rune('a'+i)), map[string]any{
			"attributePayload": map[string]any{
				"attributes": map[string]string{"temperature": v},
			},
		})
	}

	// GetCardinality
	cardOut := iotOK(t, h, http.MethodPost, "/indices/cardinality", map[string]any{
		"aggregationField": "attributes.temperature",
	})
	if cardOut["cardinality"] != float64(3) {
		t.Errorf("expected cardinality 3, got %v", cardOut["cardinality"])
	}

	// GetStatistics
	statsOut := iotOK(t, h, http.MethodPost, "/indices/statistics", map[string]any{
		"aggregationField": "attributes.temperature",
	})
	stats := statsOut["statistics"].(map[string]any)
	if stats["count"] != float64(3) || stats["average"] != float64(20) {
		t.Errorf("unexpected statistics: %v", stats)
	}

	// GetPercentiles
	pctOut := iotOK(t, h, http.MethodPost, "/indices/percentiles", map[string]any{
		"aggregationField": "attributes.temperature",
		"percents":         []float64{0, 100},
	})
	percentiles, _ := pctOut["percentiles"].([]any)
	if len(percentiles) != 2 {
		t.Fatalf("expected 2 percentiles, got %d", len(percentiles))
	}

	// GetBucketsAggregation
	bucketsOut := iotOK(t, h, http.MethodPost, "/indices/buckets", map[string]any{
		"aggregationField": "attributes.temperature",
	})
	buckets, _ := bucketsOut["buckets"].([]any)
	if len(buckets) != 3 {
		t.Errorf("expected 3 buckets, got %d (%v)", len(buckets), buckets)
	}

	// Missing aggregationField is rejected.
	rec := iotRequest(t, h, http.MethodPost, "/indices/cardinality", map[string]any{})
	if rec.Code != http.StatusBadRequest {
		t.Errorf("want 400 for missing aggregationField, got %d", rec.Code)
	}
}

// TestIndexing_SupportedOperations verifies the fleet-indexing ops are
// reported as supported and no longer dispatch through the stub handler.
func TestIndexing_SupportedOperations(t *testing.T) {
	t.Parallel()
	h := newIoTHandlerBatch1(t)

	ops := h.GetSupportedOperations()

	for _, op := range []string{
		"UpdateIndexingConfiguration",
		"GetIndexingConfiguration",
		"SearchIndex",
		"ListIndices",
		"DescribeIndex",
		"GetCardinality",
		"GetPercentiles",
		"GetStatistics",
		"GetBucketsAggregation",
	} {
		if !slices.Contains(ops, op) {
			t.Errorf("expected %s to be in GetSupportedOperations()", op)
		}
	}
}
