package cloudtrail_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloudTrailLookupEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "no_input_returns_empty_list",
			body: nil,
		},
		{
			name: "with_max_results_still_returns_empty",
			body: map[string]any{"MaxResults": 10},
		},
		{
			name: "with_lookup_attribute_still_returns_empty",
			body: map[string]any{
				"LookupAttributes": []any{
					map[string]any{
						"AttributeKey":   "EventName",
						"AttributeValue": "CreateBucket",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestCloudTrailHandler()
			rec := doCloudTrailOp(t, h, "LookupEvents", tt.body)

			assert.Equal(t, http.StatusOK, rec.Code)

			resp := parseCloudTrailResp(t, rec)
			events, ok := resp["Events"].([]any)
			require.True(t, ok)
			assert.Empty(t, events)
		})
	}
}

// TestLookupEvents verifies LookupEvents accepts various input parameters and
// returns a well-formed response.
func TestLookupEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "empty_body_returns_empty_list",
			body: nil,
		},
		{
			name: "max_results_param_accepted",
			body: map[string]any{"MaxResults": 10},
		},
		{
			name: "lookup_by_event_name",
			body: map[string]any{
				"LookupAttributes": []any{
					map[string]any{
						"AttributeKey":   "EventName",
						"AttributeValue": "CreateTrail",
					},
				},
			},
		},
		{
			name: "lookup_by_username",
			body: map[string]any{
				"LookupAttributes": []any{
					map[string]any{
						"AttributeKey":   "Username",
						"AttributeValue": "testuser",
					},
				},
			},
		},
		{
			name: "lookup_by_resource_type",
			body: map[string]any{
				"LookupAttributes": []any{
					map[string]any{
						"AttributeKey":   "ResourceType",
						"AttributeValue": "AWS::CloudTrail::Trail",
					},
				},
			},
		},
		{
			name: "lookup_by_resource_name",
			body: map[string]any{
				"LookupAttributes": []any{
					map[string]any{
						"AttributeKey":   "ResourceName",
						"AttributeValue": "my-trail",
					},
				},
			},
		},
		{
			name: "lookup_by_event_source",
			body: map[string]any{
				"LookupAttributes": []any{
					map[string]any{
						"AttributeKey":   "EventSource",
						"AttributeValue": "cloudtrail.amazonaws.com",
					},
				},
			},
		},
		{
			name: "lookup_with_time_range",
			body: map[string]any{
				"StartTime": 1700000000,
				"EndTime":   1700086400,
			},
		},
		{
			name: "lookup_with_next_token",
			body: map[string]any{
				"NextToken":  "some-continuation-token",
				"MaxResults": 50,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestCloudTrailHandler()
			rec := doCloudTrailOp(t, h, "LookupEvents", tt.body)
			assert.Equal(t, http.StatusOK, rec.Code)
			resp := parseCloudTrailResp(t, rec)
			events, ok := resp["Events"].([]any)
			require.True(t, ok, "response should contain Events array")
			assert.NotNil(t, events)
		})
	}
}

// TestCloudTrailLookupEventsFilters covers LookupEvents with various attributes.
func TestCloudTrailLookupEventsFilters(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "no_filters",
			body: map[string]any{},
		},
		{
			name: "with_event_name_filter",
			body: map[string]any{
				"LookupAttributes": []any{
					map[string]any{"AttributeKey": "EventName", "AttributeValue": "CreateTrail"},
				},
				"MaxResults": 10,
			},
		},
		{
			name: "with_event_id_filter",
			body: map[string]any{
				"LookupAttributes": []any{
					map[string]any{"AttributeKey": "EventId", "AttributeValue": "some-event-id"},
				},
			},
		},
		{
			name: "with_read_only_filter",
			body: map[string]any{
				"LookupAttributes": []any{
					map[string]any{"AttributeKey": "ReadOnly", "AttributeValue": "false"},
				},
			},
		},
		{
			name: "with_access_key_filter",
			body: map[string]any{
				"LookupAttributes": []any{
					map[string]any{"AttributeKey": "AccessKeyId", "AttributeValue": "AKIAIOSFODNN7EXAMPLE"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := doCloudTrailOp(t, h, "LookupEvents", tt.body)
			assert.Equal(t, http.StatusOK, rec.Code)
			resp := parseCloudTrailResp(t, rec)
			events, ok := resp["Events"].([]any)
			require.True(t, ok)
			assert.NotNil(t, events)
		})
	}
}
