package mediastore_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_MetricPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		wantStatus int
		withPolicy bool
	}{
		{
			name:       "put metric policy",
			op:         "PutMetricPolicy",
			wantStatus: http.StatusOK,
		},
		{
			name:       "get metric policy",
			op:         "GetMetricPolicy",
			withPolicy: true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete metric policy",
			op:         "DeleteMetricPolicy",
			withPolicy: true,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			setupRec := doRequest(t, h, "CreateContainer", map[string]any{"ContainerName": "metric-test"})
			require.Equal(t, http.StatusOK, setupRec.Code)

			metricPolicy := map[string]any{
				"ContainerLevelMetrics": "ENABLED",
			}

			if tt.withPolicy {
				putRec := doRequest(t, h, "PutMetricPolicy",
					map[string]any{"ContainerName": "metric-test", "MetricPolicy": metricPolicy})
				require.Equal(t, http.StatusOK, putRec.Code)
			}

			body := map[string]any{"ContainerName": "metric-test"}
			if tt.op == "PutMetricPolicy" {
				body["MetricPolicy"] = metricPolicy
			}

			result := doRequest(t, h, tt.op, body)
			assert.Equal(t, tt.wantStatus, result.Code)
		})
	}
}

// TestHandler_PutMetricPolicy_Validation verifies MetricPolicy validation.
// Moved (and de-prefixed) from the former parity_audit1_test.go's
// TestParity_PutMetricPolicy_Validation.
func TestHandler_PutMetricPolicy_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		policy     map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "valid_enabled",
			policy:     map[string]any{"ContainerLevelMetrics": "ENABLED"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid_disabled",
			policy:     map[string]any{"ContainerLevelMetrics": "DISABLED"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_metric_level",
			policy:     map[string]any{"ContainerLevelMetrics": "INVALID"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "too_many_rules_six",
			policy: map[string]any{
				"ContainerLevelMetrics": "ENABLED",
				"MetricPolicyRules": []any{
					map[string]any{"ObjectGroup": "a", "ObjectGroupName": "a"},
					map[string]any{"ObjectGroup": "b", "ObjectGroupName": "b"},
					map[string]any{"ObjectGroup": "c", "ObjectGroupName": "c"},
					map[string]any{"ObjectGroup": "d", "ObjectGroupName": "d"},
					map[string]any{"ObjectGroup": "e", "ObjectGroupName": "e"},
					map[string]any{"ObjectGroup": "f", "ObjectGroupName": "f"},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "exactly_five_rules_allowed",
			policy: map[string]any{
				"ContainerLevelMetrics": "ENABLED",
				"MetricPolicyRules": []any{
					map[string]any{"ObjectGroup": "a", "ObjectGroupName": "a"},
					map[string]any{"ObjectGroup": "b", "ObjectGroupName": "b"},
					map[string]any{"ObjectGroup": "c", "ObjectGroupName": "c"},
					map[string]any{"ObjectGroup": "d", "ObjectGroupName": "d"},
					map[string]any{"ObjectGroup": "e", "ObjectGroupName": "e"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			// ObjectGroup max length is 900 chars (MediaStore API model
			// ObjectGroup shape `max` trait); the real SDK's client-side
			// validator never checks this, so it must be enforced server-side.
			name: "object_group_too_long",
			policy: map[string]any{
				"ContainerLevelMetrics": "ENABLED",
				"MetricPolicyRules": []any{
					map[string]any{
						"ObjectGroup":     strings.Repeat("a", maxObjectGroupLenForTest+1),
						"ObjectGroupName": "valid",
					},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "object_group_exactly_max_length_allowed",
			policy: map[string]any{
				"ContainerLevelMetrics": "ENABLED",
				"MetricPolicyRules": []any{
					map[string]any{
						"ObjectGroup":     strings.Repeat("a", maxObjectGroupLenForTest),
						"ObjectGroupName": "valid",
					},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			// ObjectGroup's allowed character set excludes spaces.
			name: "object_group_invalid_characters",
			policy: map[string]any{
				"ContainerLevelMetrics": "ENABLED",
				"MetricPolicyRules": []any{
					map[string]any{
						"ObjectGroup":     "has a space",
						"ObjectGroupName": "valid",
					},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			// ObjectGroupName max length is 30 chars (ObjectGroupName shape
			// `max` trait).
			name: "object_group_name_too_long",
			policy: map[string]any{
				"ContainerLevelMetrics": "ENABLED",
				"MetricPolicyRules": []any{
					map[string]any{
						"ObjectGroup":     "valid",
						"ObjectGroupName": strings.Repeat("a", maxObjectGroupNameLenForTest+1),
					},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "object_group_name_exactly_max_length_allowed",
			policy: map[string]any{
				"ContainerLevelMetrics": "ENABLED",
				"MetricPolicyRules": []any{
					map[string]any{
						"ObjectGroup":     "valid",
						"ObjectGroupName": strings.Repeat("a", maxObjectGroupNameLenForTest),
					},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			// ObjectGroupName's allowed character set is [a-zA-Z0-9_] only --
			// no hyphens, unlike ObjectGroup.
			name: "object_group_name_invalid_characters",
			policy: map[string]any{
				"ContainerLevelMetrics": "ENABLED",
				"MetricPolicyRules": []any{
					map[string]any{
						"ObjectGroup":     "valid",
						"ObjectGroupName": "has-a-hyphen",
					},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createTestContainer(t, h, "metric-validation")

			rec := doRequest(t, h, "PutMetricPolicy", map[string]any{
				"ContainerName": "metric-validation",
				"MetricPolicy":  tt.policy,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// maxObjectGroupLenForTest and maxObjectGroupNameLenForTest mirror the
// unexported maxObjectGroupLen/maxObjectGroupNameLen constants in
// store.go (900/30 chars, per the MediaStore API model). This test file is
// package mediastore_test (black-box), so it cannot reference the
// unexported constants directly; duplicating the literal values here (with
// the same model-file citation) is preferred over adding a test-only export.
const (
	maxObjectGroupLenForTest     = 900
	maxObjectGroupNameLenForTest = 30
)
