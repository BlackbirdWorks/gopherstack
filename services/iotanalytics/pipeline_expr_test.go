package iotanalytics_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotanalytics"
)

// TestInMemoryBackend_RunPipelineActivity_Filter covers the filter pipeline activity's
// SQL-like expression language: comparisons, logical operators, parenthesized grouping, and
// the soft-failure paths (malformed expression, missing attribute, non-JSON payload, type
// mismatch) that drop a message rather than erroring the whole call.
func TestInMemoryBackend_RunPipelineActivity_Filter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		filter   string
		payloads [][]byte
		wantLen  int
	}{
		{
			name:     "numeric_greater_than",
			filter:   "temp > 50",
			payloads: [][]byte{[]byte(`{"temp":60}`), []byte(`{"temp":40}`), []byte(`{"temp":50}`)},
			wantLen:  1,
		},
		{
			name:     "numeric_gte",
			filter:   "temp >= 50",
			payloads: [][]byte{[]byte(`{"temp":60}`), []byte(`{"temp":40}`), []byte(`{"temp":50}`)},
			wantLen:  2,
		},
		{
			name:     "string_equality",
			filter:   "status = 'ok'",
			payloads: [][]byte{[]byte(`{"status":"ok"}`), []byte(`{"status":"bad"}`)},
			wantLen:  1,
		},
		{
			name:   "and_or_not_precedence",
			filter: "temp > 50 AND (humidity < 30 OR NOT active)",
			payloads: [][]byte{
				[]byte(`{"temp":60,"humidity":20,"active":true}`),  // temp>50 AND humidity<30 -> match
				[]byte(`{"temp":60,"humidity":80,"active":false}`), // temp>50 AND NOT active -> match
				[]byte(`{"temp":60,"humidity":80,"active":true}`),  // temp>50 AND neither -> no match
				[]byte(`{"temp":10,"humidity":20,"active":true}`),  // temp<=50 -> no match
			},
			wantLen: 2,
		},
		{
			name:     "parenthesized_arithmetic_comparison",
			filter:   "(temp - 32) / 1.8 > 20",
			payloads: [][]byte{[]byte(`{"temp":100}`), []byte(`{"temp":50}`)},
			wantLen:  1,
		},
		{
			name:     "malformed_expression_drops_all",
			filter:   "temp >",
			payloads: [][]byte{[]byte(`{"temp":100}`)},
			wantLen:  0,
		},
		{
			name:     "missing_attribute_dropped",
			filter:   "missingAttr > 1",
			payloads: [][]byte{[]byte(`{"temp":100}`)},
			wantLen:  0,
		},
		{
			name:     "non_json_payload_dropped",
			filter:   "temp > 1",
			payloads: [][]byte{[]byte(`not json`)},
			wantLen:  0,
		},
		{
			name:     "type_mismatch_dropped",
			filter:   "temp > 'fifty'",
			payloads: [][]byte{[]byte(`{"temp":100}`)},
			wantLen:  0,
		},
		{
			name:     "boolean_literal_true",
			filter:   "TRUE",
			payloads: [][]byte{[]byte(`{"x":1}`), []byte(`{"x":2}`)},
			wantLen:  2,
		},
		{
			name:     "boolean_literal_false",
			filter:   "FALSE",
			payloads: [][]byte{[]byte(`{"x":1}`)},
			wantLen:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotanalytics.NewInMemoryBackend()
			activity := iotanalytics.PipelineActivity{
				Filter: &iotanalytics.PipelineFilterActivity{Name: "f", Filter: tt.filter},
			}

			out, err := b.RunPipelineActivity(t.Context(), activity, tt.payloads)
			require.NoError(t, err)
			assert.Len(t, out, tt.wantLen)
		})
	}
}

// TestInMemoryBackend_RunPipelineActivity_Math covers the math pipeline activity: arithmetic
// expression evaluation with its result stored under the activity's Attribute, and the
// soft-failure paths that leave a payload unchanged.
func TestInMemoryBackend_RunPipelineActivity_Math(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		math      string
		attribute string
		payload   string
		wantValue float64
		wantSame  bool
	}{
		{
			name:      "fahrenheit_to_celsius",
			math:      "(temp - 32) / 1.8",
			attribute: "tempC",
			payload:   `{"temp":100}`,
			wantValue: 37.77777777777778,
		},
		{
			name:      "multiplication_and_unary_minus",
			math:      "-x * 2",
			attribute: "y",
			payload:   `{"x":5}`,
			wantValue: -10,
		},
		{
			name:      "modulo",
			math:      "x % 3",
			attribute: "r",
			payload:   `{"x":10}`,
			wantValue: 1,
		},
		{
			name:      "sqrt_function",
			math:      "sqrt(x)",
			attribute: "y",
			payload:   `{"x":16}`,
			wantValue: 4,
		},
		{
			name:      "abs_function",
			math:      "abs(x)",
			attribute: "y",
			payload:   `{"x":-7}`,
			wantValue: 7,
		},
		{
			name:      "power_function_two_args",
			math:      "power(x, 3)",
			attribute: "y",
			payload:   `{"x":2}`,
			wantValue: 8,
		},
		{
			name:      "mod_function_two_args",
			math:      "mod(x, 3)",
			attribute: "y",
			payload:   `{"x":10}`,
			wantValue: 1,
		},
		{
			name:      "trunc_function_two_args",
			math:      "trunc(x, 1)",
			attribute: "y",
			payload:   `{"x":3.14159}`,
			wantValue: 3.1,
		},
		{
			name:      "nested_function_and_arithmetic",
			math:      "round(sqrt(x)) + 1",
			attribute: "y",
			payload:   `{"x":17}`,
			wantValue: 5,
		},
		{
			name:      "unknown_function_unchanged",
			math:      "notafunction(x)",
			attribute: "y",
			payload:   `{"x":5}`,
			wantSame:  true,
		},
		{
			name:      "function_wrong_arity_unchanged",
			math:      "sqrt(x, y)",
			attribute: "y",
			payload:   `{"x":5,"y":2}`,
			wantSame:  true,
		},
		{
			name:      "malformed_expression_unchanged",
			math:      "x +",
			attribute: "y",
			payload:   `{"x":5}`,
			wantSame:  true,
		},
		{
			name:      "missing_attribute_unchanged",
			math:      "missing * 2",
			attribute: "y",
			payload:   `{"x":5}`,
			wantSame:  true,
		},
		{
			name:      "non_json_payload_unchanged",
			math:      "x * 2",
			attribute: "y",
			payload:   `not json`,
			wantSame:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotanalytics.NewInMemoryBackend()
			activity := iotanalytics.PipelineActivity{
				Math: &iotanalytics.PipelineMathActivity{Name: "m", Attribute: tt.attribute, Math: tt.math},
			}

			out, err := b.RunPipelineActivity(t.Context(), activity, [][]byte{[]byte(tt.payload)})
			require.NoError(t, err)
			require.Len(t, out, 1)

			if tt.wantSame {
				assert.Equal(t, tt.payload, string(out[0]))

				return
			}

			var decoded map[string]any
			require.NoError(t, json.Unmarshal(out[0], &decoded))
			assert.InDelta(t, tt.wantValue, decoded[tt.attribute], 1e-9)
		})
	}
}

// TestInMemoryBackend_RunPipelineActivity_PassThrough verifies that channel/datastore
// (legitimately pass-through source/sink activities in real AWS too) and unwired
// lambda/deviceRegistryEnrich/deviceShadowEnrich (no SetLambdaBackend/SetThingRegistry/
// SetThingShadowStore call -- see pipelines_wiring_test.go for the wired-path behavior)
// pass payloads through unchanged rather than erroring.
func TestInMemoryBackend_RunPipelineActivity_PassThrough(t *testing.T) {
	t.Parallel()

	payloads := [][]byte{[]byte(`{"a":1}`), []byte(`{"b":2}`)}

	tests := []struct {
		activity iotanalytics.PipelineActivity
		name     string
	}{
		{
			name: "channel",
			activity: iotanalytics.PipelineActivity{
				Channel: &iotanalytics.PipelineChannelActivity{Name: "c", ChannelName: "ch"},
			},
		},
		{
			name: "datastore",
			activity: iotanalytics.PipelineActivity{
				Datastore: &iotanalytics.PipelineDatastoreActivity{Name: "d", DatastoreName: "ds"},
			},
		},
		{
			name: "lambda",
			activity: iotanalytics.PipelineActivity{
				Lambda: &iotanalytics.PipelineLambdaActivity{Name: "l", LambdaName: "fn", BatchSize: 1},
			},
		},
		{
			name: "device_registry_enrich",
			activity: iotanalytics.PipelineActivity{
				DeviceRegistryEnrich: &iotanalytics.PipelineDeviceRegistryEnrichActivity{
					Name: "e", Attribute: "a", ThingName: "t", RoleArn: "arn:aws:iam::123456789012:role/r",
				},
			},
		},
		{
			name: "device_shadow_enrich",
			activity: iotanalytics.PipelineActivity{
				DeviceShadowEnrich: &iotanalytics.PipelineDeviceShadowEnrichActivity{
					Name: "e", Attribute: "a", ThingName: "t", RoleArn: "arn:aws:iam::123456789012:role/r",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotanalytics.NewInMemoryBackend()

			out, err := b.RunPipelineActivity(t.Context(), tt.activity, payloads)
			require.NoError(t, err)
			require.Len(t, out, len(payloads))

			for i := range payloads {
				assert.JSONEq(t, string(payloads[i]), string(out[i]))
			}
		})
	}
}
