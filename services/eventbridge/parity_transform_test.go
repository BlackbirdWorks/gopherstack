package eventbridge_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

// TestInputTransformer_ProducesValidJSON exercises the context-aware input
// transformer scanner. Placeholders in value position ({"k":<v>}) must gain
// JSON quoting/encoding so the result is valid JSON, while placeholders inside
// string literals ("<v>") must be spliced as escaped scalar content without
// doubling quotes.
func TestInputTransformer_ProducesValidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vars       map[string]any
		name       string
		template   string
		wantJSONEq string
		wantExact  string
		wantValid  bool
	}{
		{
			name:       "value_position_string_gets_quoted",
			template:   `{"k":<v>}`,
			vars:       map[string]any{"v": "hello"},
			wantJSONEq: `{"k":"hello"}`,
			wantValid:  true,
		},
		{
			name:       "value_position_number_stays_bare",
			template:   `{"n":<v>}`,
			vars:       map[string]any{"v": float64(42)},
			wantJSONEq: `{"n":42}`,
			wantValid:  true,
		},
		{
			name:       "value_position_object_spliced",
			template:   `{"detail":<d>}`,
			vars:       map[string]any{"d": map[string]any{"a": "b"}},
			wantJSONEq: `{"detail":{"a":"b"}}`,
			wantValid:  true,
		},
		{
			name:       "string_context_not_double_quoted",
			template:   `{"src":"<v>"}`,
			vars:       map[string]any{"v": "svc.name"},
			wantJSONEq: `{"src":"svc.name"}`,
			wantValid:  true,
		},
		{
			name:       "string_context_escapes_quotes",
			template:   `{"msg":"value is <v>"}`,
			vars:       map[string]any{"v": `a"b\c`},
			wantJSONEq: `{"msg":"value is a\"b\\c"}`,
			wantValid:  true,
		},
		{
			name:       "value_context_escapes_embedded_quotes",
			template:   `{"k":<v>}`,
			vars:       map[string]any{"v": `he said "hi"`},
			wantJSONEq: `{"k":"he said \"hi\""}`,
			wantValid:  true,
		},
		{
			name:       "missing_var_in_string_context_empty",
			template:   `{"k":"<missing>"}`,
			vars:       map[string]any{"missing": nil},
			wantJSONEq: `{"k":""}`,
			wantValid:  true,
		},
		{
			name:       "missing_var_in_value_context_empty_string",
			template:   `{"k":<missing>}`,
			vars:       map[string]any{"missing": nil},
			wantJSONEq: `{"k":""}`,
			wantValid:  true,
		},
		{
			name:      "unknown_placeholder_untouched",
			template:  `{"k":"<unknown>"}`,
			vars:      map[string]any{"known": "x"},
			wantExact: `{"k":"<unknown>"}`,
		},
		{
			name:      "plain_string_literal_template",
			template:  `"Event from <v>"`,
			vars:      map[string]any{"v": "text.service"},
			wantExact: `"Event from text.service"`,
			wantValid: true,
		},
		{
			name:       "no_html_escaping_of_angle_brackets",
			template:   `{"k":<v>}`,
			vars:       map[string]any{"v": "a<b>c&d"},
			wantJSONEq: `{"k":"a<b>c&d"}`,
			wantValid:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := eventbridge.SubstituteInputTemplateForTest(tt.template, tt.vars)

			if tt.wantExact != "" {
				assert.Equal(t, tt.wantExact, got)
			}
			if tt.wantJSONEq != "" {
				assert.JSONEq(t, tt.wantJSONEq, got)
			}
			if tt.wantValid {
				assert.True(t, json.Valid([]byte(got)), "expected valid JSON, got %q", got)
			}
		})
	}
}

// TestInputTransformer_ValueContextEndToEnd confirms the value-position fix
// flows through real delivery: an unquoted placeholder produces valid JSON in
// the delivered SQS message.
func TestInputTransformer_ValueContextEndToEnd(t *testing.T) {
	t.Parallel()

	sqsMock := newMockSQSSender()
	backend := setupDeliveryBackend(t, sqsMock, nil)
	queueARN := "arn:aws:sqs:us-east-1:000000000000:it-value-queue"

	_, err := backend.PutRule(t.Context(), eventbridge.PutRuleInput{
		Name:         "it-value-rule",
		EventPattern: `{"source":["it.svc"]}`,
		State:        "ENABLED",
	})
	require.NoError(t, err)

	_, err = backend.PutTargets(t.Context(), "it-value-rule", "default", []eventbridge.Target{
		{
			ID:  "t1",
			Arn: queueARN,
			InputTransformer: &eventbridge.InputTransformer{
				InputPathsMap: map[string]string{"src": "$.source"},
				// Unquoted placeholder in value position — the old code emitted
				// {"wrapped":it.svc} (invalid JSON); the fix quotes it.
				InputTemplate: `{"wrapped":<src>}`,
			},
		},
	})
	require.NoError(t, err)

	backend.PutEvents(t.Context(), []eventbridge.EventEntry{
		{Source: "it.svc", DetailType: "Evt", Detail: `{}`},
	})

	require.Eventually(t, func() bool {
		return len(sqsMock.MessagesFor(queueARN)) > 0
	}, 2*time.Second, 10*time.Millisecond)

	msg := sqsMock.MessagesFor(queueARN)[0]
	assert.True(t, json.Valid([]byte(msg)), "delivered payload must be valid JSON: %q", msg)
	assert.JSONEq(t, `{"wrapped":"it.svc"}`, msg)
}
