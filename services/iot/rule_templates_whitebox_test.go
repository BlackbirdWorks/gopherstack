package iot

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRuleMessageExpand(t *testing.T) {
	t.Parallel()

	msg := &ruleMessage{
		received: time.UnixMilli(1700000000123), topic: "a/b/c", clientID: "dev-1", account: "123456789012",
		original: []byte(`{"id":"x","n":7,"f":1.5,"ok":true,"o":{"k":"v"},"l":[{"z":"first"}],"nul":null}`),
	}

	tests := []struct {
		wantErr error
		name    string
		tmpl    string
		want    string
	}{
		{name: "literal", tmpl: "plain", want: "plain"},
		{name: "topic", tmpl: "${topic()}/x", want: "a/b/c/x"},
		{name: "topic_segment", tmpl: "${topic(2)}", want: "b"},
		{name: "topic_out_of_range", tmpl: "${topic(9)}", wantErr: errTemplateUndefined},
		{name: "timestamp", tmpl: "${timestamp()}", want: "1700000000123"},
		{name: "clientid", tmpl: "${clientid()}", want: "dev-1"},
		{name: "accountid", tmpl: "${accountid()}", want: "123456789012"},
		{name: "fields", tmpl: "${id}-${n}-${f}-${ok}", want: "x-7-1.5-true"},
		{name: "nested_and_index", tmpl: "${o.k}/${l[0].z}", want: "v/first"},
		{name: "object_renders_json", tmpl: "${o}", want: `{"k":"v"}`},
		{name: "missing_field", tmpl: "${absent}", wantErr: errTemplateUndefined},
		{name: "null_field", tmpl: "${nul}", wantErr: errTemplateUndefined},
		{name: "unsupported_function", tmpl: "${nope()}", wantErr: errTemplate},
		{name: "unterminated", tmpl: "${id", wantErr: errTemplate},
		{name: "bad_topic_arg", tmpl: "${topic(x)}", wantErr: errTemplateUndefined},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := msg.expand(tt.tmpl)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRuleMessageBatchElements(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload string
		want    []string
		batch   bool
	}{
		{name: "single", payload: `{"a":1}`, want: []string{`{"a":1}`}},
		{name: "array_elements", payload: `[{"a":1},"s"]`, batch: true, want: []string{`{"a":1}`, "s"}},
		{name: "non_array_falls_back", payload: `{"a":1}`, batch: true, want: []string{`{"a":1}`}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := (&ruleMessage{payload: []byte(tt.payload)}).batchElements(tt.batch)
			require.NoError(t, err)

			var strs []string
			for _, g := range got {
				strs = append(strs, string(g))
			}

			assert.Equal(t, tt.want, strs)
		})
	}
}

func TestRuleMessageExpandUsesOriginalPayload(t *testing.T) {
	t.Parallel()

	msg := &ruleMessage{
		topic: "a/b", original: []byte(`{"id":"x","n":2}`), payload: []byte(`{"renamed":"x"}`),
	}

	tests := []struct {
		name string
		tmpl string
		want string
	}{
		{name: "original_field", tmpl: "${id}", want: "x"},
		{name: "expression", tmpl: "${n * 3}-${upper(id)}", want: "6-X"},
		{name: "projected_alias_not_visible", tmpl: "${topic(1)}", want: "a"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := msg.expand(tt.tmpl)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	_, err := msg.expand("${renamed}")
	require.ErrorIs(t, err, errTemplateUndefined)
}
