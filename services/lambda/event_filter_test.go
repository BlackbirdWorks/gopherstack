package lambda_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// fc builds a FilterCriteria from a list of raw JSON patterns.
func fc(patterns ...string) *lambda.FilterCriteria {
	filters := make([]lambda.Filter, len(patterns))
	for i, p := range patterns {
		filters[i] = lambda.Filter{Pattern: p}
	}

	return &lambda.FilterCriteria{Filters: filters}
}

func TestLambda_EventFilterMatches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fc     *lambda.FilterCriteria
		record map[string]any
		name   string
		want   bool
	}{
		{
			name:   "nil criteria matches everything",
			fc:     nil,
			record: map[string]any{"a": "b"},
			want:   true,
		},
		{
			name:   "empty filters matches everything",
			fc:     &lambda.FilterCriteria{},
			record: map[string]any{"a": "b"},
			want:   true,
		},
		{
			name:   "exact string match",
			fc:     fc(`{"orderType":["premium"]}`),
			record: map[string]any{"orderType": "premium"},
			want:   true,
		},
		{
			name:   "exact string mismatch",
			fc:     fc(`{"orderType":["premium"]}`),
			record: map[string]any{"orderType": "standard"},
			want:   false,
		},
		{
			name:   "multi-value OR within a field",
			fc:     fc(`{"orderType":["premium","gold"]}`),
			record: map[string]any{"orderType": "gold"},
			want:   true,
		},
		{
			name:   "multiple filters are ORed",
			fc:     fc(`{"a":["x"]}`, `{"b":["y"]}`),
			record: map[string]any{"b": "y"},
			want:   true,
		},
		{
			name:   "multiple keys in one pattern are ANDed - all present",
			fc:     fc(`{"a":["x"],"b":["y"]}`),
			record: map[string]any{"a": "x", "b": "y"},
			want:   true,
		},
		{
			name:   "multiple keys in one pattern are ANDed - one missing",
			fc:     fc(`{"a":["x"],"b":["y"]}`),
			record: map[string]any{"a": "x", "b": "z"},
			want:   false,
		},
		{
			name:   "nested object match",
			fc:     fc(`{"body":{"type":["order"]}}`),
			record: map[string]any{"body": map[string]any{"type": "order"}},
			want:   true,
		},
		{
			name:   "nested object mismatch on missing nested field",
			fc:     fc(`{"body":{"type":["order"]}}`),
			record: map[string]any{"body": map[string]any{"other": "order"}},
			want:   false,
		},
		{
			name:   "nested pattern against non-object field",
			fc:     fc(`{"body":{"type":["order"]}}`),
			record: map[string]any{"body": "not-an-object"},
			want:   false,
		},
		{
			name:   "prefix operator match",
			fc:     fc(`{"region":[{"prefix":"us-"}]}`),
			record: map[string]any{"region": "us-east-1"},
			want:   true,
		},
		{
			name:   "prefix operator mismatch",
			fc:     fc(`{"region":[{"prefix":"us-"}]}`),
			record: map[string]any{"region": "eu-west-1"},
			want:   false,
		},
		{
			name:   "suffix operator match",
			fc:     fc(`{"file":[{"suffix":".json"}]}`),
			record: map[string]any{"file": "data.json"},
			want:   true,
		},
		{
			name:   "equals-ignore-case match",
			fc:     fc(`{"status":[{"equals-ignore-case":"ACTIVE"}]}`),
			record: map[string]any{"status": "active"},
			want:   true,
		},
		{
			name:   "exists true when present",
			fc:     fc(`{"id":[{"exists":true}]}`),
			record: map[string]any{"id": "abc"},
			want:   true,
		},
		{
			name:   "exists true when absent",
			fc:     fc(`{"id":[{"exists":true}]}`),
			record: map[string]any{"other": "abc"},
			want:   false,
		},
		{
			name:   "exists false when absent",
			fc:     fc(`{"id":[{"exists":false}]}`),
			record: map[string]any{"other": "abc"},
			want:   true,
		},
		{
			name:   "anything-but scalar match",
			fc:     fc(`{"state":[{"anything-but":"deleted"}]}`),
			record: map[string]any{"state": "active"},
			want:   true,
		},
		{
			name:   "anything-but scalar excluded",
			fc:     fc(`{"state":[{"anything-but":"deleted"}]}`),
			record: map[string]any{"state": "deleted"},
			want:   false,
		},
		{
			name:   "anything-but array excluded",
			fc:     fc(`{"state":[{"anything-but":["deleted","archived"]}]}`),
			record: map[string]any{"state": "archived"},
			want:   false,
		},
		{
			name:   "numeric equal",
			fc:     fc(`{"count":[{"numeric":["=",5]}]}`),
			record: map[string]any{"count": float64(5)},
			want:   true,
		},
		{
			name:   "numeric range match",
			fc:     fc(`{"price":[{"numeric":[">",10,"<=",100]}]}`),
			record: map[string]any{"price": float64(50)},
			want:   true,
		},
		{
			name:   "numeric range below",
			fc:     fc(`{"price":[{"numeric":[">",10,"<=",100]}]}`),
			record: map[string]any{"price": float64(5)},
			want:   false,
		},
		{
			name:   "numeric against non-number",
			fc:     fc(`{"price":[{"numeric":[">",10]}]}`),
			record: map[string]any{"price": "abc"},
			want:   false,
		},
		{
			name:   "empty pattern in filter does not pass everything",
			fc:     fc(""),
			record: map[string]any{"a": "b"},
			want:   false,
		},
		{
			name:   "malformed pattern is skipped",
			fc:     fc(`{not json`),
			record: map[string]any{"a": "b"},
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := lambda.EventFilterMatches(tt.fc, tt.record)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLambda_SQSFilterView(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		msg        *lambda.SQSMessage
		filterJSON string
		want       bool
	}{
		{
			name:       "json body content filter matches",
			msg:        &lambda.SQSMessage{MessageID: "1", Body: `{"type":"order","amount":42}`},
			filterJSON: `{"body":{"type":["order"]}}`,
			want:       true,
		},
		{
			name:       "json body content filter mismatch",
			msg:        &lambda.SQSMessage{MessageID: "1", Body: `{"type":"invoice"}`},
			filterJSON: `{"body":{"type":["order"]}}`,
			want:       false,
		},
		{
			name:       "raw string body exact match",
			msg:        &lambda.SQSMessage{MessageID: "1", Body: "PING"},
			filterJSON: `{"body":["PING"]}`,
			want:       true,
		},
		{
			name: "message attribute filter",
			msg: &lambda.SQSMessage{
				MessageID: "1",
				Body:      "x",
				MessageAttributes: map[string]lambda.SQSMessageAttribute{
					"priority": {DataType: "String", StringValue: "high"},
				},
			},
			filterJSON: `{"messageAttributes":{"priority":{"stringValue":["high"]}}}`,
			want:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			view := lambda.SQSFilterView(tt.msg)
			got := lambda.EventFilterMatches(fc(tt.filterJSON), view)
			assert.Equal(t, tt.want, got)
		})
	}
}
