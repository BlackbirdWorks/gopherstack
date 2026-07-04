package cloudwatchlogs_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

func TestFilterPattern_JSON(t *testing.T) {
	t.Parallel()

	const evt = `{"eventType":"UpdateTrail","user":{"type":"Root"},"latency":150,"ok":true,"tags":["a","b"]}`

	tests := []struct {
		name    string
		pattern string
		message string
		want    bool
	}{
		{name: "string_eq", pattern: `{ $.eventType = "UpdateTrail" }`, message: evt, want: true},
		{name: "string_neq", pattern: `{ $.eventType != "CreateTrail" }`, message: evt, want: true},
		{name: "nested_eq", pattern: `{ $.user.type = "Root" }`, message: evt, want: true},
		{name: "nested_miss", pattern: `{ $.user.type = "IAMUser" }`, message: evt, want: false},
		{name: "num_gt", pattern: `{ $.latency > 100 }`, message: evt, want: true},
		{name: "num_gt_false", pattern: `{ $.latency > 200 }`, message: evt, want: false},
		{name: "num_lte", pattern: `{ $.latency <= 150 }`, message: evt, want: true},
		{name: "bool_true", pattern: `{ $.ok = true }`, message: evt, want: true},
		{name: "bool_false", pattern: `{ $.ok = false }`, message: evt, want: false},
		{name: "and", pattern: `{ ($.eventType = "UpdateTrail") && ($.latency > 100) }`, message: evt, want: true},
		{
			name:    "and_false",
			pattern: `{ ($.eventType = "UpdateTrail") && ($.latency > 200) }`,
			message: evt,
			want:    false,
		},
		{name: "or", pattern: `{ ($.eventType = "X") || ($.latency = 150) }`, message: evt, want: true},
		{name: "wildcard", pattern: `{ $.eventType = "Update*" }`, message: evt, want: true},
		{name: "exists", pattern: `{ $.user.type }`, message: evt, want: true},
		{name: "exists_false", pattern: `{ $.missing }`, message: evt, want: false},
		{name: "array_wildcard", pattern: `{ $.tags[*] = "b" }`, message: evt, want: true},
		{name: "array_index", pattern: `{ $.tags[0] = "a" }`, message: evt, want: true},
		{name: "not_json", pattern: `{ $.eventType = "UpdateTrail" }`, message: "plain text", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := cloudwatchlogs.FilterPatternMatches(tt.pattern, tt.message)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFilterPattern_SpaceDelimited(t *testing.T) {
	t.Parallel()

	const line = "10.0.0.1 GET /index 200 1024"

	tests := []struct {
		name    string
		pattern string
		message string
		want    bool
	}{
		{name: "count_match", pattern: "[ip, method, path, status, bytes]", message: line, want: true},
		{name: "count_mismatch", pattern: "[ip, method, path]", message: line, want: false},
		{name: "eq_cond", pattern: "[ip, method=GET, path, status, bytes]", message: line, want: true},
		{name: "eq_cond_false", pattern: "[ip, method=POST, path, status, bytes]", message: line, want: false},
		{name: "num_gt", pattern: "[ip, method, path, status>100, bytes]", message: line, want: true},
		{name: "num_gt_false", pattern: "[ip, method, path, status>500, bytes]", message: line, want: false},
		{name: "wildcard", pattern: "[ip, method, path, status=2*, bytes]", message: line, want: true},
		{name: "neq", pattern: "[ip, method!=POST, path, status, bytes]", message: line, want: true},
		{name: "ellipsis_tail", pattern: "[ip=10.0.0.1, ...]", message: line, want: true},
		{name: "ellipsis_lead", pattern: "[..., bytes=1024]", message: line, want: true},
		{name: "ellipsis_lead_false", pattern: "[..., bytes=9999]", message: line, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := cloudwatchlogs.FilterPatternMatches(tt.pattern, tt.message)
			assert.Equal(t, tt.want, got)
		})
	}
}
