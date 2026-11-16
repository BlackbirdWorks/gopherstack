package cloudwatchlogs

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseSelectionMalformed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		criteria  string
		wantValid bool
		wantMatch bool
	}{
		{name: "in_list", criteria: `LogGroupName IN ["a","b"]`, wantValid: true, wantMatch: true},
		{name: "prefix_eq", criteria: `LogGroupNamePrefix = "a"`, wantValid: true, wantMatch: true},
		{name: "not_in", criteria: `LogGroupName NOT IN ["z"]`, wantValid: true, wantMatch: true},
		{name: "empty_list", criteria: `LogGroupName IN []`, wantValid: true},
		{name: "unquoted", criteria: `LogGroupName IN [a]`},
		{name: "unterminated", criteria: `LogGroupName IN ["a"`},
		{name: "wrong_field", criteria: `Other IN ["a"]`},
		{name: "operator_only", criteria: `LogGroupName IN`},
		{name: "number_list", criteria: `LogGroupName IN [1,2]`},
		{name: "nested", criteria: `LogGroupName IN [["a"]]`},
		{name: "null", criteria: `LogGroupName IN null`},
		{name: "binary", criteria: "\x00\xff\xfe LogGroupName"},
		{name: "huge", criteria: `LogGroupName IN [` + strings.Repeat(`"a",`, 50_000) + `"b"]`, wantValid: true,
			wantMatch: true},
		{name: "deep", criteria: `LogGroupName IN ` + strings.Repeat("[", 100_000)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sel := parseSelection(tt.criteria)
			assert.Equal(t, tt.wantValid, sel.valid)
			assert.Equal(t, tt.wantMatch, sel.matches("a"))
		})
	}
}
