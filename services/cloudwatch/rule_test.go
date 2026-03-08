package cloudwatch_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

func TestEvaluateAlarmRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		rule      string
		states    map[string]string
		wantState string
	}{
		{
			name:      "alarm_function_triggers",
			rule:      `ALARM("a")`,
			states:    map[string]string{"a": "ALARM"},
			wantState: "ALARM",
		},
		{
			name:      "alarm_function_ok",
			rule:      `ALARM("a")`,
			states:    map[string]string{"a": "OK"},
			wantState: "OK",
		},
		{
			name:      "ok_function_matches",
			rule:      `OK("a")`,
			states:    map[string]string{"a": "OK"},
			wantState: "ALARM",
		},
		{
			name:      "insufficient_data_function",
			rule:      `INSUFFICIENT_DATA("a")`,
			states:    map[string]string{"a": "INSUFFICIENT_DATA"},
			wantState: "ALARM",
		},
		{
			name:      "and_both_alarm",
			rule:      `ALARM("a") AND ALARM("b")`,
			states:    map[string]string{"a": "ALARM", "b": "ALARM"},
			wantState: "ALARM",
		},
		{
			name:      "and_one_ok",
			rule:      `ALARM("a") AND ALARM("b")`,
			states:    map[string]string{"a": "ALARM", "b": "OK"},
			wantState: "OK",
		},
		{
			name:      "or_one_alarm",
			rule:      `ALARM("a") OR ALARM("b")`,
			states:    map[string]string{"a": "OK", "b": "ALARM"},
			wantState: "ALARM",
		},
		{
			name:      "or_both_ok",
			rule:      `ALARM("a") OR ALARM("b")`,
			states:    map[string]string{"a": "OK", "b": "OK"},
			wantState: "OK",
		},
		{
			name:      "not_alarm",
			rule:      `NOT ALARM("a")`,
			states:    map[string]string{"a": "ALARM"},
			wantState: "OK",
		},
		{
			name:      "not_ok",
			rule:      `NOT ALARM("a")`,
			states:    map[string]string{"a": "OK"},
			wantState: "ALARM",
		},
		{
			name:      "parentheses",
			rule:      `(ALARM("a") OR ALARM("b")) AND ALARM("c")`,
			states:    map[string]string{"a": "OK", "b": "ALARM", "c": "ALARM"},
			wantState: "ALARM",
		},
		{
			name:      "unknown_alarm_insufficient_data",
			rule:      `ALARM("unknown")`,
			states:    map[string]string{},
			wantState: "OK",
		},
		{
			name:      "case_insensitive_and",
			rule:      `ALARM("a") and ALARM("b")`,
			states:    map[string]string{"a": "ALARM", "b": "ALARM"},
			wantState: "ALARM",
		},
		{
			name:      "unterminated_quoted_string",
			rule:      `ALARM("a`,
			states:    map[string]string{"a": "ALARM"},
			wantState: "INSUFFICIENT_DATA",
		},
		{
			name:      "missing_closing_paren",
			rule:      `(ALARM("a")`,
			states:    map[string]string{"a": "ALARM"},
			wantState: "INSUFFICIENT_DATA",
		},
		{
			name:      "empty_rule",
			rule:      ``,
			states:    map[string]string{},
			wantState: "INSUFFICIENT_DATA",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := cloudwatch.EvaluateAlarmRuleForTest(tt.rule, tt.states)
			assert.Equal(t, tt.wantState, got)
		})
	}
}
