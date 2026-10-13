package cloudwatchlogs_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

func TestMetricFilterPattern_Edges(t *testing.T) {
	t.Parallel()

	const (
		js   = `{"a":1,"b":"x","n":null,"o":{"k":2.5}}`
		line = "10.0.0.1 GET /index 200 1024"
	)

	tests := []struct {
		name    string
		pattern string
		message string
		want    bool
	}{
		{"is_null_true", `{ $.n IS NULL }`, js, true},
		{"is_null_false", `{ $.a IS NULL }`, js, false},
		{"is_not_null", `{ $.a IS NOT NULL }`, js, true},
		{"is_not_null_on_null", `{ $.n IS NOT NULL }`, js, false},
		{"not_exists_missing", `{ $.zz NOT EXISTS }`, js, true},
		{"not_exists_present", `{ $.a NOT EXISTS }`, js, false},
		{"is_null_missing", `{ $.zz IS NULL }`, js, false},
		{"exists_expr", `{ $.a EXISTS }`, js, true},
		{"and_or_precedence", `{ $.a = 1 && $.b = "y" || $.o.k > 2 }`, js, true},
		{"parens_override", `{ $.a = 1 && ($.b = "y" || $.o.k > 9) }`, js, false},
		{"float_cmp", `{ $.o.k >= 2.5 }`, js, true},
		{"neq_num", `{ $.a != 2 }`, js, true},
		{"or_terms", `?ERROR ?WARN`, "a WARN b", true},
		{"or_terms_miss", `?ERROR ?WARN`, "a INFO b", false},
		{"quoted_phrase", `"GET /index"`, line, true},
		{"quoted_phrase_miss", `"GET /other"`, line, false},
		{"and_terms", `GET 200`, line, true},
		{"and_terms_miss", `GET 404`, line, false},
		{"exclude_term", `GET -index`, line, false},
		{"space_eq_str", `[ip, m = GET, p, status = 200, bytes]`, line, true},
		{"space_gt", `[ip, m, p, status, bytes > 1000]`, line, true},
		{"space_gt_miss", `[ip, m, p, status, bytes > 5000]`, line, false},
		{"space_or", `[ip, m, p, status = 404 || status = 200, bytes]`, line, true},
		{"space_ellipsis", `[ip, ..., bytes > 100]`, line, true},
		{"space_wildcard", `[ip, m, p = *index, s, b]`, line, true},
		{"space_neq", `[ip, m != POST, p, s, b]`, line, true},
		{"space_wrong_arity", `[a, b]`, line, false},
		{"space_range_and", `[ip, m, p, status >= 200 && status < 300, b]`, line, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, cloudwatchlogs.FilterPatternMatches(tt.pattern, tt.message))
		})
	}
}
