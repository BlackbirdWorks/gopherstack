package eventbridge_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

// Messages follow aws/event-ruler JsonRuleCompiler.java.
func TestPutRule_PatternReasons(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern string
		want    string
	}{
		{"empty_object", `{"detail":{}}`, "Empty objects are not allowed"},
		{"empty_array", `{"source":[]}`, "Empty arrays are not allowed"},
		{"bad_match_value", `{"source":[[1]]}`, "Match value must be String, number, true, false, or null"},
		{"or_not_array", `{"$or":{"a":[1]}}`, "It must be an Array followed with $or."},
		{"or_one_element", `{"$or":[{"a":[1]}]}`, "There must have at least 2 Objects in $or relationship."},
		{"or_non_object", `{"$or":[1,2]}`, "Only JSON object is allowed in array of $or relationship."},
		{"anything_but_object", `{"a":[{"anything-but":{"bogus":"x"}}]}`, "Unsupported anything-but pattern: bogus"},
		{
			"anything_but_bool", `{"a":[{"anything-but":true}]}`,
			"Value of anything-but must be an array or single string/number value.",
		},
		{"anything_but_mixed", `{"a":[{"anything-but":["x",1]}]}`, "mixed type is not supported"},
		{"exists_bad", `{"a":[{"exists":"x"}]}`, "exists match pattern must be either true or false."},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := newBackend().PutRule(context.Background(), eventbridge.PutRuleInput{Name: "r", EventPattern: tt.pattern})
			require.ErrorIs(t, err, eventbridge.ErrInvalidEventPattern)
			require.ErrorContains(t, err, "Event pattern is not valid. Reason: ")
			require.ErrorContains(t, err, tt.want)
		})
	}
}
