package rum_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rum"
)

func TestMatchPattern(t *testing.T) {
	t.Parallel()

	const event = `{"event_type":"a","event_details":{"duration":1500,"url":"https://x/y","tags":["p","q"]},` +
		`"metadata":{"browserName":"Chrome"}}`

	cases := []struct {
		name    string
		pattern string
		want    bool
	}{
		{"empty", `{}`, true},
		{"exact", `{"event_type":["a"]}`, true},
		{"one_of", `{"event_type":["z","a"]}`, true},
		{"mismatch", `{"event_type":["z"]}`, false},
		{"nested", `{"metadata":{"browserName":["Chrome"]}}`, true},
		{"nested_mismatch", `{"metadata":{"browserName":["Safari"]}}`, false},
		{"missing_key", `{"nope":["x"]}`, false},
		{"numeric_gt", `{"event_details":{"duration":[{"numeric":[">",1000]}]}}`, true},
		{"numeric_range", `{"event_details":{"duration":[{"numeric":[">=",2000,"<",8000]}]}}`, false},
		{"prefix", `{"event_details":{"url":[{"prefix":"https://"}]}}`, true},
		{"anything_but", `{"event_type":[{"anything-but":"a"}]}`, false},
		{"anything_but_list", `{"event_type":[{"anything-but":["b","c"]}]}`, true},
		{"exists_true", `{"metadata":{"browserName":[{"exists":true}]}}`, true},
		{"exists_false", `{"metadata":{"osName":[{"exists":false}]}}`, true},
		{"array_element", `{"event_details":{"tags":["q"]}}`, true},
		{"object_where_array_expected", `{"event_type":{"x":["a"]}}`, false},
		{"scalar_in_pattern", `{"event_type":"a"}`, false},
		{"unknown_operator", `{"event_type":[{"bogus":"a"}]}`, false},
	}

	var doc map[string]any
	require.NoError(t, json.Unmarshal([]byte(event), &doc))

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var pattern map[string]any
			require.NoError(t, json.Unmarshal([]byte(tc.pattern), &pattern))
			assert.Equal(t, tc.want, rum.MatchPattern(pattern, doc))
		})
	}
}
