package firehose_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/firehose"
)

func TestInferColumnsWidens(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
		objs []map[string]any
	}{
		{
			name: "int then float", want: "double",
			objs: []map[string]any{{"c": json.Number("3")}, {"c": json.Number("3.5")}},
		},
		{
			name: "float then int", want: "double",
			objs: []map[string]any{{"c": json.Number("3.5")}, {"c": json.Number("3")}},
		},
		{
			name: "string then int", want: "string",
			objs: []map[string]any{{"c": "abc"}, {"c": json.Number("5")}},
		},
		{
			name: "int then string", want: "string",
			objs: []map[string]any{{"c": json.Number("5")}, {"c": "abc"}},
		},
		{
			name: "leading null", want: "bigint",
			objs: []map[string]any{{"c": nil}, {"c": json.Number("5")}},
		},
		{
			name: "only null", want: "string",
			objs: []map[string]any{{"c": nil}},
		},
		{
			name: "bool and int", want: "string",
			objs: []map[string]any{{"c": true}, {"c": json.Number("1")}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cols := firehose.InferColumns(tt.objs)
			assert.Equal(t, []firehose.SchemaColumn{{Name: "c", Type: tt.want}}, cols)
		})
	}
}
