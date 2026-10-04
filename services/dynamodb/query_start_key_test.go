package dynamodb_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func queryItemSKs(t *testing.T, resp map[string]any) []string {
	t.Helper()

	items, _ := resp["Items"].([]any)
	out := make([]string, 0, len(items))

	for _, it := range items {
		sk := it.(map[string]any)["sk"].(map[string]any)["S"].(string)
		out = append(out, sk)
	}

	return out
}

func TestQuery_ExclusiveStartKeyVariants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		startKey map[string]any
		name     string
		want     []string
		limit    int
	}{
		{name: "absent", want: []string{"o0", "o1", "o2", "o3", "o4"}},
		{name: "empty map", startKey: map[string]any{}, want: []string{"o0", "o1", "o2", "o3", "o4"}},
		{name: "empty map with limit", startKey: map[string]any{}, limit: 2, want: []string{"o0", "o1"}},
		{
			name:     "real key",
			startKey: map[string]any{"pk": map[string]any{"S": "c"}, "sk": map[string]any{"S": "o1"}},
			want:     []string{"o2", "o3", "o4"},
		},
		{
			name:     "unknown key restarts",
			startKey: map[string]any{"pk": map[string]any{"S": "c"}, "sk": map[string]any{"S": "zz"}},
			limit:    2,
			want:     []string{"o0", "o1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newBenchHandlerWithGSI(t, "StartKeyTbl")

			for i := range 5 {
				seedBenchItem(t, h, "StartKeyTbl", map[string]any{
					"pk":    map[string]any{"S": "c"},
					"sk":    map[string]any{"S": fmt.Sprintf("o%d", i)},
					"gsipk": map[string]any{"S": "g"},
				})
			}

			req := map[string]any{
				"TableName":                 "StartKeyTbl",
				"KeyConditionExpression":    "pk = :pk",
				"ExpressionAttributeValues": map[string]any{":pk": map[string]any{"S": "c"}},
			}

			if tt.startKey != nil {
				req["ExclusiveStartKey"] = tt.startKey
			}

			if tt.limit > 0 {
				req["Limit"] = tt.limit
			}

			code, resp := invokeOpB(t, h, "Query", req)
			require.Equal(t, 200, code)
			assert.Equal(t, tt.want, queryItemSKs(t, resp))
		})
	}
}
