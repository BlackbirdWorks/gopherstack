package dynamodb

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func vectorItem(values ...string) map[string]any {
	list := make([]any, 0, len(values))
	for _, v := range values {
		list = append(list, map[string]any{"N": v})
	}

	return map[string]any{"emb": map[string]any{"L": list}}
}

func TestItemVector(t *testing.T) {
	t.Parallel()

	long := make([]string, maxVectorCapacityHint+5)
	for i := range long {
		long[i] = strconv.Itoa(i)
	}

	tests := []struct {
		item   map[string]any
		name   string
		want   []float64
		dims   int
		wantOK bool
	}{
		{name: "exact", item: vectorItem("1", "2.5"), dims: 2, want: []float64{1, 2.5}, wantOK: true},
		{name: "wrong_dims", item: vectorItem("1"), dims: 2},
		{name: "bad_number", item: vectorItem("1", "x"), dims: 2},
		{name: "missing_attr", item: map[string]any{}, dims: 2},
		{name: "longer_than_hint", item: vectorItem(long...), dims: len(long), wantOK: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := itemVector(tt.item, "emb", tt.dims)
			assert.Equal(t, tt.wantOK, ok)

			if tt.want != nil {
				assert.Equal(t, tt.want, got)
			}

			if tt.wantOK {
				assert.Len(t, got, tt.dims)
			}
		})
	}
}
