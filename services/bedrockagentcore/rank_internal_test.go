package bedrockagentcore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBM25(t *testing.T) {
	t.Parallel()

	docs := []string{"the cat sat on the mat", "dogs chase cats", "completely unrelated text"}

	tests := []struct {
		name    string
		query   string
		zeroFor []int
		best    int
	}{
		{name: "exact term", query: "cat", best: 0, zeroFor: []int{1, 2}},
		{name: "multi term prefers more overlap", query: "cat mat", best: 0, zeroFor: []int{1, 2}},
		{name: "case and punctuation", query: "DOGS, chase!", best: 1, zeroFor: []int{0, 2}},
		{name: "empty query", query: "  ", best: -1, zeroFor: []int{0, 1, 2}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			scores := bm25(tt.query, docs)

			for _, i := range tt.zeroFor {
				assert.InDelta(t, 0, scores[i], 1e-12)
			}

			if tt.best >= 0 {
				for i, s := range scores {
					assert.LessOrEqual(t, s, scores[tt.best], "doc %d", i)
				}

				assert.Positive(t, scores[tt.best])
			}
		})
	}
}
