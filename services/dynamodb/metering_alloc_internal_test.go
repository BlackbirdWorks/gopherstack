package dynamodb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMeterCapacityAllocatesNothingPerRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		read  float64
		write float64
	}{
		{name: "read", read: 0.5},
		{name: "write", write: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := NewInMemoryDB()
			db.EnableCapacityMetering()

			region, table := "us-east-1", "t"
			db.meterCapacity(region, table, tc.read, tc.write)

			res := testing.Benchmark(func(b *testing.B) {
				b.Helper()
				b.ReportAllocs()

				for b.Loop() {
					db.meterCapacity(region, table, tc.read, tc.write)
				}
			})
			assert.Less(t, res.MemAllocs, uint64(res.N)/10)
		})
	}
}
