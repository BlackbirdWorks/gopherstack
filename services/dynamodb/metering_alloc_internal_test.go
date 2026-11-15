package dynamodb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

//nolint:paralleltest // testing.AllocsPerRun panics under t.Parallel
func TestMeterCapacityAllocatesNothingPerRequest(t *testing.T) {
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
			db := NewInMemoryDB()
			db.EnableCapacityMetering()

			region, table := "us-east-1", "t"
			db.meterCapacity(region, table, tc.read, tc.write)

			allocs := testing.AllocsPerRun(1000, func() { db.meterCapacity(region, table, tc.read, tc.write) })
			assert.Zero(t, allocs)
		})
	}
}
