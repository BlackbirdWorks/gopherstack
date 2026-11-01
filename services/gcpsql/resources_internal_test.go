package gcpsql

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTierShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tier   string
		cpu    int64
		memMiB int64
		ok     bool
	}{
		{tier: "db-f1-micro", cpu: 1, memMiB: 614, ok: true},
		{tier: "db-g1-small", cpu: 1, memMiB: 1740, ok: true},
		{tier: "db-custom-2-7680", cpu: 2, memMiB: 7680, ok: true},
		{tier: "db-n1-standard-4", cpu: 4, memMiB: 4 * 3840, ok: true},
		{tier: "db-n1-highmem-8", cpu: 8, memMiB: 8 * 6656, ok: true},
		{tier: "db-perf-optimized-N-4", cpu: 4, memMiB: 4 * 8192, ok: true},
		{tier: "db-custom-0-0"},
		{tier: "mystery-tier"},
		{tier: ""},
	}

	for _, tt := range tests {
		t.Run(tt.tier, func(t *testing.T) {
			t.Parallel()

			cpu, mem, ok := tierShape(tt.tier)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.cpu, cpu)
			assert.Equal(t, tt.memMiB, mem)
		})
	}
}

func TestTierLimitsClampToHost(t *testing.T) {
	t.Parallel()

	nano, mem := tierLimits("db-custom-512-1024")
	assert.Equal(t, int64(runtime.NumCPU())*nanoPerCPU, nano)
	assert.Equal(t, 1024*mib, mem)

	nano, mem = tierLimits("unknown")
	assert.Zero(t, nano)
	assert.Zero(t, mem)
}
