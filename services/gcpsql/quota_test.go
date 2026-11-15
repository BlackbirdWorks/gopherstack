package gcpsql_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sqladmin "google.golang.org/api/sqladmin/v1"
)

const quotaGiB = int64(1) << 30

func (f *fakeEngine) DiskUsage(context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.usage, nil
}

func (f *fakeEngine) SetReadOnly(_ context.Context, on bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.readOnly = append(f.readOnly, on)

	return nil
}

func (f *fakeEngine) setUsage(n int64) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.usage = n
}

func (f *fakeEngine) readOnlyCalls() []bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]bool(nil), f.readOnly...)
}

func TestDiskQuota(t *testing.T) {
	t.Parallel()

	tests := []struct {
		settings *sqladmin.Settings
		name     string
		wantFull bool
	}{
		{
			name: "fixed disk", wantFull: true,
			settings: &sqladmin.Settings{DataDiskSizeGb: 10, StorageAutoResize: new(false)},
		},
		{name: "auto resize without limit", settings: &sqladmin.Settings{DataDiskSizeGb: 10}, wantFull: false},
		{
			name: "auto resize with limit", wantFull: true,
			settings: &sqladmin.Settings{DataDiskSizeGb: 10, StorageAutoResizeLimit: 11},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.backend.SetQuotaInterval(5 * time.Millisecond)
			e.createInstance(t, "q", "POSTGRES_16", tt.settings)

			eng := e.launcher.engine(0)
			eng.setUsage(12 * quotaGiB)

			if !tt.wantFull {
				assert.Never(
					t,
					func() bool { return len(eng.readOnlyCalls()) > 0 },
					100*time.Millisecond,
					10*time.Millisecond,
				)

				return
			}

			require.Eventually(t, func() bool {
				calls := eng.readOnlyCalls()

				return len(calls) == 1 && calls[0]
			}, e.timeout, 5*time.Millisecond, "writes stop once the data outgrows the disk")

			eng.setUsage(quotaGiB)
			require.Eventually(t, func() bool {
				calls := eng.readOnlyCalls()

				return len(calls) == 2 && !calls[1]
			}, e.timeout, 5*time.Millisecond, "writes resume when space is free again")
		})
	}
}

func TestDiskCannotShrink(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, "q", "POSTGRES_16", &sqladmin.Settings{DataDiskSizeGb: 20})

	_, err := e.svc.Instances.Patch(project, "q", &sqladmin.DatabaseInstance{
		Settings: &sqladmin.Settings{DataDiskSizeGb: 10},
	}).Context(t.Context()).Do()
	require.Error(t, err)
	assert.Equal(t, http.StatusBadRequest, statusOf(t, err))

	op, err := e.svc.Instances.Patch(project, "q", &sqladmin.DatabaseInstance{
		Settings: &sqladmin.Settings{DataDiskSizeGb: 30},
	}).Context(t.Context()).Do()
	e.mustWait(t, op, err)
}
