package gcpsql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

type diskEngine struct {
	Engine
	used     int64
	readOnly bool
}

func (d *diskEngine) DiskUsage(context.Context) (int64, error) { return d.used, nil }

func (d *diskEngine) SetReadOnly(_ context.Context, on bool) error {
	d.readOnly = on

	return nil
}

func TestStorageAutoResizeGrowsTheDisk(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		limit    string
		wantSize string
		usedGiB  float64
		resize   bool
		wantFull bool
	}{
		{name: "under the size", resize: true, limit: "0", usedGiB: 5, wantSize: "10", wantFull: false},
		{name: "grows to the next GiB", resize: true, limit: "0", usedGiB: 10.5, wantSize: "11", wantFull: false},
		{name: "capped by the limit", resize: true, limit: "12", usedGiB: 20, wantSize: "12", wantFull: true},
		{name: "no auto resize", resize: false, limit: "0", usedGiB: 15, wantSize: "10", wantFull: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend()
			t.Cleanup(b.Close)

			eng := &diskEngine{used: int64(tt.usedGiB * float64(gib))}
			rec := &instRec{eng: eng}
			rec.Inst.Settings = map[string]any{
				"dataDiskSizeGb": "10", "storageAutoResize": tt.resize, "storageAutoResizeLimit": tt.limit,
			}

			_, full := b.checkQuota(t.Context(), rec, nil, false)

			assert.Equal(t, tt.wantSize, settingString(rec.Inst.Settings, "dataDiskSizeGb"))
			assert.Equal(t, tt.wantFull, full)
		})
	}
}
