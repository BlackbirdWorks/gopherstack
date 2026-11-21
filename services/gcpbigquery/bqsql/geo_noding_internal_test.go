package bqsql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNodeLines(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
		in   []lonLat
	}{
		{"no overlap", "LINESTRING(0 0, 1 1, 2 0)", []lonLat{{0, 0}, {1, 1}, {2, 0}}},
		{"self crossing kept", "LINESTRING(0 0, 2 2, 2 0, 0 2)", []lonLat{{0, 0}, {2, 2}, {2, 0}, {0, 2}}},
		{"partial overlap", "LINESTRING(0 0, 2 0, 4 0, 6 0)", []lonLat{{0, 0}, {4, 0}, {2, 0}, {6, 0}}},
		{"through pole", "LINESTRING(0 80, 180 80)", []lonLat{{0, 80}, {180, 80}, {0, 80}}},
		{"meridian opposite", "LINESTRING(0 10, 0 90, 180 80)", []lonLat{{0, 10}, {0, 90}, {180, 80}, {0, 90}}},
		{"fork", "MULTILINESTRING((0 0, 1 0, 2 0), (1 0, 1 1))", []lonLat{{0, 0}, {2, 0}, {1, 0}, {1, 1}}},
		{
			"short distinct edges not merged",
			"LINESTRING(10 10, 10.00001 10, 10.000005 10.0000001, 10.000015 10.0000001)",
			[]lonLat{{10, 10}, {10.00001, 10}, {10.000005, 10.0000001}, {10.000015, 10.0000001}},
		},
		{"same edge twice", "LINESTRING(0 0, 1 2)", []lonLat{{0, 0}, {1, 2}, {0, 0}, {1, 2}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			lines := nodeLines(tt.in)

			g := &geo{kind: gLine, lines: lines}
			if len(lines) > 1 {
				g.kind = gMultiLine
			}

			assert.Equal(t, tt.want, g.wkt())
		})
	}
}

func TestNodeLinesLargeInput(t *testing.T) {
	t.Parallel()

	pts := make([]lonLat, 0, nodingMaxEdges*3)
	for i := range nodingMaxEdges * 3 {
		pts = append(pts, lonLat{float64(i%170) - 85, float64(i%7) * 10})
	}

	assert.NotPanics(t, func() { nodeLines(pts) })
}
