package ocinosql_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueryTimestampComparison(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.mkTable(t, "events", "CREATE TABLE events (id INTEGER, at TIMESTAMP(3), PRIMARY KEY (id))")

	for id, at := range []string{"2026-01-01T00:00:00.000Z", "2026-01-01T12:00:00.500Z", "2026-02-01T00:00:00.000Z"} {
		e.put(t, "events", map[string]any{"id": id + 1, "at": at})
	}

	tests := []struct {
		name string
		sql  string
		want []int
	}{
		{"coarser literal eq", "SELECT id FROM events WHERE at = '2026-01-01T00:00:00Z'", []int{1}},
		{"date literal eq", "SELECT id FROM events WHERE at = '2026-01-01'", []int{1}},
		{"offset literal gt", "SELECT id FROM events WHERE at > '2026-01-01T13:00:00+01:00'", []int{2, 3}},
		{"literal on left", "SELECT id FROM events WHERE '2026-01-31' > at", []int{1, 2}},
		{"in list", "SELECT id FROM events WHERE at IN ('2026-02-01', '2026-01-01T12:00:00.5Z')", []int{2, 3}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, ids(e.query(t, tt.sql, 0, nil).Items))
		})
	}
}
