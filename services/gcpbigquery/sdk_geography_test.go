package gcpbigquery_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSDKMakeLine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, sql, want string
	}{
		{
			"overlap",
			`SELECT ST_ASTEXT(ST_MAKELINE([ST_MAKELINE(ST_GEOGPOINT(1, 0), ST_GEOGPOINT(10, 0)),
				ST_MAKELINE(ST_GEOGPOINT(10, 0), ST_GEOGPOINT(5, 0)), ST_MAKELINE(ST_GEOGPOINT(5, 0), ST_GEOGPOINT(5, 4))]))`,
			"MULTILINESTRING((1 0, 5 0, 10 0), (5 0, 5 4))",
		},
		{"point", `SELECT ST_ASTEXT(ST_MAKELINE(ST_GEOGPOINT(1, 2), ST_GEOGPOINT(1, 2)))`, "POINT(1 2)"},
		{
			"concat",
			`SELECT ST_ASTEXT(ST_MAKELINE([ST_GEOGFROMTEXT('LINESTRING(1 2, 2 3)'), ST_GEOGFROMTEXT('LINESTRING(2 3, 3 4)'),
				ST_GEOGPOINT(4, 5)]))`,
			"LINESTRING(1 2, 2 3, 3 4, 4 5)",
		},
	}

	env := newSDK(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rows := env.rows(t, tt.sql)
			require.Len(t, rows, 1)
			assert.Equal(t, tt.want, rows[0][0])
		})
	}
}
