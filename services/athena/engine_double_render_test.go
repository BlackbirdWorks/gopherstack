package athena_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/athena"
)

func TestSDK_IntegralDoublesKeepDecimalPoint(t *testing.T) {
	t.Parallel()

	client := sdkClientFor(t, map[string][]byte{}, map[string]*athena.GlueTableDef{})

	cases := []struct{ name, sql, want string }{
		{"array", "SELECT ARRAY[CAST(1 AS double), 2.5]", "[1.0, 2.5]"},
		{"array of ints", "SELECT ARRAY[1, 2]", "[1, 2]"},
		{"map", "SELECT MAP(ARRAY['a'], ARRAY[CAST(2 AS double)])", "{a=2.0}"},
		{"row", "SELECT ROW(CAST(3 AS double), 1)", "[3.0, 1]"},
		{"nested", "SELECT ARRAY[ARRAY[CAST(4 AS double)], ARRAY[5]]", "[[4.0], [5]]"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, [][]string{{tc.want}}, sdkRun(t, client, tc.sql).rows)
		})
	}
}
