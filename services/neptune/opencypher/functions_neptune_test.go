package opencypher_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

func TestNeptuneFunctions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		query   string
		want    string
		wantErr string
	}{
		{
			name:  "exp log",
			query: "RETURN round(exp(0)) AS a, log10(1000) AS b, log(e()) AS c",
			want:  `[{"a":1,"b":3,"c":1}]`,
		},
		{
			name:  "trig",
			query: "RETURN sin(0) AS s, cos(0) AS c, atan2(0, 1) AS a, degrees(pi()) AS d",
			want:  `[{"a":0,"c":1,"d":180,"s":0}]`,
		},
		{name: "radians", query: "RETURN round(radians(180) * 1000) AS r", want: `[{"r":3142}]`},
		{name: "join", query: `RETURN join(["abc", "def", "ghi"], ", ") AS j`, want: `[{"j":"abc, def, ghi"}]`},
		{name: "join type", query: `RETURN join([1], ",") AS j`, wantErr: "expected String"},
		{name: "remove key", query: `RETURN removeKeyFromMap({a:1, b:2}, 'a') AS m`, want: `[{"m":{"b":2}}]`},
		{
			name:  "epochmillis",
			query: `RETURN epochMillis(datetime('2022-01-01T00:01')) AS e`,
			want:  `[{"e":1640995260000}]`,
		},
		{
			name:  "datetime forms",
			query: `RETURN datetime('2022T0001') AS a, datetime('20220101T0001Z') AS b`,
			want:  `[{"a":"2022-01-01T00:01:00Z","b":"2022-01-01T00:01:00Z"}]`,
		},
		{name: "datetime bad", query: `RETURN datetime('nope') AS a`, wantErr: "Unable to parse"},
		{
			name: "temporal parts",
			query: `RETURN day(datetime('2021-06-03T00:00:00+08:00')) AS d, ` +
				`month(datetime('2021-06-01T00:00:00+08:00')) AS m, year(datetime('2021-01-01T00:00:00+08:00')) AS y`,
			want: `[{"d":2,"m":5,"y":2020}]`,
		},
		{
			name:  "textindexof",
			query: `RETURN textIndexOf('Amazon Neptune', 'e') AS i, textIndexOf('abc', 'z') AS n`,
			want:  `[{"i":8,"n":-1}]`,
		},
		{name: "collToSet", query: "RETURN collToSet([1, 6, 5, 1, 1, 5]) AS s", want: `[{"s":[1,6,5]}]`},
		{name: "collSubtract", query: "RETURN collSubtract([2, 5, 1, 0], [1, 5]) AS s", want: `[{"s":[0,2]}]`},
		{name: "collIntersection", query: "RETURN collIntersection([2, 5, 1, 0], [1, 5]) AS s", want: `[{"s":[1,5]}]`},
		{name: "collSort", query: "RETURN collSort([5, 3, 1], {order: 'desc'}) AS s", want: `[{"s":[5,3,1]}]`},
		{
			name:  "collSortMaps",
			query: "RETURN collSortMaps([{n:'A', a:25}, {n:'B', a:35}, {n:'C', a:18}], {key: 'a', order: 'desc'}) AS s",
			want:  `[{"s":[{"a":35,"n":"B"},{"a":25,"n":"A"},{"a":18,"n":"C"}]}]`,
		},
		{
			name: "collSortMulti",
			query: "RETURN collSortMulti([{n:'A', a:1}, {n:'B', a:1}, {n:'C', a:0}], " +
				"[{key:'a', order:'desc'}, {key:'n', order:'desc'}], 2, 0) AS s",
			want: `[{"s":[{"a":1,"n":"B"},{"a":1,"n":"A"}]}]`,
		},
		{name: "reduce add", query: "RETURN reduce(sum = 0, n IN [1, 2, 3] | sum + n) AS r", want: `[{"r":6}]`},
		{
			name:  "reduce concat",
			query: `RETURN reduce(s = "", x IN ["A", "B", "C"] | s + x) AS r`,
			want:  `[{"r":"ABC"}]`,
		},
		{
			name:  "reduce float mul",
			query: "RETURN reduce(p = 1.0, n IN [1.5, 2.5, 3.5] | p * n) AS r",
			want:  `[{"r":13.125}]`,
		},
		{
			name:    "reduce overflow",
			query:   "RETURN reduce(s = 9223372036854775807, x IN [2, 3] | s * x) AS r",
			wantErr: "long overflow",
		},
		{name: "reduce form", query: "RETURN reduce(s = 0, x IN [1] | s - x) AS r", wantErr: "only supports"},
		{
			name:  "toInteger saturates",
			query: "RETURN toInteger(1.0e30) AS a, toInteger(-1.0e30) AS b",
			want:  `[{"a":9223372036854775807,"b":-9223372036854775808}]`,
		},
		{name: "power operator", query: "RETURN 2 ^ 3 AS p", wantErr: "exponentiation"},
		{
			name:  "stdev",
			query: "UNWIND [2, 4, 4, 4, 5, 5, 7, 9] AS x RETURN stDevP(x) AS p, round(stDev(x) * 100) AS s",
			want:  `[{"p":2,"s":214}]`,
		},
		{
			name:  "percentileDisc",
			query: "UNWIND [1, 2, 3, 4] AS x RETURN percentileDisc(x, 0.5) AS d",
			want:  `[{"d":2}]`,
		},
		{
			name:  "percentileCont",
			query: "UNWIND [1, 2, 3, 4] AS x RETURN percentileCont(x, 0.5) AS c",
			want:  `[{"c":2.5}]`,
		},
		{
			name:    "percentile range",
			query:   "UNWIND [1, 2] AS x RETURN percentileCont(x, 1.5) AS c",
			wantErr: "0.0 to 1.0",
		},
		{name: "randomUUID", query: "RETURN size(randomUUID()) AS n", want: `[{"n":36}]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := run(t, opencypher.NewGraph(), tt.query, nil)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.JSONEq(t, tt.want, got)
		})
	}
}
