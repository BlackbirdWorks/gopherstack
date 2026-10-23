package cosmosdb_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

const (
	geoSquare  = `{"type":"Polygon","coordinates":[[[31.8,-5],[32,-5],[32,-4.7],[31.8,-4.7],[31.8,-5]]]}`
	geoPtIn    = `{"type":"Point","coordinates":[31.9,-4.8]}`
	geoPtOut   = `{"type":"Point","coordinates":[33,-4.8]}`
	geoPtEast  = `{"type":"Point","coordinates":[32.0,-4.8]}`
	geoOverlap = `{"type":"Polygon","coordinates":[[[31.9,-4.9],[33,-4.9],[33,0],[31.9,0],[31.9,-4.9]]]}`
)

func TestExecuteQuery_SpatialAndConversionFunctions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		expr string
		want string
	}{
		{
			name: "distance between points in meters",
			expr: `ST_DISTANCE(` + geoPtIn + `, ` + geoPtEast + `) > 11000 AND ` +
				`ST_DISTANCE(` + geoPtIn + `, ` + geoPtEast + `) < 11200`,
			want: `true`,
		},
		{name: "distance of identical points", expr: `ST_DISTANCE(` + geoPtIn + `, ` + geoPtIn + `)`, want: `0`},
		{name: "point to polygon inside is zero", expr: `ST_DISTANCE(` + geoPtIn + `, ` + geoSquare + `)`, want: `0`},
		{
			name: "point to polygon outside is positive",
			expr: `ST_DISTANCE(` + geoPtOut + `, ` + geoSquare + `) > 100000`,
			want: `true`,
		},
		{name: "within polygon", expr: `ST_WITHIN(` + geoPtIn + `, ` + geoSquare + `)`, want: `true`},
		{name: "outside polygon", expr: `ST_WITHIN(` + geoPtOut + `, ` + geoSquare + `)`, want: `false`},
		{
			name: "line within polygon",
			expr: `ST_WITHIN({"type":"LineString","coordinates":[[31.85,-4.9],[31.95,-4.8]]}, ` + geoSquare + `)`,
			want: `true`,
		},
		{
			name: "line crossing polygon boundary is not within",
			expr: `ST_WITHIN({"type":"LineString","coordinates":[[31.85,-4.9],[33,-4.8]]}, ` + geoSquare + `)`,
			want: `false`,
		},
		{
			name: "line intersects polygon",
			expr: `ST_INTERSECTS({"type":"LineString","coordinates":[[31.85,-4.9],[33,-4.8]]}, ` + geoSquare + `)`,
			want: `true`,
		},
		{
			name: "disjoint polygons",
			expr: `ST_INTERSECTS(` + geoSquare + `, {"type":"Polygon","coordinates":[[[40,0],[41,0],[41,1],[40,1],[40,0]]]})`,
			want: `false`,
		},
		{
			name: "overlapping polygons",
			expr: `ST_INTERSECTS(` + geoSquare + `, ` + geoOverlap + `)`,
			want: `true`,
		},
		{name: "valid point", expr: `ST_ISVALID(` + geoPtIn + `)`, want: `true`},
		{
			name: "latitude out of range",
			expr: `ST_ISVALID({"type":"Point","coordinates":[31.9,-132.8]})`,
			want: `false`,
		},
		{
			name: "unclosed polygon ring",
			expr: `ST_ISVALID({"type":"Polygon","coordinates":[[[31.8,-5],[32,-5],[32,-4.7],[31.8,-4.7]]]})`,
			want: `false`,
		},
		{name: "detailed valid", expr: `ST_ISVALIDDETAILED(` + geoSquare + `)`, want: `{"valid":true}`},
		{
			name: "detailed invalid names the ring",
			expr: `ST_ISVALIDDETAILED({"type":"Polygon","coordinates":[[[31.8,-5],[32,-5],[32,-4.7],[31.8,-4.7]]]}).valid`,
			want: `false`,
		},
		{name: "distance of non geometry", expr: `IS_DEFINED(ST_DISTANCE(1, 2))`, want: `false`},
		{name: "string to array", expr: `StringToArray('[1,2,"a"]')`, want: `[1,2,"a"]`},
		{name: "string to array rejects object", expr: `IS_DEFINED(StringToArray('{"a":1}'))`, want: `false`},
		{name: "string to object", expr: `StringToObject('{"a":{"b":2}}')`, want: `{"a":{"b":2}}`},
		{name: "string to object rejects garbage", expr: `IS_DEFINED(StringToObject('{'))`, want: `false`},
		{name: "to string of number", expr: `ToString(12.5)`, want: `"12.5"`},
		{name: "datetime to ticks", expr: `DateTimeToTicks('2020-01-02T03:04:05.6789123Z')`, want: `15779342456789123`},
		{name: "ticks to datetime", expr: `TicksToDateTime(15943368134575530)`, want: `"2020-07-09T23:20:13.4575530Z"`},
		{
			name: "datetime from parts",
			expr: `DateTimeFromParts(2020, 9, 4, 10, 52, 12, 3456789)`,
			want: `"2020-09-04T10:52:12.3456789Z"`,
		},
		{
			name: "datetime from date only",
			expr: `DateTimeFromParts(2021, 2, 3)`,
			want: `"2021-02-03T00:00:00.0000000Z"`,
		},
		{name: "datetime from invalid parts", expr: `IS_DEFINED(DateTimeFromParts(2021, 13, 3))`, want: `false`},
		{name: "regex x modifier ignores whitespace", expr: `RegexMatch('abc', 'a b c # comment', 'x')`, want: `true`},
		{name: "regex without x keeps whitespace", expr: `RegexMatch('abc', 'a b c')`, want: `false`},
		{name: "regex x keeps class whitespace", expr: `RegexMatch('a b', 'a[ ]b', 'x')`, want: `true`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rows, err := cosmosdb.ExecuteQuery("SELECT VALUE "+tt.expr+" FROM c", nil, docInfos(t, `{"id":"1"}`))
			require.NoError(t, err)
			require.Len(t, rows, 1)

			got, err := json.Marshal(rows[0])
			require.NoError(t, err)
			assert.JSONEq(t, tt.want, string(got))
		})
	}
}
