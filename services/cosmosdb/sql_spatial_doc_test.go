package cosmosdb_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

const (
	hqPoint    = `{"type":"Point","coordinates":[-122.12826822304672,47.63980239335718]}`
	rdPoint    = `{"type":"Point","coordinates":[-96.84369,46.81298]}`
	refPoint   = `{"type":"Point","coordinates":[-122.11758113953535,47.66901087006131]}`
	docPolygon = `{"type":"Polygon","coordinates":[[[31.8,-5],[32,-5],[32,-4.7],[31.8,-4.7],[31.8,-5]]]}`
	cwPolygon  = `{"type":"Polygon","coordinates":[[[31.8,-5],[31.8,-4.7],[32,-4.7],[32,-5],[31.8,-5]]]}`
	multiPoly  = `{"type":"MultiPolygon","coordinates":[[[[52,12],[53,12],[53,13],[52,13],[52,12]]],` +
		`[[[50,0],[51,0],[51,5],[50,5],[50,0]]]]}`
)

func evalOne(t *testing.T, expr string) any {
	t.Helper()

	rows, err := cosmosdb.ExecuteQuery("SELECT VALUE "+expr+" FROM c", nil, docInfos(t, `{"id":"1"}`))
	require.NoError(t, err)
	require.Len(t, rows, 1)

	return rows[0]
}

func asFloat(t *testing.T, v any) float64 {
	t.Helper()

	n, ok := v.(json.Number)
	require.True(t, ok, "%T", v)

	f, err := n.Float64()
	require.NoError(t, err)

	return f
}

func TestSpatial_GeographyMatchesTheDocumentedExamples(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		expr string
		want float64
		tol  float64
	}{
		{
			name: "distance to the headquarters",
			expr: `ST_DISTANCE(` + hqPoint + `, ` + refPoint + `)`,
			want: 3345.269817267368,
			tol:  0.5e-3,
		},
		{
			name: "distance to the research office",
			expr: `ST_DISTANCE(` + rdPoint + `, ` + refPoint + `)`,
			want: 1907438.421299902,
			tol:  1,
		},
		{name: "polygon area in square metres", expr: `ST_AREA(` + docPolygon + `)`, want: 735970283.0522614, tol: 1},
		{
			name: "a clockwise ring has the same area",
			expr: `ST_AREA(` + cwPolygon + `)`,
			want: 735970283.0522614,
			tol:  1,
		},
		{name: "points and lines have no area", expr: `ST_AREA(` + hqPoint + `)`, want: 0, tol: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.InDelta(t, tt.want, asFloat(t, evalOne(t, tt.expr)), tt.tol)
		})
	}
}

func TestSpatial_GeographySemantics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		expr string
		want string
	}{
		{
			name: "invalid latitude reason",
			expr: `ST_ISVALIDDETAILED({"type":"Point","coordinates":[133.75682784306348,-184.38876194345323]})`,
			want: `{"valid":false,"reason":"Latitude values must be between -90 and 90 degrees."}`,
		},
		{
			name: "invalid longitude reason",
			expr: `ST_ISVALIDDETAILED({"type":"Point","coordinates":[200,10]})`,
			want: `{"valid":false,"reason":"Longitude values must be between -180 and 180 degrees."}`,
		},
		{
			name: "both axes",
			expr: `ST_ISVALIDDETAILED({"type":"Point","coordinates":[200,100]})`,
			want: `{"valid":false,"reason":"Latitude values must be between -90 and 90 degrees. ` +
				`Longitude values must be between -180 and 180 degrees."}`,
		},
		{
			name: "valid point",
			expr: `ST_ISVALIDDETAILED({"type":"Point","coordinates":[-84.38876194345323,33.75682784306348]})`,
			want: `{"valid":true}`,
		},
		{name: "multipolygon is valid", expr: `ST_ISVALID(` + multiPoly + `)`, want: `true`},
		{
			name: "point in the second polygon",
			expr: `ST_WITHIN({"type":"Point","coordinates":[50.5,2]}, ` + multiPoly + `)`,
			want: `true`,
		},
		{
			name: "point between the polygons",
			expr: `ST_WITHIN({"type":"Point","coordinates":[51.5,2]}, ` + multiPoly + `)`,
			want: `false`,
		},
		{
			name: "intersects a multipolygon",
			expr: `ST_INTERSECTS({"type":"LineString","coordinates":[[49,2],[52.5,12.5]]}, ` + multiPoly + `)`,
			want: `true`,
		},
		{
			name: "multipolygon area is the sum",
			expr: `ST_AREA(` + multiPoly +
				`) > ST_AREA({"type":"Polygon","coordinates":[[[50,0],[51,0],[51,5],[50,5],[50,0]]]})`,
			want: `true`,
		},
		{
			name: "counter-clockwise ring holds its inside",
			expr: `ST_WITHIN({"type":"Point","coordinates":[31.9,-4.8]}, ` + docPolygon + `)`,
			want: `true`,
		},
		{
			name: "counter-clockwise ring excludes the outside",
			expr: `ST_WITHIN({"type":"Point","coordinates":[40,10]}, ` + docPolygon + `)`,
			want: `false`,
		},
		{
			name: "clockwise ring is the inverse region: inside excluded",
			expr: `ST_WITHIN({"type":"Point","coordinates":[31.9,-4.8]}, ` + cwPolygon + `)`,
			want: `false`,
		},
		{
			name: "clockwise ring is the inverse region: outside included",
			expr: `ST_WITHIN({"type":"Point","coordinates":[40,10]}, ` + cwPolygon + `)`,
			want: `true`,
		},
		{
			name: "boundary counts as within",
			expr: `ST_WITHIN({"type":"Point","coordinates":[31.8,-4.85]}, ` + docPolygon + `)`,
			want: `true`,
		},
		{
			name: "polygon with a hole",
			expr: `ST_WITHIN({"type":"Point","coordinates":[1,1]}, {"type":"Polygon","coordinates":[` +
				`[[0,0],[4,0],[4,4],[0,4],[0,0]],[[0.5,0.5],[0.5,1.5],[1.5,1.5],[1.5,0.5],[0.5,0.5]]]})`,
			want: `false`,
		},
		{
			name: "outside the hole",
			expr: `ST_WITHIN({"type":"Point","coordinates":[3,3]}, {"type":"Polygon","coordinates":[` +
				`[[0,0],[4,0],[4,4],[0,4],[0,0]],[[0.5,0.5],[0.5,1.5],[1.5,1.5],[1.5,0.5],[0.5,0.5]]]})`,
			want: `true`,
		},
		{
			name: "a polygon across the antimeridian",
			expr: `ST_WITHIN({"type":"Point","coordinates":[-179.5,0]}, {"type":"Polygon","coordinates":[` +
				`[[179,-1],[-179,-1],[-179,1],[179,1],[179,-1]]]})`,
			want: `true`,
		},
		{
			name: "distance across the antimeridian is short",
			expr: `ST_DISTANCE({"type":"Point","coordinates":[179.9,0]}, {"type":"Point","coordinates":[-179.9,0]}) < 25000`,
			want: `true`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := json.Marshal(evalOne(t, tt.expr))
			require.NoError(t, err)
			assert.JSONEq(t, tt.want, string(got))
		})
	}
}

func TestSpatial_GeometryContainersUsePlanarMath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		geo  string
		expr string
		want string
	}{
		{
			geo:  "Geometry",
			expr: `ST_DISTANCE({"type":"Point","coordinates":[0,0]},{"type":"Point","coordinates":[3,4]})`,
			want: `5`,
		},
		{
			geo:  "Geography",
			expr: `ST_DISTANCE({"type":"Point","coordinates":[0,0]},{"type":"Point","coordinates":[3,4]}) > 500000`,
			want: `true`,
		},
		{
			geo:  "Geometry",
			expr: `ST_AREA({"type":"Polygon","coordinates":[[[0,0],[2,0],[2,3],[0,3],[0,0]]]})`,
			want: `6`,
		},
		{
			geo: "Geometry",
			expr: `ST_WITHIN({"type":"Point","coordinates":[1,1]},` +
				`{"type":"Polygon","coordinates":[[[0,0],[0,3],[2,3],[2,0],[0,0]]]})`,
			want: `true`,
		},
		{geo: "Geometry", expr: `ST_ISVALID({"type":"Point","coordinates":[500,500]})`, want: `true`},
		{geo: "Geography", expr: `ST_ISVALID({"type":"Point","coordinates":[500,500]})`, want: `false`},
		{
			geo:  "Geometry",
			expr: `ST_DISTANCE({"type":"Point","coordinates":[5,0]},{"type":"LineString","coordinates":[[0,3],[10,3]]})`,
			want: `3`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.geo+"/"+tt.expr[:20], func(t *testing.T) {
			t.Parallel()

			b := cosmosdb.NewInMemoryBackend()
			_, err := b.CreateDatabase("d")
			require.NoError(t, err)

			_, err = b.CreateContainer("d", cosmosdb.ContainerSpec{
				ID:               "c",
				PartitionKeyPath: "/pk",
				GeospatialConfig: json.RawMessage(`{"type":"` + tt.geo + `"}`),
			})
			require.NoError(t, err)
			_, err = b.CreateDocument("d", "c", map[string]any{"id": "1", "pk": "k"}, false)
			require.NoError(t, err)

			res, err := b.RunQuery("d", "c", "SELECT VALUE "+tt.expr+" FROM c", nil, "")
			require.NoError(t, err)
			require.Len(t, res.Rows, 1)

			got, err := json.Marshal(res.Rows[0])
			require.NoError(t, err)
			assert.JSONEq(t, tt.want, string(got))
		})
	}
}
