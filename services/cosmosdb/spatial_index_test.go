package cosmosdb_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func containerBody(id, indexing, geo string) string {
	extra := ""
	if indexing != "" {
		extra += `,"indexingPolicy":` + indexing
	}

	if geo != "" {
		extra += `,"geospatialConfig":{"type":"` + geo + `"}`
	}

	return fmt.Sprintf(`{"id":%q,"partitionKey":{"paths":["/pk"],"kind":"Hash"}%s}`, id, extra)
}

func TestSpatialIndex_Validation(t *testing.T) {
	t.Parallel()

	const bbox = `"boundingBox":{"xmin":0,"ymin":0,"xmax":10,"ymax":10}`

	policy := func(spatial string) string {
		return `{"indexingMode":"consistent","includedPaths":[{"path":"/*"}],"excludedPaths":[],"spatialIndexes":[` +
			spatial + `]}`
	}

	tests := []struct {
		name    string
		spatial string
		geo     string
		want    int
	}{
		{name: "geography point and polygon", spatial: `{"path":"/location/*","types":["Point","Polygon"]}`, want: 201},
		{
			name:    "all four types",
			spatial: `{"path":"/g/?","types":["Point","Polygon","LineString","MultiPolygon"]}`, want: 201,
		},
		{
			name:    "geography ignores a bounding box",
			spatial: `{"path":"/g/*","types":["Point"],` + bbox + `}`,
			want:    201,
		},
		{name: "unknown type", spatial: `{"path":"/g/*","types":["Circle"]}`, want: 400},
		{name: "no types", spatial: `{"path":"/g/*","types":[]}`, want: 400},
		{name: "lower-case type", spatial: `{"path":"/g/*","types":["point"]}`, want: 400},
		{name: "path without a terminator", spatial: `{"path":"/g","types":["Point"]}`, want: 400},
		{name: "path without a slash", spatial: `{"path":"g/*","types":["Point"]}`, want: 400},
		{
			name:    "geometry with a bounding box",
			spatial: `{"path":"/g/*","types":["Polygon"],` + bbox + `}`, geo: "Geometry", want: 201,
		},
		{
			name:    "geometry without a bounding box",
			spatial: `{"path":"/g/*","types":["Polygon"]}`,
			geo:     "Geometry",
			want:    400,
		},
		{
			name:    "geometry with an inverted box",
			spatial: `{"path":"/g/*","types":["Polygon"],"boundingBox":{"xmin":5,"ymin":0,"xmax":1,"ymax":10}}`,
			geo:     "Geometry", want: 400,
		},
		{
			name:    "geometry with a partial box",
			spatial: `{"path":"/g/*","types":["Polygon"],"boundingBox":{"xmin":0,"ymin":0,"xmax":1}}`,
			geo:     "Geometry", want: 400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newLiveServer(t)
			resp := s.do(http.MethodPost, "/dbs/d/colls", nil, containerBody("sp", policy(tt.spatial), tt.geo))
			assert.Equal(t, tt.want, resp.status, string(resp.body))

			if tt.want != http.StatusCreated {
				return
			}

			got := s.do(http.MethodGet, "/dbs/d/colls/sp", nil, "")
			require.Equal(t, http.StatusOK, got.status)
			assert.Contains(t, string(got.body), "spatialIndexes")

			bad := policy(`{"path":"/g/*","types":["Circle"]}`)
			rep := s.do(http.MethodPut, "/dbs/d/colls/sp", nil, containerBody("sp", bad, tt.geo))
			assert.Equal(t, http.StatusBadRequest, rep.status, "replace validates the same rules")
		})
	}
}

func TestStoredProcedure_QueryHonoursIndexingPolicy(t *testing.T) {
	t.Parallel()

	const policy = `{"indexingMode":"consistent","includedPaths":[{"path":"/name/?"}],"excludedPaths":[{"path":"/*"}]}`

	const body = `function f(opts) {
		var coll = getContext().getCollection();
		coll.queryDocuments(coll.getSelfLink(), "SELECT * FROM c WHERE c.other >= 0", opts, function (err, docs, o) {
			if (err) { throw new Error(err.number + ":" + err.message); }
			getContext().getResponse().setBody({n: docs.length, next: o.continuation || null});
		});
	}`

	tests := []struct {
		name     string
		opts     string
		wantBody string
		wantCode int
	}{
		{name: "scan needed and not enabled", opts: `{}`, wantCode: 400, wantBody: "allowscanheader"},
		{name: "scan enabled", opts: `{"enableScan":true}`, wantCode: 200, wantBody: `"n":100,"next":"100"`},
		{name: "page size", opts: `{"enableScan":true,"pageSize":30}`, wantCode: 200, wantBody: `"n":30,"next":"30"`},
		{
			name: "continuation", opts: `{"enableScan":true,"continuation":"100"}`,
			wantCode: 200, wantBody: `"n":20,"next":null`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newLiveServer(t)
			require.Equal(t, http.StatusCreated,
				s.do(http.MethodPost, "/dbs/d/colls", nil, containerBody("x", policy, "")).status)

			for i := range 120 {
				doc := fmt.Sprintf(`{"id":"i%d","pk":"a","name":"n","other":%d}`, i, i)
				require.Equal(t, http.StatusCreated, s.do(http.MethodPost, "/dbs/d/colls/x/docs",
					map[string]string{"X-Ms-Documentdb-Partitionkey": pkA}, doc).status)
			}

			require.Equal(t, http.StatusCreated, s.do(http.MethodPost, "/dbs/d/colls/x/sprocs", nil,
				scriptJSON(t, map[string]string{"id": "f", "body": body})).status)

			resp := s.do(http.MethodPost, "/dbs/d/colls/x/sprocs/f",
				map[string]string{"X-Ms-Documentdb-Partitionkey": pkA}, "["+tt.opts+"]")
			assert.Equal(t, tt.wantCode, resp.status, string(resp.body))
			assert.Contains(t, strings.ReplaceAll(string(resp.body), " ", ""), tt.wantBody)
		})
	}
}
