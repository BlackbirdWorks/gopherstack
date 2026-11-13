package neptune_test

import (
	"encoding/json/v2"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func TestPropertyGraphBulkLoader(t *testing.T) {
	t.Parallel()

	const (
		gVerts = "~id,name:String,age:Int,interests:String[],~label\n" +
			"v1,marko,29,\"sailing;graphs\",person\n" +
			"v2,lop,,,software\n"
		gEdges = "~id,~from,~to,~label,weight:Double\ne1,v1,v2,created,0.4\n"
	)

	tests := []struct {
		files   map[string]string
		counts  map[string]float64
		name    string
		format  string
		extra   string
		query   string
		status  string
		errCode string
		want    []any
	}{
		{
			name: "gremlin vertices and edges", format: "csv",
			files:  map[string]string{"d/b-edges.csv": gEdges, "d/a-verts.csv": gVerts},
			status: "LOAD_COMPLETED",
			counts: map[string]float64{"totalRecords": 9, "totalDuplicates": 0, "parsingErrors": 0},
			query:  "MATCH (a:person)-[r:created]->(b:software) RETURN a.name AS a, r.weight AS w, b.name AS b",
			want: []any{map[string]any{
				"a": "marko", "w": 0.4, "b": "lop",
			}},
		},
		{
			name:   "duplicate rows",
			format: "csv",
			files: map[string]string{
				"d/v.csv": "~id,~label,name,team\nP-2,Player,Kohli,India\nP-2,Player,Kohli,India\n",
			},
			status: "LOAD_COMPLETED",
			counts: map[string]float64{"totalRecords": 6, "totalDuplicates": 3},
			query:  "MATCH (n:Player) RETURN count(n) AS c",
			want:   []any{map[string]any{"c": float64(1)}},
		},
		{
			name: "single cardinality violation", format: "csv", extra: `"failOnError":"TRUE"`,
			files:  map[string]string{"d/v.csv": "~id,age:Int(single)\nv1,1\nv1,2\n"},
			status: "LOAD_FAILED",
			counts: map[string]float64{"insertErrors": 1}, errCode: "SINGLE_CARDINALITY_VIOLATION",
			query: "MATCH (n) RETURN n.age AS age",
			want:  []any{map[string]any{"age": float64(1)}},
		},
		{
			name: "update single cardinality", format: "csv", extra: `"updateSingleCardinalityProperties":"TRUE"`,
			files:  map[string]string{"d/v.csv": "~id,age:Int(single)\nv1,1\nv1,2\n"},
			status: "LOAD_COMPLETED",
			query:  "MATCH (n) RETURN n.age AS age",
			want:   []any{map[string]any{"age": float64(2)}},
		},
		{
			name: "missing edge endpoint", format: "csv", extra: `"failOnError":"FALSE"`,
			files: map[string]string{
				"d/v.csv": "~id\nv1\n", "d/e.csv": "~id,~from,~to,~label\ne1,v1,vX,knows\n",
			},
			status: "LOAD_COMPLETED",
			counts: map[string]float64{"insertErrors": 1}, errCode: "FROM_OR_TO_VERTEX_ARE_MISSING",
			query: "MATCH ()-[r]->() RETURN count(r) AS c",
			want:  []any{map[string]any{"c": float64(0)}},
		},
		{
			name: "bad int skips row", format: "csv", extra: `"failOnError":"FALSE"`,
			files:  map[string]string{"d/v.csv": "~id,age:Int\nv1,abc\nv2,7\n"},
			status: "LOAD_COMPLETED",
			counts: map[string]float64{"datatypeMismatchErrors": 1}, errCode: "PARSING_ERROR",
			query: "MATCH (n) RETURN n.age AS age",
			want:  []any{map[string]any{"age": float64(7)}},
		},
		{
			name: "bad header fails", format: "csv",
			files:  map[string]string{"d/v.csv": "~id,x:Nope\nv1,1\n"},
			status: "LOAD_FAILED",
			counts: map[string]float64{"parsingErrors": 1}, errCode: "PARSING_ERROR",
			query: "MATCH (n) RETURN count(n) AS c",
			want:  []any{map[string]any{"c": float64(0)}},
		},
		{
			name:   "opencypher nodes and rels",
			format: "opencypher",
			extra:  `"userProvidedEdgeIds":"TRUE"`,
			files: map[string]string{
				"d/n.csv": ":ID,name:String,age:Int,:LABEL\nv1,marko,29,person\nv2,lop,,software;thing\n",
				"d/r.csv": ":ID,:START_ID,:END_ID,:TYPE,weight:Double\ne1,v1,v2,created,0.4\n",
			},
			status: "LOAD_COMPLETED",
			query:  "MATCH (a:person)-[r:created]->(b:thing) RETURN a.name AS a, a.age AS age, r.weight AS w, id(r) AS id",
			want:   []any{map[string]any{"a": "marko", "age": float64(29), "w": 0.4, "id": "e1"}},
		},
		{
			name: "opencypher id as property and spaces", format: "opencypher",
			files: map[string]string{
				"d/a.csv": "name:ID(person),age:Int,:LABEL\nmarko,29,person\n",
				"d/b.csv": "name:ID(software),:LABEL\nlop,software\n",
				"d/r.csv": ":ID,:START_ID(person),:END_ID(software),:TYPE\ne1,marko,lop,created\n",
			},
			status: "LOAD_COMPLETED",
			query:  "MATCH (a)-[:created]->(b) RETURN a.name AS a, b.name AS b",
			want:   []any{map[string]any{"a": "marko", "b": "lop"}},
		},
		{
			name: "opencypher generated rel ids", format: "opencypher", extra: `"userProvidedEdgeIds":false`,
			files: map[string]string{
				"d/n.csv": ":ID,:LABEL\na,x\nb,x\n", "d/r.csv": ":START_ID,:END_ID,:TYPE\na,b,rel\n",
			},
			status: "LOAD_COMPLETED",
			query:  "MATCH ()-[r:rel]->() RETURN count(r) AS c",
			want:   []any{map[string]any{"c": float64(1)}},
		},
		{
			name: "opencypher rel id column rejected when not user provided", format: "opencypher",
			extra: `"userProvidedEdgeIds":"FALSE"`,
			files: map[string]string{
				"d/n.csv": ":ID\na\nb\n", "d/r.csv": ":ID,:START_ID,:END_ID,:TYPE\ne1,a,b,rel\n",
			},
			status: "LOAD_FAILED", counts: map[string]float64{"parsingErrors": 1}, errCode: "PARSING_ERROR",
			query: "MATCH (n) RETURN count(n) AS c",
			want:  []any{map[string]any{"c": float64(2)}},
		},
		{
			name: "opencypher duplicate node keeps first value", format: "opencypher",
			files:  map[string]string{"d/n.csv": ":ID,age:Int,:LABEL\na,1,x\na,2,y\n"},
			status: "LOAD_COMPLETED",
			query:  "MATCH (n) RETURN n.age AS age, labels(n) AS l",
			want:   []any{map[string]any{"age": float64(1), "l": []any{"x", "y"}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newLoaderCluster(t, tt.files)
			body := `{"source":"s3://lake/d/","format":"` + tt.format + `","iamRoleArn":"arn:aws:iam::000000000000:role/r"`
			if tt.extra != "" {
				body += "," + tt.extra
			}

			code, env, raw := loaderStart(t, c, body+"}")
			require.Equal(t, http.StatusOK, code, raw)

			id, _ := env.Payload["loadId"].(string)
			got := loaderWait(t, c, id, "?errors=TRUE&details=TRUE")
			assert.Equal(t, tt.status, overall(got)["status"], "%v", got.Payload)

			for k, v := range tt.counts {
				assert.InDelta(t, v, overall(got)[k], 0, k)
			}

			if tt.errCode != "" {
				errs, _ := got.Payload["errors"].(map[string]any)
				logs, _ := errs["errorLogs"].([]any)
				require.NotEmpty(t, logs)
				assert.Equal(t, tt.errCode, logs[0].(map[string]any)["errorCode"])
			}

			res := sparqlDo(t, c, http.MethodPost, "/openCypher", "application/json", "",
				mustJSON(t, map[string]any{"query": tt.query}))
			require.Equal(t, http.StatusOK, res.status, res.body)

			var out struct {
				Results []any `json:"results"`
			}

			require.NoError(t, json.Unmarshal([]byte(res.body), &out, jsonx.V1()))
			assert.Equal(t, tt.want, out.Results)
		})
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()

	b, err := json.Marshal(v, jsonx.V1())
	require.NoError(t, err)

	return string(b)
}
