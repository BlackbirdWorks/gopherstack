package sparql_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune/sparql"
)

func TestExpressions(t *testing.T) {
	t.Parallel()

	const (
		xsd = "^^<http://www.w3.org/2001/XMLSchema#"
	)

	tests := []struct {
		name string
		expr string
		want string
	}{
		{"int arithmetic", `1 + 2 * 3 - 4`, `"3"` + xsd + `integer>`},
		{"int division is decimal", `7 / 2`, `"3.5"` + xsd + `decimal>`},
		{"decimal exact", `0.1 + 0.2`, `"0.3"` + xsd + `decimal>`},
		{"double", `1.5e0 * 2`, `"3.0E0"` + xsd + `double>`},
		{"division by zero is unbound", `1 / 0`, `-`},
		{"unary minus", `-(2 + 3)`, `"-5"` + xsd + `integer>`},
		{"comparison chain", `(1 < 2) && (2 <= 2) && (3 > 2) && !(2 != 2)`, `"true"` + xsd + `boolean>`},
		{"logical error handling or", `(1/0 > 1) || true`, `"true"` + xsd + `boolean>`},
		{"logical error handling and", `(1/0 > 1) && false`, `"false"` + xsd + `boolean>`},
		{"string compare", `"abc" < "abd"`, `"true"` + xsd + `boolean>`},
		{"mixed numeric equality", `1 = 1.0`, `"true"` + xsd + `boolean>`},
		{"incomparable types equal syntactically", `"a" = 1`, `"false"` + xsd + `boolean>`},
		{"strlen utf8", `STRLEN("héllo")`, `"5"` + xsd + `integer>`},
		{"substr", `SUBSTR("foobar", 4)`, `"bar"`},
		{"substr len", `SUBSTR("foobar", 2, 3)`, `"oob"`},
		{"strafter", `STRAFTER("a=b", "=")`, `"b"`},
		{"strstarts", `STRSTARTS("foobar", "foo") && STRENDS("foobar", "bar")`, `"true"` + xsd + `boolean>`},
		{"encode_for_uri", `ENCODE_FOR_URI("a b/c")`, `"a%20b%2Fc"`},
		{"regex replace groups", `REPLACE("2020-01-02", "(\\d+)-(\\d+)-(\\d+)", "$3/$2/$1")`, `"02/01/2020"`},
		{"concat lang", `CONCAT("a"@en, "b"@en)`, `"ab"@en`},
		{"concat mixed", `CONCAT("a"@en, "b")`, `"ab"`},
		{"ucase lang", `UCASE("abc"@en)`, `"ABC"@en`},
		{"abs ceil floor round", `ABS(-2) + CEIL(1.2) + FLOOR(1.8) + ROUND(2.5)`, `"8.0"` + xsd + `decimal>`},
		{
			"year month day",
			`YEAR("2020-03-04T05:06:07Z"^^xsd:dateTime) * 10000 + ` +
				`MONTH("2020-03-04T05:06:07Z"^^xsd:dateTime) * 100 + DAY("2020-03-04T05:06:07Z"^^xsd:dateTime)`,
			`"20200304"` + xsd + `integer>`,
		},
		{"hours minutes seconds", `HOURS("2020-03-04T05:06:07Z"^^xsd:dateTime)`, `"5"` + xsd + `integer>`},
		{
			"datetime compare",
			`"2020-01-01T10:00:00+02:00"^^xsd:dateTime = "2020-01-01T08:00:00Z"^^xsd:dateTime`,
			`"true"` + xsd + `boolean>`,
		},
		{"md5", `MD5("abc")`, `"900150983cd24fb0d6963f7d28e17f72"`},
		{"sha256", `SHA256("")`, `"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"`},
		{"cast integer", `xsd:integer("42") + xsd:integer(3.9)`, `"45"` + xsd + `integer>`},
		{"cast double", `xsd:double("1.5")`, `"1.5E0"` + xsd + `double>`},
		{"cast boolean", `xsd:boolean("1")`, `"true"` + xsd + `boolean>`},
		{"cast decimal", `xsd:decimal("2.50")`, `"2.5"` + xsd + `decimal>`},
		{"cast bad", `xsd:integer("abc")`, `-`},
		{"sameTerm", `sameTerm(1, 1.0)`, `"false"` + xsd + `boolean>`},
		{
			"isnumeric isliteral",
			`isNumeric(1) && isLiteral("a") && !isIRI("a") && isIRI(<urn:a>) && !isBlank(<urn:a>)`,
			`"true"` + xsd + `boolean>`,
		},
		{"strdt strlang", `STRDT("5", xsd:integer) + 1`, `"6"` + xsd + `integer>`},
		{"lang datatype", `DATATYPE("a"@en)`, `<http://www.w3.org/1999/02/22-rdf-syntax-ns#langString>`},
		{"datatype plain", `DATATYPE("a")`, `<http://www.w3.org/2001/XMLSchema#string>`},
		{"iri function", `IRI("urn:x")`, `<urn:x>`},
		{"coalesce", `COALESCE(?unbound, 1/0, "ok")`, `"ok"`},
		{"if error", `IF(1/0, 1, 2)`, `-`},
		{"in", `2 IN (1, 2, 3)`, `"true"` + xsd + `boolean>`},
		{"langmatches", `LANGMATCHES("en-US", "EN")`, `"true"` + xsd + `boolean>`},
		{"nan equals", `xsd:double("NaN") = xsd:double("NaN")`, `"true"` + xsd + `boolean>`},
		{"inf", `xsd:double("INF") > 1e300`, `"true"` + xsd + `boolean>`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res, err := sparql.NewStore().Query(context.Background(),
				"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> SELECT ?r WHERE { BIND("+tt.expr+" AS ?r) }")
			require.NoError(t, err)
			require.Len(t, res.Rows, 1)

			got := "-"
			if v, ok := res.Rows[0]["r"]; ok {
				got = v.String()
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestQueryFeatureGaps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  int
	}{
		{
			"nested optional",
			`SELECT * WHERE { ?p foaf:name ?n OPTIONAL { ?p foaf:knows ?k OPTIONAL { ?k foaf:age ?a } } }`,
			5,
		},
		{
			"union inside optional",
			`SELECT * WHERE { ?p foaf:name ?n OPTIONAL { { ?p foaf:age ?a } UNION { ?p ex:city ?a } } }`,
			5,
		},
		{"group by expression", `SELECT ?g (COUNT(*) AS ?c) WHERE { ?p foaf:age ?a } GROUP BY (?a > 28 AS ?g)`, 2},
		{"order by expression", `SELECT ?n WHERE { ?p foaf:name ?n } ORDER BY DESC(STRLEN(?n)) ?n`, 4},
		{"minus disjoint vars keeps rows", `SELECT ?p WHERE { ?p a foaf:Person MINUS { ?x foaf:age 30 } }`, 4},
		{
			"filter in optional uses outer vars",
			`SELECT * WHERE { ?p foaf:name ?n OPTIONAL { ?p foaf:age ?a FILTER(?a > 28) } }`,
			4,
		},
		{
			"bind then filter",
			`SELECT ?n WHERE { ?p foaf:age ?a BIND(?a + 1 AS ?b) FILTER(?b > 30) ?p foaf:name ?n }`,
			2,
		},
		{"values after pattern", `SELECT ?n WHERE { ?p foaf:name ?n } VALUES ?n { "Bob" "Alice" }`, 2},
		{"construct property path", `CONSTRUCT { ?a ex:reach ?b } WHERE { ?a foaf:knows+ ?b }`, 0},
		{"blank node patterns", `SELECT ?n WHERE { [ foaf:name ?n ; foaf:age 25 ] }`, 1},
		{"collection pattern is empty", `SELECT ?x WHERE { ?x ex:nothing (1 2) }`, 0},
		{"count distinct star", `SELECT (COUNT(DISTINCT *) AS ?c) WHERE { ?p foaf:knows ?k }`, 1},
		{"sample", `SELECT (SAMPLE(?n) AS ?s) WHERE { ?p foaf:name ?n }`, 1},
		{"sum over empty", `SELECT (SUM(?x) AS ?s) WHERE { ?p ex:nothing ?x }`, 1},
		{
			"subselect aggregate",
			`SELECT ?p ?c WHERE { { SELECT ?p (COUNT(?k) AS ?c) WHERE { ?p foaf:knows ` +
				`?k } GROUP BY ?p } FILTER(?c > 1) }`,
			1,
		},
		{"graph with filter", `SELECT ?g WHERE { GRAPH ?g { ?s ex:score ?v FILTER(?v > 15) } }`, 2},
		{"exists in graph", `SELECT ?s WHERE { ?s a foaf:Person FILTER EXISTS { GRAPH ?g { ?s ex:score ?v } } }`, 3},
		{"offset past end", `SELECT ?n WHERE { ?p foaf:name ?n } OFFSET 10`, 0},
		{"limit zero", `SELECT ?n WHERE { ?p foaf:name ?n } LIMIT 0`, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res, err := newFixtureStore(t).Query(context.Background(), pfx+tt.query)
			require.NoError(t, err)

			if tt.name == "construct property path" {
				assert.Len(t, res.Triples, 3)

				return
			}

			assert.Len(t, res.Rows, tt.want)
		})
	}
}
