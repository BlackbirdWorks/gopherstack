package cosmosdb_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

// Each case is an example from the query-language reference (Learn, "Functions"), with the output shown there.
func TestSQLFunctions_DocumentedExamples(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		expr string
		want string
	}{
		{name: "atn2", expr: `ATN2(35.175643, 129.44)`, want: `0.265344532064832`},
		{
			name: "numberbin",
			expr: `{a: NUMBERBIN(37.752, -100), b: NUMBERBIN(37.752, 10), c: NUMBERBIN(37.752, 1), ` +
				`d: NUMBERBIN(37.752, 0), e: NUMBERBIN(37.752, 0.1), f: NUMBERBIN(37.752, 0.01)}`,
			want: `{"a":100,"b":30,"c":37,"e":37.7,"f":37.75}`,
		},
		{name: "numberbin default size", expr: `NUMBERBIN(-37.5)`, want: `-38`},
		{
			name: "intadd",
			expr: `{n: INTADD(20, 10), z: INTADD(20, 0), d: INTADD(20, 0.10)}`,
			want: `{"n":30,"z":20}`,
		},
		{
			name: "intdiv",
			expr: `{a: INTDIV(10, 2), b: INTDIV(10, -2), c: INTDIV(-10, -2), d: INTDIV(10, 10), e: INTDIV(10, 0), ` +
				`f: INTDIV(10, 0.1)}`,
			want: `{"a":5,"b":-5,"c":5,"d":1}`,
		},
		{
			name: "intmod",
			expr: `{a: INTMOD(12, 5), b: INTMOD(12, -5), c: INTMOD(-12, -5), d: INTMOD(15, 5), e: INTMOD(12, 0), ` +
				`f: INTMOD(12, 0.2)}`,
			want: `{"a":2,"b":2,"c":-2,"d":0}`,
		},
		{
			name: "intmul",
			expr: `{a: INTMUL(5, 2), b: INTMUL(5, -2), c: INTMUL(-5, -2), d: INTMUL(5, INTMUL(5, 5)), ` +
				`e: INTMUL(5, 0), f: INTMUL(5, 0.5)}`,
			want: `{"a":10,"b":-10,"c":10,"d":125,"e":0}`,
		},
		{name: "intsub", expr: `{a: INTSUB(20, 10), b: INTSUB(20, 0.5)}`, want: `{"a":10}`},
		{
			name: "intbitand",
			expr: `{a: INTBITAND(15, 25), b: INTBITAND(15, 0), c: INTBITAND(15, 15), d: INTBITAND(15, 1.5)}`,
			want: `{"a":9,"b":0,"c":15}`,
		},
		{name: "intbitor", expr: `INTBITOR(15, 25)`, want: `31`},
		{name: "intbitxor", expr: `INTBITXOR(15, 25)`, want: `22`},
		{
			name: "intbitnot",
			expr: `{a: INTBITNOT(65), b: INTBITNOT(0), c: INTBITNOT(0.1)}`,
			want: `{"a":-66,"b":-1}`,
		},
		{
			name: "intbitleftshift",
			expr: `{a: INTBITLEFTSHIFT(16, 4), b: INTBITLEFTSHIFT(16, 0.4)}`, want: `{"a":256}`,
		},
		{
			name: "intbitrightshift",
			expr: `{a: INTBITRIGHTSHIFT(16, 4), b: INTBITRIGHTSHIFT(16, 0.4)}`, want: `{"a":1}`,
		},
		{name: "int overflow has no result", expr: `IS_DEFINED(INTMUL(9223372036854775807, 2))`, want: `false`},
		{name: "choose first", expr: `CHOOSE(1, "Vimero", "Hydration", "Pack")`, want: `"Vimero"`},
		{
			name: "choose by index",
			expr: `{i0: CHOOSE(0, "Mt.", "Hood"), i1: CHOOSE(1, "Mt.", "Hood"), i2: CHOOSE(2, "Mt.", "Hood"), ` +
				`i3: CHOOSE(3, "Mt.", "Hood")}`,
			want: `{"i1":"Mt.","i2":"Hood"}`,
		},
		{
			name: "objecttoarray",
			expr: `OBJECTTOARRAY({"a": "12345", "b": "67890"})`,
			want: `[{"k":"a","v":"12345"},{"k":"b","v":"67890"}]`,
		},
		{
			name: "objecttoarray custom names",
			expr: `OBJECTTOARRAY({"color": "blue"}, "key", "value")`,
			want: `[{"key":"color","value":"blue"}]`,
		},
		{
			name: "array_contains_all",
			expr: `{a: ARRAY_CONTAINS_ALL([1, true, "3", [1,2,3]], 1, true, "3", [1,2,3]), ` +
				`b: ARRAY_CONTAINS_ALL([1, 2, 3, 4], 2, 3, 4, 5), c: ARRAY_CONTAINS_ALL([1, 2, 3, 4], 1, undefined), ` +
				`d: ARRAY_CONTAINS_ALL([1, 2, 3, 4], 5, 6, 7, 8), e: ARRAY_CONTAINS_ALL([], 1, 2, 3), ` +
				`f: ARRAY_CONTAINS_ALL([1, 2, 3, 4], 5, undefined)}`,
			want: `{"a":true,"b":false,"d":false,"e":false,"f":false}`,
		},
		{
			name: "array_contains_any",
			expr: `{a: ARRAY_CONTAINS_ANY([1, 2, 3], 5, 2), b: ARRAY_CONTAINS_ANY([1, 2, 3], 5, 6), ` +
				`c: ARRAY_CONTAINS_ANY([1, 2, 3], 5, undefined)}`,
			want: `{"a":true,"b":false}`,
		},
		{
			name: "substringafter",
			expr: `{a: SUBSTRINGAFTER("contoso|north|2026", "|"), b: SUBSTRINGAFTER("bike::touring::frame", "::")}`,
			want: `{"a":"north|2026","b":"touring::frame"}`,
		},
		{name: "substringbefore", expr: `SUBSTRINGBEFORE("contoso|north|2026", "|")`, want: `"contoso"`},
		{
			name: "lastsubstringbefore",
			expr: `{a: LASTSUBSTRINGBEFORE("/products/shoes/boots", "/"), b: LASTSUBSTRINGBEFORE("A-B-C", "-")}`,
			want: `{"a":"/products/shoes","b":"A-B"}`,
		},
		{name: "lastsubstringafter", expr: `LASTSUBSTRINGAFTER("/products/shoes/boots", "/")`, want: `"boots"`},
		{
			name: "stringjoin",
			expr: `{a: STRINGJOIN(["Iropa", "Mountain", "Bike"], " "), b: STRINGJOIN(["Iropa", "Mountain", "Bike"], ""), ` +
				`c: STRINGJOIN(["Iropa", "Mountain", "Bike"], undefined), d: STRINGJOIN(["6", "7", "4", "3"], "A"), ` +
				`e: STRINGJOIN(["Adventure", "LT"], "Works")}`,
			want: `{"a":"Iropa Mountain Bike","b":"IropaMountainBike","d":"6A7A4A3","e":"AdventureWorksLT"}`,
		},
		{
			name: "stringsplit",
			expr: `{a: STRINGSPLIT("Handlebar", "e"), b: STRINGSPLIT("CARBON_STEEL_BIKE_WHEEL", "_"), ` +
				`c: STRINGSPLIT("xenmoun mountain bike", "moun"), d: STRINGSPLIT("AluminumBikeFrame", undefined), ` +
				`e: STRINGSPLIT("Helmet", ""), f: STRINGSPLIT("", "")}`,
			want: `{"a":["Handl","bar"],"b":["CARBON","STEEL","BIKE","WHEEL"],"c":["xen"," ","tain bike"],` +
				`"e":["Helmet"],"f":[""]}`,
		},
		{
			name: "regexextract",
			expr: `{a: REGEXEXTRACT("SKU-93821-BLUE", "[A-Z]+"), b: REGEXEXTRACT("SKU-93821-BLUE", "[0-9]+"), ` +
				`c: REGEXEXTRACT("sku-93821-blue sku-48152-red", "sku-([0-9]+)-[a-z]+", "i", 1)}`,
			want: `{"a":"SKU","b":"93821","c":"93821"}`,
		},
		{
			name: "regexextractall",
			expr: `{a: REGEXEXTRACTALL("A12-B34-C56", "[0-9]+"), b: REGEXEXTRACTALL("north-east-west", "[a-z]+"), ` +
				`c: REGEXEXTRACTALL("sku-93821-blue sku-48152-red", "sku-([0-9]+)-[a-z]+", "i", 1)}`,
			want: `{"a":["12","34","56"],"b":["north","east","west"],"c":["93821","48152"]}`,
		},
		{
			name: "regexreplace",
			expr: `{a: REGEXREPLACE("A12-B34-C56", "[0-9]+", "#"), b: REGEXREPLACEALL("A12-B34-C56", "[0-9]+", "#")}`,
			want: `{"a":"A#-B34-C56","b":"A#-B#-C#"}`,
		},
		{
			name: "datetimebin",
			expr: `{day: DATETIMEBIN("2021-01-08T18:35:00.0000000", "dd"), ` +
				`hour: DATETIMEBIN("2021-01-08T18:35:00.0000000", "hh"), ` +
				`sec: DATETIMEBIN("2021-01-08T18:35:00.0000000", "ss"), ` +
				`five: DATETIMEBIN("2021-01-08T18:35:00.0000000", "hh", 5), ` +
				`seven: DATETIMEBIN("2021-01-08T18:35:00.0000000", "dd", 7), ` +
				`win: DATETIMEBIN("2021-01-08T18:35:00.0000000", "dd", 7, "1601-01-01T00:00:00.0000000")}`,
			want: `{"day":"2021-01-08T00:00:00.0000000Z","hour":"2021-01-08T18:00:00.0000000Z",` +
				`"sec":"2021-01-08T18:35:00.0000000Z","five":"2021-01-08T15:00:00.0000000Z",` +
				`"seven":"2021-01-07T00:00:00.0000000Z","win":"2021-01-04T00:00:00.0000000Z"}`,
		},
		{
			name: "datetimebin invalid",
			expr: `{a: IS_DEFINED(DATETIMEBIN("2021-01-08T18:35:00", "xx")), ` +
				`b: IS_DEFINED(DATETIMEBIN("2021-01-08", "dd", 0)), ` +
				`c: IS_DEFINED(DATETIMEBIN("2021-01-08", "dd", -1)), d: IS_DEFINED(DATETIMEBIN("nope", "dd")), ` +
				`e: IS_DEFINED(DATETIMEBIN("2021-01-08", "dd", 1, "1500-01-01T00:00:00"))}`,
			want: `{"a":false,"b":false,"c":false,"d":false,"e":false}`,
		},
		{
			name: "iif",
			expr: `{a: IIF(true, 123, 456), b: IIF(false, 123, 456), c: IIF(123, 123, 456), d: IIF("ABC", 123, 456), ` +
				`e: IIF([1,2,3], 123, 456), f: IIF({"name": "Alice"}, 123, 456)}`,
			want: `{"a":123,"b":456,"c":456,"d":456,"e":456,"f":456}`,
		},
		{
			name: "is_datetime",
			expr: `{c1: IS_DATETIME("2024-12-12"), c2: IS_DATETIME("2024-12-12Z"), c3: IS_DATETIME("2024-12-12 20:21:25"), ` +
				`c4: IS_DATETIME("2024-12-12T20:21:25Z"), c5: IS_DATETIME("2024-12-12T20:21:25.123"), ` +
				`c6: IS_DATETIME("2024-12-12 20:21:25.123Z"), c7: IS_DATETIME("2024-12-12T20Z"), ` +
				`c8: IS_DATETIME("2024-12-12 20:21:25."), c9: IS_DATETIME("2024-13-12"), c10: IS_DATETIME(5)}`,
			want: `{"c1":true,"c2":true,"c3":true,"c4":true,"c5":true,"c6":true,"c7":false,"c8":false,"c9":false,"c10":false}`,
		},
		{
			name: "trigonometry",
			expr: `{sin: SIN(0), cos: COS(0), tan: TAN(0), asin: ASIN(1), acos: ACOS(1), atan: ATAN(0), ` +
				`bad: IS_DEFINED(ACOS(2)), cot0: IS_DEFINED(COT(0))}`,
			want: `{"sin":0,"cos":1,"tan":0,"asin":1.5707963267948966,"acos":0,"atan":0,"bad":false,"cot0":false}`,
		},
		{
			name: "degrees and radians",
			expr: `{d: DEGREES(PI()), r: RADIANS(180)}`,
			want: `{"d":180,"r":3.141592653589793}`,
		},
		{name: "rand is in range", expr: `RAND() >= 0 AND RAND() < 1`, want: `true`},
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

func TestSQLFunctions_DocumentIDIsPerItem(t *testing.T) {
	t.Parallel()

	b := cosmosdb.NewInMemoryBackend()
	_, err := b.CreateDatabase("d")
	require.NoError(t, err)
	_, err = b.CreateContainer("d", cosmosdb.ContainerSpec{ID: "c", PartitionKeyPath: "/pk"})
	require.NoError(t, err)

	for _, id := range []string{"a", "b"} {
		_, err = b.CreateDocument("d", "c", map[string]any{"id": id, "pk": "k"}, false)
		require.NoError(t, err)
	}

	res, err := b.RunQuery("d", "c", "SELECT c.id, DOCUMENTID(c) AS n FROM c ORDER BY c.id", nil, "")
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)

	first, second := res.Rows[0].(map[string]any), res.Rows[1].(map[string]any)
	assert.NotEqual(t, first["n"], second["n"])
}

func TestSQL_OrderByWithGroupByIsRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		ok    bool
	}{
		{name: "group by alone", query: "SELECT c.pk FROM c GROUP BY c.pk", ok: true},
		{name: "order by alone", query: "SELECT c.pk FROM c ORDER BY c.pk", ok: true},
		{name: "both", query: "SELECT c.pk FROM c GROUP BY c.pk ORDER BY c.pk"},
		{name: "both with an aggregate", query: "SELECT c.pk, COUNT(1) AS n FROM c GROUP BY c.pk ORDER BY n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := cosmosdb.ParseQuery(tt.query)
			if tt.ok {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.Contains(t, err.Error(), "'ORDER BY' is not supported in presence of GROUP BY.")
		})
	}
}

const productDoc = `{"id":"1","name":"Remdriel Shoes","price":15,"tags":[{"key":"fabric","value":"leather"},` +
	`{"key":"x","value":"y"}],"sizes":[{"key":"5"},{"key":"9"}],"colors":["cobalt","jam"]}`

// The statements are the SELECT and subquery pages' examples: FROM-less queries, positional names, multi-value
// subqueries as join sources, scalar and ARRAY subqueries.
func TestSQL_DocumentedStatementForms(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{name: "static strings", query: `SELECT "Cosmic", "Works"`, want: `[{"$1":"Cosmic","$2":"Works"}]`},
		{name: "static value", query: `SELECT VALUE "Cosmic Works"`, want: `["Cosmic Works"]`},
		{
			name:  "scalar subqueries",
			query: `SELECT (SELECT VALUE 1) AS a, (SELECT VALUE 2) AS b`,
			want:  `[{"a":1,"b":2}]`,
		},
		{name: "undefined value", query: `SELECT VALUE undefined`, want: `[]`},
		{name: "undefined projection", query: `SELECT undefined`, want: `[{}]`},
		{name: "exists of an undefined value", query: `SELECT VALUE EXISTS (SELECT VALUE undefined)`, want: `[false]`},
		{name: "exists of an empty object", query: `SELECT VALUE EXISTS (SELECT undefined)`, want: `[true]`},
		{
			name: "join an anonymous multi-value subquery",
			query: `SELECT VALUE COUNT(1) FROM products p ` +
				`JOIN (SELECT VALUE t FROM t IN p.tags WHERE t.key IN ("fabric","material"))`,
			want: `[1]`,
		},
		{
			name: "evaluate once with a join subquery",
			query: `SELECT VALUE {subtotal: p.price, total: totalPrice} FROM products p ` +
				`JOIN (SELECT VALUE p.price * 1.25) totalPrice WHERE totalPrice < 22.25`,
			want: `[{"subtotal":15,"total":18.75}]`,
		},
		{
			name: "aggregate join subqueries",
			query: `SELECT p.name, colorCount FROM products p JOIN (SELECT VALUE COUNT(1) FROM c IN p.colors) AS colorCount ` +
				`WHERE colorCount >= 2`,
			want: `[{"name":"Remdriel Shoes","colorCount":2}]`,
		},
		{
			name: "reference data join",
			query: `SELECT s.key, m.unit FROM products p JOIN s IN p.sizes ` +
				`JOIN m IN (SELECT VALUE [{size:'5', unit:'cm'}]) WHERE s.key = m.size`,
			want: `[{"key":"5","unit":"cm"}]`,
		},
		{
			name: "array expression after FROM",
			query: `SELECT p.name, z.s.key AS sizes FROM products p JOIN z IN (SELECT VALUE ARRAY(SELECT s FROM s IN p.sizes ` +
				`WHERE STRINGTONUMBER(s.key) <= 8))`,
			want: `[{"name":"Remdriel Shoes","sizes":"5"}]`,
		},
		{
			name:  "filtering scalar subquery",
			query: `SELECT p.id, (SELECT p.name WHERE CONTAINS(p.name, "Shoes")).name FROM products p`,
			want:  `[{"id":"1","name":"Remdriel Shoes"}]`,
		},
		{
			name:  "countif facets",
			query: `SELECT COUNTIF(p.price < 25) AS under25, COUNTIF(p.price >= 25) AS over FROM products p`,
			want:  `[{"under25":1,"over":0}]`,
		},
		{name: "select star needs a source", query: `SELECT *`, want: ``},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rows, err := cosmosdb.ExecuteQuery(tt.query, nil, docInfos(t, productDoc))
			if tt.want == "" {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			got, marshalErr := json.Marshal(rows)
			require.NoError(t, marshalErr)
			assert.JSONEq(t, tt.want, string(got))
		})
	}
}
