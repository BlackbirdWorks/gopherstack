package azurearm

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogicTextFunctionsDocExamples(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want any
		name string
		expr string
	}{
		{name: "slice from", expr: "@slice('Hello World', 2)", want: "llo World"},
		{name: "slice past end", expr: "@slice('Hello World', 30)", want: ""},
		{name: "slice end before start", expr: "@slice('Hello World', 10, 2)", want: ""},
		{name: "slice zero", expr: "@slice('Hello World', 0)", want: "Hello World"},
		{name: "slice range", expr: "@slice('Hello World', 2, 5)", want: "llo"},
		{name: "slice long end", expr: "@slice('Hello World', 6, 20)", want: "World"},
		{name: "slice negative start", expr: "@slice('Hello World', -2)", want: "ld"},
		{name: "slice negative end", expr: "@slice('Hello World', 3, -1)", want: "lo Worl"},
		{name: "slice equal", expr: "@slice('Hello World', 3, 3)", want: ""},
		{name: "slice start before begin", expr: "@slice('Hello', -50, 2)", want: "He"},
		{name: "slice unicode", expr: "@slice('héllo', 1, 3)", want: "él"},
		{name: "nth first", expr: "@nthIndexOf('123456789123465789', '1', 1)", want: int64(0)},
		{name: "nth second", expr: "@nthIndexOf('123456789123465789', '1', 2)", want: int64(9)},
		{name: "nth multi char", expr: "@nthIndexOf('123456789123465789', '12', 2)", want: int64(9)},
		{name: "nth missing", expr: "@nthIndexOf('123456789123465789', '6', 4)", want: int64(-1)},
		{name: "nth negative", expr: "@nthIndexOf('123456789123465789', '1', -1)", want: int64(9)},
		{name: "nth negative second", expr: "@nthIndexOf('123456789123465789', '1', -2)", want: int64(0)},
		{name: "nth negative missing", expr: "@nthIndexOf('123456789123465789', '1', -3)", want: int64(-1)},
		{name: "nth ignores case", expr: "@nthIndexOf('Abab', 'B', 2)", want: int64(3)},
		{name: "binary", expr: "@binary('hello')", want: "aGVsbG8="},
		{
			name: "decodeDataUri", want: "hello",
			expr: "@decodeDataUri('data:text/plain;charset=utf-8;base64,aGVsbG8=')",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := evalLogicTemplate(tc.expr, logicTestCtx())
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestLogicTimeZoneFunctionsDocExamples(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want string
		name string
		expr string
	}{
		{
			name: "convertFromUtc", want: "2018-01-01T00:00:00.0000000",
			expr: "@convertFromUtc('2018-01-01T08:00:00.0000000Z', 'Pacific Standard Time')",
		},
		{
			name: "convertFromUtc format", want: "Monday, January 1, 2018",
			expr: "@convertFromUtc('2018-01-01T08:00:00.0000000Z', 'Pacific Standard Time', 'D')",
		},
		{
			name: "convertFromUtc daylight", want: "2018-07-01T01:00:00.0000000",
			expr: "@convertFromUtc('2018-07-01T08:00:00.0000000Z', 'Pacific Standard Time')",
		},
		{
			name: "convertFromUtc punctuation", want: "2018-01-01T09:00:00.0000000",
			expr: "@convertFromUtc('2018-01-01T08:00:00Z', 'W. Europe Standard Time')",
		},
		{
			name: "convertFromUtc iana", want: "2018-01-01T17:00:00.0000000",
			expr: "@convertFromUtc('2018-01-01T08:00:00Z', 'Asia/Tokyo')",
		},
		{
			name: "convertFromUtc offset zone", want: "2018-01-01T20:00:00.0000000",
			expr: "@convertFromUtc('2018-01-01T08:00:00Z', 'UTC+12')",
		},
		{
			name: "convertTimeZone", want: "2018-01-01T00:00:00.0000000",
			expr: "@convertTimeZone('2018-01-01T08:00:00.0000000Z', 'UTC', 'Pacific Standard Time')",
		},
		{
			name: "convertTimeZone format", want: "Monday, January 1, 2018",
			expr: "@convertTimeZone('2018-01-01T08:00:00.0000000Z', 'UTC', 'Pacific Standard Time', 'D')",
		},
		{
			name: "convertTimeZone between zones", want: "2018-01-01T11:00:00.0000000",
			expr: "@convertTimeZone('2018-01-01T08:00:00', 'Pacific Standard Time', 'Eastern Standard Time')",
		},
		{
			name: "convertToUtc", want: "2018-01-01T08:00:00.0000000Z",
			expr: "@convertToUtc('01/01/2018 00:00:00', 'Pacific Standard Time')",
		},
		{
			name: "convertToUtc format", want: "Monday, January 1, 2018",
			expr: "@convertToUtc('01/01/2018 00:00:00', 'Pacific Standard Time', 'D')",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := evalLogicTemplate(tc.expr, logicTestCtx())
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestLogicParseDateTimeDocExamples(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		expr string
		want string
	}{
		{name: "fr slash", expr: "@parseDateTime('20/10/2014', 'fr-fr')", want: "2014-10-20T00:00:00.0000000"},
		{name: "fr name", expr: "@parseDateTime('20 octobre 2010', 'fr-FR')", want: "2010-10-20T00:00:00.0000000"},
		{
			name: "es weekday", expr: "@parseDateTime('martes 20 octubre 2020', 'es-es')",
			want: "2020-10-20T00:00:00.0000000",
		},
		{
			name: "format ddMMyyyy", expr: "@parseDateTime('21052019', 'fr-fr', 'ddMMyyyy')",
			want: "2019-05-21T00:00:00.0000000",
		},
		{
			name: "format yyyyMMdd", expr: "@parseDateTime('20190521', 'fr-fr', 'yyyyMMdd')",
			want: "2019-05-21T00:00:00.0000000",
		},
		{
			name: "format with escape", expr: `@parseDateTime('10/20/2014 15h', 'en-US', 'MM/dd/yyyy HH\h')`,
			want: "2014-10-20T15:00:00.0000000",
		},
		{name: "default locale", expr: "@parseDateTime('10/20/2014 3:04 PM')", want: "2014-10-20T15:04:00.0000000"},
		{name: "iso with zone", expr: "@parseDateTime('2014-10-20T15:04:05Z')", want: "2014-10-20T15:04:05.0000000Z"},
		{
			name: "standard specifier", expr: "@parseDateTime('2019-05-21T10:11:12', 'en-us', 's')",
			want: "2019-05-21T10:11:12.0000000",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := evalLogicTemplate(tc.expr, logicTestCtx())
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestLogicExtFunctionErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		expr string
	}{
		{name: "slice non integer", expr: "@slice('abc', 'x')"},
		{name: "slice arity", expr: "@slice('abc')"},
		{name: "nth zero", expr: "@nthIndexOf('abc', 'a', 0)"},
		{name: "decodeDataUri invalid", expr: "@decodeDataUri('hello')"},
		{name: "unknown zone", expr: "@convertFromUtc('2018-01-01T08:00:00Z', 'Atlantis Standard Time')"},
		{name: "traversal zone", expr: "@convertFromUtc('2018-01-01T08:00:00Z', '../../etc/passwd')"},
		{name: "bad timestamp", expr: "@convertToUtc('nope', 'UTC')"},
		{name: "parse bad locale", expr: "@parseDateTime('20/10/2014', 'xx-xx')"},
		{name: "parse bad value", expr: "@parseDateTime('not a date', 'fr-fr')"},
		{name: "parse bad day", expr: "@parseDateTime('31022019', 'fr-fr', 'ddMMyyyy')"},
		{name: "parse trailing text", expr: "@parseDateTime('2105201900', 'fr-fr', 'ddMMyyyy')"},
		{name: "xml invalid", expr: "@xml('<a>')"},
		{name: "xml array root", expr: "@xml(json('{\"a\":[1,2]}'))"},
		{name: "xml two roots", expr: "@xml(json('{\"a\":1,\"b\":2}'))"},
		{name: "xml raw array", expr: "@xml(createArray(1, 2))"},
		{name: "xpath invalid", expr: "@xpath(xml('<a/>'), '/a[')"},
		{name: "xpath unknown function", expr: "@xpath(xml('<a/>'), 'system-property(1)')"},
		{name: "xpath prefix", expr: "@xpath(xml('<a/>'), '/p:a')"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := evalLogicTemplate(tc.expr, logicTestCtx())
			require.Error(t, err)
		})
	}
}

const (
	xmlProduce = `<?xml version="1.0"?>
<produce>
  <item>
    <name>Gala</name>
    <type>apple</type>
    <count>20</count>
  </item>
  <item>
    <name>Honeycrisp</name>
    <type>apple</type>
    <count>10</count>
  </item>
</produce>`
	xmlProduceAttrs = `<?xml version="1.0"?>
<produce>
  <item>
    <name expired='true' price='12'>Gala</name>
    <type>apple</type>
    <count>20</count>
  </item>
  <item>
    <name expired='false' price='40'>Honeycrisp</name>
    <type>apple</type>
    <count>10</count>
  </item>
</produce>`
	xmlProduceOneAttr = `<produce><item><name expired='true'>Gala</name></item>` +
		`<item><name>Honeycrisp</name></item></produce>`
	xmlNamespaced = `<?xml version="1.0"?><file xmlns="https://contoso.com"><location>Paris</location></file>`
)

func TestLogicXPathDocExamples(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want any
		name string
		doc  string
		expr string
	}{
		{
			name: "text nodes", doc: xmlProduce, expr: "/produce/item/name/text()",
			want: []any{"Gala", "Honeycrisp"},
		},
		{
			name: "first name", doc: xmlProduce, expr: "/produce/item/name[1]",
			want: []any{"<name>Gala</name>", "<name>Honeycrisp</name>"},
		},
		{
			name: "last name", doc: xmlProduce, expr: "/produce/item[last()]/name",
			want: "<name>Honeycrisp</name>",
		},
		{
			name: "last predicate", doc: xmlProduce, expr: "/produce/item/name[last()]",
			want: []any{"<name>Gala</name>", "<name>Honeycrisp</name>"},
		},
		{
			name: "attribute exists", doc: xmlProduceAttrs, expr: "//name[@expired]",
			want: []any{
				`<name expired="true" price="12">Gala</name>`, `<name expired="false" price="40">Honeycrisp</name>`,
			},
		},
		{
			name: "attribute equals", doc: xmlProduceOneAttr, expr: "//name[@expired = 'true']",
			want: `<name expired="true">Gala</name>`,
		},
		{
			name: "attribute greater", doc: xmlProduceAttrs, expr: "//name[@price>35]",
			want: `<name expired="false" price="40">Honeycrisp</name>`,
		},
		{name: "sum", doc: xmlProduce, expr: "sum(/produce/item/count)", want: int64(30)},
		{
			name: "name function", doc: xmlNamespaced, expr: `/*[name()="file"]/*[name()="location"]`,
			want: `<location xmlns="https://contoso.com">Paris</location>`,
		},
		{
			name: "local-name and namespace-uri", doc: xmlNamespaced,
			expr: `/*[local-name()="file" and namespace-uri()="https://contoso.com"]/*[local-name()="location"]`,
			want: `<location xmlns="https://contoso.com">Paris</location>`,
		},
		{
			name: "string function", doc: xmlNamespaced, expr: `string(/*[name()="file"]/*[name()="location"])`,
			want: "Paris",
		},
		{name: "no match", doc: xmlProduce, expr: "/produce/missing", want: []any{}},
		{name: "count", doc: xmlProduce, expr: "count(//item)", want: int64(2)},
		{name: "contains", doc: xmlProduce, expr: "//item[contains(name, 'Honey')]/count/text()", want: "10"},
		{name: "position", doc: xmlProduce, expr: "//item[position() = 1]/name/text()", want: "Gala"},
		{name: "union", doc: xmlProduce, expr: "//name/text() | //type/text()", want: []any{
			"Gala", "apple", "Honeycrisp", "apple",
		}},
		{name: "parent axis", doc: xmlProduce, expr: "//count[. = 10]/../name/text()", want: "Honeycrisp"},
		{
			name: "following sibling", doc: xmlProduce, expr: "//item[1]/name/following-sibling::count/text()",
			want: "20",
		},
		{name: "arithmetic", doc: xmlProduce, expr: "sum(//count) div count(//count) + 1", want: int64(16)},
		{name: "boolean", doc: xmlProduce, expr: "count(//item) = 2 and not(false())", want: true},
		{name: "descendant axis", doc: xmlProduce, expr: "count(/descendant::name)", want: int64(2)},
		{name: "preceding axis", doc: xmlProduce, expr: "count(//item[2]/preceding::name)", want: int64(1)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := logicXPath([]any{tc.doc, tc.expr})
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestLogicXMLDocExamples(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		expr string
		want string
	}{
		{name: "string", expr: "@xml('<name>Sophia Owen</name>')", want: "<name>Sophia Owen</name>"},
		{
			name: "object", expr: `@xml(json('{ "name": "Sophia Owen" }'))`,
			want: "<name>Sophia Owen</name>",
		},
		{
			name: "nested object", expr: `@xml(json('{"person": {"name": "Sophia Owen", "city": "Seattle"}}'))`,
			want: "<person><city>Seattle</city><name>Sophia Owen</name></person>",
		},
		{
			name: "array in root",
			want: "<root><array><ID>1</ID><Name>James</Name></array><array><ID>2</ID><Name>John</Name></array></root>",
			expr: `@xml(json('{"root":{"array":[{ "ID": 1, "Name": "James" },{ "ID": 2, "Name": "John" }]}}'))`,
		},
		{
			name: "attributes and text", expr: `@xml(json('{"a":{"@id":"7","#text":"x<y"}}'))`,
			want: `<a id="7">x&lt;y</a>`,
		},
		{name: "escaped scalar", expr: `@xml(json('{"a":"1 & 2"}'))`, want: "<a>1 &amp; 2</a>"},
		{name: "null and bool", expr: `@xml(json('{"a":{"b":null,"c":true}}'))`, want: "<a><b/><c>true</c></a>"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := evalLogicTemplate(tc.expr, logicTestCtx())
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestLogicXMLRejectsExternalContent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		doc  string
	}{
		{name: "doctype", doc: `<!DOCTYPE a [<!ENTITY x SYSTEM "file:///etc/passwd">]><a>&x;</a>`},
		{name: "entity reference", doc: `<a>&x;</a>`},
		{name: "text before root", doc: `x<a/>`},
		{name: "mismatched", doc: `<a><b></a></b>`},
		{name: "undeclared prefix", doc: `<p:a/>`},
		{name: "deep nesting", doc: strings.Repeat("<a>", xmlMaxDepth+1) + strings.Repeat("</a>", xmlMaxDepth+1)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := logicXML([]any{tc.doc})
			require.Error(t, err)
		})
	}
}

func TestLogicXPathBudget(t *testing.T) {
	t.Parallel()

	doc := "<r>" + strings.Repeat("<a><b/></a>", 3000) + "</r>"

	_, err := logicXPath([]any{doc, "//*[count(//*) > 0]"})
	require.Error(t, err)
}
