package sparql

import (
	"bytes"
	"encoding/json/v2"
	"encoding/xml"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

// ResultFormat is a query-result serialization.
type ResultFormat uint8

// Query-result formats.
const (
	ResultJSON ResultFormat = iota
	ResultXML
	ResultCSV
	ResultTSV
	ResultBoolean
)

const (
	keyType  = "type"
	keyValue = "value"
)

func jsonTerm(t Term) map[string]string {
	switch t.Kind {
	case KindIRI:
		return map[string]string{keyType: "uri", keyValue: t.Value}
	case KindBlank:
		return map[string]string{keyType: "bnode", keyValue: t.Value}
	case KindNone, KindLiteral:
	}

	m := map[string]string{keyType: "literal", keyValue: t.Value}

	switch {
	case t.Lang != "":
		m["xml:lang"] = t.Lang
	case t.Datatype != "":
		m["datatype"] = t.Datatype
	}

	return m
}

// WriteResult serialises a SELECT or ASK result.
func WriteResult(r *Result, f ResultFormat) ([]byte, error) {
	if r.Kind == ResultAsk {
		return writeAsk(r.Bool, f)
	}

	switch f {
	case ResultXML:
		return writeSelectXML(r)
	case ResultCSV:
		return writeSelectSV(r, false), nil
	case ResultTSV:
		return writeSelectSV(r, true), nil
	default:
		return writeSelectJSON(r)
	}
}

func writeAsk(v bool, f ResultFormat) ([]byte, error) {
	switch f {
	case ResultXML:
		return []byte(`<?xml version="1.0"?>` + "\n" +
			`<sparql xmlns="http://www.w3.org/2005/sparql-results#"><head/><boolean>` +
			boolStr(v) + `</boolean></sparql>` + "\n"), nil
	case ResultBoolean:
		return []byte(boolStr(v)), nil
	default:
		return json.Marshal(map[string]any{"head": map[string]any{}, "boolean": v}, jsonx.V1())
	}
}

func boolStr(v bool) string {
	if v {
		return lexTrue
	}

	return lexFalse
}

func writeSelectJSON(r *Result) ([]byte, error) {
	bindings := make([]map[string]map[string]string, 0, len(r.Rows))

	for _, row := range r.Rows {
		m := make(map[string]map[string]string, len(row))

		for _, v := range r.Vars {
			if t, ok := row[v]; ok {
				m[v] = jsonTerm(t)
			}
		}

		bindings = append(bindings, m)
	}

	vars := r.Vars
	if vars == nil {
		vars = []string{}
	}

	return json.Marshal(map[string]any{
		"head":    map[string]any{"vars": vars},
		"results": map[string]any{"bindings": bindings},
	}, jsonx.V1())
}

func writeSelectXML(r *Result) ([]byte, error) {
	var b bytes.Buffer

	b.WriteString(
		`<?xml version="1.0"?>` + "\n" + `<sparql xmlns="http://www.w3.org/2005/sparql-results#">` + "\n  <head>\n",
	)

	for _, v := range r.Vars {
		b.WriteString(`    <variable name="` + xmlEscape(v) + `"/>` + "\n")
	}

	b.WriteString("  </head>\n  <results>\n")

	for _, row := range r.Rows {
		b.WriteString("    <result>\n")

		for _, v := range r.Vars {
			t, ok := row[v]
			if !ok {
				continue
			}

			b.WriteString(`      <binding name="` + xmlEscape(v) + `">` + xmlTerm(t) + "</binding>\n")
		}

		b.WriteString("    </result>\n")
	}

	b.WriteString("  </results>\n</sparql>\n")

	return b.Bytes(), nil
}

func xmlText(s string) string {
	var b bytes.Buffer

	_ = xml.EscapeText(&b, []byte(s))

	return b.String()
}

func xmlTerm(t Term) string {
	switch t.Kind {
	case KindIRI:
		return "<uri>" + xmlText(t.Value) + "</uri>"
	case KindBlank:
		return "<bnode>" + xmlText(t.Value) + "</bnode>"
	case KindNone, KindLiteral:
	}

	switch {
	case t.Lang != "":
		return `<literal xml:lang="` + xmlEscape(t.Lang) + `">` + xmlText(t.Value) + "</literal>"
	case t.Datatype != "":
		return `<literal datatype="` + xmlEscape(t.Datatype) + `">` + xmlText(t.Value) + "</literal>"
	}

	return "<literal>" + xmlText(t.Value) + "</literal>"
}

func csvField(s string) string {
	if strings.ContainsAny(s, ",\"\r\n") {
		return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
	}

	return s
}

func writeSelectSV(r *Result, tsv bool) []byte {
	sep := ","
	if tsv {
		sep = "\t"
	}

	var b bytes.Buffer

	heads := make([]string, len(r.Vars))
	for i, v := range r.Vars {
		heads[i] = v
		if tsv {
			heads[i] = "?" + v
		}
	}

	b.WriteString(strings.Join(heads, sep) + "\r\n")

	for _, row := range r.Rows {
		cells := make([]string, len(r.Vars))

		for i, v := range r.Vars {
			t, ok := row[v]

			switch {
			case !ok:
			case tsv:
				cells[i] = t.String()
			case t.Kind == KindBlank:
				cells[i] = csvField("_:" + t.Value)
			default:
				cells[i] = csvField(t.Value)
			}
		}

		b.WriteString(strings.Join(cells, sep) + "\r\n")
	}

	return b.Bytes()
}
