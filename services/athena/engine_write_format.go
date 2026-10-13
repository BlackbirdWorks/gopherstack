package athena

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"math"
	"strconv"
	"strings"
)

func encodeTextRows(res *sqlResult, rows []int, cols []outCol, delim string) []byte {
	var buf bytes.Buffer

	for _, r := range rows {
		for i, c := range cols {
			if i > 0 {
				buf.WriteString(delim)
			}

			v, null := res.cell(r, c.src)
			if null {
				v = hiveNullToken
			}

			buf.WriteString(v)
		}

		buf.WriteByte('\n')
	}

	return buf.Bytes()
}

func encodeCSVRows(res *sqlResult, rows []int, cols []outCol, delim string) []byte {
	var buf bytes.Buffer

	w := csv.NewWriter(&buf)
	w.Comma = []rune(delim)[0]
	rec := make([]string, len(cols))

	for _, r := range rows {
		for i, c := range cols {
			rec[i], _ = res.cell(r, c.src)
		}

		_ = w.Write(rec)
	}

	w.Flush()

	return buf.Bytes()
}

func encodeJSONLines(res *sqlResult, rows []int, cols []outCol) []byte {
	var buf bytes.Buffer

	for _, r := range rows {
		buf.WriteByte('{')

		for i, c := range cols {
			if i > 0 {
				buf.WriteByte(',')
			}

			key, _ := json.Marshal(c.name)
			buf.Write(key)
			buf.WriteByte(':')

			v, null := res.cell(r, c.src)
			buf.WriteString(jsonLiteral(v, null, c.typ))
		}

		buf.WriteString("}\n")
	}

	return buf.Bytes()
}

// jsonLiteral renders a cell as a JSON value typed by its column type.
func jsonLiteral(v string, null bool, typ string) string {
	if null {
		return "null"
	}

	switch kindOf(typ) {
	case kindInt:
		if _, err := strconv.ParseInt(v, 10, 64); err == nil {
			return v
		}
	case kindFloat:
		if f, err := strconv.ParseFloat(v, 64); err == nil && !math.IsNaN(f) && !math.IsInf(f, 0) {
			return v
		}
	case kindBool:
		if v == strTrue || v == strFalse {
			return v
		}
	case kindString, kindText:
	}

	b, _ := json.Marshal(strings.ToValidUTF8(v, "�"))

	return string(b)
}
