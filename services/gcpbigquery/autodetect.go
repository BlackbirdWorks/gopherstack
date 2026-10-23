package gcpbigquery

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

const detectSample = 500

func detectSchema(lc loadConfig, in []loadInput) ([]bqsql.Field, error) {
	if len(in) == 0 {
		return nil, fmt.Errorf("%w: no data to detect a schema from", errLoad)
	}

	switch lc.Format {
	case formatCSV:
		return detectCSV(lc, in[0].data)
	case formatJSON:
		return detectJSON(lc, in[0].data)
	case formatParquet:
		return detectParquet(in[0].data)
	}

	return nil, fmt.Errorf("%w: cannot autodetect %s", errLoad, lc.Format)
}

func csvCellKind(s string) bqsql.Kind {
	switch {
	case s == "":
		return bqsql.KindUnknown
	case strings.EqualFold(s, "true") || strings.EqualFold(s, "false"):
		return bqsql.KindBool
	}

	if _, err := strconv.ParseInt(s, 10, 64); err == nil {
		return bqsql.KindInt
	}

	if _, err := strconv.ParseFloat(s, 64); err == nil && !strings.ContainsAny(s, "nNiI") {
		return bqsql.KindFloat
	}

	if _, err := bqsql.ParseTemporal(bqsql.KindDate, s); err == nil && len(s) == len(bqsql.DateLayout) {
		return bqsql.KindDate
	}

	if _, err := bqsql.ParseTemporal(bqsql.KindTimestamp, s); err == nil && strings.ContainsAny(s, ":") {
		return bqsql.KindTimestamp
	}

	return bqsql.KindString
}

func mergeKind(a, b bqsql.Kind) bqsql.Kind {
	switch {
	case a == bqsql.KindUnknown:
		return b
	case b == bqsql.KindUnknown || a == b:
		return a
	case a.IsNumber() && b.IsNumber():
		return bqsql.KindFloat
	}

	return bqsql.KindString
}

func detectCSV(lc loadConfig, data []byte) ([]bqsql.Field, error) {
	recs, err := readCSVRecords(lc, data)
	if err != nil {
		return nil, err
	}

	if len(recs) == 0 {
		return nil, fmt.Errorf("%w: cannot autodetect a schema from an empty file", errLoad)
	}

	width := 0
	for _, r := range recs[:min(len(recs), detectSample)] {
		width = max(width, len(r))
	}

	header := lc.SkipRows > 0 || looksLikeHeader(recs, width)
	body := recs

	if header {
		body = recs[min(max(lc.SkipRows, 1), len(recs)):]
	}

	kinds := make([]bqsql.Kind, width)
	for _, r := range body[:min(len(body), detectSample)] {
		for i, c := range r {
			kinds[i] = mergeKind(kinds[i], csvCellKind(c))
		}
	}

	fields := make([]bqsql.Field, width)
	for i := range fields {
		name := "string_field_" + strconv.Itoa(i)
		if header && i < len(recs[0]) {
			name = sanitizeName(recs[0][i], i)
		}

		k := kinds[i]
		if k == bqsql.KindUnknown {
			k = bqsql.KindString
		}

		fields[i] = bqsql.Field{Name: name, Type: k.RESTName(), Mode: bqsql.ModeNullable}
	}

	return dedupeNames(fields), nil
}

// looksLikeHeader reports a first row of non-numeric text above rows with at least one typed column.
func looksLikeHeader(recs [][]string, width int) bool {
	if len(recs) < 2 { //nolint:mnd // a header needs at least one data row
		return false
	}

	for _, c := range recs[0] {
		if k := csvCellKind(c); k != bqsql.KindString && k != bqsql.KindUnknown {
			return false
		}
	}

	for col := range width {
		for _, r := range recs[1:min(len(recs), detectSample)] {
			if col < len(r) {
				if k := csvCellKind(r[col]); k != bqsql.KindString && k != bqsql.KindUnknown {
					return true
				}
			}
		}
	}

	return false
}

func invalidNameChars() *regexp.Regexp { return regexp.MustCompile(`[^A-Za-z0-9_]`) }

func sanitizeName(s string, idx int) string {
	s = invalidNameChars().ReplaceAllString(strings.TrimSpace(s), "_")
	if s == "" {
		return "string_field_" + strconv.Itoa(idx)
	}

	if s[0] >= '0' && s[0] <= '9' {
		s = "_" + s
	}

	return s
}

func dedupeNames(fs []bqsql.Field) []bqsql.Field {
	seen := map[string]int{}

	for i := range fs {
		l := strings.ToLower(fs[i].Name)
		seen[l]++

		if seen[l] > 1 {
			fs[i].Name += "_" + strconv.Itoa(seen[l]-1)
		}
	}

	return fs
}

func detectJSON(lc loadConfig, data []byte) ([]bqsql.Field, error) {
	dec := json.NewDecoder(bytes.NewReader(decodeText(data, lc.Encoding)))
	dec.UseNumber()

	var fields []bqsql.Field

	for range detectSample {
		var obj map[string]any

		err := dec.Decode(&obj)
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("%w: invalid JSON: %w", errLoad, err)
		}

		fields = mergeObject(fields, obj)
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("%w: cannot autodetect a schema from empty data", errLoad)
	}

	return fields, nil
}

func mergeObject(fields []bqsql.Field, obj map[string]any) []bqsql.Field {
	for _, k := range sortedKeys(obj) {
		nf, ok := inferJSONField(k, obj[k])
		if !ok {
			continue
		}

		idx := -1

		for i, f := range fields {
			if strings.EqualFold(f.Name, k) {
				idx = i
			}
		}

		if idx < 0 {
			fields = append(fields, nf)

			continue
		}

		fields[idx] = mergeField(fields[idx], nf)
	}

	return fields
}

func mergeField(a, b bqsql.Field) bqsql.Field {
	if a.Type == litRecord && b.Type == litRecord {
		for _, sub := range b.Fields {
			a.Fields = mergeObjectField(a.Fields, sub)
		}

		return a
	}

	ka, _ := bqsql.KindOf(a.Type)
	kb, _ := bqsql.KindOf(b.Type)
	a.Type = mergeKind(ka, kb).RESTName()

	return a
}

func mergeObjectField(fields []bqsql.Field, nf bqsql.Field) []bqsql.Field {
	for i, f := range fields {
		if strings.EqualFold(f.Name, nf.Name) {
			fields[i] = mergeField(f, nf)

			return fields
		}
	}

	return append(fields, nf)
}

func inferJSONField(name string, v any) (bqsql.Field, bool) {
	f := bqsql.Field{Name: name, Mode: bqsql.ModeNullable}

	switch t := v.(type) {
	case nil:
		return f, false
	case []any:
		for _, e := range t {
			if ef, ok := inferJSONField(name, e); ok {
				ef.Mode = bqsql.ModeRepeated

				return ef, true
			}
		}

		return f, false
	case map[string]any:
		f.Type = litRecord
		f.Fields = mergeObject(nil, t)

		return f, len(f.Fields) > 0
	case bool:
		f.Type = litBoolean
	case json.Number:
		f.Type = litInteger
		if _, err := t.Int64(); err != nil {
			f.Type = litFloat
		}
	case string:
		f.Type = csvCellKind(t).RESTName()
		if csvCellKind(t) == bqsql.KindUnknown || csvCellKind(t) == bqsql.KindBool || csvCellKind(t) == bqsql.KindInt ||
			csvCellKind(t) == bqsql.KindFloat {
			f.Type = "STRING"
		}
	}

	return f, true
}

func detectParquet(data []byte) ([]bqsql.Field, error) {
	f, err := parquetio.Open(data)
	if err != nil {
		return nil, fmt.Errorf("parquet: %w", err)
	}

	cols := f.Columns()
	kinds := make([]bqsql.Kind, len(cols))
	seen := 0

	_ = f.ForEach(cols, func(vals []any) error {
		for i, v := range vals {
			kinds[i] = mergeKind(kinds[i], goValueKind(v))
		}

		seen++
		if seen >= detectSample {
			return io.EOF
		}

		return nil
	})

	fields := make([]bqsql.Field, len(cols))
	for i, c := range cols {
		k := kinds[i]
		if k == bqsql.KindUnknown {
			k = bqsql.KindString
		}

		fields[i] = bqsql.Field{Name: sanitizeName(c, i), Type: k.RESTName(), Mode: bqsql.ModeNullable}
	}

	return dedupeNames(fields), nil
}

func goValueKind(v any) bqsql.Kind {
	switch t := v.(type) {
	case nil:
		return bqsql.KindUnknown
	case bool:
		return bqsql.KindBool
	case int, int32, int64, uint32, uint64:
		return bqsql.KindInt
	case float32, float64:
		return bqsql.KindFloat
	case string:
		if k := csvCellKind(t); k == bqsql.KindDate || k == bqsql.KindTimestamp {
			return k
		}
	}

	return bqsql.KindString
}
