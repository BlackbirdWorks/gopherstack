package azuremonitor

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"time"

	"github.com/blackbirdworks/gopherstack/services/azuremonitor/kql"
)

// convertValue converts a decoded JSON value to the engine representation of typ.
func convertValue(typ kql.Type, v any) (any, error) {
	if v == nil {
		if typ == kql.TString {
			return "", nil
		}

		return nil, nil //nolint:nilnil // missing value is null
	}

	switch typ { //nolint:exhaustive // timespan is not a stream type
	case kql.TString, kql.TGUID:
		if s, ok := v.(string); ok {
			return s, nil
		}
	case kql.TInt, kql.TLong:
		if n, ok := numberToInt(v); ok {
			return n, nil
		}
	case kql.TReal:
		if f, ok := numberToFloat(v); ok {
			return f, nil
		}
	case kql.TBool:
		if b, ok := v.(bool); ok {
			return b, nil
		}
	case kql.TDatetime:
		switch x := v.(type) {
		case time.Time:
			return x, nil
		case string:
			if t, ok := kql.ParseDatetime(x); ok {
				return t, nil
			}
		}
	case kql.TDynamic:
		return normalizeDynamic(v), nil
	}

	return nil, fmt.Errorf("value %v does not match type %s", v, typ)
}

func numberToInt(v any) (int64, bool) {
	switch x := v.(type) {
	case json.Number:
		n, err := x.Int64()

		return n, err == nil
	case int64:
		return x, true
	case float64:
		if x == math.Trunc(x) && math.Abs(x) < 1<<53 {
			return int64(x), true
		}
	}

	return 0, false
}

func numberToFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case json.Number:
		f, err := x.Float64()

		return f, err == nil
	case float64:
		return x, true
	case int64:
		return float64(x), true
	}

	return 0, false
}

func normalizeDynamic(v any) any {
	switch x := v.(type) {
	case float64:
		return kql.NormalizeJSON(x)
	case json.Number:
		if n, err := x.Int64(); err == nil {
			return n
		}

		f, _ := x.Float64()

		return f
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = normalizeDynamic(e)
		}

		return out
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[k] = normalizeDynamic(e)
		}

		return out
	}

	return v
}

// Ingest stores the records of one upload. Records are decoded JSON objects (numbers as json.Number).
func (b *Backend) Ingest(ruleID, stream string, records []map[string]any) *apiError {
	b.mu.Lock("Ingest")
	defer b.mu.Unlock()

	dcr, ok := b.dcrByImmutableID(ruleID)
	if !ok {
		return newErr(http.StatusNotFound, "NotFound", "Data collection rule with immutable Id '%s' not found.", ruleID)
	}

	spec, err := parseDCR(dcr.Props)
	if err != nil {
		return newErr(http.StatusInternalServerError, "InternalServerError", "%s", err.Error())
	}

	cols, ok := spec.streamColumns(stream)
	if !ok {
		return newErr(http.StatusBadRequest, "InvalidStream",
			"The stream '%s' is not declared in the data collection rule.", stream)
	}

	var flows []flow

	for _, f := range spec.flows {
		for _, s := range f.streams {
			if s == stream {
				flows = append(flows, f)
			}
		}
	}

	if len(flows) == 0 {
		return newErr(http.StatusBadRequest, "InvalidStream",
			"No data flow of the data collection rule uses the stream '%s'.", stream)
	}

	src, aerr := b.streamRel(cols, records)
	if aerr != nil {
		return aerr
	}

	for _, f := range flows {
		if aerr = b.deliver(spec, f, stream, src); aerr != nil {
			return aerr
		}
	}

	return nil
}

func (b *Backend) streamRel(cols []Column, records []map[string]any) (*kql.Rel, *apiError) {
	rel := &kql.Rel{Cols: columnsToKQL(cols)}

	for i, rec := range records {
		row := make([]any, len(cols))

		for j, c := range cols {
			v, err := convertValue(kql.Type(c.Type), rec[c.Name])
			if err != nil {
				if b.settings.LenientIngest {
					row = nil

					break
				}

				return nil, newErr(http.StatusBadRequest, "InvalidPayload",
					"Record %d, column '%s' (%s): %v", i, c.Name, c.Type, err)
			}

			row[j] = v
		}

		if row != nil {
			rel.Rows = append(rel.Rows, row)
		}
	}

	return rel, nil
}

func (b *Backend) deliver(spec *dcrSpec, f flow, stream string, src *kql.Rel) *apiError {
	out := src

	if f.transform != "" && f.transform != sourceStream {
		res, err := kql.Execute(f.transform, kql.Options{Now: b.now(), Resolver: sourceResolver{src}})
		if err != nil {
			return newErr(http.StatusBadRequest, "InvalidTransform", "transformKql failed: %v", err)
		}

		out = res
	}

	outStream := f.output
	if outStream == "" {
		outStream = stream
	}

	for _, d := range f.dests {
		wsID, ok := spec.logAnalytics[d]
		if !ok {
			continue
		}

		w, found := b.findWorkspaceLocked(wsID)
		if !found {
			return newErr(http.StatusNotFound, "NotFound", "The destination workspace '%s' was not found.", wsID)
		}

		t, found := w.table(outputTable(outStream))
		if !found {
			return newErr(http.StatusBadRequest, "InvalidOutputTable",
				"The table for output stream '%s' does not exist in workspace '%s'.", outStream, w.Name)
		}

		if aerr := b.insert(t, out); aerr != nil {
			return aerr
		}
	}

	return nil
}

func (b *Backend) insert(t *Table, out *kql.Rel) *apiError {
	idx := make([]int, len(t.Columns))

	for i, c := range t.Columns {
		idx[i] = -1

		for j, oc := range out.Cols {
			if oc.Name == c.Name {
				idx[i] = j
			}
		}
	}

	now := b.now()
	rows := make([][]any, 0, len(out.Rows))

	for r, orow := range out.Rows {
		row := make([]any, len(t.Columns))

		for i, c := range t.Columns {
			var v any
			if idx[i] >= 0 {
				v = orow[idx[i]]
			}

			cv, err := coerceCell(kql.Type(c.Type), v)
			if err != nil {
				if b.settings.LenientIngest {
					row = nil

					break
				}

				return newErr(http.StatusBadRequest, "InvalidPayload",
					"Row %d: column '%s' of table '%s': %v", r, c.Name, t.Name, err)
			}

			if c.Name == timeGenerated && cv == nil {
				cv = now
			}

			row[i] = cv
		}

		if row != nil {
			rows = append(rows, row)
		}
	}

	t.Rows = append(t.Rows, rows...)

	return nil
}

// coerceCell converts an engine value to a table column's type (strings parse into datetimes etc.).
func coerceCell(typ kql.Type, v any) (any, error) {
	if v == nil {
		return convertValue(typ, nil)
	}

	switch x := v.(type) {
	case time.Time:
		if typ == kql.TDatetime {
			return x, nil
		}

		if typ == kql.TString {
			return kql.FormatTime(x), nil
		}
	case time.Duration:
		if typ == kql.TString {
			return kql.FormatTimespan(x), nil
		}
	case int64:
		switch typ { //nolint:exhaustive // other types fall through to the generic converter
		case kql.TReal:
			return float64(x), nil
		case kql.TString:
			return kql.ToString(x), nil
		}
	case float64:
		if typ == kql.TString {
			return kql.ToString(x), nil
		}
	case bool:
		if typ == kql.TString {
			return kql.ToString(x), nil
		}
	}

	return convertValue(typ, v)
}
