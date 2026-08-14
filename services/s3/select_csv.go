package s3

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

var errMissingSQLColumn = errors.New("MissingSQLColumn")

// evaluateCSVQuery reads all CSV rows from data, applies the SQL query, and streams results.
// All rows are collected before evaluation so that ORDER BY and aggregate functions work correctly.
func evaluateCSVQuery(
	w io.Writer,
	query *sqlQuery,
	data []byte,
	req *selectRequest,
) (int64, error) {
	csvIn := req.InputSerialization.CSV
	fileHeaderInfo := csvFileHeaderInfo(csvIn)
	r := newCSVReader(csvIn, data)

	rows, headers, err := readCSVRows(r, fileHeaderInfo)
	if err != nil {
		return 0, err
	}

	// Validate named column references against actual headers when USE mode is active.
	if fileHeaderInfo == csvFileHeaderInfoUse && len(rows) > 0 {
		if valErr := validateCSVQueryColumns(query, rows[0]); valErr != nil {
			return 0, valErr
		}
	}

	resultRows, err := evalQuery(query, rows)
	if err != nil {
		return 0, err
	}

	if len(resultRows) == 0 {
		return 0, nil
	}

	resultBytes, err := serializeCSVQueryResults(
		resultRows, req.OutputSerialization, csvOutputColumnOrder(query, headers),
	)
	if err != nil {
		return 0, err
	}

	if len(resultBytes) == 0 {
		return 0, nil
	}

	if wErr := writeSelectEvent(w, "Records", "application/octet-stream", resultBytes); wErr != nil {
		return 0, wErr
	}

	return int64(len(resultBytes)), nil
}

func csvFileHeaderInfo(csvIn *selectCSVInput) string {
	if csvIn != nil && csvIn.FileHeaderInfo != "" {
		return strings.ToUpper(csvIn.FileHeaderInfo)
	}

	return "NONE"
}

func newCSVReader(csvIn *selectCSVInput, data []byte) *csv.Reader {
	fieldDelim := ','
	if csvIn != nil && csvIn.FieldDelimiter != "" {
		fieldDelim = rune(csvIn.FieldDelimiter[0])
	}

	r := csv.NewReader(bytes.NewReader(data))
	r.Comma = fieldDelim
	r.LazyQuotes = true
	r.TrimLeadingSpace = true

	if csvIn != nil && csvIn.Comments != "" {
		r.Comment = rune(csvIn.Comments[0])
	}

	return r
}

// readCSVRows returns the parsed rows plus the headers in their original file
// order, so callers can reproduce that order in CSV output instead of
// serializing rows as unordered Go maps (see csvOutputColumnOrder).
func readCSVRows(r *csv.Reader, fileHeaderInfo string) ([]map[string]string, []string, error) {
	var headers []string
	var rows []map[string]string
	firstRecord := true

	for {
		rec, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, nil, fmt.Errorf("reading CSV: %w", err)
		}

		if firstRecord {
			firstRecord = false
			headers = prepareCSVHeaders(fileHeaderInfo, rec)

			if fileHeaderInfo == csvFileHeaderInfoUse {
				continue
			}
		}

		rows = append(rows, csvRecordToMap(headers, rec))
	}

	return rows, headers, nil
}

func csvRecordToMap(headers []string, rec []string) map[string]string {
	rowMap := make(map[string]string, len(headers))
	for i, h := range headers {
		if i < len(rec) {
			rowMap[h] = rec[i]
		}
	}

	return rowMap
}

func prepareCSVHeaders(fileHeaderInfo string, firstRecord []string) []string {
	var headers []string
	switch fileHeaderInfo {
	case csvFileHeaderInfoUse:
		headers = firstRecord
	default: // IGNORE or NONE
		for i := range firstRecord {
			headers = append(headers, fmt.Sprintf("_%d", i+1))
		}
	}

	return headers
}

func serializeCSVQueryResults(
	resultRows []map[string]string,
	out selectOutputSerialization,
	order []string,
) ([]byte, error) {
	if out.JSON != nil {
		return serializeCSVRowsAsJSON(resultRows, out.JSON)
	}

	buf := serializeCSVRows(resultRows, out.CSV, order)

	return buf, nil
}

// csvOutputColumnOrder returns the column order CSV output must use.
//
// CSV rows are stored as map[string]string, which has no iteration order, so
// serializing them by ranging the map (or by encoding/csv's own sort) turns
// "SELECT s.name, s.age FROM s3object s" into whatever order Go's map or a
// sortedKeys() alphabetical fallback picks - previously that meant
// "SELECT s.name, s.age" and "SELECT *" over a "name,age" header both came
// back as "age,name", silently transposing the client's requested columns.
//
// An explicit SELECT list has a well-defined order (the query itself);
// SELECT * uses the source CSV's header order when known. A nil/empty
// return means the caller has no order information (JSON input's SELECT *,
// since json.Unmarshal into map[string]any discards field order) and must
// fall back to alphabetical - a known, documented gap, not a fix here.
func csvOutputColumnOrder(q *sqlQuery, headers []string) []string {
	if !q.selectAll {
		order := make([]string, len(q.columns))
		for i, col := range q.columns {
			order[i] = columnName(col, i)
		}

		return order
	}

	return headers
}

func serializeCSVRowsAsJSON(rows []map[string]string, jsonOut *selectJSONOutput) ([]byte, error) {
	delim := "\n"
	if jsonOut.RecordDelimiter != "" {
		delim = jsonOut.RecordDelimiter
	}

	var buf bytes.Buffer

	for _, row := range rows {
		jsonBytes, err := json.Marshal(mapStringToAny(row))
		if err != nil {
			return nil, err
		}

		buf.Write(jsonBytes)
		buf.WriteString(delim)
	}

	return buf.Bytes(), nil
}

// serializeCSVRows serializes result rows to CSV format using order as the
// column order, falling back to sortedKeys per row when order is empty (see
// csvOutputColumnOrder for when that happens and why).
func serializeCSVRows(rows []map[string]string, csvOut *selectCSVOutput, order []string) []byte {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)

	if csvOut != nil && csvOut.FieldDelimiter != "" {
		w.Comma = rune(csvOut.FieldDelimiter[0])
	}

	for _, row := range rows {
		keys := order
		if len(keys) == 0 {
			keys = sortedKeys(row)
		}

		record := make([]string, len(keys))

		for i, k := range keys {
			record[i] = row[k]
		}

		_ = w.Write(record)
	}

	w.Flush()

	return buf.Bytes()
}

// validateCSVQueryColumns validates that all named column references in the query
// (non-positional, non-wildcard) exist in the provided header row.
// Returns an error with code MissingSQLColumn if an unknown column is referenced.
func validateCSVQueryColumns(q *sqlQuery, headerRow map[string]string) error {
	if q.selectAll {
		return nil
	}
	for _, col := range q.columns {
		if ref, ok := col.expr.(*sqlColumnRef); ok {
			name := ref.name
			// Positional refs (_1, _2...) and qualified refs (alias.col) are always valid.
			if len(name) > 0 && name[0] == '_' {
				continue
			}
			if strings.Contains(name, ".") {
				continue
			}
			if _, found := headerRow[name]; !found {
				return fmt.Errorf("%w: column %q does not exist in table", errMissingSQLColumn, name)
			}
		}
	}

	return nil
}

// mapStringToAny converts map[string]string to map[string]any.
func mapStringToAny(m map[string]string) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}

	return out
}
