package s3

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"strings"
)

// evaluateCSVQuery reads CSV rows from data, applies the SQL query, and serializes results.
func evaluateCSVQuery(query *sqlQuery, data []byte, req *selectRequest) ([]byte, int64, error) {
	csvIn := req.InputSerialization.CSV

	fieldDelim := ','
	if csvIn != nil && csvIn.FieldDelimiter != "" {
		fieldDelim = rune(csvIn.FieldDelimiter[0])
	}

	fileHeaderInfo := "NONE"
	if csvIn != nil && csvIn.FileHeaderInfo != "" {
		fileHeaderInfo = strings.ToUpper(csvIn.FileHeaderInfo)
	}

	r := csv.NewReader(bytes.NewReader(data))
	r.Comma = fieldDelim
	r.LazyQuotes = true
	r.TrimLeadingSpace = true

	if csvIn != nil && csvIn.Comments != "" {
		r.Comment = rune(csvIn.Comments[0])
	}

	allRecords, err := r.ReadAll()
	if err != nil {
		return nil, 0, fmt.Errorf("reading CSV: %w", err)
	}

	if len(allRecords) == 0 {
		return nil, 0, nil
	}

	rows := buildCSVRows(fileHeaderInfo, allRecords)

	resultRows, evalErr := evalQuery(query, rows)
	if evalErr != nil {
		return nil, 0, evalErr
	}

	if len(resultRows) == 0 {
		return nil, 0, nil
	}

	resultBytes, serialErr := serializeCSVQueryResults(resultRows, req.OutputSerialization)
	if serialErr != nil {
		return nil, 0, serialErr
	}

	return resultBytes, int64(len(resultBytes)), nil
}

func buildCSVRows(fileHeaderInfo string, allRecords [][]string) []map[string]string {
	var headers []string
	var dataRows [][]string

	switch fileHeaderInfo {
	case "USE":
		headers = allRecords[0]
		dataRows = allRecords[1:]
	case "IGNORE":
		for i := range allRecords[0] {
			headers = append(headers, fmt.Sprintf("_%d", i+1))
		}

		dataRows = allRecords[1:]
	default: // NONE
		for i := range allRecords[0] {
			headers = append(headers, fmt.Sprintf("_%d", i+1))
		}

		dataRows = allRecords
	}

	rows := make([]map[string]string, 0, len(dataRows))

	for _, rec := range dataRows {
		row := make(map[string]string, len(headers))
		for i, h := range headers {
			if i < len(rec) {
				row[h] = rec[i]
			}
		}

		rows = append(rows, row)
	}

	return rows
}

func serializeCSVQueryResults(resultRows []map[string]string, out selectOutputSerialization) ([]byte, error) {
	if out.JSON != nil {
		return serializeCSVRowsAsJSON(resultRows, out.JSON)
	}

	buf := serializeCSVRows(resultRows, out.CSV)

	return buf, nil
}

func serializeCSVRowsAsJSON(rows []map[string]string, jsonOut *selectJSONOutput) ([]byte, error) {
	delim := "\n"
	if jsonOut.RecordDelimiter != "" {
		delim = jsonOut.RecordDelimiter
	}

	var buf bytes.Buffer

	for _, row := range rows {
		jsonBytes, err := marshalJSONRow(mapStringToAny(row))
		if err != nil {
			return nil, err
		}

		buf.Write(jsonBytes)
		buf.WriteString(delim)
	}

	return buf.Bytes(), nil
}

// serializeCSVRows serializes result rows to CSV format.
func serializeCSVRows(rows []map[string]string, csvOut *selectCSVOutput) []byte {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)

	if csvOut != nil && csvOut.FieldDelimiter != "" {
		w.Comma = rune(csvOut.FieldDelimiter[0])
	}

	for _, row := range rows {
		keys := sortedKeys(row)
		record := make([]string, len(keys))

		for i, k := range keys {
			record[i] = row[k]
		}

		_ = w.Write(record)
	}

	w.Flush()

	return buf.Bytes()
}

// mapStringToAny converts map[string]string to map[string]any.
func mapStringToAny(m map[string]string) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}

	return out
}
