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

// evaluateCSVQuery reads all CSV rows from data, applies the SQL query, and streams results.
// All rows are collected before evaluation so that ORDER BY and aggregate functions work correctly.
func evaluateCSVQuery(w io.Writer, query *sqlQuery, data []byte, req *selectRequest) (int64, error) {
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

	var headers []string
	var rows []map[string]string
	firstRecord := true

	for {
		rec, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return 0, fmt.Errorf("reading CSV: %w", err)
		}

		if firstRecord {
			firstRecord = false
			headers = prepareCSVHeaders(fileHeaderInfo, rec)

			if fileHeaderInfo == "USE" {
				continue
			}
		}

		rowMap := make(map[string]string, len(headers))
		for i, h := range headers {
			if i < len(rec) {
				rowMap[h] = rec[i]
			}
		}

		rows = append(rows, rowMap)
	}

	resultRows, err := evalQuery(query, rows)
	if err != nil {
		return 0, err
	}

	if len(resultRows) == 0 {
		return 0, nil
	}

	resultBytes, err := serializeCSVQueryResults(resultRows, req.OutputSerialization)
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

func prepareCSVHeaders(fileHeaderInfo string, firstRecord []string) []string {
	var headers []string
	switch fileHeaderInfo {
	case "USE":
		headers = firstRecord
	default: // IGNORE or NONE
		for i := range firstRecord {
			headers = append(headers, fmt.Sprintf("_%d", i+1))
		}
	}

	return headers
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
		jsonBytes, err := json.Marshal(mapStringToAny(row))
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
