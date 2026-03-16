package s3

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// evaluateJSONQuery reads JSON from data, applies the SQL query, and streams results.
func evaluateJSONQuery(w io.Writer, query *sqlQuery, data []byte, req *selectRequest) (int64, error) {
	jsonIn := req.InputSerialization.JSON
	jsonType := "DOCUMENT"
	if jsonIn != nil && jsonIn.Type != "" {
		jsonType = strings.ToUpper(jsonIn.Type)
	}

	var totalBytesReturned int64
	var returnedRowsCount int

	if jsonType == "LINES" {
		scanner := bufio.NewScanner(bytes.NewReader(data))
		// Increase the buffer to handle large JSON lines (up to 10MB).
		const maxScanTokenSize = 10 * 1024 * 1024
		scanner.Buffer(make([]byte, bufio.MaxScanTokenSize), maxScanTokenSize)
		for scanner.Scan() {
			if query.limit > 0 && returnedRowsCount >= query.limit {
				break
			}

			line := scanner.Bytes()
			if len(bytes.TrimSpace(line)) == 0 {
				continue
			}

			var row map[string]any
			if err := json.Unmarshal(line, &row); err != nil {
				return totalBytesReturned, fmt.Errorf("parsing JSON line: %w", err)
			}

			resultRows, evalErr := evalQueryJSON(query, []map[string]any{row})
			if evalErr != nil {
				return totalBytesReturned, evalErr
			}

			if len(resultRows) > 0 {
				resultBytes, serialErr := serializeJSONQueryResults(resultRows, req.OutputSerialization)
				if serialErr != nil {
					return totalBytesReturned, serialErr
				}

				if len(resultBytes) > 0 {
					if err := writeSelectEvent(w, "Records", "application/octet-stream", resultBytes); err != nil {
						return totalBytesReturned, err
					}
					totalBytesReturned += int64(len(resultBytes))
					returnedRowsCount += len(resultRows)
				}
			}
		}

		if err := scanner.Err(); err != nil {
			return totalBytesReturned, fmt.Errorf("scanning JSON lines: %w", err)
		}
	} else {
		var doc any
		if err := json.Unmarshal(data, &doc); err != nil {
			return 0, fmt.Errorf("parsing JSON document: %w", err)
		}

		var rows []map[string]any
		switch v := doc.(type) {
		case []any:
			for _, item := range v {
				if obj, ok := item.(map[string]any); ok {
					rows = append(rows, obj)
				}
			}
		case map[string]any:
			rows = append(rows, v)
		}

		resultRows, evalErr := evalQueryJSON(query, rows)
		if evalErr != nil {
			return 0, evalErr
		}

		if len(resultRows) > 0 {
			resultBytes, serialErr := serializeJSONQueryResults(resultRows, req.OutputSerialization)
			if serialErr != nil {
				return 0, serialErr
			}

			if len(resultBytes) > 0 {
				if err := writeSelectEvent(w, "Records", "application/octet-stream", resultBytes); err != nil {
					return 0, err
				}
				totalBytesReturned = int64(len(resultBytes))
			}
		}
	}

	return totalBytesReturned, nil
}

func serializeJSONQueryResults(resultRows []map[string]any, out selectOutputSerialization) ([]byte, error) {
	if out.CSV != nil {
		return serializeJSONRowsAsCSV(resultRows, out.CSV)
	}

	return serializeJSONRows(resultRows, out.JSON)
}

func serializeJSONRowsAsCSV(rows []map[string]any, csvOut *selectCSVOutput) ([]byte, error) {
	strRows := make([]map[string]string, 0, len(rows))

	for _, row := range rows {
		strRow := make(map[string]string, len(row))
		for k, v := range row {
			strRow[k] = fmt.Sprintf("%v", v)
		}

		strRows = append(strRows, strRow)
	}

	buf := serializeCSVRows(strRows, csvOut)

	return buf, nil
}

func serializeJSONRows(rows []map[string]any, jsonOut *selectJSONOutput) ([]byte, error) {
	delim := "\n"
	if jsonOut != nil && jsonOut.RecordDelimiter != "" {
		delim = jsonOut.RecordDelimiter
	}

	var buf bytes.Buffer

	for _, row := range rows {
		jsonBytes, err := json.Marshal(row)
		if err != nil {
			return nil, err
		}

		buf.Write(jsonBytes)
		buf.WriteString(delim)
	}

	return buf.Bytes(), nil
}
