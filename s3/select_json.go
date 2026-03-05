package s3

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// evaluateJSONQuery reads JSON rows from data, applies the SQL query, and serializes results.
func evaluateJSONQuery(query *sqlQuery, data []byte, req *selectRequest) ([]byte, int64, error) {
	jsonIn := req.InputSerialization.JSON

	jsonType := "LINES"
	if jsonIn != nil && jsonIn.Type != "" {
		jsonType = strings.ToUpper(jsonIn.Type)
	}

	rows, parseErr := parseJSONRows(data, jsonType)
	if parseErr != nil {
		return nil, 0, parseErr
	}

	resultRows, evalErr := evalQueryJSON(query, rows)
	if evalErr != nil {
		return nil, 0, evalErr
	}

	if len(resultRows) == 0 {
		return nil, 0, nil
	}

	resultBytes, serialErr := serializeJSONQueryResults(resultRows, req.OutputSerialization)
	if serialErr != nil {
		return nil, 0, serialErr
	}

	return resultBytes, int64(len(resultBytes)), nil
}

func parseJSONRows(data []byte, jsonType string) ([]map[string]any, error) {
	if jsonType == "DOCUMENT" {
		return parseJSONDocument(data)
	}

	return parseJSONLines(data)
}

func parseJSONDocument(data []byte) ([]map[string]any, error) {
	var doc any
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("parsing JSON document: %w", err)
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

	return rows, nil
}

func parseJSONLines(data []byte) ([]map[string]any, error) {
	var rows []map[string]any

	for line := range strings.SplitSeq(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var obj map[string]any
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			return nil, fmt.Errorf("parsing JSON line %q: %w", line, err)
		}

		rows = append(rows, obj)
	}

	return rows, nil
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
		jsonBytes, err := marshalJSONRow(row)
		if err != nil {
			return nil, err
		}

		buf.Write(jsonBytes)
		buf.WriteString(delim)
	}

	return buf.Bytes(), nil
}

// marshalJSONRow serializes a single row to compact JSON.
func marshalJSONRow(row map[string]any) ([]byte, error) {
	return json.Marshal(row)
}
