package gcplogging

import (
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"google.golang.org/protobuf/encoding/protojson"
)

func bqField(name, typ string) map[string]any {
	return map[string]any{"name": name, "type": typ, "mode": "NULLABLE"}
}

// bqSchema is the column set exported entries use; structured payloads are JSON columns.
func bqSchema() []map[string]any {
	return []map[string]any{
		bqField("logName", "STRING"), bqField("timestamp", "TIMESTAMP"), bqField("receiveTimestamp", "TIMESTAMP"),
		bqField("severity", "STRING"), bqField("insertId", "STRING"), bqField("textPayload", "STRING"),
		bqField("jsonPayload", "JSON"), bqField("protoPayload", "JSON"), bqField("resource", "JSON"),
		bqField("labels", "JSON"), bqField("httpRequest", "JSON"), bqField("trace", "STRING"),
		bqField("spanId", "STRING"), bqField("traceSampled", "BOOLEAN"),
	}
}

func bqRow(e *loggingpb.LogEntry) (map[string]any, error) {
	raw, err := protojson.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("encode entry: %w", err)
	}

	doc := map[string]any{}
	if err = json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("decode entry: %w", err)
	}

	row := map[string]any{}

	for _, f := range bqSchema() {
		name, _ := f["name"].(string)

		v, ok := doc[name]
		if !ok {
			continue
		}

		if f["type"] == "JSON" {
			enc, _ := json.Marshal(v)
			v = string(enc)
		}

		row[name] = v
	}

	if _, ok := row["severity"]; !ok {
		row["severity"] = "DEFAULT"
	}

	row["timestamp"] = e.GetTimestamp().AsTime().UTC().Format(time.RFC3339Nano)

	return row, nil
}
