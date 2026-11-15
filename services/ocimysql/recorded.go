package ocimysql

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	featureReadEndpoint = "readEndpoint"
	featureSecure       = "secureConnections"
)

// recordedFeatures names the DB system features that are stored and echoed but need an Oracle-side service the
// emulator does not run: the MySQL REST service, the database console, the read endpoint load balancer and a
// customer certificate for secure connections.
func recordedFeatures(extra map[string]jsontext.Value) []string {
	var out []string

	for _, k := range []string{"rest", "databaseConsole", featureReadEndpoint, featureSecure} {
		raw, ok := extra[k]
		if !ok {
			continue
		}

		var v map[string]any
		if json.Unmarshal(raw, &v, jsonx.V1()) != nil {
			continue
		}

		if featureOn(k, v) {
			out = append(out, k)
		}
	}

	slices.Sort(out)

	return out
}

func featureOn(key string, v map[string]any) bool {
	switch key {
	case featureReadEndpoint:
		on, _ := v["isEnabled"].(bool)

		return on
	case featureSecure:
		t, _ := v["certificateGenerationType"].(string)

		return t == "BYOC"
	}

	st, _ := v["status"].(string)

	return st == enabled
}

func logRecordedFeatures(ctx context.Context, id string, extra map[string]jsontext.Value) {
	for _, f := range recordedFeatures(extra) {
		logger.Load(ctx).InfoContext(ctx, "ocimysql: feature recorded, not provisioned (needs an Oracle-side service)",
			"dbSystem", id, "feature", f)
	}
}
