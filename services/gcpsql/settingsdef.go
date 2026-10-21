package gcpsql

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"
)

const retainDays = 7

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)

	return m
}

// deepCopy copies maps and slices so a response never aliases state a later request mutates.
func deepCopy(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, x := range t {
			out[k] = deepCopy(x)
		}

		return out
	case []any:
		out := make([]any, len(t))
		for i, x := range t {
			out[i] = deepCopy(x)
		}

		return out
	}

	return v
}

// fillDefaults adds the keys of defaults that dst lacks, recursing into nested objects.
func fillDefaults(dst, defaults map[string]any) {
	for k, dv := range defaults {
		cur, ok := dst[k]
		if !ok {
			dst[k] = deepCopy(dv)

			continue
		}

		if cm, isMap := cur.(map[string]any); isMap {
			if dm, dIsMap := dv.(map[string]any); dIsMap {
				fillDefaults(cm, dm)
			}
		}
	}
}

// mergePatch applies src onto dst: objects merge, anything else replaces, null deletes.
func mergePatch(dst, src map[string]any) {
	for k, v := range src {
		if v == nil {
			delete(dst, k)

			continue
		}

		sm, srcIsMap := v.(map[string]any)
		dm, dstIsMap := dst[k].(map[string]any)

		if srcIsMap && dstIsMap {
			mergePatch(dm, sm)

			continue
		}

		dst[k] = deepCopy(v)
	}
}

func settingsDefaults(version, region string) map[string]any {
	backup := map[string]any{
		keyKind: "sql#backupConfiguration", "startTime": "00:00", "enabled": false,
		"transactionLogRetentionDays": retainDays,
		"backupRetentionSettings":     map[string]any{"retentionUnit": "COUNT", "retainedBackups": retainDays},
	}

	if strings.HasPrefix(version, "MYSQL") {
		backup["binaryLogEnabled"] = false
	} else {
		backup["pointInTimeRecoveryEnabled"] = false
	}

	return map[string]any{
		keyKind: kindSettings, "activationPolicy": "ALWAYS", "availabilityType": "ZONAL",
		"dataDiskSizeGb": "10", "dataDiskType": "PD_SSD", "pricingPlan": "PER_USE",
		"replicationType": "SYNCHRONOUS", "storageAutoResize": true, "storageAutoResizeLimit": "0",
		"settingsVersion": "1", "edition": "ENTERPRISE", "connectorEnforcement": "NOT_REQUIRED",
		"deletionProtectionEnabled": false, "backupConfiguration": backup,
		"ipConfiguration": map[string]any{
			"ipv4Enabled": true, "sslMode": "ALLOW_UNENCRYPTED_AND_ENCRYPTED", "requireSsl": false,
		},
		"locationPreference": map[string]any{keyKind: "sql#locationPreference", "zone": region + "-a"},
	}
}

func settingString(s map[string]any, key string) string {
	v, _ := s[key].(string)

	return v
}

// flagsOf extracts settings.databaseFlags.
func flagsOf(s map[string]any) []Flag {
	list, _ := s["databaseFlags"].([]any)
	out := make([]Flag, 0, len(list))

	for _, it := range list {
		m := asMap(it)
		out = append(out, Flag{Name: settingString(m, "name"), Value: settingString(m, "value")})
	}

	return out
}

func flagsSig(f []Flag) string {
	var sb strings.Builder

	for _, x := range f {
		sb.WriteString(x.Name + "=" + x.Value + "\n")
	}

	return sb.String()
}

func etagOf(v any) string {
	data, _ := json.Marshal(v)
	sum := sha256.Sum256(data)

	return base64.RawURLEncoding.EncodeToString(sum[:16])
}
