package timestreaminfluxdb

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const hoursPerDay = 24

// ParameterEnv translates an InfluxDBv2 parameter group document into influxd environment settings.
// Every InfluxDBv2 parameter has an influxd flag of the same name in kebab case (INFLUXD_<FLAG>);
// groups for other engines are stored but not applied.
func ParameterEnv(raw json.RawMessage) []string {
	var union map[string]json.RawMessage
	if json.Unmarshal(raw, &union) != nil {
		return nil
	}

	var fields map[string]json.RawMessage
	if json.Unmarshal(union["InfluxDBv2"], &fields) != nil {
		return nil
	}

	env := make([]string, 0, len(fields))

	for name, val := range fields {
		if v, ok := paramValue(name, val); ok {
			env = append(env, "INFLUXD_"+envName(name)+"="+v)
		}
	}

	slices.Sort(env)

	return env
}

// envName turns storageWalMaxWriteDelay into STORAGE_WAL_MAX_WRITE_DELAY.
func envName(param string) string {
	var sb strings.Builder

	for i, r := range param {
		if unicode.IsUpper(r) && i > 0 {
			sb.WriteByte('_')
		}

		sb.WriteRune(unicode.ToUpper(r))
	}

	return sb.String()
}

func paramValue(name string, raw json.RawMessage) (string, bool) {
	if isDurationParam(name) {
		return durationValue(raw)
	}

	var v any
	if json.Unmarshal(raw, &v) != nil {
		return "", false
	}

	switch t := v.(type) {
	case bool:
		return strconv.FormatBool(t), true
	case string:
		return t, true
	case float64:
		return strconv.FormatInt(int64(t), 10), true
	}

	return "", false
}

func durationValue(raw json.RawMessage) (string, bool) {
	var d struct {
		DurationType string `json:"durationType"`
		Value        int64  `json:"value"`
	}
	if json.Unmarshal(raw, &d) != nil {
		return "", false
	}

	unit := map[string]time.Duration{
		"milliseconds": time.Millisecond, "seconds": time.Second, "minutes": time.Minute,
		"hours": time.Hour, "days": hoursPerDay * time.Hour,
	}[d.DurationType]
	if unit == 0 {
		return "", false
	}

	return fmt.Sprint(time.Duration(d.Value) * unit), true
}

// paramEnvLocked is the influxd environment of the parameter group an instance references.
func (b *InMemoryBackend) paramEnvLocked(groupID string) []string {
	g, ok := b.groups.Get(groupID)
	if !ok {
		return nil
	}

	return ParameterEnv(g.Parameters)
}
