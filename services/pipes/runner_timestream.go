package pipes

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

var (
	errTimestreamTargetARN = errors.New("malformed Timestream table ARN")
	errTimestreamMapping   = errors.New("timestream mapping")
)

const (
	timeFieldTimestampFormat = "TIMESTAMP_FORMAT"
	epochUnitMilliseconds    = "MILLISECONDS"
	measureTypeMulti         = "MULTI"
	timestreamARNParts       = 6
)

// TimestreamRecord is one Timestream record built from a pipe event.
type TimestreamRecord struct {
	MeasureName      string
	MeasureValue     string
	MeasureValueType string
	Time             string
	TimeUnit         string
	Dimensions       []TimestreamRecordValue
	MeasureValues    []TimestreamRecordValue
	Version          int64
}

// TimestreamRecordValue is a typed name/value pair (a dimension or a multi-measure attribute).
type TimestreamRecordValue struct {
	Name  string
	Value string
	Type  string
}

// PipeTimestreamWriter writes records to the Timestream table database/table in region.
type PipeTimestreamWriter interface {
	WriteTimestreamRecords(ctx context.Context, region, database, table string, records []TimestreamRecord) error
}

// parseTimestreamTableARN splits arn:aws:timestream:region:acct:database/DB/table/TBL.
func parseTimestreamTableARN(arn string) (string, string, string, error) {
	parts := strings.SplitN(arn, ":", timestreamARNParts)
	if len(parts) < timestreamARNParts {
		return "", "", "", fmt.Errorf("%w: %q", errTimestreamTargetARN, arn)
	}

	res := strings.Split(parts[5], "/")
	if len(res) != 4 || res[0] != "database" || res[2] != "table" || res[1] == "" || res[3] == "" {
		return "", "", "", fmt.Errorf("%w: %q", errTimestreamTargetARN, arn)
	}

	return parts[3], res[1], res[3], nil
}

func (r *Runner) invokeTimestreamTarget(ctx context.Context, p *Pipe, payload []byte) error {
	if r.jobs.Timestream == nil {
		return fmt.Errorf("%w: timestream target %q", ErrTargetInvokerUnwired, p.Target)
	}

	region, db, table, err := parseTimestreamTableARN(p.Target)
	if err != nil {
		return err
	}

	if p.TargetParameters == nil || p.TargetParameters.TimestreamParameters == nil {
		return fmt.Errorf("%w: TimestreamParameters is required", errTimestreamMapping)
	}

	records, err := BuildTimestreamRecords(p.TargetParameters.TimestreamParameters, splitPipeEvents(payload))
	if err != nil {
		return err
	}

	return r.jobs.Timestream.WriteTimestreamRecords(ctx, region, db, table, records)
}

// splitPipeEvents returns the events of a batch payload: the Records array, a bare JSON array,
// or the payload itself as a single event.
func splitPipeEvents(payload []byte) []any {
	var v any
	if err := json.Unmarshal(payload, &v); err != nil {
		return nil
	}

	switch t := v.(type) {
	case []any:
		return t
	case map[string]any:
		if recs, ok := t["Records"].([]any); ok {
			return recs
		}
	}

	return []any{v}
}

// BuildTimestreamRecords maps each event through the pipe's Timestream parameters: one record per
// single-measure mapping and one per multi-measure mapping, per event.
func BuildTimestreamRecords(tp *TimestreamParameters, events []any) ([]TimestreamRecord, error) {
	var out []TimestreamRecord

	for i, ev := range events {
		base, err := timestreamBaseRecord(tp, ev)
		if err != nil {
			return nil, fmt.Errorf("event %d: %w", i, err)
		}

		for _, m := range tp.SingleMeasureMappings {
			val, verr := resolvePipeValue(ev, m.MeasureValue)
			if verr != nil {
				return nil, fmt.Errorf("event %d: measure %q: %w", i, m.MeasureName, verr)
			}

			rec := base
			rec.MeasureName, rec.MeasureValue, rec.MeasureValueType = m.MeasureName, val, m.MeasureValueType
			out = append(out, rec)
		}

		for _, m := range tp.MultiMeasureMappings {
			rec := base
			rec.MeasureName, rec.MeasureValueType = m.MultiMeasureName, measureTypeMulti

			for _, a := range m.MultiMeasureAttributeMappings {
				val, verr := resolvePipeValue(ev, a.MeasureValue)
				if verr != nil {
					return nil, fmt.Errorf("event %d: measure %q: %w", i, a.MultiMeasureAttributeName, verr)
				}

				rec.MeasureValues = append(rec.MeasureValues, TimestreamRecordValue{
					Name: a.MultiMeasureAttributeName, Value: val, Type: a.MeasureValueType,
				})
			}

			out = append(out, rec)
		}
	}

	return out, nil
}

func timestreamBaseRecord(tp *TimestreamParameters, ev any) (TimestreamRecord, error) {
	rec := TimestreamRecord{Version: 1}

	ts, err := resolvePipeValue(ev, tp.TimeValue)
	if err != nil {
		return rec, fmt.Errorf("TimeValue: %w", err)
	}

	if rec.Time, rec.TimeUnit, err = timestreamTime(tp, ts); err != nil {
		return rec, err
	}

	if tp.VersionValue != "" {
		vs, verr := resolvePipeValue(ev, tp.VersionValue)
		if verr != nil {
			return rec, fmt.Errorf("VersionValue: %w", verr)
		}

		rec.Version, err = strconv.ParseInt(vs, 10, 64)
		if err != nil || rec.Version < 1 {
			return rec, fmt.Errorf("%w: VersionValue %q must be an integer >= 1", errTimestreamMapping, vs)
		}
	}

	for _, d := range tp.DimensionMappings {
		val, derr := resolvePipeValue(ev, d.DimensionValue)
		if derr != nil {
			return rec, fmt.Errorf("dimension %q: %w", d.DimensionName, derr)
		}

		rec.Dimensions = append(rec.Dimensions, TimestreamRecordValue{
			Name: d.DimensionName, Value: val, Type: d.DimensionValueType,
		})
	}

	return rec, nil
}

// timestreamTime converts the resolved time value to a WriteRecords Time and TimeUnit.
// TIMESTAMP_FORMAT values are parsed and sent as epoch milliseconds; EPOCH values keep EpochTimeUnit.
func timestreamTime(tp *TimestreamParameters, v string) (string, string, error) {
	if tp.TimeFieldType == timeFieldTimestampFormat {
		t, err := time.Parse(javaTimeLayout(tp.TimestampFormat), v)
		if err != nil {
			return "", "", fmt.Errorf("%w: TimeValue %q does not match %q", errTimestreamMapping, v, tp.TimestampFormat)
		}

		return strconv.FormatInt(t.UnixMilli(), 10), epochUnitMilliseconds, nil
	}

	whole, _, _ := strings.Cut(v, ".")
	if _, err := strconv.ParseInt(whole, 10, 64); err != nil {
		return "", "", fmt.Errorf("%w: TimeValue %q is not an epoch number", errTimestreamMapping, v)
	}

	unit := tp.EpochTimeUnit
	if unit == "" {
		unit = epochUnitMilliseconds
	}

	return whole, unit, nil
}

// javaTimeLayout converts a Java SimpleDateFormat pattern to a Go time layout.
func javaTimeLayout(f string) string {
	repl := []struct{ java, goLayout string }{
		{"yyyy", "2006"}, {"yy", "06"}, {"MMM", "Jan"}, {"MM", "01"}, {"dd", "02"},
		{"HH", "15"}, {"hh", "03"}, {"mm", "04"}, {"ss", "05"}, {"SSS", "000"},
		{"XXX", "Z07:00"}, {"Z", "-0700"}, {"z", "MST"}, {"a", "PM"},
	}

	var b strings.Builder

	for i := 0; i < len(f); {
		if f[i] == '\'' {
			end := strings.IndexByte(f[i+1:], '\'')
			if end < 0 {
				b.WriteString(f[i+1:])

				break
			}

			b.WriteString(f[i+1 : i+1+end])
			i += end + len("''")

			continue
		}

		matched := false

		for _, r := range repl {
			if strings.HasPrefix(f[i:], r.java) {
				b.WriteString(r.goLayout)
				i += len(r.java)
				matched = true

				break
			}
		}

		if !matched {
			b.WriteByte(f[i])
			i++
		}
	}

	return b.String()
}

// resolvePipeValue resolves a Pipes dynamic path ($.a.b[0]) against ev, or returns a static value as is.
func resolvePipeValue(ev any, spec string) (string, error) {
	if !strings.HasPrefix(spec, "$") {
		return spec, nil
	}

	cur := ev

	for seg := range strings.SplitSeq(strings.TrimPrefix(strings.TrimPrefix(spec, "$"), "."), ".") {
		if seg == "" {
			continue
		}

		next, ok := stepPipePath(cur, seg)
		if !ok {
			return "", fmt.Errorf("%w: path %q not found in event", errTimestreamMapping, spec)
		}

		cur = next
	}

	return pipeScalarString(cur), nil
}

func stepPipePath(cur any, seg string) (any, bool) {
	key, idx, hasIdx := seg, 0, false

	if open := strings.IndexByte(seg, '['); open > 0 && strings.HasSuffix(seg, "]") {
		n, err := strconv.Atoi(seg[open+1 : len(seg)-1])
		if err != nil {
			return nil, false
		}

		key, idx, hasIdx = seg[:open], n, true
	}

	cur = decodeEmbeddedJSON(cur)

	m, ok := cur.(map[string]any)
	if !ok {
		return nil, false
	}

	val, ok := m[key]
	if !ok {
		if k, kok := m["kinesis"].(map[string]any); kok {
			val, ok = k[key]
		}
	}

	if !ok {
		return nil, false
	}

	if !hasIdx {
		return val, true
	}

	arr, ok := val.([]any)
	if !ok || idx < 0 || idx >= len(arr) {
		return nil, false
	}

	return arr[idx], true
}

// decodeEmbeddedJSON parses a JSON string (an SQS body, a base64 Kinesis blob) so paths can descend into it.
func decodeEmbeddedJSON(v any) any {
	s, ok := v.(string)
	if !ok {
		return v
	}

	var out any
	if json.Unmarshal([]byte(s), &out) == nil {
		return out
	}

	if raw, err := base64.StdEncoding.DecodeString(s); err == nil && json.Unmarshal(raw, &out) == nil {
		return out
	}

	return v
}

func pipeScalarString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(t)
	case nil:
		return ""
	}

	b, _ := json.Marshal(v)

	return string(b)
}
