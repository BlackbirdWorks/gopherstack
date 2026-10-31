package timestreamquery

import (
	"context"
	"fmt"
	"strings"
	"time"
)

const runStatusManualTriggerFailure = "MANUAL_TRIGGER_FAILURE"

// ExecuteScheduledQuery runs the scheduled query once for invocationTime ("@scheduled_runtime")
// and ingests its result rows into the target table, recording the run outcome.
func (b *InMemoryBackend) ExecuteScheduledQuery(ctx context.Context, arnStr string, invocationTime time.Time) error {
	b.mu.RLock("ExecuteScheduledQuery")
	found, err := b.lookupByARN(arnStr)
	if err != nil {
		b.mu.RUnlock()

		return err
	}

	sq := cloneScheduledQuery(found)
	b.mu.RUnlock()

	ctx = context.WithValue(ctx, regionContextKey{}, regionFromARN(arnStr, b.defaultRegion))
	started := time.Now()
	stats, runErr := b.runScheduledQuery(ctx, sq, invocationTime)
	stats.ExecutionTimeInMillis = time.Since(started).Milliseconds()

	b.mu.Lock("ExecuteScheduledQuery")
	defer b.mu.Unlock()

	cur, lookupErr := b.lookupByARN(arnStr)
	if lookupErr != nil {
		return nil //nolint:nilerr // deleted while running: the run result has nowhere to go
	}

	cur.LastRunTime = invocationTime
	cur.LastRunStats = &stats
	cur.LastRunStatus = runStatusManualTriggerSuccess
	cur.LastRunFailure = ""

	if runErr != nil {
		cur.LastRunStatus = runStatusManualTriggerFailure
		cur.LastRunFailure = runErr.Error()
	}

	return nil
}

func (b *InMemoryBackend) runScheduledQuery(
	ctx context.Context, sq *ScheduledQuery, invocationTime time.Time,
) (ExecutionStats, error) {
	var stats ExecutionStats

	rs, err := b.execute(ctx, sq.QueryString, map[string]string{"@scheduled_runtime": formatTS(invocationTime.UTC())})
	if err != nil {
		return stats, err
	}

	stats.QueryResultRows = int64(len(rs.rows))
	stats.CumulativeBytesScanned = rs.scanned
	stats.BytesMetered = statusFor(rs).CumulativeBytesMetered

	src := b.recordSource()
	if sq.TargetDetail == nil || src == nil {
		return stats, nil
	}

	recs, err := scheduledRecords(rs, sq.TargetDetail, invocationTime)
	if err != nil {
		return stats, err
	}

	if len(recs) == 0 {
		return stats, nil
	}

	region := getRegion(ctx, b.defaultRegion)
	if err = src.AppendRecords(region, sq.TargetDatabase, sq.TargetTable, recs); err != nil {
		return stats, err
	}

	stats.RecordsIngested = int64(len(recs))

	for _, r := range recs {
		stats.DataWrites += recordBytes(r)
	}

	return stats, nil
}

type rowView struct {
	idx  map[string]int
	data []Datum
}

func (v rowView) get(col string) (string, bool) {
	i, ok := v.idx[col]
	if !ok || i >= len(v.data) || v.data[i].ScalarValue == nil {
		return "", false
	}

	return *v.data[i].ScalarValue, true
}

// scheduledRecords maps result rows onto records per TargetConfiguration.TimestreamConfiguration.
func scheduledRecords(rs *resultSet, td *ScheduledQueryTargetDetail, version time.Time) ([]SourceRecord, error) {
	idx := make(map[string]int, len(rs.cols))
	for i, c := range rs.cols {
		idx[c.Name] = i
	}

	for _, need := range append([]string{td.TimeColumn, td.MeasureNameColumn}, dimensionColumns(td)...) {
		if _, ok := idx[need]; need != "" && !ok {
			return nil, fmt.Errorf("column '%s' named in the target configuration is not in the query result", need)
		}
	}

	var out []SourceRecord

	for _, row := range rs.rows {
		v := rowView{idx: idx, data: row.Data}

		raw, ok := v.get(td.TimeColumn)
		if !ok {
			continue
		}

		t, ok := parseTSValue(raw)
		if !ok {
			return nil, fmt.Errorf("time column '%s' value '%s' is not a timestamp", td.TimeColumn, raw)
		}

		base := SourceRecord{Time: t, Version: version.UnixMilli(), Dimensions: dimensionsOf(v, td)}
		out = append(out, recordsForRow(v, td, base)...)
	}

	return out, nil
}

func dimensionColumns(td *ScheduledQueryTargetDetail) []string {
	cols := make([]string, 0, len(td.DimensionMappings))
	for _, d := range td.DimensionMappings {
		cols = append(cols, d.Name)
	}

	return cols
}

func dimensionsOf(v rowView, td *ScheduledQueryTargetDetail) []SourceDimension {
	var dims []SourceDimension

	for _, d := range td.DimensionMappings {
		if val, ok := v.get(d.Name); ok {
			dims = append(dims, SourceDimension{Name: d.Name, Value: val})
		}
	}

	return dims
}

func recordsForRow(v rowView, td *ScheduledQueryTargetDetail, base SourceRecord) []SourceRecord {
	nameVal, _ := v.get(td.MeasureNameColumn)

	var out []SourceRecord

	if mm := td.MultiMeasureMappings; mm != nil {
		name := mm.TargetMultiMeasureName
		if td.MeasureNameColumn != "" {
			name = nameVal
		}

		if r, ok := multiRecord(v, base, name, mm.MultiMeasureAttributeMappings); ok {
			out = append(out, r)
		}
	}

	for _, m := range td.MixedMeasureMappings {
		if td.MeasureNameColumn != "" && m.MeasureName != nameVal {
			continue
		}

		target := firstNonEmpty(m.TargetMeasureName, m.MeasureName, m.SourceColumn)

		if strings.EqualFold(m.MeasureValueType, measureTypeMulti) {
			if r, ok := multiRecord(v, base, target, m.MultiMeasureAttributeMappings); ok {
				out = append(out, r)
			}

			continue
		}

		if val, ok := v.get(m.SourceColumn); ok {
			r := base
			r.MeasureName = target
			r.MeasureValue = val
			r.MeasureValueType = strings.ToUpper(m.MeasureValueType)
			out = append(out, r)
		}
	}

	return out
}

func multiRecord(v rowView, base SourceRecord, name string, attrs []MultiMeasureAttributeMapping) (SourceRecord, bool) {
	r := base
	r.MeasureName = name
	r.MeasureValueType = measureTypeMulti

	for _, a := range attrs {
		if val, ok := v.get(a.SourceColumn); ok {
			r.Measures = append(r.Measures, SourceMeasure{
				Name:  firstNonEmpty(a.TargetMultiMeasureAttributeName, a.SourceColumn),
				Value: val,
				Type:  strings.ToUpper(a.MeasureValueType),
			})
		}
	}

	return r, len(r.Measures) > 0
}

func firstNonEmpty(xs ...string) string {
	for _, x := range xs {
		if x != "" {
			return x
		}
	}

	return ""
}
