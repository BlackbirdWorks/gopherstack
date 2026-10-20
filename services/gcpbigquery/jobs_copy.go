package gcpbigquery

import (
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

func cloneValue(v any) any {
	arr, ok := v.([]any)
	if !ok {
		return v
	}

	out := make([]any, len(arr))
	for i, e := range arr {
		out[i] = cloneValue(e)
	}

	return out
}

func cloneRows(rows [][]any) [][]any {
	out := make([][]any, len(rows))

	for i, r := range rows {
		c, _ := cloneValue(r).([]any)
		out[i] = c
	}

	return out
}

func tableRefs(cfg map[string]any) []tableRef {
	var raw []any

	if one, ok := cfg["sourceTable"].(map[string]any); ok {
		raw = append(raw, one)
	}

	if many, ok := cfg["sourceTables"].([]any); ok {
		raw = append(raw, many...)
	}

	out := make([]tableRef, 0, len(raw))

	for _, r := range raw {
		m, _ := r.(map[string]any)
		ids := refIDs(map[string]any{"r": m}, "r")
		out = append(out, tableRef{ProjectID: ids[litProjectid], DatasetID: ids["datasetId"], TableID: ids["tableId"]})
	}

	return out
}

// sameSchema reports whether two schemas have equal column names, types and modes.
func sameSchema(a, b []bqsql.Field) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if !strings.EqualFold(a[i].Name, b[i].Name) || a[i].Type != b[i].Type || a[i].Mode != b[i].Mode ||
			!sameSchema(a[i].Fields, b[i].Fields) {
			return false
		}
	}

	return true
}

// execCopy runs a copy job; the caller holds the write lock.
func (b *InMemoryBackend) execCopy(_ string, cfg map[string]any, dryRun bool) (*outcome, error) {
	srcs := tableRefs(cfg)
	dstIDs := refIDs(cfg, "destinationTable")
	dst := tableRef{ProjectID: dstIDs[litProjectid], DatasetID: dstIDs["datasetId"], TableID: dstIDs["tableId"]}

	if len(srcs) == 0 || dst.TableID == "" {
		return nil, errInvalid("Required parameter is missing: copy sourceTable and destinationTable")
	}

	var (
		fields []bqsql.Field
		rows   [][]any
		bytes  int64
	)

	for i, s := range srcs {
		_, t, err := b.tableLocked(s.ProjectID, s.DatasetID, s.TableID)
		if err != nil {
			return nil, err
		}

		if t.ViewQuery != "" {
			return nil, errInvalid("Cannot copy %s:%s.%s: it is a view", s.ProjectID, s.DatasetID, s.TableID)
		}

		if i == 0 {
			fields = t.Schema
		} else if !sameSchema(fields, t.Schema) {
			return nil, errInvalid("Source tables have different schemas")
		}

		rows = append(rows, cloneRows(t.Rows)...)
		bytes += tableBytes(t)
	}

	out := &outcome{
		Stats: map[string]any{
			"copiedRows":         strconv.Itoa(len(rows)),
			"copiedLogicalBytes": strconv.FormatInt(bytes, 10),
		},
	}
	if dryRun {
		return out, nil
	}

	create, _ := cfg["createDisposition"].(string)
	write, _ := cfg["writeDisposition"].(string)

	if jerr := b.writeDestination(dst, fields, rows, create, write, true); jerr != nil {
		out.Err = jerr
	} else {
		out.Dest = &dst
	}

	return out, nil
}
