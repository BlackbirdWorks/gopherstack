package gcpbigquery

import (
	"sort"
	"strconv"

	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

const selfLinkBase = "https://bigquery.googleapis.com/bigquery/v2/"

func schemaWire(fields []bqsql.Field) map[string]any {
	if fields == nil {
		fields = []bqsql.Field{}
	}

	return map[string]any{"fields": fields}
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

func datasetWire(project, id string, d *Dataset) map[string]any {
	out := cloneMap(d.Meta)
	out[litKind] = "bigquery#dataset"
	out["id"] = project + ":" + id
	out[litSelflink] = selfLinkBase + "projects/" + project + "/datasets/" + id
	out["datasetReference"] = map[string]any{"datasetId": id, litProjectid: project}
	out[litLocation] = d.Location
	out[litCreationtime] = msString(d.Created)
	out["lastModifiedTime"] = msString(d.Modified)
	out[litEtag] = etagOf("dataset", project, id, msString(d.Modified))

	if _, ok := out["access"]; !ok {
		out["access"] = defaultAccess()
	}

	out[litType] = "DEFAULT"
	out["isCaseInsensitive"] = false
	out["maxTimeTravelHours"] = "168"

	return out
}

func defaultAccess() []map[string]any {
	return []map[string]any{
		{litRole: "WRITER", litSpecialGroup: "projectWriters"},
		{litRole: "OWNER", litSpecialGroup: "projectOwners"},
		{litRole: "OWNER", "userByEmail": "gopherstack@localhost"},
		{litRole: "READER", litSpecialGroup: "projectReaders"},
	}
}

func datasetSummary(project, id string, d *Dataset) map[string]any {
	out := map[string]any{
		litKind: "bigquery#dataset", "id": project + ":" + id, litLocation: d.Location,
		"datasetReference": map[string]any{"datasetId": id, litProjectid: project},
	}

	for _, k := range []string{"labels", litFriendlyname} {
		if v, ok := d.Meta[k]; ok {
			out[k] = v
		}
	}

	return out
}

func tableBytes(t *Table) int64 {
	var n int64

	for _, row := range t.Rows {
		for i, f := range t.Schema {
			if i < len(row) {
				n += bqsql.LogicalBytes(f, row[i])
			}
		}
	}

	return n
}

func tableWire(project, ds, id string, t *Table, location string) map[string]any {
	out := cloneMap(t.Meta)
	out[litKind] = "bigquery#table"
	out["id"] = project + ":" + ds + "." + id
	out[litSelflink] = selfLinkBase + "projects/" + project + "/datasets/" + ds + "/tables/" + id
	out["tableReference"] = tableRef{ProjectID: project, DatasetID: ds, TableID: id}
	out["schema"] = schemaWire(t.Schema)
	out[litCreationtime] = msString(t.Created)
	out["lastModifiedTime"] = msString(t.Modified)
	out[litEtag] = etagOf("table", project, ds, id, strconv.FormatInt(t.Seq, 10))
	out[litType] = t.kind()
	out[litLocation] = location

	if t.ViewQuery != "" {
		out["view"] = map[string]any{"query": t.ViewQuery, litUselegacysql: false}
	} else {
		out["numRows"] = strconv.Itoa(len(t.Rows))
		out["numBytes"] = msString(tableBytes(t))
		out["numLongTermBytes"] = "0"
		out["numActiveLogicalBytes"] = msString(tableBytes(t))
	}

	return out
}

func tableSummary(project, ds, id string, t *Table) map[string]any {
	out := map[string]any{
		litKind: "bigquery#table", "id": project + ":" + ds + "." + id, litType: t.kind(),
		"tableReference": tableRef{ProjectID: project, DatasetID: ds, TableID: id},
		litCreationtime:  msString(t.Created),
	}

	keys := []string{"labels", "timePartitioning", "clustering", "rangePartitioning", litFriendlyname, "expirationTime"}

	for _, k := range keys {
		if v, ok := t.Meta[k]; ok {
			out[k] = v
		}
	}

	if t.ViewQuery != "" {
		out["view"] = map[string]any{litUselegacysql: false}
	}

	return out
}
