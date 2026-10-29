package athena

import (
	"context"
	"database/sql"
	"encoding/json"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/iceberg"
)

var icebergMetaTables = []string{"snapshots", "history", "files", "manifests", "refs"}

// icebergMetaSuffix splits "tbl$snapshots" into the table and metadata table names.
func icebergMetaSuffix(name string) (string, string, bool) {
	base, meta, ok := strings.Cut(name, "$")
	if !ok {
		return name, "", false
	}

	for _, m := range icebergMetaTables {
		if strings.EqualFold(meta, m) {
			return base, m, true
		}
	}

	return name, "", false
}

func tsText(ms int64) string { return time.UnixMilli(ms).UTC().Format(tsMillisLayout) }

func mapJSON(m map[string]string) string {
	b, _ := json.Marshal(m)

	return string(b)
}

func optID(p *int64) any {
	if p == nil {
		return nil
	}

	return *p
}

func (t *planTable) loadIcebergMetaTable(
	ctx context.Context, conn *sql.Conn, l *tableLoader, tbl *icebergTable, snap iceberg.Snapshot, hasSnap bool,
) error {
	var cols []Column

	var rows [][]any

	switch t.metaTable {
	case "snapshots":
		cols, rows = snapshotsTable(tbl.meta)
	case "history":
		cols, rows = historyTable(tbl.meta)
	case "refs":
		cols, rows = refsTable(tbl.meta)
	case "manifests":
		var err error

		if cols, rows, err = l.manifestsTable(ctx, snap, hasSnap); err != nil {
			return err
		}
	default:
		var err error

		if cols, rows, err = l.filesTable(ctx, snap, hasSnap); err != nil {
			return err
		}
	}

	return fillTable(ctx, conn, t.schema, t.name, cols, func(stmt *sql.Stmt, _ []colKind) error {
		for _, r := range rows {
			if err := l.checkRow(ctx); err != nil {
				return err
			}

			if _, err := stmt.ExecContext(ctx, r...); err != nil {
				return err
			}
		}

		return nil
	})
}

func snapshotsTable(m *iceberg.TableMetadata) ([]Column, [][]any) {
	cols := []Column{
		{Name: "committed_at", Type: "timestamp"}, {Name: "snapshot_id", Type: "bigint"},
		{Name: "parent_id", Type: "bigint"}, {Name: "operation", Type: "varchar"},
		{Name: "manifest_list", Type: "varchar"}, {Name: "summary", Type: "varchar"},
	}
	rows := make([][]any, 0, len(m.Snapshots))

	for _, s := range m.Snapshots {
		rows = append(rows, []any{
			tsText(s.TimestampMS), s.SnapshotID, optID(s.ParentSnapshotID),
			s.Summary["operation"], s.ManifestList, mapJSON(s.Summary),
		})
	}

	return cols, rows
}

func historyTable(m *iceberg.TableMetadata) ([]Column, [][]any) {
	cols := []Column{
		{Name: "made_current_at", Type: "timestamp"}, {Name: "snapshot_id", Type: "bigint"},
		{Name: "parent_id", Type: "bigint"}, {Name: "is_current_ancestor", Type: "boolean"},
	}
	ancestors := map[int64]bool{}

	for id := m.CurrentSnapshotID; id != iceberg.NoSnapshot; {
		s, ok := m.SnapshotByID(id)
		if !ok {
			break
		}

		ancestors[id] = true

		if s.ParentSnapshotID == nil {
			break
		}

		id = *s.ParentSnapshotID
	}

	rows := make([][]any, 0, len(m.SnapshotLog))

	for _, e := range m.SnapshotLog {
		var parent any

		if s, ok := m.SnapshotByID(e.SnapshotID); ok {
			parent = optID(s.ParentSnapshotID)
		}

		rows = append(rows, []any{tsText(e.TimestampMS), e.SnapshotID, parent, boolInt(ancestors[e.SnapshotID])})
	}

	return cols, rows
}

func refsTable(m *iceberg.TableMetadata) ([]Column, [][]any) {
	cols := []Column{
		{Name: "name", Type: "varchar"}, {Name: "type", Type: "varchar"}, {Name: "snapshot_id", Type: "bigint"},
	}
	names := make([]string, 0, len(m.Refs))

	for n := range m.Refs {
		names = append(names, n)
	}

	sort.Strings(names)

	rows := make([][]any, 0, len(names))
	for _, n := range names {
		r := m.Refs[n]
		rows = append(rows, []any{n, strings.ToUpper(r.Type), r.SnapshotID})
	}

	return cols, rows
}

func (l *tableLoader) manifestsTable(
	ctx context.Context, snap iceberg.Snapshot, hasSnap bool,
) ([]Column, [][]any, error) {
	cols := []Column{
		{Name: "path", Type: "varchar"}, {Name: "length", Type: "bigint"},
		{Name: "partition_spec_id", Type: "integer"}, {Name: "added_snapshot_id", Type: "bigint"},
		{Name: "added_data_files_count", Type: "integer"}, {Name: "added_rows_count", Type: "bigint"},
		{Name: "existing_data_files_count", Type: "integer"}, {Name: "existing_rows_count", Type: "bigint"},
		{Name: "deleted_data_files_count", Type: "integer"}, {Name: "deleted_rows_count", Type: "bigint"},
	}

	if !hasSnap || snap.ManifestList == "" {
		return cols, nil, nil
	}

	raw, err := l.readURI(ctx, snap.ManifestList)
	if err != nil {
		return nil, nil, icebergFailf("ICEBERG_MISSING_METADATA", "Manifest list not found: %s", snap.ManifestList)
	}

	list, err := iceberg.ReadManifestList(raw)
	if err != nil {
		return nil, nil, icebergFailf("ICEBERG_INVALID_METADATA", "Invalid manifest list: %v", err)
	}

	rows := make([][]any, 0, len(list))
	for _, m := range list {
		rows = append(rows, []any{
			m.Path, m.Length, int64(m.SpecID), m.AddedSnapshotID, int64(m.AddedFiles), m.AddedRows,
			int64(m.ExistingFiles), m.ExistingRows, int64(m.DeletedFiles), m.DeletedRows,
		})
	}

	return cols, rows, nil
}

func (l *tableLoader) filesTable(ctx context.Context, snap iceberg.Snapshot, hasSnap bool) ([]Column, [][]any, error) {
	cols := []Column{
		{Name: "content", Type: "integer"}, {Name: "file_path", Type: "varchar"},
		{Name: "file_format", Type: "varchar"}, {Name: "spec_id", Type: "integer"},
		{Name: "partition", Type: "varchar"}, {Name: "record_count", Type: "bigint"},
		{Name: "file_size_in_bytes", Type: "bigint"},
	}

	if !hasSnap {
		return cols, nil, nil
	}

	files, err := l.snapshotFiles(ctx, snap)
	if err != nil {
		return nil, nil, err
	}

	var rows [][]any

	for _, f := range append(files.data, files.deletes...) {
		df := f.entry.DataFile
		part, _ := json.Marshal(df.Partition)
		rows = append(rows, []any{
			int64(df.Content), df.Path, df.Format, int64(f.spec), string(part), df.RecordCount, df.FileSize,
		})
	}

	return cols, rows, nil
}
