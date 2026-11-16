package athena

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/iceberg"
)

const (
	secondsPerDayInt = 86400
	colRowPath       = "$path"
	colRowPos        = "$pos"
	travelTimestamp  = "ts"
	travelVersion    = "ver"
	tsMillisLayout   = "2006-01-02 15:04:05.000"
)

// travelSpec is a FOR TIMESTAMP/VERSION AS OF clause on a table reference.
type travelSpec struct {
	kind  string
	value string
}

// icebergTable is a table's resolved current metadata.
type icebergTable struct {
	meta     *iceberg.TableMetadata
	location string
	display  string
}

// icebergError is a failure surfaced with an Athena error name prefix (ICEBERG_MISSING_METADATA: ...).
type icebergError struct{ msg string }

func (e *icebergError) Error() string { return e.msg }

func icebergFailf(code, format string, args ...any) error {
	return &icebergError{msg: code + ": " + fmt.Sprintf(format, args...)}
}

var errBadTimestamp = errors.New("unparseable timestamp")

// openIceberg reads the table's current metadata.json named by the catalog.
func (l *tableLoader) openIceberg(ctx context.Context, def *GlueTableDef, display string) (*icebergTable, error) {
	loc := lowerParam(def.Parameters, paramMetadataLoc)
	if loc == "" {
		return nil, icebergFailf(
			"ICEBERG_INVALID_METADATA",
			"Table is missing [%s] property: %s",
			paramMetadataLoc,
			display,
		)
	}

	data, err := l.readURI(ctx, loc)
	if err != nil {
		if errors.Is(err, errMissingObject) {
			return nil, icebergFailf("ICEBERG_MISSING_METADATA",
				"Metadata not found in metadata location for table %s", display)
		}

		return nil, icebergFailf("ICEBERG_FILESYSTEM_ERROR", "Failed to read metadata for table %s: %v", display, err)
	}

	meta, err := iceberg.ParseMetadata(data)
	if err != nil {
		return nil, icebergFailf("ICEBERG_INVALID_METADATA", "Invalid metadata file for table %s: %v", display, err)
	}

	return &icebergTable{meta: meta, location: loc, display: display}, nil
}

// pickSnapshot applies time travel; ok=false means the table has no data yet.
func (t *icebergTable) pickSnapshot(tr travelSpec) (iceberg.Snapshot, bool, error) {
	switch tr.kind {
	case travelVersion:
		id, err := strconv.ParseInt(strings.TrimSpace(tr.value), 10, 64)
		if err != nil {
			return iceberg.Snapshot{}, false, icebergFailf(
				"INVALID_ARGUMENTS",
				"Invalid Iceberg snapshot ID: %s",
				tr.value,
			)
		}

		s, ok := t.meta.SnapshotByID(id)
		if !ok {
			return iceberg.Snapshot{}, false, icebergFailf("INVALID_ARGUMENTS",
				"Iceberg snapshot ID does not exists: %d", id)
		}

		return s, true, nil
	case travelTimestamp:
		at, err := parseTravelTime(tr.value)
		if err != nil {
			return iceberg.Snapshot{}, false, icebergFailf("INVALID_ARGUMENTS", "Invalid timestamp %q", tr.value)
		}

		s, err := t.meta.SnapshotAsOf(at)
		if err != nil {
			return iceberg.Snapshot{}, false, icebergFailf("INVALID_ARGUMENTS",
				"No version history table %s at or before %s", t.display, at.UTC().Format(tsMillisLayout+" MST"))
		}

		return s, true, nil
	default:
		s, ok := t.meta.CurrentSnapshot()

		return s, ok, nil
	}
}

func parseTravelTime(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	loc := time.UTC

	if i := strings.LastIndex(s, " "); i > 0 && i > strings.Index(s, ":") {
		if l, err := time.LoadLocation(s[i+1:]); err == nil {
			loc, s = l, strings.TrimSpace(s[:i])
		}
	}

	for _, layout := range []string{
		"2006-01-02 15:04:05.999999999", time.RFC3339Nano, dateLayout,
	} {
		if t, err := time.ParseInLocation(layout, s, loc); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("%w %q", errBadTimestamp, s)
}

func (t *icebergTable) schemaFor(s iceberg.Snapshot, hasSnap bool) iceberg.Schema {
	if hasSnap && s.SchemaID != nil {
		if sc, ok := t.meta.SchemaByID(*s.SchemaID); ok {
			return sc
		}
	}

	return t.meta.CurrentSchema()
}

func icebergColumns(sc iceberg.Schema, display string) ([]Column, error) {
	cols := make([]Column, 0, len(sc.Fields))

	for _, f := range sc.Fields {
		ht, err := iceberg.HiveType(f.Type)
		if err != nil {
			return nil, fmt.Errorf("%w: column %s of %s: %w", errUnsupportedFormat, f.Name, display, err)
		}

		cols = append(cols, Column{Name: f.Name, Type: ht, Comment: f.Doc})
	}

	return cols, nil
}

// manifestFiles lists the data and delete files of a snapshot.
type snapshotFiles struct {
	data    []fileEntry
	deletes []fileEntry
}

type fileEntry struct {
	entry iceberg.ManifestEntry
	spec  int
	seq   int64
}

func (l *tableLoader) snapshotFiles(ctx context.Context, snap iceberg.Snapshot) (snapshotFiles, error) {
	var out snapshotFiles

	if snap.ManifestList == "" {
		return out, nil
	}

	listBytes, err := l.readURI(ctx, snap.ManifestList)
	if err != nil {
		return out, icebergFailf("ICEBERG_MISSING_METADATA", "Manifest list not found: %s", snap.ManifestList)
	}

	manifests, err := iceberg.ReadManifestList(listBytes)
	if err != nil {
		return out, icebergFailf("ICEBERG_INVALID_METADATA", "Invalid manifest list %s: %v", snap.ManifestList, err)
	}

	for _, mf := range manifests {
		body, rerr := l.readURI(ctx, mf.Path)
		if rerr != nil {
			return out, icebergFailf("ICEBERG_MISSING_METADATA", "Manifest not found: %s", mf.Path)
		}

		entries, perr := iceberg.ReadManifest(body)
		if perr != nil {
			return out, icebergFailf("ICEBERG_INVALID_METADATA", "Invalid manifest %s: %v", mf.Path, perr)
		}

		for _, e := range entries {
			if e.Status == iceberg.StatusDeleted {
				continue
			}

			seq := mf.SequenceNumber
			if e.SequenceNumber != nil {
				seq = *e.SequenceNumber
			}

			fe := fileEntry{entry: e, spec: mf.SpecID, seq: seq}
			if e.DataFile.Content == iceberg.ContentData {
				out.data = append(out.data, fe)
			} else {
				out.deletes = append(out.deletes, fe)
			}
		}
	}

	return out, nil
}

// icebergDeletes are a snapshot's delete files: position deletes by data file path and equality deletes.
type icebergDeletes struct {
	pos map[string][]deleteSet
	eq  []eqDelete
}

// eqDelete is one equality delete file: rows (keyed by tupleKey over ids) removed from older data files.
type eqDelete struct {
	rows map[string]struct{}
	part map[string]any
	ids  []int
	seq  int64
	spec int
}

const (
	tupleSep  = "\x1f"
	tupleNull = "\x00"
)

func tupleKey(cells []string, nulls []bool) string {
	var sb strings.Builder

	for i, c := range cells {
		if nulls[i] {
			sb.WriteString(tupleNull)
		} else {
			sb.WriteString(c)
		}

		sb.WriteString(tupleSep)
	}

	return sb.String()
}

// appliesTo reports whether the delete covers data written at an older sequence in the same partition scope.
func (d eqDelete) appliesTo(f fileEntry) bool {
	if d.seq <= f.seq {
		return false
	}

	if len(d.part) == 0 {
		return true
	}

	return d.spec == f.spec && partitionKey(d.part) == partitionKey(f.entry.DataFile.Partition)
}

func (l *tableLoader) openDeleteFile(ctx context.Context, path, format string) (columnarFile, error) {
	data, err := l.readURI(ctx, path)
	if err != nil {
		return nil, icebergFailf("ICEBERG_CANNOT_OPEN_SPLIT", "Cannot open delete file %s", path)
	}

	kind := fmtParquet
	if strings.EqualFold(format, "ORC") {
		kind = fmtORC
	}

	pf, err := openColumnar(kind, data)
	if err != nil {
		return nil, icebergFailf("ICEBERG_CANNOT_OPEN_SPLIT", "Cannot open delete file %s: %v", path, err)
	}

	return pf, nil
}

func (l *tableLoader) equalityDelete(ctx context.Context, d fileEntry) (eqDelete, error) {
	df := d.entry.DataFile
	out := eqDelete{ids: df.EqualityIDs, seq: d.seq, spec: d.spec, part: df.Partition, rows: map[string]struct{}{}}

	pf, err := l.openDeleteFile(ctx, df.Path, df.Format)
	if err != nil {
		return out, err
	}

	byID := pf.ColumnIDs()
	names := make([]string, len(df.EqualityIDs))

	for i, id := range df.EqualityIDs {
		n, ok := byID[id]
		if !ok {
			return out, icebergFailf("ICEBERG_INVALID_METADATA",
				"Equality delete file %s has no column with field id %d", df.Path, id)
		}

		names[i] = n
	}

	err = pf.ForEach(names, func(vals []any) error {
		cells, nulls := make([]string, len(vals)), make([]bool, len(vals))

		for i, v := range vals {
			if v == nil {
				nulls[i] = true
			} else {
				cells[i] = parquetCell(v)
			}
		}

		out.rows[tupleKey(cells, nulls)] = struct{}{}

		return nil
	})
	if err != nil {
		return out, icebergFailf("ICEBERG_CURSOR_ERROR", "Cannot read delete file %s: %v", df.Path, err)
	}

	return out, nil
}

// loadDeletes reads every position and equality delete file of a snapshot.
func (l *tableLoader) loadDeletes(ctx context.Context, files []fileEntry) (*icebergDeletes, error) {
	out := &icebergDeletes{pos: map[string][]deleteSet{}}

	for _, d := range files {
		if d.entry.DataFile.Content == iceberg.ContentEqualityDeletes {
			eq, err := l.equalityDelete(ctx, d)
			if err != nil {
				return nil, err
			}

			out.eq = append(out.eq, eq)

			continue
		}

		pf, err := l.openDeleteFile(ctx, d.entry.DataFile.Path, d.entry.DataFile.Format)
		if err != nil {
			return nil, err
		}

		perFile := map[string]map[int64]bool{}

		err = pf.ForEach([]string{colFilePath, colPosName}, func(vals []any) error {
			p, _ := vals[0].(string)
			n, _ := vals[1].(int64)

			if perFile[p] == nil {
				perFile[p] = map[int64]bool{}
			}

			perFile[p][n] = true

			return nil
		})
		if err != nil {
			return nil, icebergFailf(
				"ICEBERG_CURSOR_ERROR", "Cannot read delete file %s: %v", d.entry.DataFile.Path, err)
		}

		for p, set := range perFile {
			out.pos[p] = append(out.pos[p], deleteSet{seq: d.seq, rows: set})
		}
	}

	return out, nil
}

// eqColumns lists the schema column names the equality deletes compare, so the loader reads them.
func (d *icebergDeletes) eqColumns(schema iceberg.Schema) []string {
	var names []string

	for _, e := range d.eq {
		for _, id := range e.ids {
			for _, f := range schema.Fields {
				if f.ID == id {
					names = append(names, f.Name)
				}
			}
		}
	}

	return names
}

type eqCheck struct {
	rows map[string]struct{}
	cols []int
}

// columnIndexes maps equality field ids to schema column positions; ok=false when one is missing.
func columnIndexes(schema iceberg.Schema, ids []int) ([]int, bool) {
	cols := make([]int, 0, len(ids))

	for _, id := range ids {
		for i, fld := range schema.Fields {
			if fld.ID == id {
				cols = append(cols, i)
			}
		}
	}

	return cols, len(cols) == len(ids)
}

func (c eqCheck) hits(rec []string, nulls []bool) bool {
	cells, ns := make([]string, len(c.cols)), make([]bool, len(c.cols))

	for i, ci := range c.cols {
		cells[i], ns[i] = rec[ci], nulls[ci]
	}

	_, hit := c.rows[tupleKey(cells, ns)]

	return hit
}

// rejecter builds the row filter for one data file, nil when no equality delete applies.
func (d *icebergDeletes) rejecter(schema iceberg.Schema, f fileEntry) func(rec []string, nulls []bool) bool {
	var checks []eqCheck

	for _, e := range d.eq {
		if !e.appliesTo(f) {
			continue
		}

		if cols, ok := columnIndexes(schema, e.ids); ok {
			checks = append(checks, eqCheck{rows: e.rows, cols: cols})
		}
	}

	if len(checks) == 0 {
		return nil
	}

	return func(rec []string, nulls []bool) bool {
		for _, c := range checks {
			if c.hits(rec, nulls) {
				return true
			}
		}

		return false
	}
}

type deleteSet struct {
	rows map[int64]bool
	seq  int64
}

func skipRows(sets []deleteSet, dataSeq int64) map[int64]bool {
	var out map[int64]bool

	for _, s := range sets {
		if dataSeq > s.seq {
			continue
		}

		if out == nil {
			out = map[int64]bool{}
		}

		for r := range s.rows {
			out[r] = true
		}
	}

	return out
}

func (t *planTable) loadIceberg(ctx context.Context, conn *sql.Conn, l *tableLoader) error {
	tbl, err := l.openIceberg(ctx, t.def, t.display)
	if err != nil {
		return err
	}

	snap, hasSnap, err := tbl.pickSnapshot(t.travel)
	if err != nil {
		return err
	}

	if t.metaTable != "" {
		return t.loadIcebergMetaTable(ctx, conn, l, tbl, snap, hasSnap)
	}

	schema := tbl.meta.CurrentSchema()
	if t.travel.kind != "" {
		schema = tbl.schemaFor(snap, hasSnap)
	}

	t.loadedAt = tbl.location

	cols, err := icebergColumns(schema, t.display)
	if err != nil {
		return err
	}

	nreal := len(cols)
	if t.rowIDs {
		cols = append(cols, Column{Name: colRowPath, Type: columnTypeString}, Column{Name: colRowPos, Type: typeBigint})
	}

	return fillTable(ctx, conn, t.schema, t.name, cols, func(stmt *sql.Stmt, kinds []colKind) error {
		if !hasSnap {
			return nil
		}

		sink := &rowSink{stmt: stmt, kinds: kinds, nargs: len(cols), rowTypes: rowTypesOf(cols)}

		return t.loadSnapshotFiles(ctx, l, tbl, snap, schema, cols[:nreal], sink)
	})
}

func (t *planTable) loadSnapshotFiles(
	ctx context.Context, l *tableLoader, tbl *icebergTable, snap iceberg.Snapshot, schema iceberg.Schema,
	cols []Column, sink *rowSink,
) error {
	files, err := l.snapshotFiles(ctx, snap)
	if err != nil {
		return err
	}

	dels, err := l.loadDeletes(ctx, files.deletes)
	if err != nil {
		return err
	}

	l.want = l.want.with(dels.eqColumns(schema))

	for _, f := range files.data {
		if !t.rowIDs && tbl.pruned(l.filters, schema, f) {
			continue
		}

		if err = l.loadIcebergDataFile(ctx, tbl, schema, cols, f, dels, sink, t.rowIDs); err != nil {
			return err
		}
	}

	return nil
}

func (l *tableLoader) loadIcebergDataFile(
	ctx context.Context, tbl *icebergTable, schema iceberg.Schema, cols []Column, f fileEntry,
	dels *icebergDeletes, sink *rowSink, rowIDs bool,
) error {
	df := f.entry.DataFile
	sink.reject = dels.rejecter(schema, f)
	isAvro := strings.EqualFold(df.Format, "AVRO")
	kind := fmtParquet

	if strings.EqualFold(df.Format, "ORC") {
		kind = fmtORC
	}

	if !strings.EqualFold(df.Format, "PARQUET") && !isAvro && kind != fmtORC {
		return fmt.Errorf("%w: Iceberg data file format %s (%s)", errUnsupportedFormat, df.Format, df.Path)
	}

	if isAvro {
		data, rerr := l.readURI(ctx, df.Path)
		if rerr != nil {
			return icebergFailf("ICEBERG_CANNOT_OPEN_SPLIT", "Cannot open data file %s", df.Path)
		}

		opts := avroOpts{skip: skipRows(dels.pos[df.Path], f.seq), rowIDs: rowIDs, path: df.Path}

		return l.decodeAvro(ctx, bytes.NewReader(data), cols, sink, opts)
	}

	bucket, key, ok := splitS3URI(df.Path)
	if !ok {
		return icebergFailf("ICEBERG_CANNOT_OPEN_SPLIT", "Invalid data file location %q", df.Path)
	}

	spec, _ := tbl.meta.SpecByID(f.spec)
	fcs := make([]fileCol, len(cols), len(cols)+dmlHidden)

	for i, c := range cols {
		fcs[i] = fileCol{name: c.Name, hive: c.Type, fileName: c.Name, id: schema.Fields[i].ID}

		for _, pf := range spec.Fields {
			if pf.SourceID != schema.Fields[i].ID || !strings.EqualFold(pf.Transform, "identity") {
				continue
			}

			if v, has := df.Partition[pf.Name]; has && v != nil {
				s := partitionText(iceberg.PrimitiveName(schema.Fields[i].Type), v)
				fcs[i].constant = &s
			}
		}
	}

	if rowIDs {
		p := df.Path
		fcs = append(fcs, fileCol{name: colRowPath, constant: &p}, fileCol{name: colRowPos, rowPos: true})
	}

	return l.loadColumnarFile(ctx, bucket, key, kind, fcs, skipRows(dels.pos[df.Path], f.seq), sink)
}

// partitionText renders a manifest partition value as the text a column read would produce.
func partitionText(icebergType string, v any) string {
	switch n := v.(type) {
	case int32, int:
		if icebergType == typeDate {
			return time.Unix(toInt64(n)*secondsPerDayInt, 0).UTC().Format(dateLayout)
		}
	case int64:
		if icebergType == typeTimestamp || icebergType == "timestamptz" {
			return time.UnixMicro(n).UTC().Format(tsMillisLayout)
		}
	}

	return fmt.Sprint(v)
}

func toInt64(v any) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int32:
		return int64(n)
	case int64:
		return n
	default:
		return 0
	}
}
