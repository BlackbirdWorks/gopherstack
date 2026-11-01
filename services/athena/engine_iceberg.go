package athena

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/iceberg"
	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
)

const (
	secondsPerDayInt = 86400
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

// positionDeletes loads position delete files; equality deletes are rejected.
func (l *tableLoader) positionDeletes(ctx context.Context, files []fileEntry) (map[string][]deleteSet, error) {
	out := map[string][]deleteSet{}

	for _, d := range files {
		if d.entry.DataFile.Content == iceberg.ContentEqualityDeletes {
			return nil, fmt.Errorf("%w: Iceberg equality delete files", errUnsupportedFormat)
		}

		data, err := l.readURI(ctx, d.entry.DataFile.Path)
		if err != nil {
			return nil, icebergFailf("ICEBERG_CANNOT_OPEN_SPLIT", "Cannot open delete file %s", d.entry.DataFile.Path)
		}

		pf, err := parquetio.Open(data)
		if err != nil {
			return nil, icebergFailf(
				"ICEBERG_CANNOT_OPEN_SPLIT",
				"Cannot open delete file %s: %v",
				d.entry.DataFile.Path,
				err,
			)
		}

		perFile := map[string]map[int64]bool{}

		err = pf.ForEach([]string{"file_path", "pos"}, func(vals []any) error {
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
				"ICEBERG_CURSOR_ERROR",
				"Cannot read delete file %s: %v",
				d.entry.DataFile.Path,
				err,
			)
		}

		for p, set := range perFile {
			out[p] = append(out[p], deleteSet{seq: d.seq, rows: set})
		}
	}

	return out, nil
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

	schema := tbl.schemaFor(snap, hasSnap)

	cols, err := icebergColumns(schema, t.display)
	if err != nil {
		return err
	}

	return fillTable(ctx, conn, t.schema, t.name, cols, func(stmt *sql.Stmt, kinds []colKind) error {
		if !hasSnap {
			return nil
		}

		files, ferr := l.snapshotFiles(ctx, snap)
		if ferr != nil {
			return ferr
		}

		dels, ferr := l.positionDeletes(ctx, files.deletes)
		if ferr != nil {
			return ferr
		}

		sink := &rowSink{stmt: stmt, kinds: kinds, nargs: len(cols)}

		for _, f := range files.data {
			if lerr := l.loadIcebergDataFile(ctx, tbl, schema, cols, f, dels, sink); lerr != nil {
				return lerr
			}
		}

		return nil
	})
}

func (l *tableLoader) loadIcebergDataFile(
	ctx context.Context, tbl *icebergTable, schema iceberg.Schema, cols []Column, f fileEntry,
	dels map[string][]deleteSet, sink *rowSink,
) error {
	df := f.entry.DataFile
	if !strings.EqualFold(df.Format, "PARQUET") {
		return fmt.Errorf("%w: Iceberg data file format %s (%s)", errUnsupportedFormat, df.Format, df.Path)
	}

	bucket, key, ok := splitS3URI(df.Path)
	if !ok {
		return icebergFailf("ICEBERG_CANNOT_OPEN_SPLIT", "Invalid data file location %q", df.Path)
	}

	spec, _ := tbl.meta.SpecByID(f.spec)
	fcs := make([]fileCol, len(cols))

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

	return l.loadColumnarFile(ctx, bucket, key, fcs, skipRows(dels[df.Path], f.seq), sink)
}

// partitionText renders a manifest partition value as the text a column read would produce.
func partitionText(icebergType string, v any) string {
	switch n := v.(type) {
	case int32, int:
		if icebergType == "date" {
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
