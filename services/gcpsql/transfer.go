package gcpsql

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
)

var (
	errNoEngine = errors.New(
		"this operation needs a database engine, but the instance runs state-only (no container runtime)",
	)
	errNoStore = errors.New("no object store is configured")
)

// ExportInput is the exportContext of instances.export.
type ExportInput struct {
	CSV       *struct{ SelectQuery string } `json:"csvExportOptions"`
	FileType  string                        `json:"fileType"`
	URI       string                        `json:"uri"`
	Databases []string                      `json:"databases"`
}

// ImportInput is the importContext of instances.import.
type ImportInput struct {
	CSV *struct {
		Table   string   `json:"table"`
		Columns []string `json:"columns"`
	} `json:"csvImportOptions"`
	FileType string `json:"fileType"`
	URI      string `json:"uri"`
	Database string `json:"database"`
}

func parseGSURI(uri string) (string, string, error) {
	u, err := url.Parse(uri)
	if err != nil || u.Scheme != "gs" || u.Host == "" || strings.TrimPrefix(u.Path, "/") == "" {
		return "", "", errInvalid("Invalid URI %q: expected gs://bucket/object.", uri)
	}

	return u.Host, strings.TrimPrefix(u.Path, "/"), nil
}

func (b *InMemoryBackend) store() (ObjectStore, error) {
	b.mu.RLock("store")
	defer b.mu.RUnlock()

	if b.objects == nil {
		return nil, errNoStore
	}

	return b.objects, nil
}

func gzipBytes(data []byte) ([]byte, error) {
	var buf bytes.Buffer

	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(data); err != nil {
		return nil, fmt.Errorf("gzip: %w", err)
	}

	if err := zw.Close(); err != nil {
		return nil, fmt.Errorf("gzip: %w", err)
	}

	return buf.Bytes(), nil
}

func gunzipIfNeeded(data []byte) ([]byte, error) {
	const magic = 2
	if len(data) < magic || data[0] != 0x1f || data[1] != 0x8b {
		return data, nil
	}

	zr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("gunzip: %w", err)
	}

	out, err := io.ReadAll(io.LimitReader(zr, maxDumpBytes+1))
	if err != nil {
		return nil, fmt.Errorf("gunzip: %w", err)
	}

	if len(out) > maxDumpBytes {
		return nil, errTooLarge
	}

	return out, nil
}

// ExportInstance writes a SQL dump or CSV query result to object storage.
func (b *InMemoryBackend) ExportInstance(
	project, instance string,
	in ExportInput,
	raw map[string]any,
) (*Operation, error) {
	bucket, object, err := parseGSURI(in.URI)
	if err != nil {
		return nil, err
	}

	fileType := orDefault(in.FileType, "SQL")
	if fileType != "SQL" && fileType != fileCSV {
		return nil, errInvalid("Export file type %q is not supported by the emulator; use SQL or CSV.", fileType)
	}

	if fileType == fileCSV && (in.CSV == nil || in.CSV.SelectQuery == "" || len(in.Databases) != 1) {
		return nil, errInvalid("A CSV export needs csvExportOptions.selectQuery and exactly one database.")
	}

	b.mu.Lock("ExportInstance")
	defer b.mu.Unlock()

	rec, err := b.opRec(project, instance)
	if err != nil {
		return nil, err
	}

	for _, d := range in.Databases {
		if _, ok := rec.Databases[d]; !ok {
			return nil, errInvalid("Database %q does not exist.", d)
		}
	}

	return b.submitLocked(rec, project, "EXPORT", opMeta{export: raw}, func(ctx context.Context) error {
		data, ctype, xerr := b.exportData(ctx, rec, fileType, in)
		if xerr != nil {
			return xerr
		}

		if strings.HasSuffix(object, ".gz") {
			if data, xerr = gzipBytes(data); xerr != nil {
				return xerr
			}

			ctype = "application/gzip"
		}

		store, xerr := b.store()
		if xerr != nil {
			return xerr
		}

		return store.WriteObject(bucket, object, data, ctype)
	}), nil
}

func (b *InMemoryBackend) exportData(
	ctx context.Context,
	rec *instRec,
	fileType string,
	in ExportInput,
) ([]byte, string, error) {
	eng := b.engineOf(rec)
	if eng == nil {
		return nil, "", errNoEngine
	}

	if fileType == fileCSV {
		data, err := csvOf(ctx, eng, in.Databases[0], in.CSV.SelectQuery)

		return data, "text/csv", err
	}

	dbs := b.exportableDBs(rec, in.Databases)
	data, err := eng.Dump(ctx, dbs, DumpOptions{Markers: len(dbs) > 1, Portable: true})

	return data, "application/sql", err
}

func csvOf(ctx context.Context, eng Engine, db, query string) ([]byte, error) {
	rows, err := eng.Query(ctx, db, query)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer

	w := csv.NewWriter(&buf)
	if err = w.WriteAll(rows[1:]); err != nil {
		return nil, fmt.Errorf("csv: %w", err)
	}

	return buf.Bytes(), nil
}

// ImportInstance loads a SQL file or CSV from object storage.
func (b *InMemoryBackend) ImportInstance(
	project, instance string,
	in ImportInput,
	raw map[string]any,
) (*Operation, error) {
	bucket, object, err := parseGSURI(in.URI)
	if err != nil {
		return nil, err
	}

	fileType := orDefault(in.FileType, "SQL")

	switch {
	case fileType != "SQL" && fileType != fileCSV:
		return nil, errInvalid("Import file type %q is not supported by the emulator; use SQL or CSV.", fileType)
	case fileType == fileCSV && (in.CSV == nil || in.CSV.Table == "" || in.Database == ""):
		return nil, errInvalid("A CSV import needs a database and csvImportOptions.table.")
	}

	b.mu.Lock("ImportInstance")
	defer b.mu.Unlock()

	rec, err := b.writableRec(project, instance)
	if err != nil {
		return nil, err
	}

	return b.submitLocked(rec, project, "IMPORT", opMeta{imp: raw}, func(ctx context.Context) error {
		return b.runImport(ctx, rec, fileType, bucket, object, in)
	}), nil
}

func (b *InMemoryBackend) runImport(
	ctx context.Context,
	rec *instRec,
	fileType, bucket, object string,
	in ImportInput,
) error {
	eng := b.engineOf(rec)
	if eng == nil {
		return errNoEngine
	}

	store, err := b.store()
	if err != nil {
		return err
	}

	data, err := store.ReadObject(bucket, object)
	if err != nil {
		return fmt.Errorf("read gs://%s/%s: %w", bucket, object, err)
	}

	if data, err = gunzipIfNeeded(data); err != nil {
		return err
	}

	if fileType == fileCSV {
		return importCSV(ctx, eng, in, data)
	}

	if err = eng.Restore(ctx, in.Database, data); err != nil {
		return err
	}

	return b.reconcile(ctx, rec, eng)
}

func importCSV(ctx context.Context, eng Engine, in ImportInput, data []byte) error {
	r := csv.NewReader(bytes.NewReader(data))
	r.FieldsPerRecord = -1

	rows, err := r.ReadAll()
	if err != nil {
		return fmt.Errorf("csv: %w", err)
	}

	if len(rows) == 0 {
		return nil
	}

	return eng.InsertRows(ctx, in.Database, in.CSV.Table, in.CSV.Columns, rows)
}
