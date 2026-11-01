package athena

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
)

const (
	tableTypeIceberg = "ICEBERG"
	tableTypeDelta   = "DELTA"
	paramTableType   = "table_type"
	paramMetadataLoc = "metadata_location"
	paramPrevMetaLoc = "previous_metadata_location"
	externalTrue     = "TRUE"
)

var errMissingObject = errors.New("object not found")

func tableTypeOf(def *GlueTableDef) string {
	return strings.ToUpper(strings.TrimSpace(lowerParam(def.Parameters, paramTableType)))
}

// fileCol maps one table column onto a data file: the Parquet column to read
// (matched by field id first, then name) or a constant for columns the file lacks.
type fileCol struct {
	constant *string
	name     string
	hive     string
	fileName string
	id       int
}

// fillTable creates the SQLite table for cols and lets fill insert rows.
func fillTable(
	ctx context.Context, conn *sql.Conn, schema, name string, cols []Column,
	fill func(stmt *sql.Stmt, kinds []colKind) error,
) error {
	decls := make([]string, len(cols))
	marks := make([]string, len(cols))
	kinds := make([]colKind, len(cols))

	for i, c := range cols {
		decls[i] = quoteIdent(c.Name) + " " + sqliteDecl(c.Type)
		marks[i] = "?"
		kinds[i] = kindOf(c.Type)
	}

	target := quoteIdent(schema) + "." + quoteIdent(name)

	if err := execAll(ctx, conn, "CREATE TABLE "+target+" ("+strings.Join(decls, ", ")+")"); err != nil {
		return err
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	//nolint:gosec // target is built from quoteIdent-quoted names
	stmt, err := tx.PrepareContext(ctx, "INSERT INTO "+target+" VALUES ("+strings.Join(marks, ",")+")")
	if err != nil {
		_ = tx.Rollback()

		return err
	}

	defer func() { _ = stmt.Close() }()

	if err = fill(stmt, kinds); err != nil {
		_ = tx.Rollback()

		return err
	}

	return tx.Commit()
}

// readObject loads a whole S3 object, transparently gunzipping, within the scan limit.
func (l *tableLoader) readObject(ctx context.Context, bucket, key string) ([]byte, error) {
	out, err := l.get.GetObject(ctx, &sdk_s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return nil, fmt.Errorf("%w: s3://%s/%s: %w", errMissingObject, bucket, key, err)
	}
	defer func() { _ = out.Body.Close() }()

	data, err := io.ReadAll(io.LimitReader(out.Body, l.limits.MaxScanBytes+1))
	if err != nil {
		return nil, err
	}

	if int64(len(data)) > l.limits.MaxScanBytes {
		return nil, fmt.Errorf("%w: more than %d bytes", errScanLimit, l.limits.MaxScanBytes)
	}

	if len(data) >= gzipMagicLen && data[0] == gzipMagic0 && data[1] == gzipMagic1 {
		gz, gerr := gzip.NewReader(bytes.NewReader(data))
		if gerr != nil {
			return nil, gerr
		}

		defer func() { _ = gz.Close() }()

		if data, err = io.ReadAll(io.LimitReader(gz, l.limits.MaxScanBytes*decompressRatio)); err != nil {
			return nil, err
		}
	}

	return data, nil
}

func (l *tableLoader) readURI(ctx context.Context, uri string) ([]byte, error) {
	bucket, key, ok := splitS3URI(uri)
	if !ok {
		return nil, fmt.Errorf("%w %q", errBadLocation, uri)
	}

	return l.readObject(ctx, bucket, key)
}

// boundCol is a table column bound to a Parquet column of one file.
type boundCol struct {
	file string
	pos  int
}

// resolveFileCols binds each table column to the file's Parquet column.
func resolveFileCols(pf *parquetio.File, cols []fileCol) []boundCol {
	ids := pf.ColumnIDs()
	have := map[string]string{}

	for _, c := range pf.Columns() {
		have[strings.ToLower(c)] = c
	}

	var out []boundCol

	for i, c := range cols {
		if c.fileName == "" && c.id <= 0 {
			continue
		}

		if n, ok := ids[c.id]; ok && c.id > 0 {
			out = append(out, boundCol{file: n, pos: i})

			continue
		}

		if n, ok := have[strings.ToLower(orDefault(c.fileName, c.name))]; ok {
			out = append(out, boundCol{file: n, pos: i})
		}
	}

	return out
}

// loadColumnarFile streams one Parquet data file into sink. skip rows are
// zero-based positions removed by position deletes.
func (l *tableLoader) loadColumnarFile(
	ctx context.Context, bucket, key string, cols []fileCol, skip map[int64]bool, sink *rowSink,
) error {
	data, err := l.readObject(ctx, bucket, key)
	if err != nil {
		return fmt.Errorf("HIVE_CANNOT_OPEN_SPLIT: Error opening split s3://%s/%s: %w", bucket, key, err)
	}

	pf, err := parquetio.Open(data)
	if err != nil {
		return fmt.Errorf("HIVE_CANNOT_OPEN_SPLIT: Error opening split s3://%s/%s: %w", bucket, key, err)
	}

	var wanted []boundCol

	names := []string{}

	for _, b := range resolveFileCols(pf, cols) {
		if l.want.has(cols[b.pos].name) {
			wanted = append(wanted, b)
			names = append(names, b.file)
		}
	}

	l.scanned += pf.ScanBytes(names)
	if l.scanned > l.limits.MaxScanBytes {
		return fmt.Errorf("%w: more than %d bytes", errScanLimit, l.limits.MaxScanBytes)
	}

	var row int64

	err = pf.ForEach(names, func(vals []any) error {
		cur := row
		row++

		if skip[cur] {
			return nil
		}

		rec, nulls := l.fileRow(cols, wanted, vals)

		return l.insertJSON(ctx, sink, rec, nulls)
	})
	if errors.Is(err, parquetio.ErrBadFile) {
		return fmt.Errorf("HIVE_CURSOR_ERROR: s3://%s/%s: %w", bucket, key, err)
	}

	return err
}

// fileRow assembles one table row: constants first, then the values read from the file.
func (l *tableLoader) fileRow(cols []fileCol, wanted []boundCol, vals []any) ([]string, []bool) {
	rec := make([]string, len(cols))
	nulls := make([]bool, len(cols))

	for i, c := range cols {
		if c.constant != nil && l.want.has(c.name) {
			rec[i] = *c.constant
		} else {
			nulls[i] = true
		}
	}

	for j, v := range vals {
		if v != nil {
			rec[wanted[j].pos], nulls[wanted[j].pos] = parquetCell(v), false
		}
	}

	return rec, nulls
}
