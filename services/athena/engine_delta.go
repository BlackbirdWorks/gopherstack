package athena

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/deltalog"
)

// deltaStorage reads a Delta table's objects through the engine's S3 access.
type deltaStorage struct {
	l      *tableLoader
	bucket string
	prefix string
}

func newDeltaStorage(l *tableLoader, location string) (*deltaStorage, error) {
	bucket, prefix, ok := splitS3URI(location)
	if !ok {
		return nil, fmt.Errorf("%w %q", errBadLocation, location)
	}

	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	return &deltaStorage{l: l, bucket: bucket, prefix: prefix}, nil
}

func (d *deltaStorage) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string

	var token *string

	for {
		out, err := d.l.list.ListObjectsV2(ctx, &sdk_s3.ListObjectsV2Input{
			Bucket: aws.String(d.bucket), Prefix: aws.String(d.prefix + prefix), ContinuationToken: token,
		})
		if err != nil {
			return nil, err
		}

		for _, o := range out.Contents {
			keys = append(keys, strings.TrimPrefix(aws.ToString(o.Key), d.prefix))
		}

		if !aws.ToBool(out.IsTruncated) || out.NextContinuationToken == nil {
			return keys, nil
		}

		token = out.NextContinuationToken
	}
}

func (d *deltaStorage) Get(ctx context.Context, key string) ([]byte, error) {
	return d.l.readObject(ctx, d.bucket, d.prefix+key)
}

func deltaFailure(err error, display string) error {
	switch {
	case errors.Is(err, deltalog.ErrNotDeltaTable):
		return fmt.Errorf("DELTA_LAKE_INVALID_SCHEMA: %s is not a Delta Lake table: %w", display, err)
	case errors.Is(err, deltalog.ErrUnsupported):
		return fmt.Errorf("%w: %w", errUnsupportedFormat, err)
	case errors.Is(err, deltalog.ErrBadLog):
		return fmt.Errorf("DELTA_LAKE_BAD_DATA: %w", err)
	default:
		return fmt.Errorf("HIVE_METASTORE_ERROR: cannot read Delta log of %s: %w", display, err)
	}
}

func (l *tableLoader) loadDeltaSnapshot(ctx context.Context, location, display string) (*deltalog.Snapshot, error) {
	st, err := newDeltaStorage(l, location)
	if err != nil {
		return nil, err
	}

	snap, err := deltalog.Load(ctx, st)
	if err != nil {
		return nil, deltaFailure(err, display)
	}

	return snap, nil
}

func (t *planTable) loadDelta(ctx context.Context, conn *sql.Conn, l *tableLoader) error {
	if t.travel.kind != "" {
		return fmt.Errorf("%w: time travel is not supported for Delta Lake tables", errUnsupportedFormat)
	}

	snap, err := l.loadDeltaSnapshot(ctx, t.def.Location, t.display)
	if err != nil {
		return err
	}

	st, err := newDeltaStorage(l, t.def.Location)
	if err != nil {
		return err
	}

	cols := make([]Column, len(snap.Columns))
	for i, c := range snap.Columns {
		cols[i] = Column{Name: c.Name, Type: c.Type}
	}

	return fillTable(ctx, conn, t.schema, t.name, cols, func(stmt *sql.Stmt, kinds []colKind) error {
		sink := &rowSink{stmt: stmt, kinds: kinds, nargs: len(cols)}

		for _, f := range snap.Files {
			bucket, key := st.bucket, st.prefix+f.Path

			if b, k, abs := splitS3URI(f.Path); abs {
				bucket, key = b, k
			}

			if lerr := l.loadColumnarFile(ctx, bucket, key, deltaFileCols(snap, f), nil, sink); lerr != nil {
				return lerr
			}
		}

		return nil
	})
}

// deltaFileCols binds the table columns to one data file; partition columns come from the log.
func deltaFileCols(snap *deltalog.Snapshot, f deltalog.AddFile) []fileCol {
	fcs := make([]fileCol, len(snap.Columns))

	for i, c := range snap.Columns {
		fcs[i] = fileCol{name: c.Name, hive: c.Type, fileName: c.PhysicalName}
		if snap.ColumnMapping == "id" {
			fcs[i].id = c.ID
		}

		if !slices.Contains(snap.PartitionColumns, c.Name) {
			continue
		}

		v, ok := f.PartitionValues[c.PhysicalName]
		if !ok {
			v = f.PartitionValues[c.Name]
		}

		fcs[i].constant, fcs[i].fileName, fcs[i].id = v, "", 0
	}

	return fcs
}
