package firehose

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
)

var errNoSchemaColumns = errors.New("schema table has no columns")

// SchemaColumn is one column of the Glue table named by SchemaConfiguration.
type SchemaColumn struct {
	Name string
	Type string
}

// GlueSchemaSource resolves the columns of a Glue table for format conversion.
type GlueSchemaSource interface {
	TableColumns(catalogID, database, table string) ([]SchemaColumn, error)
}

// SetGlueSchemaSource wires the Glue catalog used to resolve SchemaConfiguration.
func (b *InMemoryBackend) SetGlueSchemaSource(g GlueSchemaSource) {
	b.glue = g
}

func isParquetConversion(cfg *DataFormatConversionConfig) bool {
	return cfg != nil && cfg.OutputFormatConfiguration != nil && cfg.OutputFormatConfiguration.Serializer != nil &&
		cfg.OutputFormatConfiguration.Serializer.ParquetSerDe != nil
}

// parquetColumns resolves the output schema from Glue, or infers it from the
// records when no catalog is wired.
func (b *InMemoryBackend) parquetColumns(
	cfg *DataFormatConversionConfig,
	objs []map[string]any,
) ([]SchemaColumn, error) {
	if b.glue == nil {
		return inferColumns(objs), nil
	}

	sc := cfg.SchemaConfiguration

	cols, err := b.glue.TableColumns(sc.CatalogID, sc.DatabaseName, sc.TableName)
	if err != nil {
		return nil, err
	}

	if len(cols) == 0 {
		return nil, errNoSchemaColumns
	}

	return cols, nil
}

func inferColumns(objs []map[string]any) []SchemaColumn {
	types := map[string]string{}

	for _, o := range objs {
		for k, v := range o {
			if _, ok := types[k]; !ok || types[k] == "string" {
				types[k] = inferType(v)
			}
		}
	}

	names := make([]string, 0, len(types))
	for k := range types {
		names = append(names, k)
	}

	sort.Strings(names)

	cols := make([]SchemaColumn, len(names))
	for i, n := range names {
		cols[i] = SchemaColumn{Name: n, Type: types[n]}
	}

	return cols
}

func inferType(v any) string {
	switch t := v.(type) {
	case bool:
		return "boolean"
	case json.Number:
		if _, err := t.Int64(); err == nil {
			return "bigint"
		}

		return "double"
	default:
		return "string"
	}
}

func decodeObject(rec []byte) (map[string]any, bool) {
	dec := json.NewDecoder(bytes.NewReader(bytes.TrimSpace(rec)))
	dec.UseNumber()

	var obj map[string]any
	if err := dec.Decode(&obj); err != nil || obj == nil {
		return nil, false
	}

	return obj, true
}

func cellFor(obj map[string]any, col string) any {
	v, ok := obj[col]
	if !ok {
		for k, x := range obj {
			if strings.EqualFold(k, col) {
				v = x

				break
			}
		}
	}

	switch t := v.(type) {
	case nil:
		return nil
	case string:
		return t
	case bool:
		return t
	case json.Number:
		return t.String()
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return nil
		}

		return string(b)
	}
}

// convertToParquet deserializes the records and encodes them as one Parquet
// file. Records that are not JSON objects or do not fit the schema are
// returned as failures; body is nil when no record converted.
func (b *InMemoryBackend) convertToParquet(
	cfg *DataFormatConversionConfig, records [][]byte,
) ([]byte, [][]byte, [][]byte, error) {
	deser := cfg.InputFormatConfiguration.Deserializer

	var (
		objs   []map[string]any
		raws   [][]byte
		failed [][]byte
	)

	for _, rec := range records {
		obj, ok := decodeObject(rec)
		if !ok {
			failed = append(failed, rec)

			continue
		}

		if deser.OpenXJSONSerDe != nil {
			obj = applyOpenXMappings(deser.OpenXJSONSerDe, obj)
		}

		objs, raws = append(objs, obj), append(raws, rec)
	}

	if len(objs) == 0 {
		return nil, nil, failed, nil
	}

	cols, err := b.parquetColumns(cfg, objs)
	if err != nil {
		return nil, nil, append(failed, raws...), fmt.Errorf("resolving schema %s.%s: %w",
			cfg.SchemaConfiguration.DatabaseName, cfg.SchemaConfiguration.TableName, err)
	}

	codec := cfg.OutputFormatConfiguration.Serializer.ParquetSerDe.Compression

	pcols := make([]parquetio.Column, len(cols))
	for i, c := range cols {
		pcols[i] = parquetio.Column{Name: c.Name, Type: c.Type}
	}

	rows := make([][]any, len(objs))
	for i, o := range objs {
		row := make([]any, len(cols))
		for j, c := range cols {
			row[j] = cellFor(o, c.Name)
		}

		rows[i] = row
	}

	var buf bytes.Buffer

	if err = parquetio.Write(&buf, pcols, rows, codec); err == nil {
		return buf.Bytes(), raws, failed, nil
	}

	return b.convertEach(pcols, rows, raws, failed, codec)
}

// convertEach isolates the records a batch write rejected.
func (b *InMemoryBackend) convertEach(
	cols []parquetio.Column, rows [][]any, raws, failed [][]byte, codec string,
) ([]byte, [][]byte, [][]byte, error) {
	var (
		good [][]any
		ok   [][]byte
	)

	for i, r := range rows {
		if err := parquetio.Write(&bytes.Buffer{}, cols, [][]any{r}, codec); err != nil {
			failed = append(failed, raws[i])

			continue
		}

		good, ok = append(good, r), append(ok, raws[i])
	}

	if len(good) == 0 {
		return nil, nil, failed, nil
	}

	var buf bytes.Buffer
	if err := parquetio.Write(&buf, cols, good, codec); err != nil {
		return nil, nil, append(failed, ok...), err
	}

	return buf.Bytes(), ok, failed, nil
}

// putParquetObject converts recs to one Parquet object and writes it. It
// returns the records that were delivered, and those that failed conversion.
func (b *InMemoryBackend) putParquetObject(
	ctx context.Context, recs [][]byte, dest *S3DestinationDescription, prefix, streamName string,
) (string, int, [][]byte, [][]byte, error) {
	if b.s3 == nil {
		return "", 0, recs, nil, nil
	}

	body, ok, failed, err := b.convertToParquet(dest.DataFormatConversion, recs)
	if err != nil {
		b.logDeliveryIssue(ctx, dest.CloudWatchLoggingOptions, streamName, "format conversion failed", err)
	}

	if body == nil {
		return "", 0, nil, failed, nil
	}

	key, size, perr := b.putBody(ctx, compressedBody{body: body, extension: ".parquet"},
		dest.BucketARN, prefix, dest.FileExtension, streamName)

	return key, size, ok, failed, perr
}
