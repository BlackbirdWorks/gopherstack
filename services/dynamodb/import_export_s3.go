package dynamodb

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

// maxImportObjectBytes caps how many bytes are read from a single source object,
// bounding memory use and guarding against decompression bombs.
const maxImportObjectBytes = 256 * 1024 * 1024

// importScannerBufferBytes is the initial bufio.Scanner buffer size for parsing
// newline-delimited import records.
const importScannerBufferBytes = 64 * 1024

// errUnsupportedImportFormat is returned when an InputFormat we cannot parse
// (currently ION) is requested.
var errUnsupportedImportFormat = errors.New("unsupported import format")

// S3Accessor is the subset of S3 operations DynamoDB needs to read ImportTable
// source objects and write ExportTableToPointInTime output. It is satisfied by
// the in-process S3 backend, wired in cli.go alongside the Firehose→S3 wiring.
type S3Accessor interface {
	GetObject(ctx context.Context, in *s3sdk.GetObjectInput) (*s3sdk.GetObjectOutput, error)
	ListObjectsV2(
		ctx context.Context,
		in *s3sdk.ListObjectsV2Input,
	) (*s3sdk.ListObjectsV2Output, error)
	PutObject(ctx context.Context, in *s3sdk.PutObjectInput) (*s3sdk.PutObjectOutput, error)
}

// SetS3Backend wires the S3 backend used for ImportTable / ExportTableToPointInTime.
func (db *InMemoryDB) SetS3Backend(s3 S3Accessor) {
	db.mu.Lock("SetS3Backend")
	db.s3 = s3
	db.mu.Unlock()
}

// s3Backend returns the wired S3 accessor, or nil when none is configured.
func (db *InMemoryDB) s3Backend() S3Accessor {
	db.mu.RLock("s3Backend")
	defer db.mu.RUnlock()

	return db.s3
}

// importResult accumulates per-import counters.
type importResult struct {
	imported  int64
	processed int64
	bytes     int64
	errors    int64
}

// importFromS3 reads every object under the source bucket/prefix, parses each
// according to inputFormat, and PutItems the parsed items into tableName. It
// returns the accumulated counters. A nil S3 accessor yields an empty result
// (the table is still created — matching the load-bearing behavior callers need).
func (db *InMemoryDB) importFromS3(
	ctx context.Context,
	tableName string,
	src *types.S3BucketSource,
	inputFormat types.InputFormat,
	compression types.InputCompressionType,
	opts *types.InputFormatOptions,
) (importResult, error) {
	var res importResult

	s3 := db.s3Backend()
	if s3 == nil || src == nil {
		return res, nil
	}

	bucket := aws.ToString(src.S3Bucket)
	prefix := aws.ToString(src.S3KeyPrefix)

	keys, err := listSourceKeys(ctx, s3, bucket, prefix)
	if err != nil {
		return res, err
	}

	for _, key := range keys {
		data, getErr := readSourceObject(ctx, s3, bucket, key, compression)
		if getErr != nil {
			return res, getErr
		}

		res.bytes += int64(len(data))

		items, parseErr := parseImportItems(data, inputFormat, opts)
		if parseErr != nil {
			return res, parseErr
		}

		for _, item := range items {
			res.processed++
			if putErr := db.putImportedItem(ctx, tableName, item); putErr != nil {
				res.errors++

				continue
			}
			res.imported++
		}
	}

	return res, nil
}

// listSourceKeys returns all object keys under bucket/prefix, following pagination.
func listSourceKeys(
	ctx context.Context,
	s3 S3Accessor,
	bucket, prefix string,
) ([]string, error) {
	var (
		keys  []string
		token *string
	)

	for {
		out, err := s3.ListObjectsV2(ctx, &s3sdk.ListObjectsV2Input{
			Bucket:            &bucket,
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("list import source objects: %w", err)
		}

		for i := range out.Contents {
			keys = append(keys, aws.ToString(out.Contents[i].Key))
		}

		if out.IsTruncated == nil || !*out.IsTruncated || out.NextContinuationToken == nil {
			break
		}
		token = out.NextContinuationToken
	}

	return keys, nil
}

// readSourceObject fetches a single object and decompresses it when needed. GZIP is
// inferred from the requested compression type or a ".gz" suffix.
func readSourceObject(
	ctx context.Context,
	s3 S3Accessor,
	bucket, key string,
	compression types.InputCompressionType,
) ([]byte, error) {
	out, err := s3.GetObject(ctx, &s3sdk.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return nil, fmt.Errorf("read import source object %q: %w", key, err)
	}
	defer func() { _ = out.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(out.Body, maxImportObjectBytes))
	if err != nil {
		return nil, fmt.Errorf("read import source object %q: %w", key, err)
	}

	gzipped := compression == types.InputCompressionTypeGzip || strings.HasSuffix(key, ".gz")
	if !gzipped {
		return raw, nil
	}

	gz, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("gunzip import source object %q: %w", key, err)
	}
	defer func() { _ = gz.Close() }()

	decoded, err := io.ReadAll(io.LimitReader(gz, maxImportObjectBytes))
	if err != nil {
		return nil, fmt.Errorf("gunzip import source object %q: %w", key, err)
	}

	return decoded, nil
}

// parseImportItems parses object bytes into DynamoDB wire items based on format.
func parseImportItems(
	data []byte,
	inputFormat types.InputFormat,
	opts *types.InputFormatOptions,
) ([]map[string]any, error) {
	switch inputFormat {
	case types.InputFormatDynamodbJson, types.InputFormat(""):
		return parseDynamoDBJSONLines(data)
	case types.InputFormatCsv:
		return parseCSVItems(data, opts)
	default: // ION and any future formats
		return nil, fmt.Errorf("%w: %s", errUnsupportedImportFormat, inputFormat)
	}
}

// parseDynamoDBJSONLines parses newline-delimited {"Item": {...}} records, the
// format produced by ExportTableToPointInTime and accepted by ImportTable.
func parseDynamoDBJSONLines(data []byte) ([]map[string]any, error) {
	var items []map[string]any

	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 0, importScannerBufferBytes), maxImportObjectBytes)

	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}

		var rec struct {
			Item map[string]any `json:"Item"`
		}
		if err := json.Unmarshal(line, &rec); err != nil {
			return nil, fmt.Errorf("parse DynamoDB JSON line: %w", err)
		}

		if rec.Item != nil {
			items = append(items, rec.Item)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan DynamoDB JSON: %w", err)
	}

	return items, nil
}

// parseCSVItems parses CSV rows into items, mapping every column to a String (S)
// attribute (matching AWS CSV import semantics). Headers come from
// InputFormatOptions.Csv.HeaderList when supplied, otherwise the first row.
func parseCSVItems(data []byte, opts *types.InputFormatOptions) ([]map[string]any, error) {
	reader := csv.NewReader(bytes.NewReader(data))
	reader.FieldsPerRecord = -1

	var headers []string
	if opts != nil && opts.Csv != nil {
		if d := aws.ToString(opts.Csv.Delimiter); d != "" {
			reader.Comma = rune(d[0])
		}
		headers = opts.Csv.HeaderList
	}

	rows, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parse CSV: %w", err)
	}

	if len(headers) == 0 {
		if len(rows) == 0 {
			return nil, nil
		}
		headers = rows[0]
		rows = rows[1:]
	}

	items := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		item := make(map[string]any, len(headers))
		for i, h := range headers {
			if i >= len(row) || row[i] == "" {
				continue
			}
			item[h] = map[string]any{"S": row[i]}
		}
		if len(item) > 0 {
			items = append(items, item)
		}
	}

	return items, nil
}

// exportTableToS3 serialises a table's items as gzip-compressed, newline-delimited
// DynamoDB-JSON ({"Item": {...}} per line) and writes them to the given S3
// bucket/key, plus a small manifest object alongside. It returns the number of
// items exported. A nil S3 accessor is a no-op (returns 0, nil), preserving the
// prior "export recorded, no data written" behaviour. The output is directly
// re-importable via ImportTable (InputFormat=DYNAMODB_JSON, GZIP).
func (db *InMemoryDB) exportTableToS3(
	ctx context.Context,
	tableARN, bucket, dataKey, manifestKey string,
) (int64, error) {
	s3 := db.s3Backend()
	if s3 == nil {
		return 0, nil
	}

	items := db.snapshotItemsByTableARN(tableARN)

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	enc := json.NewEncoder(gz)

	var count int64
	for i := range items {
		if err := enc.Encode(struct {
			Item map[string]any `json:"Item"`
		}{Item: items[i]}); err != nil {
			_ = gz.Close()

			return 0, fmt.Errorf("encode export item: %w", err)
		}
		count++
	}

	if err := gz.Close(); err != nil {
		return 0, fmt.Errorf("finalise export gzip: %w", err)
	}

	data := buf.Bytes()
	if _, err := s3.PutObject(ctx, &s3sdk.PutObjectInput{
		Bucket: &bucket,
		Key:    &dataKey,
		Body:   bytes.NewReader(data),
	}); err != nil {
		return 0, fmt.Errorf("write export data object: %w", err)
	}

	manifest, _ := json.Marshal(map[string]any{
		"version":       1,
		"tableArn":      tableARN,
		"itemCount":     count,
		"dataFileS3Key": dataKey,
	})
	if _, err := s3.PutObject(ctx, &s3sdk.PutObjectInput{
		Bucket: &bucket,
		Key:    &manifestKey,
		Body:   bytes.NewReader(manifest),
	}); err != nil {
		return 0, fmt.Errorf("write export manifest object: %w", err)
	}

	return count, nil
}

// snapshotItemsByTableARN returns deep copies of all items in the table whose
// TableArn matches, or nil when no such table exists.
func (db *InMemoryDB) snapshotItemsByTableARN(tableARN string) []map[string]any {
	db.mu.RLock("snapshotItemsByTableARN")
	defer db.mu.RUnlock()

	for _, regionTables := range db.Tables {
		for _, t := range regionTables {
			if t.TableArn != tableARN {
				continue
			}

			t.mu.RLock("snapshotItemsByTableARN")
			items := make([]map[string]any, 0, len(t.Items))
			for i := range t.Items {
				items = append(items, deepCopyItem(t.Items[i]))
			}
			t.mu.RUnlock()

			return items
		}
	}

	return nil
}

// putImportedItem writes a single wire item into the target table via PutItem so
// that indexes, streams, and validation are all applied consistently.
func (db *InMemoryDB) putImportedItem(
	ctx context.Context,
	tableName string,
	item map[string]any,
) error {
	sdkItem, err := models.ToSDKItem(item)
	if err != nil {
		return err
	}

	_, err = db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &tableName,
		Item:      sdkItem,
	})

	return err
}
