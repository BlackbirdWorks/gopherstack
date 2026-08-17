package kinesisanalytics

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	// maxSampleRecords bounds how many records DiscoverInputSchema samples from a source. Not
	// an AWS-documented constant -- a deliberate implementation choice to keep sampling cheap.
	maxSampleRecords = 10
	// maxS3SampleBytes bounds how much of an S3 object DiscoverInputSchema reads before giving
	// up looking for sample records, so a huge object can't be read into memory wholesale.
	maxS3SampleBytes = 64 * 1024
	// defaultVarcharLen is the SqlType length used for an inferred VARCHAR column with no
	// observed non-empty sample value to size against.
	defaultVarcharLen = 4
)

// DiscoverInputSchema samples real records from the requested source and infers a SourceSchema
// from them. Reachability depends on which readers are wired: SetS3ObjectReader for an
// S3Configuration source, SetKinesisStreamReader for a ResourceARN naming a Kinesis stream.
// Neither is called by cli.go today (see README's known gaps), so every request currently
// returns ErrUnableToDetectSchema -- the same error real AWS returns when it cannot reach or
// sample the named source, since DiscoverInputSchema's error set has no
// ResourceNotFoundException (see ErrUnableToDetectSchema's doc comment).
func (b *InMemoryBackend) DiscoverInputSchema(
	ctx context.Context,
	resourceARN string,
	s3cfg *s3ConfigurationInput,
) (*SourceSchema, []string, [][]string, error) {
	var (
		raw []string
		err error
	)

	switch {
	case s3cfg != nil:
		raw, err = b.sampleS3Object(ctx, s3cfg)
	case resourceARN != "":
		raw, err = b.sampleKinesisStream(resourceARN)
	default:
		return nil, nil, nil, fmt.Errorf("%w: no source configured", ErrUnableToDetectSchema)
	}

	if err != nil {
		return nil, nil, nil, err
	}

	schema, rows, err := inferSchema(raw)
	if err != nil {
		return nil, nil, nil, err
	}

	return schema, raw, rows, nil
}

// sampleS3Object reads up to maxS3SampleBytes of the configured S3 object and returns its
// newline-delimited JSON records, capped at maxSampleRecords. KDA v1's console schema-discovery
// tool works this way against JSON sources; CSV sources are not sampled (documented gap, not
// fabricated -- see README).
func (b *InMemoryBackend) sampleS3Object(ctx context.Context, cfg *s3ConfigurationInput) ([]string, error) {
	b.mu.RLock("sampleS3Object")
	reader := b.s3Reader
	b.mu.RUnlock()

	if reader == nil {
		return nil, fmt.Errorf("%w: no S3 backend wired", ErrUnableToDetectSchema)
	}

	bucket := bucketNameFromARN(cfg.BucketARN)
	if bucket == "" {
		return nil, fmt.Errorf("%w: cannot parse bucket name from %q", ErrUnableToDetectSchema, cfg.BucketARN)
	}

	key := cfg.FileKey

	out, err := reader.GetObject(ctx, &sdk_s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnableToDetectSchema, err)
	}
	defer out.Body.Close()

	body, err := io.ReadAll(io.LimitReader(out.Body, maxS3SampleBytes))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnableToDetectSchema, err)
	}

	raw := sampleJSONLines(body, maxSampleRecords)
	if len(raw) == 0 {
		return nil, fmt.Errorf("%w: no JSON records found in s3://%s/%s", ErrUnableToDetectSchema, bucket, key)
	}

	return raw, nil
}

// sampleKinesisStream reads up to maxSampleRecords records across the stream's shards, starting
// each shard from TRIM_HORIZON (the reader's only supported starting point).
func (b *InMemoryBackend) sampleKinesisStream(resourceARN string) ([]string, error) {
	b.mu.RLock("sampleKinesisStream")
	reader := b.kinesisReader
	b.mu.RUnlock()

	if reader == nil {
		return nil, fmt.Errorf("%w: no Kinesis backend wired", ErrUnableToDetectSchema)
	}

	streamName := kinesisStreamNameFromARN(resourceARN)
	if streamName == "" {
		return nil, fmt.Errorf("%w: cannot parse stream name from %q", ErrUnableToDetectSchema, resourceARN)
	}

	shardIDs, err := reader.ListShards(streamName)
	if err != nil || len(shardIDs) == 0 {
		return nil, fmt.Errorf("%w: stream %q has no shards", ErrUnableToDetectSchema, streamName)
	}

	raw := make([]string, 0, maxSampleRecords)

	for _, shardID := range shardIDs {
		if len(raw) >= maxSampleRecords {
			break
		}

		iter, iterErr := reader.GetShardIterator(streamName, shardID)
		if iterErr != nil {
			continue
		}

		records, _, recErr := reader.GetRecords(iter, maxSampleRecords-len(raw))
		if recErr != nil {
			continue
		}

		for _, rec := range records {
			raw = append(raw, string(rec))
		}
	}

	if len(raw) == 0 {
		return nil, fmt.Errorf("%w: stream %q has no records to sample", ErrUnableToDetectSchema, streamName)
	}

	return raw, nil
}

// bucketNameFromARN extracts the bucket name from an S3 bucket ARN of the form
// "arn:aws:s3:::bucket-name" (S3 bucket ARNs carry no region/account segments).
func bucketNameFromARN(bucketARN string) string {
	_, name, found := strings.Cut(bucketARN, ":::")
	if !found {
		return ""
	}

	return name
}

// kinesisStreamNameFromARN extracts the stream name from a Kinesis stream ARN of the form
// "arn:aws:kinesis:<region>:<account>:stream/<name>", mirroring
// services/firehose/kinesis_source.go's identical helper.
func kinesisStreamNameFromARN(streamARN string) string {
	_, name, found := strings.Cut(streamARN, ":stream/")
	if !found {
		return ""
	}

	return name
}

// sampleJSONLines returns up to limit non-empty, valid-JSON lines from body.
func sampleJSONLines(body []byte, limit int) []string {
	lines := bytes.Split(body, []byte("\n"))
	out := make([]string, 0, limit)

	for _, line := range lines {
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) == 0 || !json.Valid(trimmed) {
			continue
		}

		out = append(out, string(trimmed))
		if len(out) >= limit {
			break
		}
	}

	return out
}

// inferSchema decodes raw as JSON objects and infers a RecordColumn per observed key, sorted
// alphabetically for deterministic output (real AWS's own column ordering isn't documented, so
// this doesn't attempt to reproduce it -- only the column set/types/values are meant to be real).
func inferSchema(raw []string) (*SourceSchema, [][]string, error) {
	records := decodeSampleRecords(raw)
	if len(records) == 0 {
		return nil, nil, fmt.Errorf("%w: no JSON object records among sampled data", ErrUnableToDetectSchema)
	}

	columns := collectColumnNames(records)

	cols := make([]RecordColumn, len(columns))
	rows := make([][]string, len(records))

	for i := range rows {
		rows[i] = make([]string, len(columns))
	}

	for ci, key := range columns {
		cols[ci] = RecordColumn{Name: key, SQLType: inferColumnType(records, key)}

		for ri, rec := range records {
			rows[ri][ci] = stringifyValue(rec[key])
		}
	}

	schema := &SourceSchema{
		RecordFormat: RecordFormat{
			RecordFormatType: recordFormatJSON,
			MappingParameters: &MappingParameters{
				JSONMappingParameters: &JSONMappingParameters{RecordRowPath: "$"},
			},
		},
		RecordColumns: cols,
	}

	return schema, rows, nil
}

// decodeSampleRecords decodes each raw line as a JSON object, silently skipping lines that
// don't decode to an object (e.g. a bare JSON array or scalar).
func decodeSampleRecords(raw []string) []map[string]any {
	records := make([]map[string]any, 0, len(raw))

	for _, r := range raw {
		dec := json.NewDecoder(strings.NewReader(r))
		dec.UseNumber()

		var m map[string]any
		if err := dec.Decode(&m); err != nil {
			continue
		}

		records = append(records, m)
	}

	return records
}

// collectColumnNames returns the sorted union of keys across records.
func collectColumnNames(records []map[string]any) []string {
	seen := make(map[string]bool)

	var keys []string

	for _, rec := range records {
		for k := range rec {
			if !seen[k] {
				seen[k] = true

				keys = append(keys, k)
			}
		}
	}

	sort.Strings(keys)

	return keys
}

// inferColumnType infers a SqlType for key from its observed values across records: BOOLEAN if
// every non-null value is a bool, INTEGER/DOUBLE if every non-null value is numeric (DOUBLE if
// any has a fractional part), otherwise VARCHAR(N) sized to the longest observed value.
func inferColumnType(records []map[string]any, key string) string {
	var sawBool, sawInt, sawFloat, sawOther bool

	maxLen := 0

	for _, rec := range records {
		v, ok := rec[key]
		if !ok || v == nil {
			continue
		}

		switch val := v.(type) {
		case bool:
			sawBool = true
		case json.Number:
			if _, err := strconv.ParseInt(val.String(), 10, 64); err == nil {
				sawInt = true
			} else {
				sawFloat = true
			}
		case string:
			sawOther = true
			maxLen = max(maxLen, len(val))
		default:
			sawOther = true

			if b, err := json.Marshal(val); err == nil {
				maxLen = max(maxLen, len(b))
			}
		}
	}

	switch {
	case sawOther:
		return fmt.Sprintf("VARCHAR(%d)", max(maxLen, defaultVarcharLen))
	case sawFloat:
		return "DOUBLE"
	case sawInt:
		return "INTEGER"
	case sawBool:
		return "BOOLEAN"
	default:
		return fmt.Sprintf("VARCHAR(%d)", defaultVarcharLen)
	}
}

// stringifyValue renders a decoded JSON value as the text ParsedInputRecords carries for it.
func stringifyValue(v any) string {
	switch val := v.(type) {
	case nil:
		return ""
	case string:
		return val
	case json.Number:
		return val.String()
	case bool:
		return strconv.FormatBool(val)
	default:
		b, err := json.Marshal(val)
		if err != nil {
			return ""
		}

		return string(b)
	}
}
