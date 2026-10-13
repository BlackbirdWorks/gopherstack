package athena

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	sdk_s3_types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	encryptionOptionSSES3  = "SSE_S3"
	encryptionOptionSSEKMS = "SSE_KMS"
)

// writeResultObject writes a succeeded query execution's result to
// ResultConfiguration.OutputLocation as a CSV object, when S3 is wired and an
// output location is set. A no-op otherwise, matching this repo's
// unwired-hook-stays-permissive convention: results remain stored/echoed via
// GetQueryExecution/GetQueryResults regardless.
//
// Real AWS Athena writes a result object per query execution under
// OutputLocation, keyed by QueryExecutionId; the pinned SDK
// (types.ResultConfiguration.OutputLocation) documents only the directory,
// not an object key or file format. gopherstack approximates both: the key
// is "<prefix>/<QueryExecutionId>.csv", and the body is a plain CSV (header
// row of column names, then one row per result row) via encoding/csv --
// this is a disclosed approximation, not a verified wire shape.
func (b *InMemoryBackend) writeResultObject(id string, rc ResultConfiguration, result *sqlResult) {
	if b.s3 == nil || rc.OutputLocation == "" {
		return
	}

	bucket, key, ok := resultObjectKey(rc.OutputLocation, id)
	if !ok {
		return
	}

	input := &sdk_s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(resultCSV(result)),
	}

	switch rc.EncryptionConfiguration.EncryptionOption {
	case encryptionOptionSSES3:
		input.ServerSideEncryption = sdk_s3_types.ServerSideEncryptionAes256
	case encryptionOptionSSEKMS:
		input.ServerSideEncryption = sdk_s3_types.ServerSideEncryptionAwsKms
		if rc.EncryptionConfiguration.KmsKey != "" {
			input.SSEKMSKeyId = aws.String(rc.EncryptionConfiguration.KmsKey)
		}
	}

	_, _ = b.s3.PutObject(context.Background(), input)

	if len(result.columns) == 0 {
		return
	}

	meta := *input
	meta.Key = aws.String(key + ".metadata")
	meta.Body = bytes.NewReader(resultMetadata(result))
	_, _ = b.s3.PutObject(context.Background(), &meta)
}

// resultMetadata serialises the column names and types of a result set; AWS's
// own .metadata layout is binary and undocumented, so this is JSON.
func resultMetadata(result *sqlResult) []byte {
	type col struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}

	cols := make([]col, len(result.columns))
	for i, c := range result.columns {
		cols[i] = col{Name: c.name, Type: c.typ}
	}

	out, _ := json.Marshal(map[string]any{"columns": cols})

	return out
}

// resultObjectKey splits an "s3://bucket/prefix" OutputLocation into a bucket
// and the object key "<prefix>/<id>.csv" (no prefix: just "<id>.csv").
func resultObjectKey(outputLocation, id string) (string, string, bool) {
	const s3Scheme = "s3://"
	if !strings.HasPrefix(outputLocation, s3Scheme) {
		return "", "", false
	}

	bucket, prefix, _ := strings.Cut(strings.TrimPrefix(outputLocation, s3Scheme), "/")
	if bucket == "" {
		return "", "", false
	}

	prefix = strings.Trim(prefix, "/")
	if prefix != "" {
		prefix += "/"
	}

	return bucket, prefix + id + ".csv", true
}

// resultCSV serializes a query result as Athena does: every field quoted, a
// header row of column names, then one row per result row. A nil result
// still produces a valid, empty CSV.
func resultCSV(result *sqlResult) []byte {
	var buf bytes.Buffer

	if result == nil {
		return buf.Bytes()
	}

	header := make([]string, len(result.columns))
	for i, c := range result.columns {
		header[i] = c.name
	}

	writeQuotedRow(&buf, header, nil)

	for i, row := range result.rows {
		var nulls []bool
		if i < len(result.nulls) {
			nulls = result.nulls[i]
		}

		writeQuotedRow(&buf, row, nulls)
	}

	return buf.Bytes()
}

// writeQuotedRow quotes every field except SQL NULLs, which are left empty
// and unquoted so they stay distinguishable from "".
func writeQuotedRow(buf *bytes.Buffer, fields []string, nulls []bool) {
	for i, f := range fields {
		if i > 0 {
			buf.WriteByte(',')
		}

		if i < len(nulls) && nulls[i] {
			continue
		}

		buf.WriteByte('"')
		buf.WriteString(strings.ReplaceAll(f, `"`, `""`))
		buf.WriteByte('"')
	}

	buf.WriteByte('\n')
}
