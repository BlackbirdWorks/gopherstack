package glue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	lakeMetaLimit      = 32 << 20
	lakeScanBuf        = 1 << 20
	defaultLakeDepth   = 10
	classificationParq = "parquet"
	serdeParquetHive   = serdeParquet
	parquetInput       = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
	parquetOutput      = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
)

var errLakeNoS3 = errors.New("S3 is not available to crawl lake-format tables")

func (r *crawlRun) lakeContext() (context.Context, context.CancelFunc, error) {
	if r.b.crawlerS3 == nil {
		return nil, nil, errLakeNoS3
	}

	ctx, cancel := context.WithTimeout(context.Background(), crawlerS3Timeout)

	return ctx, cancel, nil
}

func (b *InMemoryBackend) listKeys(ctx context.Context, bucket, prefix string) ([]string, error) {
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	var keys []string

	var token *string

	for {
		out, err := b.crawlerS3.ListObjectsV2(ctx, &sdk_s3.ListObjectsV2Input{
			Bucket: aws.String(bucket), Prefix: aws.String(prefix), ContinuationToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("list s3://%s/%s: %w", bucket, prefix, err)
		}

		for _, o := range out.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}

		if !aws.ToBool(out.IsTruncated) || out.NextContinuationToken == nil {
			return keys, nil
		}

		token = out.NextContinuationToken
	}
}

func (b *InMemoryBackend) readObject(ctx context.Context, bucket, key string) ([]byte, error) {
	out, err := b.crawlerS3.GetObject(ctx, &sdk_s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return nil, fmt.Errorf("read s3://%s/%s: %w", bucket, key, err)
	}
	defer out.Body.Close()

	return io.ReadAll(io.LimitReader(out.Body, lakeMetaLimit))
}

func (b *InMemoryBackend) putObject(ctx context.Context, bucket, key string, body []byte) error {
	_, err := b.crawlerS3.PutObject(ctx, &sdk_s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(body),
	})
	if err != nil {
		return fmt.Errorf("write s3://%s/%s: %w", bucket, key, err)
	}

	return nil
}

// lakeRoots finds table roots below a crawl path: every key with marker as the
// parent folder of a file accepted by match, within maxDepth folders of base.
func lakeRoots(keys []string, base, marker string, match func(file string) bool, maxDepth int, excl []string) []string {
	if maxDepth <= 0 {
		maxDepth = defaultLakeDepth
	}

	set := map[string]bool{}

	for _, k := range keys {
		dir, file := splitKey(k)
		if !match(file) || lastPathSegment(dir) != marker {
			continue
		}

		root := strings.TrimSuffix(strings.TrimSuffix(dir, marker), "/")
		rel := strings.Trim(strings.TrimPrefix(root, base), "/")

		depth := 0
		if rel != "" {
			depth = strings.Count(rel, "/") + 1
		}

		if depth > maxDepth || (rel != "" && crawlExcluded(rel, excl)) {
			continue
		}

		set[root] = true
	}

	out := make([]string, 0, len(set))
	for root := range set {
		out = append(out, root)
	}

	sort.Strings(out)

	return out
}

func splitKey(k string) (string, string) {
	dir, file, found := strings.CutLast(k, "/")
	if !found {
		return "", k
	}

	return dir, file
}

func joinKey(prefix, rest string) string {
	if prefix == "" {
		return rest
	}

	return prefix + "/" + rest
}

func moveToPartitionKeys(cols []Column, names []string) ([]Column, []Column) {
	var parts []Column

	for _, n := range names {
		i := slices.IndexFunc(cols, func(c Column) bool { return strings.EqualFold(c.Name, n) })
		if i < 0 {
			parts = append(parts, Column{Name: n, Type: avroString})

			continue
		}

		parts = append(parts, cols[i])
		cols = slices.Delete(slices.Clone(cols), i, i+1)
	}

	return cols, parts
}

func decodeJSON(data []byte, v any) error {
	return json.NewDecoder(bytes.NewReader(data)).Decode(v)
}
