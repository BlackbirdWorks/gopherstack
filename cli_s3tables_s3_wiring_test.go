package main

import (
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	s3tablestypes "github.com/aws/aws-sdk-go-v2/service/s3tables/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3TablesInitialMetadataMaterialized(t *testing.T) {
	t.Parallel()

	tests := []struct {
		meta     s3tablestypes.TableMetadata
		name     string
		wantMeta bool
	}{
		{name: "no_metadata"},
		{
			name:     "schema",
			wantMeta: true,
			meta: &s3tablestypes.TableMetadataMemberIceberg{Value: s3tablestypes.IcebergMetadata{
				Schema: &s3tablestypes.IcebergSchema{Fields: []s3tablestypes.SchemaField{
					{Name: aws.String("id"), Type: aws.String("long"), Required: true},
					{Name: aws.String("ts"), Type: aws.String("timestamp")},
				}},
				Properties: map[string]string{"write.format.default": "parquet"},
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			tc := s3tables.NewFromConfig(fx.cfg)

			tb, err := tc.CreateTableBucket(
				t.Context(),
				&s3tables.CreateTableBucketInput{Name: aws.String("wh-bucket")},
			)
			require.NoError(t, err)

			_, err = tc.CreateNamespace(t.Context(), &s3tables.CreateNamespaceInput{
				TableBucketARN: tb.Arn, Namespace: []string{"ns"},
			})
			require.NoError(t, err)

			_, err = tc.CreateTable(t.Context(), &s3tables.CreateTableInput{
				TableBucketARN: tb.Arn, Namespace: aws.String("ns"), Name: aws.String("tbl"),
				Format: s3tablestypes.OpenTableFormatIceberg, Metadata: tt.meta,
			})
			require.NoError(t, err)

			loc, err := tc.GetTableMetadataLocation(t.Context(), &s3tables.GetTableMetadataLocationInput{
				TableBucketARN: tb.Arn, Namespace: aws.String("ns"), Name: aws.String("tbl"),
			})
			require.NoError(t, err)

			if !tt.wantMeta {
				assert.Empty(t, aws.ToString(loc.MetadataLocation))

				return
			}

			require.NotEmpty(t, aws.ToString(loc.MetadataLocation))
			assert.True(t, strings.HasPrefix(*loc.MetadataLocation, aws.ToString(loc.WarehouseLocation)+"/metadata/"))

			bucket, key, _ := strings.Cut(strings.TrimPrefix(*loc.MetadataLocation, "s3://"), "/")
			obj, err := s3.NewFromConfig(fx.cfg, func(o *s3.Options) { o.UsePathStyle = true }).GetObject(
				t.Context(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)},
			)
			require.NoError(t, err)

			raw, err := io.ReadAll(obj.Body)
			require.NoError(t, err)

			var doc map[string]any
			require.NoError(t, json.Unmarshal(raw, &doc))
			assert.InDelta(t, 2, doc["format-version"], 0)
			assert.Equal(t, aws.ToString(loc.WarehouseLocation), doc["location"])
			assert.InDelta(t, 2, doc["last-column-id"], 0)
		})
	}
}
