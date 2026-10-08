package s3tables_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3tablessdk "github.com/aws/aws-sdk-go-v2/service/s3tables"
	"github.com/aws/aws-sdk-go-v2/service/s3tables/document"
	s3tablestypes "github.com/aws/aws-sdk-go-v2/service/s3tables/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3tables"
)

func TestCreateTable_IcebergMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		meta    s3tablestypes.TableMetadata
		name    string
		wantErr bool
	}{
		{name: "none"},
		{
			name: "schema",
			meta: &s3tablestypes.TableMetadataMemberIceberg{Value: s3tablestypes.IcebergMetadata{
				Schema: &s3tablestypes.IcebergSchema{Fields: []s3tablestypes.SchemaField{
					{Name: aws.String("id"), Type: aws.String("long"), Required: true},
				}},
				Properties: map[string]string{"write.format.default": "parquet"},
			}},
		},
		{
			name: "schema_v2",
			meta: &s3tablestypes.TableMetadataMemberIceberg{Value: s3tablestypes.IcebergMetadata{
				SchemaV2: &s3tablestypes.IcebergSchemaV2{
					Type: s3tablestypes.SchemaV2FieldTypeStruct,
					Fields: []s3tablestypes.SchemaV2Field{{
						Id: aws.Int32(1), Name: aws.String("tags"), Required: aws.Bool(false),
						Type: document.NewLazyDocument(
							map[string]any{"type": "list", "element-id": 2, "element": "string"},
						),
					}},
				},
			}},
		},
		{
			name: "schema_field_missing_type",
			meta: &s3tablestypes.TableMetadataMemberIceberg{Value: s3tablestypes.IcebergMetadata{
				Schema: &s3tablestypes.IcebergSchema{Fields: []s3tablestypes.SchemaField{
					{Name: aws.String("id")},
				}},
			}},
			wantErr: true,
		},
		{
			name: "write_order_bad_direction",
			meta: &s3tablestypes.TableMetadataMemberIceberg{Value: s3tablestypes.IcebergMetadata{
				WriteOrder: &s3tablestypes.IcebergSortOrder{
					OrderId: aws.Int32(1),
					Fields: []s3tablestypes.IcebergSortField{{
						Direction: "sideways", NullOrder: s3tablestypes.IcebergNullOrderNullsFirst,
						SourceId: aws.Int32(1), Transform: aws.String("identity"),
					}},
				},
			}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := s3tables.NewInMemoryBackend("123456789012", "us-east-1")
			client := newTestS3TablesClient(t, s3tables.NewHandler(backend))
			ctx := t.Context()

			tb, err := client.CreateTableBucket(
				ctx,
				&s3tablessdk.CreateTableBucketInput{Name: aws.String("meta-bucket")},
			)
			require.NoError(t, err)
			_, err = client.CreateNamespace(ctx, &s3tablessdk.CreateNamespaceInput{
				TableBucketARN: tb.Arn, Namespace: []string{"ns"},
			})
			require.NoError(t, err)

			out, err := client.CreateTable(ctx, &s3tablessdk.CreateTableInput{
				TableBucketARN: tb.Arn, Namespace: aws.String("ns"), Name: aws.String("tbl"),
				Format: s3tablestypes.OpenTableFormatIceberg, Metadata: tt.meta,
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			table, err := backend.GetTableByARN(aws.ToString(out.TableARN))
			require.NoError(t, err)
			assert.Equal(t, tt.meta != nil, table.Metadata != nil)
		})
	}
}
