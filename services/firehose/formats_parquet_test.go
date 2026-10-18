package firehose_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
	"github.com/blackbirdworks/gopherstack/services/firehose"
)

var errNoSuchTable = errors.New("EntityNotFoundException")

type fakeGlueSchema struct {
	tables map[string][]firehose.SchemaColumn
}

func (f *fakeGlueSchema) TableColumns(_, db, table string) ([]firehose.SchemaColumn, error) {
	cols, ok := f.tables[db+"."+table]
	if !ok {
		return nil, errNoSuchTable
	}

	return cols, nil
}

func parquetStream(table, compression string) firehose.CreateDeliveryStreamInput {
	return firehose.CreateDeliveryStreamInput{
		Name: "pq-stream",
		S3Destination: &firehose.S3DestinationDescription{
			BucketARN:         "arn:aws:s3:::pq-bucket",
			ErrorOutputPrefix: "conv-errors/",
			DataFormatConversion: &firehose.DataFormatConversionConfig{
				Enabled:             true,
				SchemaConfiguration: &firehose.SchemaConfiguration{DatabaseName: "db", TableName: table},
				InputFormatConfiguration: &firehose.InputFormatConfiguration{
					Deserializer: &firehose.Deserializer{
						OpenXJSONSerDe: &firehose.OpenXJSONSerDe{CaseInsensitive: true},
					},
				},
				OutputFormatConfiguration: &firehose.OutputFormatConfiguration{
					Serializer: &firehose.Serializer{ParquetSerDe: &firehose.ParquetSerDe{Compression: compression}},
				},
			},
		},
	}
}

func TestParquetConversionWithGlueSchema(t *testing.T) {
	t.Parallel()

	cols := []firehose.SchemaColumn{
		{Name: "id", Type: "bigint"}, {Name: "name", Type: "string"},
		{Name: "price", Type: "decimal(10,2)"}, {Name: "seen", Type: "timestamp"}, {Name: "extra", Type: "string"},
	}

	cases := []struct {
		name        string
		compression string
	}{
		{"snappy default", ""}, {"gzip", "GZIP"}, {"uncompressed", "UNCOMPRESSED"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s3mock := &mockS3Storer{}
			b := firehose.NewInMemoryBackend("000000000000", flushRegion)
			b.SetS3Backend(s3mock)
			b.SetGlueSchemaSource(&fakeGlueSchema{tables: map[string][]firehose.SchemaColumn{"db.t": cols}})

			_, err := b.CreateDeliveryStream(context.TODO(), parquetStream("t", tc.compression))
			require.NoError(t, err)

			_, err = b.PutRecordBatch(context.TODO(), "pq-stream", [][]byte{
				[]byte(`{"ID":1,"Name":"ann","price":12.5,"seen":"2024-03-05 10:30:00"}`),
				[]byte(`{"id":"not-a-number","name":"bad"}`),
				[]byte(`{"id":2,"name":"bob","price":3}`),
			})
			require.NoError(t, err)
			b.FlushAll(context.Background())

			var data, errOut *mockS3PutCall

			for _, c := range s3mock.calls {
				if strings.Contains(c.key, "conv-errors/") {
					errOut = c
				} else {
					data = c
				}
			}

			require.NotNil(t, data)
			require.NotNil(t, errOut)
			assert.Contains(t, string(errOut.body), "not-a-number")
			assert.True(t, strings.HasSuffix(data.key, ".parquet") || strings.Contains(data.key, "pq-stream"))

			pf, perr := parquetio.Open(data.body)
			require.NoError(t, perr)
			assert.Equal(t, []string{"id", "name", "price", "seen", "extra"}, pf.Columns())

			var rows [][]any

			require.NoError(t, pf.ForEach(pf.Columns(), func(v []any) error {
				rows = append(rows, v)

				return nil
			}))
			assert.Equal(t, [][]any{
				{int64(1), "ann", "12.50", "2024-03-05 10:30:00.000", nil},
				{int64(2), "bob", "3.00", nil, nil},
			}, rows)
		})
	}
}

func TestParquetConversionMissingSchemaTable(t *testing.T) {
	t.Parallel()

	s3mock := &mockS3Storer{}
	b := firehose.NewInMemoryBackend("000000000000", flushRegion)
	b.SetS3Backend(s3mock)
	b.SetGlueSchemaSource(&fakeGlueSchema{tables: map[string][]firehose.SchemaColumn{}})

	_, err := b.CreateDeliveryStream(context.TODO(), parquetStream("ghost", ""))
	require.NoError(t, err)

	err = b.PutRecord(context.TODO(), "pq-stream", []byte(`{"id":1}`))
	require.NoError(t, err)
	b.FlushAll(context.Background())

	require.Len(t, s3mock.calls, 1)
	assert.Contains(t, s3mock.calls[0].key, "conv-errors/")
	assert.Contains(t, string(s3mock.calls[0].body), `{"id":1}`)
	assert.Equal(t, int64(1), firehose.StreamFailedRecords(b, flushRegion, "pq-stream"))
}
