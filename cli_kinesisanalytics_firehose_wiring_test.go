package main

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	fhtypes "github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesisanalytics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKinesisAnalyticsDiscoverFirehoseSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantErrCode string
		records     []string
		wantRaw     int
	}{
		{
			name:    "json_records",
			records: []string{`{"ticker":"AMZN","price":42}`, `{"ticker":"GOOG","price":101.5}`},
			wantRaw: 2,
		},
		{name: "no_records", wantErrCode: "UnableToDetectSchemaException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			fh := firehose.NewFromConfig(fx.cfg)

			out, err := fh.CreateDeliveryStream(t.Context(), &firehose.CreateDeliveryStreamInput{
				DeliveryStreamName: aws.String("ka-src"),
				ExtendedS3DestinationConfiguration: &fhtypes.ExtendedS3DestinationConfiguration{
					BucketARN: aws.String("arn:aws:s3:::ka-src-bucket"),
					RoleARN:   aws.String("arn:aws:iam::000000000000:role/fh"),
				},
			})
			require.NoError(t, err)

			for _, rec := range tt.records {
				_, err = fh.PutRecord(t.Context(), &firehose.PutRecordInput{
					DeliveryStreamName: aws.String("ka-src"), Record: &fhtypes.Record{Data: []byte(rec)},
				})
				require.NoError(t, err)
			}

			disc, err := kinesisanalytics.NewFromConfig(fx.cfg).DiscoverInputSchema(
				t.Context(), &kinesisanalytics.DiscoverInputSchemaInput{
					ResourceARN: out.DeliveryStreamARN,
					RoleARN:     aws.String("arn:aws:iam::000000000000:role/ka"),
				},
			)

			if tt.wantErrCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantErrCode, discoverSchemaErrorCode(err))

				return
			}

			require.NoError(t, err)
			assert.Len(t, disc.RawInputRecords, tt.wantRaw)
			require.Len(t, disc.InputSchema.RecordColumns, 2)
			assert.Equal(t, "price", aws.ToString(disc.InputSchema.RecordColumns[0].Name))
			assert.Equal(t, "ticker", aws.ToString(disc.InputSchema.RecordColumns[1].Name))
		})
	}
}
