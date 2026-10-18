package cur_test

import (
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cursdk "github.com/aws/aws-sdk-go-v2/service/costandusagereportservice"
	curtypes "github.com/aws/aws-sdk-go-v2/service/costandusagereportservice/types"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (f *fixture) object(t *testing.T, key string) []byte {
	t.Helper()

	out, err := f.s3.GetObject(
		t.Context(),
		&sdk_s3.GetObjectInput{Bucket: aws.String("cur-bucket"), Key: aws.String(key)},
	)
	require.NoError(t, err)

	b, err := io.ReadAll(out.Body)
	require.NoError(t, err)

	return b
}

func (f *fixture) keys(t *testing.T) []string {
	t.Helper()

	out, err := f.s3.ListObjectsV2(t.Context(), &sdk_s3.ListObjectsV2Input{Bucket: aws.String("cur-bucket")})
	require.NoError(t, err)

	keys := make([]string, 0, len(out.Contents))
	for _, o := range out.Contents {
		keys = append(keys, aws.ToString(o.Key))
	}

	return keys
}

func (f *fixture) status(t *testing.T, name string) *curtypes.ReportStatus {
	t.Helper()

	out, err := f.client.DescribeReportDefinitions(t.Context(), &cursdk.DescribeReportDefinitionsInput{})
	require.NoError(t, err)

	for _, d := range out.ReportDefinitions {
		if aws.ToString(d.ReportName) == name {
			return d.ReportStatus
		}
	}

	return nil
}

func TestDelivery_CSVGzip(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.bucket(t)
	ctx := t.Context()

	_, err := f.client.PutReportDefinition(ctx, &cursdk.PutReportDefinitionInput{ReportDefinition: def("daily")})
	require.NoError(t, err)

	f.backend.DeliverDue(ctx)

	st := f.status(t, "daily")
	require.NotNil(t, st)
	assert.Equal(t, curtypes.LastStatusSuccess, st.LastStatus)

	dataKey := ""
	manifestKey := "reports/daily/20260301-20260401/daily-Manifest.json"

	for _, k := range f.keys(t) {
		if len(k) > 6 && k[len(k)-7:] == ".csv.gz" {
			dataKey = k
		}
	}

	require.NotEmpty(t, dataKey)
	assert.Contains(t, dataKey, "reports/daily/20260301-20260401/")

	zr, err := gzip.NewReader(bytes.NewReader(f.object(t, dataKey)))
	require.NoError(t, err)

	rows, err := csv.NewReader(zr).ReadAll()
	require.NoError(t, err)
	require.Len(t, rows, 3)
	assert.Equal(t, "identity/LineItemId", rows[0][0])
	assert.Contains(t, rows[0], "lineItem/ResourceId")
	assert.Equal(t, testAccount, rows[1][8])

	var m map[string]any
	require.NoError(t, json.Unmarshal(f.object(t, manifestKey), &m))
	assert.Equal(t, "daily", m["reportName"])
	assert.Equal(t, "20260301T000000.000Z", m["billingPeriod"].(map[string]any)["start"])
	assert.Equal(t, []any{dataKey}, m["reportKeys"])

	before := len(f.keys(t))
	f.backend.DeliverDue(ctx)
	assert.Len(t, f.keys(t), before)

	f.clock.advance(24 * time.Hour)
	f.backend.DeliverDue(ctx)
	assert.Greater(t, len(f.keys(t)), before)
}

func TestDelivery_Variants(t *testing.T) {
	t.Parallel()

	cases := []struct {
		mutate  func(d *curtypes.ReportDefinition)
		name    string
		wantKey string
		rows    int
	}{
		{func(d *curtypes.ReportDefinition) { d.TimeUnit = curtypes.TimeUnitHourly }, "hourly", ".csv.gz", 48},
		{func(d *curtypes.ReportDefinition) { d.TimeUnit = curtypes.TimeUnitMonthly }, "monthly", ".csv.gz", 1},
		{func(d *curtypes.ReportDefinition) {
			d.Format, d.Compression = curtypes.ReportFormatParquet, curtypes.CompressionFormatParquet
			d.ReportVersioning = curtypes.ReportVersioningOverwriteReport
		}, "parquet", ".snappy.parquet", 0},
		{func(d *curtypes.ReportDefinition) { d.Compression = curtypes.CompressionFormatZip }, "zip", ".csv.zip", 0},
		{func(d *curtypes.ReportDefinition) {
			d.AdditionalArtifacts = []curtypes.AdditionalArtifact{
				curtypes.AdditionalArtifactRedshift, curtypes.AdditionalArtifactQuicksight,
			}
		}, "artifacts", "-RedshiftCommands.sql", 0},
		{func(d *curtypes.ReportDefinition) {
			d.Format, d.Compression = curtypes.ReportFormatParquet, curtypes.CompressionFormatParquet
			d.ReportVersioning = curtypes.ReportVersioningOverwriteReport
			d.AdditionalArtifacts = []curtypes.AdditionalArtifact{curtypes.AdditionalArtifactAthena}
		}, "athena", "year=2026/month=3/athena-00001.snappy.parquet", 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			f.bucket(t)

			d := def(tc.name)
			tc.mutate(d)

			_, err := f.client.PutReportDefinition(t.Context(), &cursdk.PutReportDefinitionInput{ReportDefinition: d})
			require.NoError(t, err)
			f.backend.DeliverDue(t.Context())

			var found string

			for _, k := range f.keys(t) {
				if len(k) >= len(tc.wantKey) && k[len(k)-len(tc.wantKey):] == tc.wantKey {
					found = k
				}
			}

			require.NotEmpty(t, found, "keys: %v", f.keys(t))

			if tc.rows > 0 {
				zr, zerr := gzip.NewReader(bytes.NewReader(f.object(t, found)))
				require.NoError(t, zerr)

				rows, rerr := csv.NewReader(zr).ReadAll()
				require.NoError(t, rerr)
				assert.Len(t, rows, tc.rows+1)
			}
		})
	}
}

func TestDelivery_NoBucket(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.bucket(t)

	_, err := f.client.PutReportDefinition(t.Context(), &cursdk.PutReportDefinitionInput{ReportDefinition: def("r")})
	require.NoError(t, err)

	_, err = f.s3.DeleteBucket(t.Context(), &sdk_s3.DeleteBucketInput{Bucket: aws.String("cur-bucket")})
	require.NoError(t, err)

	f.backend.DeliverDue(t.Context())

	st := f.status(t, "r")
	require.NotNil(t, st)
	assert.Equal(t, curtypes.LastStatusErrorNoBucket, st.LastStatus)
}
