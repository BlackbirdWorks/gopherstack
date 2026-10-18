package bcmdataexports_test

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	bcmsdk "github.com/aws/aws-sdk-go-v2/service/bcmdataexports"
	bcmtypes "github.com/aws/aws-sdk-go-v2/service/bcmdataexports/types"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/billingexport"
	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/services/bcmdataexports"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

const (
	testAccount = "123456789012"
	bucketName  = "exports-bucket"
)

type fakeCosts []billingexport.CostLine

func (f fakeCosts) CostLines(start, end time.Time) []billingexport.CostLine {
	var out []billingexport.CostLine

	for _, l := range f {
		if !l.Day.Before(start) && l.Day.Before(end) {
			out = append(out, l)
		}
	}

	return out
}

type fixture struct {
	client  *bcmsdk.Client
	backend *bcmdataexports.InMemoryBackend
	s3      *s3backend.InMemoryBackend
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	backend := bcmdataexports.NewInMemoryBackend(testAccount, "us-east-1")
	now := time.Date(2026, 3, 15, 10, 30, 0, 0, time.UTC)
	backend.SetClock(func() time.Time { return now })

	s3b := s3backend.NewInMemoryBackend(nil)
	backend.SetObjectStore(s3b)
	backend.SetCostSource(fakeCosts{
		{
			Day: time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC), Service: "Amazon Simple Storage Service",
			Region: "us-east-1", UsageType: "TimedStorage-ByteHrs", Account: testAccount,
			Blended: 24, Unblended: 24, Usage: 48,
		},
		{
			Day: time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC), Service: "AWS Lambda",
			Region: "us-east-1", UsageType: "Lambda-GB-Second", Account: testAccount,
			Blended: 12, Unblended: 12, Usage: 24,
		},
	})

	_, err := s3b.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucketName)})
	require.NoError(t, err)

	url := sdktest.Serve(t, bcmdataexports.NewHandler(backend))

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := bcmsdk.NewFromConfig(cfg, func(o *bcmsdk.Options) { o.BaseEndpoint = aws.String(url) })

	return &fixture{client: client, backend: backend, s3: s3b}
}

func (f *fixture) settle(t *testing.T) {
	t.Helper()

	for range 5 {
		f.backend.ProcessExecutions(t.Context())
	}
}

func (f *fixture) keys(t *testing.T) []string {
	t.Helper()

	out, err := f.s3.ListObjectsV2(t.Context(), &sdk_s3.ListObjectsV2Input{Bucket: aws.String(bucketName)})
	require.NoError(t, err)

	keys := make([]string, 0, len(out.Contents))
	for _, o := range out.Contents {
		keys = append(keys, aws.ToString(o.Key))
	}

	return keys
}

func (f *fixture) object(t *testing.T, key string) []byte {
	t.Helper()

	out, err := f.s3.GetObject(
		t.Context(),
		&sdk_s3.GetObjectInput{Bucket: aws.String(bucketName), Key: aws.String(key)},
	)
	require.NoError(t, err)

	b, err := io.ReadAll(out.Body)
	require.NoError(t, err)

	return b
}

func export(name, query string) *bcmtypes.Export {
	return &bcmtypes.Export{
		Name:        aws.String(name),
		Description: aws.String("test export"),
		DataQuery:   &bcmtypes.DataQuery{QueryStatement: aws.String(query)},
		DestinationConfigurations: &bcmtypes.DestinationConfigurations{S3Destination: &bcmtypes.S3Destination{
			S3Bucket: aws.String(bucketName), S3Prefix: aws.String("out"), S3Region: aws.String("us-east-1"),
			S3OutputConfigurations: &bcmtypes.S3OutputConfigurations{
				OutputType:  bcmtypes.S3OutputTypeCustom,
				Format:      bcmtypes.FormatOptionTextOrCsv,
				Compression: bcmtypes.CompressionOptionGzip,
				Overwrite:   bcmtypes.OverwriteOptionOverwriteReport,
			},
		}},
		RefreshCadence: &bcmtypes.RefreshCadence{Frequency: bcmtypes.FrequencyOptionSynchronous},
	}
}

func (f *fixture) create(t *testing.T, e *bcmtypes.Export) string {
	t.Helper()

	out, err := f.client.CreateExport(t.Context(), &bcmsdk.CreateExportInput{Export: e})
	require.NoError(t, err)

	return aws.ToString(out.ExportArn)
}

func apiCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}
