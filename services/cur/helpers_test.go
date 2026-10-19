package cur_test

import (
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	cursdk "github.com/aws/aws-sdk-go-v2/service/costandusagereportservice"
	curtypes "github.com/aws/aws-sdk-go-v2/service/costandusagereportservice/types"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/billingexport"
	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/services/cur"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

const testAccount = "123456789012"

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

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
	client  *cursdk.Client
	backend *cur.InMemoryBackend
	s3      *s3backend.InMemoryBackend
	clock   *fakeClock
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	clock := &fakeClock{t: time.Date(2026, 3, 15, 10, 30, 0, 0, time.UTC)}
	backend := cur.NewInMemoryBackend(testAccount, "us-east-1")
	backend.SetClock(clock.now)

	s3b := s3backend.NewInMemoryBackend(nil)
	backend.SetObjectStore(s3b)
	backend.SetCostSource(fakeCosts{
		{
			Day: time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC), Service: "Amazon Simple Storage Service",
			Region: "us-east-1", UsageType: "TimedStorage-ByteHrs", Account: testAccount,
			Blended: 24, Unblended: 24, Usage: 48,
		},
		{
			Day: time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC), Service: "Amazon Simple Storage Service",
			Region: "us-east-1", UsageType: "TimedStorage-ByteHrs", Account: testAccount,
			Blended: 12, Unblended: 12, Usage: 24,
		},
	})

	url := sdktest.Serve(t, cur.NewHandler(backend))

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := cursdk.NewFromConfig(cfg, func(o *cursdk.Options) { o.BaseEndpoint = aws.String(url) })

	return &fixture{client: client, backend: backend, s3: s3b, clock: clock}
}

func (f *fixture) bucket(t *testing.T) {
	t.Helper()

	_, err := f.s3.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String("cur-bucket")})
	require.NoError(t, err)
}

func def(name string) *curtypes.ReportDefinition {
	return &curtypes.ReportDefinition{
		ReportName:               aws.String(name),
		TimeUnit:                 curtypes.TimeUnitDaily,
		Format:                   curtypes.ReportFormatCsv,
		Compression:              curtypes.CompressionFormatGzip,
		S3Bucket:                 aws.String("cur-bucket"),
		S3Prefix:                 aws.String("reports"),
		S3Region:                 curtypes.AWSRegionUsStandard,
		AdditionalSchemaElements: []curtypes.SchemaElement{curtypes.SchemaElementResources},
	}
}

func apiCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}
