package resourceexplorer2_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdk "github.com/aws/aws-sdk-go-v2/service/resourceexplorer2"
	rtypes "github.com/aws/aws-sdk-go-v2/service/resourceexplorer2/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/services/resourceexplorer2"
)

const testAccount = awsmeta.DefaultAccount

type fakeSource []resourceexplorer2.SourceResource

func (f fakeSource) Resources(context.Context) []resourceexplorer2.SourceResource { return f }

type fakeOrg struct{ enabled bool }

func (f fakeOrg) ServiceAccessEnabled(string) bool { return f.enabled }

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

type fixture struct {
	backend *resourceexplorer2.InMemoryBackend
	clock   *fakeClock
	url     string
}

func defaultResources() fakeSource {
	a := testAccount

	return fakeSource{
		{ARN: "arn:aws:s3:::alpha-bucket", Tags: map[string]string{"env": "prod", "team": "a"}},
		{ARN: "arn:aws:sqs:us-east-1:" + a + ":orders-queue", Tags: map[string]string{"env": "dev"}},
		{ARN: "arn:aws:lambda:us-east-1:" + a + ":function:hello"},
		{ARN: "arn:aws:dynamodb:us-east-1:" + a + ":table/users", Tags: map[string]string{"env": "prod"}},
		{ARN: "arn:aws:ec2:us-west-2:" + a + ":instance/i-0123", Tags: map[string]string{"team": "b"}},
		{ARN: "arn:aws:iam::" + a + ":role/admin"},
		{ARN: "arn:aws:sns:us-east-1:999999999999:foreign-topic"},
	}
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	b := resourceexplorer2.NewInMemoryBackend(testAccount, "us-east-1")
	clock := &fakeClock{t: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	b.SetClock(clock.now)
	b.SetResourceSource(defaultResources())
	b.SetOrgAccess(fakeOrg{})

	return &fixture{backend: b, clock: clock, url: sdktest.Serve(t, resourceexplorer2.NewHandler(b))}
}

func (f *fixture) clientIn(t *testing.T, region, accessKey string) *sdk.Client {
	t.Helper()

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(region),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, "test", "")),
	)
	require.NoError(t, err)

	return sdk.NewFromConfig(cfg, func(o *sdk.Options) { o.BaseEndpoint = aws.String(f.url) })
}

func (f *fixture) client(t *testing.T) *sdk.Client {
	t.Helper()

	return f.clientIn(t, "us-east-1", "test")
}

func apiCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}

// setupViewed creates an index and a default view in the client's Region and returns the view ARN.
func setupViewed(t *testing.T, c *sdk.Client, props ...string) string {
	t.Helper()

	_, err := c.CreateIndex(t.Context(), &sdk.CreateIndexInput{})
	require.NoError(t, err)

	return createView(t, c, "all", "", props...)
}

func createView(t *testing.T, c *sdk.Client, name, filter string, props ...string) string {
	t.Helper()

	in := &sdk.CreateViewInput{ViewName: aws.String(name)}
	if filter != "" {
		in.Filters = &rtypes.SearchFilter{FilterString: aws.String(filter)}
	}

	for _, p := range props {
		in.IncludedProperties = append(in.IncludedProperties, rtypes.IncludedProperty{Name: aws.String(p)})
	}

	out, err := c.CreateView(t.Context(), in)
	require.NoError(t, err)

	_, err = c.AssociateDefaultView(t.Context(), &sdk.AssociateDefaultViewInput{ViewArn: out.View.ViewArn})
	require.NoError(t, err)

	return aws.ToString(out.View.ViewArn)
}

func newServedURL(t *testing.T, b *resourceexplorer2.InMemoryBackend) string {
	t.Helper()

	return sdktest.Serve(t, resourceexplorer2.NewHandler(b))
}
