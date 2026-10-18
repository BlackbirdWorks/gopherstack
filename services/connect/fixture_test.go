package connect_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	cn "github.com/blackbirdworks/gopherstack/services/connect"
)

const testAccount = "123456789012"

type fixture struct {
	backend *cn.Backend
	handler *cn.Handler
	client  *sdk.Client
	clock   *testClock
}

type testClock struct {
	t  time.Time
	mu sync.Mutex
}

func (c *testClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.t
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.t = c.t.Add(d)
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	clock := &testClock{t: time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)}
	b := cn.NewBackend(testAccount, "us-east-1")
	b.SetClock(clock.now)

	h := cn.NewHandler(b)
	url := sdktest.Serve(t, h)

	cfg, err := awscfg.LoadDefaultConfig(t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(testAccount, "test", "")),
	)
	require.NoError(t, err)

	return &fixture{
		backend: b, handler: h, clock: clock,
		client: sdk.NewFromConfig(cfg, func(o *sdk.Options) { o.BaseEndpoint = aws.String(url) }),
	}
}

func apiCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}

// newInstance creates an instance and returns its ID.
func (f *fixture) newInstance(t *testing.T, alias string) string {
	t.Helper()

	out, err := f.client.CreateInstance(t.Context(), &sdk.CreateInstanceInput{
		IdentityManagementType: types.DirectoryTypeConnectManaged,
		InstanceAlias:          aws.String(alias),
		InboundCallsEnabled:    aws.Bool(true),
		OutboundCallsEnabled:   aws.Bool(true),
	})
	require.NoError(t, err)

	return aws.ToString(out.Id)
}

func (f *fixture) defaultQueue(t *testing.T, inst string) (string, string, string) {
	t.Helper()

	qs, err := f.client.ListQueues(t.Context(), &sdk.ListQueuesInput{InstanceId: aws.String(inst)})
	require.NoError(t, err)
	require.Len(t, qs.QueueSummaryList, 1)

	q, err := f.client.DescribeQueue(
		t.Context(),
		&sdk.DescribeQueueInput{InstanceId: aws.String(inst), QueueId: qs.QueueSummaryList[0].Id},
	)
	require.NoError(t, err)

	ps, err := f.client.ListRoutingProfiles(t.Context(), &sdk.ListRoutingProfilesInput{InstanceId: aws.String(inst)})
	require.NoError(t, err)
	require.Len(t, ps.RoutingProfileSummaryList, 1)

	return aws.ToString(
			q.Queue.QueueId,
		), aws.ToString(
			q.Queue.HoursOfOperationId,
		), aws.ToString(
			ps.RoutingProfileSummaryList[0].Id,
		)
}

type fakeResolvers struct {
	lambdas map[string]bool
	buckets map[string]bool
	objects map[string]bool
}

func (f fakeResolvers) LambdaFunctionExists(_ context.Context, arn string) bool {
	return f.lambdas[arn]
}
func (f fakeResolvers) BucketExists(_ context.Context, b string) bool    { return f.buckets[b] }
func (f fakeResolvers) ObjectExists(_ context.Context, b, k string) bool { return f.objects[b+"/"+k] }
