package rum_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsrum "github.com/aws/aws-sdk-go-v2/service/rum"
	rumtypes "github.com/aws/aws-sdk-go-v2/service/rum/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/services/rum"
)

const (
	testAccount = "123456789012"
	testRegion  = "us-east-1"
)

var errNoLogGroup = errors.New("no such log group")

type fakeLogs struct {
	groups map[string]bool
	events map[string][]rum.LogEvent
	mu     sync.Mutex
}

func (f *fakeLogs) EnsureLogGroup(_ context.Context, _, group string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.groups[group] = true

	return nil
}

func (f *fakeLogs) PutLogEvents(_ context.Context, _, group, _ string, events []rum.LogEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.groups[group] {
		return errNoLogGroup
	}

	f.events[group] = append(f.events[group], events...)

	return nil
}

func (f *fakeLogs) logged(group string) []rum.LogEvent {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]rum.LogEvent(nil), f.events[group]...)
}

type fakeMetrics struct {
	points []cwmetric.Point
	mu     sync.Mutex
}

func (f *fakeMetrics) EmitMetric(p cwmetric.Point) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.points = append(f.points, p)

	return nil
}

func (f *fakeMetrics) named(ns, name string) []cwmetric.Point {
	f.mu.Lock()
	defer f.mu.Unlock()

	var out []cwmetric.Point

	for _, p := range f.points {
		if p.Namespace == ns && p.Name == name {
			out = append(out, p)
		}
	}

	return out
}

type fakeClock struct {
	t  time.Time
	mu sync.Mutex
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.t
}

type fixture struct {
	client  *awsrum.Client
	backend *rum.InMemoryBackend
	logs    *fakeLogs
	metrics *fakeMetrics
	clock   *fakeClock
	url     string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	return newFixtureFromBackend(t, rum.NewInMemoryBackend(testAccount, testRegion))
}

func newFixtureFromBackend(t *testing.T, backend *rum.InMemoryBackend) *fixture {
	t.Helper()

	clock := &fakeClock{t: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	logs := &fakeLogs{groups: map[string]bool{}, events: map[string][]rum.LogEvent{}}
	metrics := &fakeMetrics{}

	backend.SetClock(clock.now)
	backend.SetLogSink(logs)
	backend.SetMetricEmitter(metrics)

	url := sdktest.Serve(t, rum.NewHandler(backend))

	return &fixture{
		client:  clientFor(t, url, testAccount),
		backend: backend,
		logs:    logs,
		metrics: metrics,
		clock:   clock,
		url:     url,
	}
}

// clientFor returns a client whose access key (a 12-digit account ID) selects the calling account.
func clientFor(t *testing.T, url, account string) *awsrum.Client {
	t.Helper()

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(testRegion),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(account, "test", "")),
	)
	require.NoError(t, err)

	return awsrum.NewFromConfig(cfg, func(o *awsrum.Options) {
		o.BaseEndpoint = aws.String(url)
		o.HTTPClient = dialTo(strings.TrimPrefix(url, "http://"))
	})
}

// dialTo sends every request to addr: PutRumEvents prefixes the host with "dataplane.", which cannot resolve in tests.
func dialTo(addr string) aws.HTTPClient {
	return awshttp.NewBuildableClient().WithTransportOptions(func(tr *http.Transport) {
		tr.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
			var d net.Dialer

			return d.DialContext(ctx, network, addr)
		}
	})
}

func apiCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}

// createMonitor creates a web app monitor and returns its ID.
func (f *fixture) createMonitor(t *testing.T, name string, opts ...func(*awsrum.CreateAppMonitorInput)) string {
	t.Helper()

	in := &awsrum.CreateAppMonitorInput{Name: aws.String(name), Domain: aws.String("example.com")}
	for _, o := range opts {
		o(in)
	}

	out, err := f.client.CreateAppMonitor(t.Context(), in)
	require.NoError(t, err)

	return aws.ToString(out.Id)
}

func monitorARN(name string) string {
	return "arn:aws:rum:" + testRegion + ":" + testAccount + ":appmonitor/" + name
}

type eventSpec struct {
	id      string
	typ     string
	details string
	meta    string
	ts      float64
}

func (e eventSpec) event() rumtypes.RumEvent {
	ts := time.Unix(int64(e.ts), 0)

	ev := rumtypes.RumEvent{
		Id: aws.String(e.id), Type: aws.String(e.typ), Timestamp: &ts, Details: aws.String(e.details),
	}
	if e.meta != "" {
		ev.Metadata = aws.String(e.meta)
	}

	return ev
}

// putEvents sends one batch of events to the app monitor.
func (f *fixture) putEvents(t *testing.T, id, name string, events ...eventSpec) error {
	t.Helper()

	evs := make([]rumtypes.RumEvent, len(events))
	for i, e := range events {
		evs[i] = e.event()
	}

	_, err := f.client.PutRumEvents(t.Context(), &awsrum.PutRumEventsInput{
		Id:      aws.String(id),
		BatchId: aws.String("batch-1"),
		AppMonitorDetails: &rumtypes.AppMonitorDetails{
			Id:      aws.String(id),
			Name:    aws.String(name),
			Version: aws.String("1.0.0"),
		},
		UserDetails: &rumtypes.UserDetails{SessionId: aws.String("sess-1"), UserId: aws.String("user-1")},
		RumEvents:   evs,
	})

	return err
}

func mustJSON(t *testing.T, s string) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal([]byte(s), &m))

	return m
}
