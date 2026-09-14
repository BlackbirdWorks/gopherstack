package cloudwatchlogs_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	cwlsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

// ptrLeakCaptureTransport records the raw bytes of the last HTTP response,
// then replays them so the real SDK deserializer still sees the full body.
type ptrLeakCaptureTransport struct {
	body []byte
	mu   sync.Mutex
}

// Body returns the last captured response body. The SDK retry loop may
// call Do from a different goroutine than the test asserting on it.
func (c *ptrLeakCaptureTransport) Body() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.body
}

func (c *ptrLeakCaptureTransport) Do(req *http.Request) (*http.Response, error) {
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	b, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	resp.Body.Close()

	c.mu.Lock()
	c.body = b
	c.mu.Unlock()
	resp.Body = io.NopCloser(bytes.NewReader(b))

	return resp, nil
}

// newCapturingWireTestCloudWatchLogsClient is newTestCloudWatchLogsClient
// plus a transport that stashes each response's raw bytes on capture.Body(),
// so a test can assert on the wire JSON while also proving the real client
// decodes it.
func newCapturingWireTestCloudWatchLogsClient(
	t *testing.T, h *cloudwatchlogs.Handler,
) (*cwlsdk.Client, *ptrLeakCaptureTransport) {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(cwlTagsRTRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	capture := &ptrLeakCaptureTransport{}

	client := cwlsdk.NewFromConfig(cfg, func(o *cwlsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.HTTPClient = capture
	})

	return client, capture
}

// seedPtrLeakEvents puts n log events onto a fresh log group/stream through
// the real SDK client and returns their names.
func seedPtrLeakEvents(t *testing.T, client *cwlsdk.Client, logGroup, logStream string, n int) {
	t.Helper()

	ctx := t.Context()

	_, err := client.CreateLogGroup(ctx, &cwlsdk.CreateLogGroupInput{LogGroupName: aws.String(logGroup)})
	require.NoError(t, err)
	_, err = client.CreateLogStream(ctx, &cwlsdk.CreateLogStreamInput{
		LogGroupName: aws.String(logGroup), LogStreamName: aws.String(logStream),
	})
	require.NoError(t, err)

	events := make([]cwltypes.InputLogEvent, 0, n)
	base := time.Now().UnixMilli()

	for i := range n {
		events = append(events, cwltypes.InputLogEvent{
			Message:   aws.String("event"),
			Timestamp: aws.Int64(base + int64(i)*1000),
		})
	}

	_, err = client.PutLogEvents(ctx, &cwlsdk.PutLogEventsInput{
		LogGroupName:  aws.String(logGroup),
		LogStreamName: aws.String(logStream),
		LogEvents:     events,
	})
	require.NoError(t, err)
}

// TestGetLogEvents_PtrStripped covers gopherstack-b518o: real
// types.OutputLogEvent (cloudwatchlogs v1.86.0 types/types.go:2077-2091) has
// no ptr member, so GetLogEvents' raw body must not carry the key even
// though Ptr is persisted on every stored OutputLogEvent. The response is
// also decoded through the real typed client to prove the strip didn't
// break the shape.
func TestGetLogEvents_PtrStripped(t *testing.T) {
	t.Parallel()

	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())
	client, capture := newCapturingWireTestCloudWatchLogsClient(t, h)

	const logGroup = "/ptr-leak/strip"
	const logStream = "stream-1"

	seedPtrLeakEvents(t, client, logGroup, logStream, 3)

	out, err := client.GetLogEvents(t.Context(), &cwlsdk.GetLogEventsInput{
		LogGroupName:  aws.String(logGroup),
		LogStreamName: aws.String(logStream),
		StartFromHead: aws.Bool(true),
	})
	require.NoError(t, err)
	require.Len(t, out.Events, 3)

	require.NotEmpty(t, capture.Body())
	assert.NotContains(t, string(capture.Body()), `"ptr"`,
		"GetLogEvents response must not carry the persisted-only Ptr field on the wire")
}

// TestGetLogEvents_Pagination_StillResumes proves the wire twin didn't
// regress GetLogEvents' forward/backward pagination: nextForwardToken and
// nextBackwardToken still page through the full event set with no
// duplicates and nothing missing.
func TestGetLogEvents_Pagination_StillResumes(t *testing.T) {
	t.Parallel()

	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())
	client, _ := newCapturingWireTestCloudWatchLogsClient(t, h)
	ctx := t.Context()

	const logGroup = "/ptr-leak/paginate"
	const logStream = "stream-1"
	const total = 25
	const pageSize = 4

	seedPtrLeakEvents(t, client, logGroup, logStream, total)

	t.Run("forward", func(t *testing.T) {
		t.Parallel()

		var nextToken *string
		var seen int
		var lastToken string

		for pages := 0; ; pages++ {
			require.Less(t, pages, total, "forward pagination loop did not terminate")

			out, err := client.GetLogEvents(ctx, &cwlsdk.GetLogEventsInput{
				LogGroupName:  aws.String(logGroup),
				LogStreamName: aws.String(logStream),
				StartFromHead: aws.Bool(true),
				Limit:         aws.Int32(pageSize),
				NextToken:     nextToken,
			})
			require.NoError(t, err)

			seen += len(out.Events)

			forward := aws.ToString(out.NextForwardToken)
			require.NotEmpty(t, forward)

			if forward == lastToken {
				break
			}

			lastToken = forward
			nextToken = out.NextForwardToken
		}

		assert.Equal(t, total, seen, "forward pagination must cover every event exactly once")
	})

	t.Run("backward", func(t *testing.T) {
		t.Parallel()

		// Walk to the end first, then page backward, matching real
		// GetLogEvents semantics (nextBackwardToken returns the window
		// immediately preceding the one it was issued from).
		endOut, err := client.GetLogEvents(ctx, &cwlsdk.GetLogEventsInput{
			LogGroupName:  aws.String(logGroup),
			LogStreamName: aws.String(logStream),
			StartFromHead: aws.Bool(false),
			Limit:         aws.Int32(pageSize),
		})
		require.NoError(t, err)

		seen := len(endOut.Events)
		nextToken := endOut.NextBackwardToken
		var lastToken string

		for pages := 0; ; pages++ {
			require.Less(t, pages, total, "backward pagination loop did not terminate")

			backward := aws.ToString(nextToken)
			require.NotEmpty(t, backward)

			if backward == lastToken {
				break
			}

			out, pageErr := client.GetLogEvents(ctx, &cwlsdk.GetLogEventsInput{
				LogGroupName:  aws.String(logGroup),
				LogStreamName: aws.String(logStream),
				Limit:         aws.Int32(pageSize),
				NextToken:     nextToken,
			})
			require.NoError(t, pageErr)

			if len(out.Events) == 0 {
				break
			}

			seen += len(out.Events)
			lastToken = backward
			nextToken = out.NextBackwardToken
		}

		assert.Equal(t, total, seen, "backward pagination must cover every event exactly once")
	})
}

// TestGetLogEvents_PtrStillWorksInternally proves the wire strip only
// removes Ptr from GetLogEvents' response body: the backend still populates
// Ptr on every OutputLogEvent (log_events.go:367) and GetLogRecord can still
// resolve it, matching the existing pagination bookkeeping use.
func TestGetLogEvents_PtrStillWorksInternally(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackend()
	ctx := t.Context()

	_, err := backend.CreateLogGroup(ctx, "ptr-leak-internal", "", "")
	require.NoError(t, err)
	_, err = backend.CreateLogStream(ctx, "ptr-leak-internal", "stream-1")
	require.NoError(t, err)

	_, err = backend.PutLogEvents(ctx, "ptr-leak-internal", "stream-1", "", []cloudwatchlogs.InputLogEvent{
		{Message: "hello", Timestamp: time.Now().UnixMilli()},
	})
	require.NoError(t, err)

	events, _, _, err := backend.GetLogEvents(ctx, "ptr-leak-internal", "stream-1", nil, nil, 0, "", true)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.NotEmpty(t, events[0].Ptr, "Ptr must still be populated internally")

	record, err := backend.GetLogRecord(ctx, events[0].Ptr)
	require.NoError(t, err, "GetLogRecord must still resolve the internally-used Ptr")
	assert.Equal(t, "hello", record["@message"])
}

// TestFilterLogEvents_EventIDStillOnWire is the control: FilteredLogEvent
// is a distinct type from OutputLogEvent (models.go) and already carries its
// own real "eventId" member (types.FilteredLogEvent, cloudwatchlogs v1.86.0
// types/types.go:944-964); the GetLogEvents fix must not touch it.
func TestFilterLogEvents_EventIDStillOnWire(t *testing.T) {
	t.Parallel()

	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())
	client, capture := newCapturingWireTestCloudWatchLogsClient(t, h)

	const logGroup = "/ptr-leak/filter"
	const logStream = "stream-1"

	seedPtrLeakEvents(t, client, logGroup, logStream, 2)

	out, err := client.FilterLogEvents(t.Context(), &cwlsdk.FilterLogEventsInput{
		LogGroupName: aws.String(logGroup),
	})
	require.NoError(t, err)
	require.Len(t, out.Events, 2)

	for _, ev := range out.Events {
		assert.NotEmpty(t, aws.ToString(ev.EventId), "FilterLogEvents must still carry a real eventId")
	}

	require.NotEmpty(t, capture.Body())
	assert.Contains(t, string(capture.Body()), `"eventId"`,
		"FilterLogEvents must still carry eventId on the wire")
}
