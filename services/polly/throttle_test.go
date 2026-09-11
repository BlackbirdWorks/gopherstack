package polly_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/polly"
)

// streamBody encodes a minimal one-event TextEvent body for
// StartSpeechSynthesisStream, matching the shape speech_test.go's own
// StartSpeechSynthesisStream tests use.
func streamBody(t *testing.T) *bytes.Buffer {
	t.Helper()

	var body bytes.Buffer
	encoder := eventstream.NewEncoder()
	message := eventstream.Message{Payload: []byte(`{"Text":"hi","TextType":"text"}`)}
	message.Headers.Set(":event-type", eventstream.StringValue("TextEvent"))
	require.NoError(t, encoder.Encode(&body, message))

	return &body
}

func streamRequest(t *testing.T) *http.Request {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/v1/synthesisStream", streamBody(t))
	req.Header.Set("X-Amzn-Engine", "generative")
	req.Header.Set("X-Amzn-Outputformat", "mp3")
	req.Header.Set("X-Amzn-Voiceid", "Ruth")

	return req
}

// fakeClock is a mutable, manually-advanced time source for
// InMemoryBackend.WithClock -- deterministic sliding-window tests without
// time.Sleep. Not safe for concurrent use; these tests drive the backend
// from a single goroutine.
type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }

// TestBeginSpeechSynthesisStream_ThrottleWindow proves gopherstack-80h3:
// StartSpeechSynthesisStream's real 8-tps (lowered here via WithStreamLimits
// for a terser test) per-engine sliding one-second window is enforced. N
// requests within one second pass, the (N+1)th is throttled, and the window
// resets once a full second has elapsed.
func TestBeginSpeechSynthesisStream_ThrottleWindow(t *testing.T) {
	t.Parallel()

	clock := &fakeClock{now: time.Now()}
	backend := polly.NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion).
		WithClock(clock.Now).
		WithStreamLimits(3, 100)

	for i := range 3 {
		release, err := backend.BeginSpeechSynthesisStream("generative")
		require.NoError(t, err, "request %d within the tps budget must pass", i)
		release()
	}

	_, err := backend.BeginSpeechSynthesisStream("generative")
	require.Error(t, err)
	require.ErrorIs(t, err, polly.ErrThrottling)

	clock.now = clock.now.Add(2 * time.Second)

	release, err := backend.BeginSpeechSynthesisStream("generative")
	require.NoError(t, err, "a new second must reset the window")
	release()
}

// TestBeginSpeechSynthesisStream_PerEngineIndependence proves the throttle
// window is keyed per engine: exhausting one engine's budget does not
// throttle another engine's request in the same instant.
func TestBeginSpeechSynthesisStream_PerEngineIndependence(t *testing.T) {
	t.Parallel()

	clock := &fakeClock{now: time.Now()}
	backend := polly.NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion).
		WithClock(clock.Now).
		WithStreamLimits(1, 100)

	release, err := backend.BeginSpeechSynthesisStream("generative")
	require.NoError(t, err)
	release()

	_, err = backend.BeginSpeechSynthesisStream("generative")
	require.Error(t, err)
	require.ErrorIs(t, err, polly.ErrThrottling)

	release, err = backend.BeginSpeechSynthesisStream("some-other-engine")
	require.NoError(t, err, "a different engine's window must be independent")
	release()
}

// TestBeginSpeechSynthesisStream_ConcurrencyCap proves the real 8-concurrent-
// request cap (lowered here via WithStreamLimits) is enforced independently
// of the throttle window: the clock advances well past one second between
// each acquire, so only the concurrency cap -- never the tps window -- can
// be responsible for the (N+1)th rejection.
func TestBeginSpeechSynthesisStream_ConcurrencyCap(t *testing.T) {
	t.Parallel()

	clock := &fakeClock{now: time.Now()}
	backend := polly.NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion).
		WithClock(clock.Now).
		WithStreamLimits(100, 3)

	releases := make([]func(), 0, 3)
	for i := range 3 {
		clock.now = clock.now.Add(2 * time.Second)
		release, err := backend.BeginSpeechSynthesisStream("generative")
		require.NoError(t, err, "request %d within the concurrency budget must pass", i)
		releases = append(releases, release)
	}

	clock.now = clock.now.Add(2 * time.Second)
	_, err := backend.BeginSpeechSynthesisStream("generative")
	require.Error(t, err)
	require.ErrorIs(t, err, polly.ErrServiceQuotaExceeded)

	releases[0]()

	clock.now = clock.now.Add(2 * time.Second)
	release, err := backend.BeginSpeechSynthesisStream("generative")
	require.NoError(t, err, "freeing one slot must let the next request through")
	release()
}

// TestStartSpeechSynthesisStream_ThrottlingHTTPWireShape proves the HTTP-level
// error shape for a throttled StartSpeechSynthesisStream request: real Polly
// returns HTTP 400 with error code "ThrottlingException" -- confirmed via
// aws-sdk-go-v2/service/polly@v1.60.4/deserializers.go's
// awsRestjson1_deserializeOpErrorStartSpeechSynthesisStream switch and
// botocore's polly/2016-06-10/service-2.json ThrottlingException shape
// (error.httpStatusCode: 400).
func TestStartSpeechSynthesisStream_ThrottlingHTTPWireShape(t *testing.T) {
	t.Parallel()

	backend := polly.NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion).
		WithStreamLimits(1, 100)
	h := polly.NewHandler(backend)

	first := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(streamRequest(t), first)))
	require.Equal(t, http.StatusOK, first.Code)

	second := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(streamRequest(t), second)))
	require.Equal(t, http.StatusBadRequest, second.Code)

	var out map[string]string
	require.NoError(t, json.Unmarshal(second.Body.Bytes(), &out))
	assert.Equal(t, "ThrottlingException", out["__type"])
}

// TestStartSpeechSynthesisStream_ServiceQuotaHTTPWireShape proves the
// HTTP-level error shape for a StartSpeechSynthesisStream request over the
// concurrent-request cap: real Polly returns HTTP 402 (Payment Required)
// with error code "ServiceQuotaExceededException" -- confirmed via
// botocore's polly/2016-06-10/service-2.json ServiceQuotaExceededException
// shape (error.httpStatusCode: 402), which the Go SDK types don't carry.
// Concurrency is filled directly through the backend (rather than real
// overlapping goroutines) for a deterministic, sleep-free trigger.
func TestStartSpeechSynthesisStream_ServiceQuotaHTTPWireShape(t *testing.T) {
	t.Parallel()

	backend := polly.NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion).
		WithStreamLimits(100, 1)
	h := polly.NewHandler(backend)

	_, err := backend.BeginSpeechSynthesisStream("generative")
	require.NoError(t, err, "fill the only concurrency slot and leave it unreleased")

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(streamRequest(t), rec)))
	require.Equal(t, http.StatusPaymentRequired, rec.Code)

	var out map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "ServiceQuotaExceededException", out["__type"])
}
