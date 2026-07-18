package polly_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/polly"
)

func newHandler() *polly.Handler {
	return polly.NewHandler(polly.NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion))
}

func request(
	t *testing.T,
	handler *polly.Handler,
	method, target string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var payload []byte
	if body != nil {
		var err error
		payload, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(method, target, bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	ctx := echo.New().NewContext(req, rec)
	require.NoError(t, handler.Handler()(ctx))

	return rec
}

func responseMap(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out
}

func startTask(t *testing.T, handler *polly.Handler, text string) (string, string) {
	t.Helper()

	rec := request(t, handler, http.MethodPost, "/v1/synthesisTasks", map[string]any{
		"OutputS3BucketName": "speech-output",
		"OutputFormat":       "mp3",
		"Text":               text,
		"VoiceId":            "Joanna",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	task := responseMap(t, rec)["SynthesisTask"].(map[string]any)

	return task["TaskId"].(string), task["TaskStatus"].(string)
}

func TestHandlerMetadataAndRouting(t *testing.T) {
	t.Parallel()

	handler := newHandler()
	tests := []struct {
		name     string
		method   string
		path     string
		wantOp   string
		resource string
		matches  bool
	}{
		{name: "speech", method: http.MethodPost, path: "/v1/speech", wantOp: "SynthesizeSpeech", matches: true},
		{
			name: "speech_stream", method: http.MethodPost, path: "/v1/synthesisStream",
			wantOp: "StartSpeechSynthesisStream", matches: true,
		},
		{
			name:     "put_lexicon",
			method:   http.MethodPut,
			path:     "/v1/lexicons/medical",
			wantOp:   "PutLexicon",
			resource: "medical",
			matches:  true,
		},
		{
			name:     "task",
			method:   http.MethodGet,
			path:     "/v1/synthesisTasks/123",
			wantOp:   "GetSpeechSynthesisTask",
			resource: "123",
			matches:  true,
		},
		{name: "pinpoint_route", method: http.MethodGet, path: "/v1/apps", wantOp: "Unknown", matches: false},
		{name: "foreign", method: http.MethodGet, path: "/other", wantOp: "Unknown", matches: false},
	}

	assert.Equal(t, "Polly", handler.Name())
	assert.Equal(t, "polly", handler.ChaosServiceName())
	assert.Contains(t, handler.GetSupportedOperations(), "DescribeVoices")
	assert.Equal(t, []string{config.DefaultRegion}, handler.ChaosRegions())
	assert.Positive(t, handler.MatchPriority())

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(test.method, test.path, nil)
			ctx := echo.New().NewContext(req, httptest.NewRecorder())
			assert.Equal(t, test.matches, handler.RouteMatcher()(ctx))
			assert.Equal(t, test.wantOp, handler.ExtractOperation(ctx))
			assert.Equal(t, test.resource, handler.ExtractResource(ctx))
		})
	}
}

func TestResetAndUnknownRoutes(t *testing.T) {
	t.Parallel()

	handler := newHandler()
	startTask(t, handler, "task")
	handler.Reset()

	rec := request(t, handler, http.MethodGet, "/v1/synthesisTasks", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Empty(t, responseMap(t, rec)["SynthesisTasks"].([]any))

	rec = request(t, handler, http.MethodGet, "/v1/not-supported", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "Unknown")
}
