package polly_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream"
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
		{name: "foreign", method: http.MethodGet, path: "/other", wantOp: "Unknown", matches: false},
	}

	assert.Equal(t, "Polly", handler.Name())
	assert.Equal(t, "polly", handler.ChaosServiceName())
	assert.Contains(t, handler.GetSupportedOperations(), "DescribeVoices")
	assert.Equal(t, []string{config.DefaultRegion}, handler.ChaosRegions())
	assert.Greater(t, handler.MatchPriority(), 0)

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

func TestStartSpeechSynthesisStream(t *testing.T) {
	t.Parallel()

	var body bytes.Buffer
	encoder := eventstream.NewEncoder()
	for _, text := range []string{"hello ", "world"} {
		message := eventstream.Message{Payload: []byte(`{"Text":"` + text + `","TextType":"text"}`)}
		message.Headers.Set(":event-type", eventstream.StringValue("TextEvent"))
		require.NoError(t, encoder.Encode(&body, message))
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/synthesisStream", &body)
	req.Header.Set("X-Amzn-Engine", "generative")
	req.Header.Set("X-Amzn-Outputformat", "mp3")
	req.Header.Set("X-Amzn-Voiceid", "Joanna")
	rec := httptest.NewRecorder()
	ctx := echo.New().NewContext(req, rec)
	require.NoError(t, newHandler().Handler()(ctx))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/vnd.amazon.eventstream", rec.Header().Get("Content-Type"))

	decoder := eventstream.NewDecoder()
	audio, err := decoder.Decode(rec.Body, nil)
	require.NoError(t, err)
	assert.Equal(t, "AudioEvent", audio.Headers.Get(":event-type").String())
	assert.Contains(t, string(audio.Payload), "hello world")

	closed, err := decoder.Decode(rec.Body, nil)
	require.NoError(t, err)
	assert.Equal(t, "StreamClosedEvent", closed.Headers.Get(":event-type").String())
	assert.Contains(t, string(closed.Payload), "11")
}

func TestSynthesizeSpeechFormats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		format      string
		rate        string
		textType    string
		contentType string
		marks       []string
		bodyPart    string
	}{
		{name: "mp3", format: "mp3", rate: "22050", textType: "text", contentType: "audio/mpeg", bodyPart: "POLLY:mp3"},
		{
			name:        "ogg",
			format:      "ogg_vorbis",
			rate:        "16000",
			textType:    "text",
			contentType: "audio/ogg",
			bodyPart:    "POLLY:ogg_vorbis",
		},
		{
			name:        "pcm_ssml",
			format:      "pcm",
			rate:        "8000",
			textType:    "ssml",
			contentType: "audio/pcm",
			bodyPart:    "POLLY:pcm",
		},
		{
			name:        "word_mark",
			format:      "json",
			rate:        "22050",
			textType:    "text",
			contentType: "application/x-json-stream",
			marks:       []string{"word"},
			bodyPart:    `"type":"word"`,
		},
		{
			name:        "sentence_ssml_viseme_marks",
			format:      "json",
			rate:        "16000",
			textType:    "ssml",
			contentType: "application/x-json-stream",
			marks:       []string{"sentence", "ssml", "viseme"},
			bodyPart:    `"type":"viseme"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			rec := request(t, newHandler(), http.MethodPost, "/v1/speech", map[string]any{
				"Engine":          "standard",
				"OutputFormat":    test.format,
				"SampleRate":      test.rate,
				"SpeechMarkTypes": test.marks,
				"Text":            "hello world",
				"TextType":        test.textType,
				"VoiceId":         "Joanna",
			})

			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, test.contentType, rec.Header().Get("Content-Type"))
			assert.Equal(t, "11", rec.Header().Get("x-amzn-RequestCharacters"))
			assert.Contains(t, rec.Body.String(), test.bodyPart)
		})
	}
}

func TestSynthesizeSpeechValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body map[string]any
	}{
		{
			name: "json_requires_marks",
			body: map[string]any{"OutputFormat": "json", "Text": "hello", "VoiceId": "Joanna"},
		},
		{
			name: "marks_require_json",
			body: map[string]any{
				"OutputFormat":    "mp3",
				"SpeechMarkTypes": []string{"word"},
				"Text":            "hello",
				"VoiceId":         "Joanna",
			},
		},
		{
			name: "invalid_rate",
			body: map[string]any{"OutputFormat": "pcm", "SampleRate": "48000", "Text": "hello", "VoiceId": "Joanna"},
		},
		{
			name: "unsupported_neural_voice",
			body: map[string]any{"Engine": "neural", "Text": "hello", "VoiceId": "Aditi"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			rec := request(t, newHandler(), http.MethodPost, "/v1/speech", test.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "InvalidParameterValueException")
		})
	}
}

func TestTaskLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		text      string
		final     string
		hasReason bool
	}{
		{name: "completed", text: "render this", final: "completed"},
		{name: "failed", text: "render [fail] this", final: "failed", hasReason: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			handler := newHandler()
			id, initial := startTask(t, handler, test.text)
			assert.Equal(t, "scheduled", initial)

			first := request(t, handler, http.MethodGet, "/v1/synthesisTasks/"+id, nil)
			require.Equal(t, http.StatusOK, first.Code)
			task := responseMap(t, first)["SynthesisTask"].(map[string]any)
			assert.Equal(t, "inProgress", task["TaskStatus"])

			second := request(t, handler, http.MethodGet, "/v1/synthesisTasks/"+id, nil)
			task = responseMap(t, second)["SynthesisTask"].(map[string]any)
			assert.Equal(t, test.final, task["TaskStatus"])
			if test.hasReason {
				assert.NotEmpty(t, task["TaskStatusReason"])
			}

			list := request(t, handler, http.MethodGet, "/v1/synthesisTasks?Status="+test.final, nil)
			require.Equal(t, http.StatusOK, list.Code)
			tasks := responseMap(t, list)["SynthesisTasks"].([]any)
			require.Len(t, tasks, 1)
			assert.Equal(t, id, tasks[0].(map[string]any)["TaskId"])
		})
	}
}

func TestLexiconCRUD(t *testing.T) {
	t.Parallel()

	handler := newHandler()
	tests := []struct {
		name   string
		method string
		path   string
		body   any
		code   int
		find   string
	}{
		{
			name: "put_first", method: http.MethodPut, path: "/v1/lexicons/zeta",
			body: map[string]any{"Content": `<lexicon alphabet="ipa" xml:lang="en-US"><lexeme></lexeme></lexicon>`},
			code: http.StatusOK,
		},
		{
			name: "put_second", method: http.MethodPut, path: "/v1/lexicons/alpha",
			body: map[string]any{"Content": `<lexicon alphabet="x-sampa" xml:lang="en-GB"></lexicon>`},
			code: http.StatusOK,
		},
		{
			name:   "get",
			method: http.MethodGet,
			path:   "/v1/lexicons/zeta",
			code:   http.StatusOK,
			find:   `"LanguageCode":"en-US"`,
		},
		{name: "list", method: http.MethodGet, path: "/v1/lexicons", code: http.StatusOK, find: `"Name":"alpha"`},
		{name: "delete", method: http.MethodDelete, path: "/v1/lexicons/zeta", code: http.StatusOK},
		{
			name:   "missing",
			method: http.MethodGet,
			path:   "/v1/lexicons/zeta",
			code:   http.StatusNotFound,
			find:   "LexiconNotFoundException",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rec := request(t, handler, test.method, test.path, test.body)
			assert.Equal(t, test.code, rec.Code)
			if test.find != "" {
				assert.Contains(t, rec.Body.String(), test.find)
			}
		})
	}
}

func TestDescribeVoicesFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		query      string
		count      int
		contains   string
		notContain string
	}{
		{name: "neural", query: "?Engine=neural", count: 3, notContain: "Aditi"},
		{name: "male_english", query: "?LanguageCode=en-US&Gender=Male", count: 1, contains: "Matthew"},
		{name: "additional_language_disabled", query: "?LanguageCode=hi-IN", count: 0},
		{
			name:     "additional_language_enabled",
			query:    "?LanguageCode=hi-IN&IncludeAdditionalLanguageCodes=true",
			count:    1,
			contains: "Aditi",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			rec := request(t, newHandler(), http.MethodGet, "/v1/voices"+test.query, nil)
			require.Equal(t, http.StatusOK, rec.Code)
			voices := responseMap(t, rec)["Voices"].([]any)
			assert.Len(t, voices, test.count)
			assert.Contains(t, rec.Body.String(), test.contains)
			if test.notContain != "" {
				assert.NotContains(t, rec.Body.String(), test.notContain)
			}
		})
	}
}

func TestTaskTags(t *testing.T) {
	t.Parallel()

	handler := newHandler()
	id, _ := startTask(t, handler, "tagged task")
	taskARN := handler.Backend.TaskARN(id)
	path := "/v1/tags/" + url.PathEscape(taskARN)

	tests := []struct {
		name   string
		method string
		target string
		body   any
		find   []string
	}{
		{
			name: "tag", method: http.MethodPost, target: path,
			body: map[string]any{
				"Tags": []map[string]string{{"Key": "env", "Value": "dev"}, {"Key": "team", "Value": "audio"}},
			},
		},
		{name: "list", method: http.MethodGet, target: path, find: []string{"env", "team"}},
		{name: "untag", method: http.MethodDelete, target: path + "?tagKeys=env"},
		{name: "list_removed", method: http.MethodGet, target: path, find: []string{"team"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rec := request(t, handler, test.method, test.target, test.body)
			require.Equal(t, http.StatusOK, rec.Code)
			for _, find := range test.find {
				assert.Contains(t, rec.Body.String(), find)
			}
			if strings.Contains(test.name, "removed") {
				assert.NotContains(t, rec.Body.String(), `"env"`)
			}
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
