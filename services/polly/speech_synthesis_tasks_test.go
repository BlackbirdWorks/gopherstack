package polly_test

import (
	"encoding/base64"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/polly"
)

func TestStartTaskTextLimit(t *testing.T) {
	t.Parallel()

	backend := polly.NewInMemoryBackend()
	_, err := backend.StartSpeechSynthesisTask(
		polly.SynthesisOptions{Text: strings.Repeat("a", 100001), VoiceID: "Joanna"},
		"bucket", "", "",
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, polly.ErrTextLengthExceeded)
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

func TestTaskListPaginationAndValidation(t *testing.T) {
	t.Parallel()

	handler := newHandler()
	for _, text := range []string{"first", "second", "third"} {
		startTask(t, handler, text)
	}

	first := request(t, handler, http.MethodGet, "/v1/synthesisTasks?MaxResults=2", nil)
	require.Equal(t, http.StatusOK, first.Code)
	firstPage := responseMap(t, first)
	require.Len(t, firstPage["SynthesisTasks"].([]any), 2)
	token := firstPage["NextToken"].(string)
	require.NotEmpty(t, token)

	second := request(t, handler, http.MethodGet, "/v1/synthesisTasks?MaxResults=2&NextToken="+token, nil)
	require.Equal(t, http.StatusOK, second.Code)
	assert.Len(t, responseMap(t, second)["SynthesisTasks"].([]any), 1)

	invalidQueries := []struct {
		query   string
		wantErr string
	}{
		{query: "?MaxResults=0", wantErr: "InvalidParameterValueException"},
		{query: "?MaxResults=bad", wantErr: "InvalidParameterValueException"},
		{query: "?Status=nope", wantErr: "InvalidParameterValueException"},
		{query: "?NextToken=bad", wantErr: "InvalidNextTokenException"},
	}
	for _, tc := range invalidQueries {
		rec := request(t, handler, http.MethodGet, "/v1/synthesisTasks"+tc.query, nil)
		assert.Equal(t, http.StatusBadRequest, rec.Code, tc.query)
		assert.Contains(t, rec.Body.String(), tc.wantErr, tc.query)
	}

	missing := request(t, handler, http.MethodGet, "/v1/synthesisTasks/not-created", nil)
	// AWS models SynthesisTaskNotFoundException with httpStatusCode 400, not 404.
	assert.Equal(t, http.StatusBadRequest, missing.Code)
	assert.Contains(t, missing.Body.String(), "SynthesisTaskNotFoundException")
}

// TestStartSpeechSynthesisTaskRequiredAndLimit verifies that
// StartSpeechSynthesisTask rejects missing OutputS3BucketName
// (InvalidParameterValueException) and text exceeding 100000 characters
// (TextLengthExceededException).
func TestStartSpeechSynthesisTaskRequiredAndLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantErr  string
		wantCode int
	}{
		{
			name: "missing bucket returns 400",
			body: map[string]any{
				"OutputFormat": "mp3",
				"Text":         "hello",
				"VoiceId":      "Joanna",
			},
			wantCode: http.StatusBadRequest,
			wantErr:  "InvalidParameterValueException",
		},
		{
			name: "text over 100000 returns 400",
			body: map[string]any{
				"OutputS3BucketName": "out-bucket",
				"OutputFormat":       "mp3",
				"Text":               strings.Repeat("a", 100001),
				"VoiceId":            "Joanna",
			},
			wantCode: http.StatusBadRequest,
			wantErr:  "TextLengthExceededException",
		},
		{
			name: "text at 100000 succeeds",
			body: map[string]any{
				"OutputS3BucketName": "out-bucket",
				"OutputFormat":       "mp3",
				"Text":               strings.Repeat("a", 100000),
				"VoiceId":            "Joanna",
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := request(t, newHandler(), http.MethodPost, "/v1/synthesisTasks", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.wantCode == http.StatusBadRequest {
				assert.Contains(t, rec.Body.String(), tc.wantErr)
			}
		})
	}
}

// TestStartSpeechSynthesisTaskOutputURIFormat verifies that the OutputUri
// field is constructed correctly: s3://<bucket>/<taskID>.<ext>. AWS sets the
// extension based on OutputFormat (ogg_vorbis → .ogg, others match format name).
func TestStartSpeechSynthesisTaskOutputURIFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		format string
		ext    string
	}{
		{name: "mp3", format: "mp3", ext: "mp3"},
		{name: "ogg_vorbis", format: "ogg_vorbis", ext: "ogg"},
		{name: "pcm", format: "pcm", ext: "pcm"},
		{name: "json", format: "json", ext: "json"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			body := map[string]any{
				"OutputS3BucketName": "my-bucket",
				"OutputFormat":       tc.format,
				"Text":               "test audio",
				"VoiceId":            "Joanna",
			}
			if tc.format == "json" {
				body["SpeechMarkTypes"] = []string{"word"}
			}

			rec := request(t, newHandler(), http.MethodPost, "/v1/synthesisTasks", body)
			require.Equal(t, http.StatusOK, rec.Code)

			task := responseMap(t, rec)["SynthesisTask"].(map[string]any)
			id := task["TaskId"].(string)
			uri := task["OutputUri"].(string)

			assert.True(t, strings.HasPrefix(uri, "s3://my-bucket/"), "OutputUri must use bucket")
			assert.True(t, strings.HasSuffix(uri, id+"."+tc.ext), "OutputUri must end with taskID.ext")
		})
	}
}

// TestListSpeechSynthesisTasksOpaqueToken verifies that the pagination token
// for ListSpeechSynthesisTasks is opaque (base64-encoded).
func TestListSpeechSynthesisTasksOpaqueToken(t *testing.T) {
	t.Parallel()

	handler := newHandler()

	for range 2 {
		startTask(t, handler, "pagination token test")
	}

	rec := request(t, handler, http.MethodGet, "/v1/synthesisTasks?MaxResults=1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	out := responseMap(t, rec)

	token, ok := out["NextToken"].(string)
	require.True(t, ok, "NextToken must be present when more results exist")
	assert.NotEmpty(t, token)

	decoded, err := base64.StdEncoding.DecodeString(token)
	require.NoError(t, err, "NextToken should be base64-encoded")
	assert.NotEmpty(t, decoded)
}

// TestListNextTokenOmittedWhenEmpty verifies the list endpoints omit
// NextToken from the response when there are no further pages (AWS omits it),
// rather than always emitting an empty NextToken key.
func TestListNextTokenOmittedWhenEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{name: "list_speech_synthesis_tasks", path: "/v1/synthesisTasks"},
		{name: "list_lexicons", path: "/v1/lexicons"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := request(t, h, http.MethodGet, tt.path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			out := responseMap(t, rec)
			_, present := out["NextToken"]
			assert.False(t, present, "NextToken must be omitted when empty")
		})
	}
}
