package polly_test

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/polly"
)

func TestTagResourceNotFound(t *testing.T) {
	t.Parallel()

	backend := polly.NewInMemoryBackend()
	unknown := "arn:aws:polly:us-east-1:123456789012:synthesis-task/nope"
	tags := []polly.Tag{{Key: "k", Value: "v"}}

	err := backend.TagResource(unknown, tags)
	require.ErrorIs(t, err, polly.ErrResourceNotFound)

	err = backend.UntagResource(unknown, []string{"k"})
	require.ErrorIs(t, err, polly.ErrResourceNotFound)

	_, err = backend.ListTagsForResource(unknown)
	require.ErrorIs(t, err, polly.ErrResourceNotFound)
}

func TestTagValidationLimits(t *testing.T) {
	t.Parallel()

	backend := polly.NewInMemoryBackend()
	task, err := backend.StartSpeechSynthesisTask(
		polly.SynthesisOptions{Text: "hello", VoiceID: "Joanna"},
		"bucket", "", "",
	)
	require.NoError(t, err)
	arn := backend.TaskARN(task.TaskID)

	tests := []struct {
		name string
		tags []polly.Tag
	}{
		{name: "key_too_long", tags: []polly.Tag{{Key: strings.Repeat("k", 129), Value: "v"}}},
		{name: "value_too_long", tags: []polly.Tag{{Key: "k", Value: strings.Repeat("v", 257)}}},
		{name: "duplicate_key", tags: []polly.Tag{{Key: "k", Value: "v1"}, {Key: "k", Value: "v2"}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			tagErr := backend.TagResource(arn, test.tags)
			require.Error(t, tagErr)
			assert.ErrorIs(t, tagErr, polly.ErrValidation)
		})
	}
}

func TestTagCountLimit(t *testing.T) {
	t.Parallel()

	backend := polly.NewInMemoryBackend()
	task, err := backend.StartSpeechSynthesisTask(
		polly.SynthesisOptions{Text: "hello", VoiceID: "Joanna"},
		"bucket", "", "",
	)
	require.NoError(t, err)
	arn := backend.TaskARN(task.TaskID)

	bulk := make([]polly.Tag, 50)
	for i := range bulk {
		bulk[i] = polly.Tag{Key: strings.Repeat("k", i+1), Value: "v"}
	}
	require.NoError(t, backend.TagResource(arn, bulk))

	err = backend.TagResource(arn, []polly.Tag{{Key: "extra", Value: "v"}})
	require.Error(t, err)
	assert.ErrorIs(t, err, polly.ErrValidation)
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
		rec := request(t, handler, test.method, test.target, test.body)
		require.Equal(t, http.StatusOK, rec.Code, test.name)
		for _, find := range test.find {
			assert.Contains(t, rec.Body.String(), find, test.name)
		}
		if strings.Contains(test.name, "removed") {
			assert.NotContains(t, rec.Body.String(), `"env"`, test.name)
		}
	}

	missing := request(
		t,
		handler,
		http.MethodGet,
		"/v1/tags/"+url.PathEscape("arn:aws:polly:us-east-1:000000000000:missing"),
		nil,
	)
	assert.Equal(t, http.StatusNotFound, missing.Code)
	assert.Contains(t, missing.Body.String(), "ResourceNotFoundException")
}

// TestTagResourceValidation verifies that TagResource enforces AWS tag
// constraints: duplicate keys rejected, key exceeding 128 chars rejected,
// value exceeding 256 chars rejected.
func TestTagResourceValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tags     []map[string]string
		wantCode int
	}{
		{
			name:     "valid tags accepted",
			tags:     []map[string]string{{"Key": "env", "Value": "prod"}},
			wantCode: http.StatusOK,
		},
		{
			name: "duplicate key rejected",
			tags: []map[string]string{
				{"Key": "env", "Value": "prod"},
				{"Key": "env", "Value": "dev"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "key over 128 chars rejected",
			tags:     []map[string]string{{"Key": strings.Repeat("k", 129), "Value": "v"}},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "value over 256 chars rejected",
			tags:     []map[string]string{{"Key": "k", "Value": strings.Repeat("v", 257)}},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty key rejected",
			tags:     []map[string]string{{"Key": "", "Value": "v"}},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			handler := newHandler()
			id, _ := startTask(t, handler, "tag validation test")
			taskARN := handler.Backend.TaskARN(id)

			tags := make([]map[string]string, len(tc.tags))
			copy(tags, tc.tags)

			rec := request(t, handler, http.MethodPost, "/v1/tags/"+taskARN,
				map[string]any{"Tags": tags})
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.wantCode == http.StatusBadRequest {
				assert.Contains(t, rec.Body.String(), "InvalidParameterValueException")
			}
		})
	}
}

// TestTagResourceCountLimit verifies that TagResource rejects a batch that
// would push the resource over the 50-tag limit. AWS returns
// InvalidParameterValueException when the limit would be exceeded.
func TestTagResourceCountLimit(t *testing.T) {
	t.Parallel()

	handler := newHandler()
	id, _ := startTask(t, handler, "tag count limit test")
	taskARN := handler.Backend.TaskARN(id)

	batch1 := make([]map[string]string, 50)
	for i := range 50 {
		batch1[i] = map[string]string{"Key": fmt.Sprintf("key%d", i), "Value": "v"}
	}
	rec := request(t, handler, http.MethodPost, "/v1/tags/"+taskARN, map[string]any{"Tags": batch1})
	require.Equal(t, http.StatusOK, rec.Code, "50 tags should be accepted")

	overflow := []map[string]string{{"Key": "overflow", "Value": "x"}}
	rec = request(t, handler, http.MethodPost, "/v1/tags/"+taskARN, map[string]any{"Tags": overflow})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterValueException")
}
