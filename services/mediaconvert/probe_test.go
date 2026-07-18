package mediaconvert_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProbe_SingleInputFile(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"inputFiles": []any{
			map[string]any{"fileInput": "s3://my-bucket/input.mp4"},
		},
	}

	resp, code := parseJSONResponse(t, h, http.MethodPost, "/2017-08-29/probe", body)
	assert.Equal(t, http.StatusOK, code)

	results, _ := resp["probeResults"].([]any)
	assert.Len(t, results, 1, "one probeResult per input file")
}

func TestProbe_MultipleInputFiles(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"inputFiles": []any{
			map[string]any{"fileInput": "s3://bucket/a.mp4"},
			map[string]any{"fileInput": "s3://bucket/b.mp4"},
		},
	}

	resp, code := parseJSONResponse(t, h, http.MethodPost, "/2017-08-29/probe", body)
	assert.Equal(t, http.StatusOK, code)

	results, _ := resp["probeResults"].([]any)
	assert.Len(t, results, 2)
}

func TestProbe_EmptyInputFiles(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"inputFiles": []any{},
	}

	resp, code := parseJSONResponse(t, h, http.MethodPost, "/2017-08-29/probe", body)
	assert.Equal(t, http.StatusOK, code)

	results, _ := resp["probeResults"].([]any)
	assert.Empty(t, results)
}

func TestProbe_NoInputFilesField(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	resp, code := parseJSONResponse(t, h, http.MethodPost, "/2017-08-29/probe", map[string]any{})
	assert.Equal(t, http.StatusOK, code)

	results, _ := resp["probeResults"].([]any)
	assert.Empty(t, results)
}

func TestProbe_ResultContainsContainer(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"inputFiles": []any{
			map[string]any{"fileInput": "s3://bucket/video.mp4"},
		},
	}

	resp, code := parseJSONResponse(t, h, http.MethodPost, "/2017-08-29/probe", body)
	require.Equal(t, http.StatusOK, code)

	results, _ := resp["probeResults"].([]any)
	require.Len(t, results, 1)

	entry, _ := results[0].(map[string]any)
	probeResult, _ := entry["probeResult"].(map[string]any)
	container, _ := probeResult["container"].(map[string]any)
	assert.NotEmpty(t, container, "probeResult must include a container field")
}
