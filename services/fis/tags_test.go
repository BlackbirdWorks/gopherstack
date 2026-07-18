package fis_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fis"
)

func TestFISHandler_TagResource_ListTags_UntagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a template to get its ARN.
	body := map[string]any{
		"roleArn":        "arn:aws:iam::000000000000:role/FISRole",
		"stopConditions": []map[string]any{{"source": "none"}},
		"targets":        map[string]any{},
		"actions":        map[string]any{},
	}

	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp struct {
		ExperimentTemplate struct {
			ID  string `json:"id"`
			Arn string `json:"arn"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &createResp)
	arnStr := createResp.ExperimentTemplate.Arn

	// TagResource.
	tagPath := "/tags/" + arnStr
	tagBody := map[string]any{"tags": map[string]string{"env": "prod", "owner": "team"}}

	rec2 := doRequest(t, h, http.MethodPost, tagPath, tagBody)
	assert.Equal(t, http.StatusNoContent, rec2.Code)

	// ListTagsForResource.
	rec3 := doRequest(t, h, http.MethodGet, tagPath, nil)
	assert.Equal(t, http.StatusOK, rec3.Code)

	var tagsResp struct {
		Tags map[string]string `json:"tags"`
	}

	mustJSON(t, rec3, &tagsResp)
	assert.Equal(t, "prod", tagsResp.Tags["env"])
	assert.Equal(t, "team", tagsResp.Tags["owner"])

	// UntagResource.
	rec4 := doRequest(t, h, http.MethodDelete, tagPath+"?tagKeys=env", nil)
	assert.Equal(t, http.StatusNoContent, rec4.Code)

	// Verify tag removed.
	rec5 := doRequest(t, h, http.MethodGet, tagPath, nil)
	var tagsResp2 struct {
		Tags map[string]string `json:"tags"`
	}

	mustJSON(t, rec5, &tagsResp2)
	assert.NotContains(t, tagsResp2.Tags, "env")
	assert.Equal(t, "team", tagsResp2.Tags["owner"])
}

// ----------------------------------------
// Invalid request tests
// ----------------------------------------

func TestFISHandler_TagResource_InvalidJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	e := echo.New()
	req := httptest.NewRequest(
		http.MethodPost,
		"/tags/arn:aws:fis:us-east-1:000:experiment-template/EXTabc",
		bytes.NewReader([]byte("not-json")),
	)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ----------------------------------------
// List actions with action providers
// ----------------------------------------

func TestFISHandler_TagExperiment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	templateID := createTestTemplate(t, h)

	// Start experiment to get ARN.
	rec := doRequest(t, h, http.MethodPost, "/experiments", map[string]any{
		"experimentTemplateId": templateID,
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var expResp struct {
		Experiment struct {
			ID  string `json:"id"`
			Arn string `json:"arn"`
		} `json:"experiment"`
	}

	mustJSON(t, rec, &expResp)
	arnStr := expResp.Experiment.Arn
	require.NotEmpty(t, arnStr)

	// TagResource on experiment.
	tagPath := "/tags/" + arnStr
	rec2 := doRequest(t, h, http.MethodPost, tagPath, map[string]any{
		"tags": map[string]string{"phase": "test"},
	})
	assert.Equal(t, http.StatusNoContent, rec2.Code)

	// ListTagsForResource on experiment.
	rec3 := doRequest(t, h, http.MethodGet, tagPath, nil)
	assert.Equal(t, http.StatusOK, rec3.Code)

	var tagsResp struct {
		Tags map[string]string `json:"tags"`
	}

	mustJSON(t, rec3, &tagsResp)
	assert.Equal(t, "test", tagsResp.Tags["phase"])

	// UntagResource on experiment.
	rec4 := doRequest(t, h, http.MethodDelete, tagPath+"?tagKeys=phase", nil)
	assert.Equal(t, http.StatusNoContent, rec4.Code)
}

// ----------------------------------------
// Provider tests
// ----------------------------------------

func TestFISHandler_TagResource_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Tagging a non-existent ARN should return 404.
	rec := doRequest(
		t,
		h,
		http.MethodPost,
		"/tags/arn:aws:fis:us-east-1:000:experiment-template/EXTdoesnotexist",
		map[string]any{
			"tags": map[string]string{"key": "val"},
		},
	)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestFISHandler_UntagResource_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(
		t,
		h,
		http.MethodDelete,
		"/tags/arn:aws:fis:us-east-1:000:experiment-template/EXTdoesnotexist?tagKeys=key",
		nil,
	)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestFISHandler_ListTags_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/tags/arn:aws:fis:us-east-1:000:experiment-template/EXTdoesnotexist", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ----------------------------------------
// Experiment completes when no timed actions (maxDuration == 0)
// ----------------------------------------

func TestTagResource_TooManyTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	// Get the ARN of the template.
	rec := doRequest(t, h, http.MethodGet, "/experimentTemplates/"+tplID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var tplResp struct {
		ExperimentTemplate struct {
			Arn string `json:"arn"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &tplResp)
	arnStr := tplResp.ExperimentTemplate.Arn

	// Build 50 unique tag keys.
	tags50 := make(map[string]string)
	letters := "abcdefghijklmnopqrstuvwxyz"

	for i := range 50 {
		key := string([]byte{letters[i/26], letters[i%26]})
		tags50[key] = "v"
	}

	rec2 := doRequest(t, h, http.MethodPost, "/tags/"+arnStr, map[string]any{"tags": tags50})
	assert.Equal(t, http.StatusNoContent, rec2.Code)

	// Adding one more tag should fail with TooManyTagsException.
	rec3 := doRequest(t, h, http.MethodPost, "/tags/"+arnStr, map[string]any{
		"tags": map[string]string{"overflow": "yes"},
	})
	assert.Equal(t, http.StatusBadRequest, rec3.Code)
}

func TestTagResource_InvalidKeyPrefix(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	rec := doRequest(t, h, http.MethodGet, "/experimentTemplates/"+tplID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var tplResp struct {
		ExperimentTemplate struct {
			Arn string `json:"arn"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &tplResp)

	rec2 := doRequest(t, h, http.MethodPost, "/tags/"+tplResp.ExperimentTemplate.Arn, map[string]any{
		"tags": map[string]string{"aws:reserved": "value"},
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// ----------------------------------------
// Issue #19 — ListExperiments pagination
// ----------------------------------------

func TestListTagsForResource_NonNilWhenEmpty(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()
	b.AddTemplateInternal(&fis.ExperimentTemplate{
		ID:   "EXT-tagsnone",
		Arn:  "arn:aws:fis:us-east-1:000000000000:experiment-template/EXT-tagsnone",
		Tags: map[string]string{},
	})

	tags, err := b.ListTagsForResource("arn:aws:fis:us-east-1:000000000000:experiment-template/EXT-tagsnone")
	require.NoError(t, err)
	assert.NotNil(t, tags)
	assert.Empty(t, tags)
}

// ----------------------------------------
// Safety lever preserved across persistence
// ----------------------------------------
