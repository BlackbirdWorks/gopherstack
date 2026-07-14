package mediaconvert_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMediaConvert_Tags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a queue to tag
	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/queues", map[string]any{
		"name": "tag-queue",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	// The ARN for the queue
	arn := "arn:aws:mediaconvert:us-east-1:123456789012:queues/tag-queue"

	// TagResource: the real MediaConvert API sends the target ARN in the
	// JSON body (POST /2017-08-29/tags, no ARN in the URL).
	rec = doRequest(t, h, http.MethodPost, "/2017-08-29/tags", map[string]any{
		"arn": arn,
		"tags": map[string]string{
			"env": "test",
		},
	})
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	// ListTagsForResource
	rec = doRequest(t, h, http.MethodGet, "/2017-08-29/tags/"+arn, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var tagsOut map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&tagsOut))
	resourceTags, _ := tagsOut["resourceTags"].(map[string]any)
	tags, _ := resourceTags["tags"].(map[string]any)
	assert.Equal(t, "test", tags["env"])

	// UntagResource: the real MediaConvert API uses PUT (not DELETE) with
	// tagKeys carried in the JSON body (not a query parameter).
	rec = doRequest(t, h, http.MethodPut, "/2017-08-29/tags/"+arn, map[string]any{
		"tagKeys": []string{"env"},
	})
	assert.Equal(t, http.StatusNoContent, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/2017-08-29/tags/"+arn, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	tagsOut = map[string]any{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&tagsOut))
	resourceTags, _ = tagsOut["resourceTags"].(map[string]any)
	assert.Empty(t, resourceTags["tags"])
}

func TestMediaConvert_ListJobsWithFilters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// ListJobs with query params (exercises StartJobsQuery/jobMatchesFilters)
	rec := doRequest(t, h, http.MethodGet, "/2017-08-29/jobs?status=SUBMITTED&maxResults=10", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}
