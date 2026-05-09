package mediaconvert_test

import (
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

	// TagResource
	rec = doRequest(t, h, http.MethodPost, "/2017-08-29/tags/"+arn, map[string]any{
		"tags": map[string]string{
			"env": "test",
		},
	})
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	// ListTagsForResource
	rec = doRequest(t, h, http.MethodGet, "/2017-08-29/tags/"+arn, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// UntagResource
	rec = doRequest(t, h, http.MethodDelete, "/2017-08-29/tags/"+arn+"?tagKeys=env", nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestMediaConvert_ListJobsWithFilters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// ListJobs with query params (exercises StartJobsQuery/jobMatchesFilters)
	rec := doRequest(t, h, http.MethodGet, "/2017-08-29/jobs?status=SUBMITTED&maxResults=10", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}
