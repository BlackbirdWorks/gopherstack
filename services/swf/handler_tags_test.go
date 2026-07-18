package swf_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHandler_Tags(t *testing.T) {
	t.Parallel()

	h := newTestSWFHandler(t)
	domain := "tag-domain"
	createSWFDomain(t, h, domain)

	arn := "arn:aws:swf:us-east-1:123456789012:/domain/" + domain

	// TagResource
	rec := doSWFRequest(t, h, "TagResource", map[string]any{
		"resourceArn": arn,
		"tags": []map[string]any{
			{"key": "env", "value": "test"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListTagsForResource
	rec = doSWFRequest(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": arn,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// UntagResource
	rec = doSWFRequest(t, h, "UntagResource", map[string]any{
		"resourceArn": arn,
		"tagKeys":     []string{"env"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}
