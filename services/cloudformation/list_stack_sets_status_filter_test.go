package cloudformation_test

import (
	"encoding/xml"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListStackSets_StatusFilter locks in ListStackSetsInput's Status member
// (cloudformation@v1.76.1 api_op_ListStackSets.go:75-76) -- the handler
// previously read only NextToken, so a Status=DELETED filter silently
// returned every (necessarily ACTIVE, since DeleteStackSet hard-deletes its
// row) StackSet instead of the empty list a real client would get back.
func TestListStackSets_StatusFilter(t *testing.T) {
	t.Parallel()

	h := newHandler()
	postFormValues(t, h, url.Values{
		"Action":       {"CreateStackSet"},
		"StackSetName": {"status-filter-ss"},
		"TemplateBody": {simpleTemplate},
	}).mustOK(t)

	type listResponse struct {
		XMLName xml.Name `xml:"ListStackSetsResponse"`
		Result  struct {
			Summaries []struct {
				StackSetName string `xml:"StackSetName"`
			} `xml:"Summaries>member"`
		} `xml:"ListStackSetsResult"`
	}

	resp := postFormValues(t, h, url.Values{
		"Action": {"ListStackSets"},
		"Status": {"DELETED"},
	})
	resp.mustOK(t)

	var out listResponse
	require.NoError(t, xml.Unmarshal([]byte(resp.Body), &out))
	assert.Empty(
		t,
		out.Result.Summaries,
		"no DELETED StackSets exist; filter must not fall back to returning everything",
	)
}
