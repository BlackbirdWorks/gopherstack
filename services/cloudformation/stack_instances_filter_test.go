package cloudformation_test

import (
	"encoding/xml"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListStackInstances_AccountAndRegionFilter locks in
// ListStackInstancesInput's StackInstanceAccount/StackInstanceRegion
// members (cloudformation@v1.76.1 api_op_ListStackInstances.go) -- the
// handler previously read only StackSetName and NextToken, so a real
// client's account/region filter never reached the backend and every call
// returned every instance in the StackSet regardless of what was asked for.
func TestListStackInstances_AccountAndRegionFilter(t *testing.T) {
	t.Parallel()

	h := newHandler()
	postFormValues(t, h, url.Values{
		"Action":       {"CreateStackSet"},
		"StackSetName": {"filter-instances-ss"},
		"TemplateBody": {simpleTemplate},
	}).mustOK(t)
	postFormValues(t, h, url.Values{
		"Action":            {"CreateStackInstances"},
		"StackSetName":      {"filter-instances-ss"},
		"Accounts.member.1": {"111111111111"},
		"Accounts.member.2": {"222222222222"},
		"Regions.member.1":  {"us-east-1"},
	}).mustOK(t)

	type instanceXML struct {
		Account string `xml:"Account"`
		Region  string `xml:"Region"`
	}
	type listResponse struct {
		XMLName xml.Name `xml:"ListStackInstancesResponse"`
		Result  struct {
			Summaries []instanceXML `xml:"Summaries>member"`
		} `xml:"ListStackInstancesResult"`
	}

	resp := postFormValues(t, h, url.Values{
		"Action":               {"ListStackInstances"},
		"StackSetName":         {"filter-instances-ss"},
		"StackInstanceAccount": {"111111111111"},
	})
	resp.mustOK(t)

	var out listResponse
	require.NoError(t, xml.Unmarshal([]byte(resp.Body), &out))
	require.Len(t, out.Result.Summaries, 1)
	assert.Equal(t, "111111111111", out.Result.Summaries[0].Account)
}
