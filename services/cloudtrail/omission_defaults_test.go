package cloudtrail_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPutEventSelectors_Defaults locks in two documented defaults on a basic
// EventSelector (types.EventSelector, own doc comments):
//   - IncludeManagementEvents: "By default, the value is true." -- the real
//     SDK type is *bool, so an omitted key must resolve to true, not the Go
//     zero value false.
//   - ReadWriteType: "By default, the value is All."
//
// A client that supplies only DataResources (the common case for a
// data-event-only selector) and omits both fields must get these defaults
// echoed back by GetEventSelectors, not an inverted/empty value.
func TestPutEventSelectors_Defaults(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	doCloudTrailOp(t, h, "CreateTrail", map[string]any{
		"Name":         "defaults-trail",
		"S3BucketName": "my-bucket",
	})

	rec := doCloudTrailOp(t, h, "PutEventSelectors", map[string]any{
		"TrailName": "defaults-trail",
		"EventSelectors": []map[string]any{
			{
				"DataResources": []map[string]any{
					{"Type": "AWS::S3::Object", "Values": []string{"arn:aws:s3:::my-bucket/"}},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	resp := parseCloudTrailResp(t, rec)
	selectors, ok := resp["EventSelectors"].([]any)
	require.True(t, ok)
	require.Len(t, selectors, 1)

	sel, ok := selectors[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, true, sel["IncludeManagementEvents"], "IncludeManagementEvents defaults to true when omitted")
	assert.Equal(t, "All", sel["ReadWriteType"], "ReadWriteType defaults to All when omitted")

	// GetEventSelectors must echo the same resolved defaults, not the raw request.
	getRec := doCloudTrailOp(t, h, "GetEventSelectors", map[string]any{"TrailName": "defaults-trail"})
	require.Equal(t, http.StatusOK, getRec.Code)
	getResp := parseCloudTrailResp(t, getRec)
	getSelectors, ok := getResp["EventSelectors"].([]any)
	require.True(t, ok)
	require.Len(t, getSelectors, 1)
	getSel, ok := getSelectors[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, true, getSel["IncludeManagementEvents"])
	assert.Equal(t, "All", getSel["ReadWriteType"])
}

// TestPutEventSelectors_ExplicitFalseSurvives proves an explicit
// IncludeManagementEvents:false is honored, not overwritten by the default
// -- the fix must distinguish "omitted" from "explicitly false".
func TestPutEventSelectors_ExplicitFalseSurvives(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	doCloudTrailOp(t, h, "CreateTrail", map[string]any{
		"Name":         "explicit-false-trail",
		"S3BucketName": "my-bucket",
	})

	rec := doCloudTrailOp(t, h, "PutEventSelectors", map[string]any{
		"TrailName": "explicit-false-trail",
		"EventSelectors": []map[string]any{
			{
				"IncludeManagementEvents": false,
				"ReadWriteType":           "ReadOnly",
				"DataResources":           []any{},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	resp := parseCloudTrailResp(t, rec)
	selectors, ok := resp["EventSelectors"].([]any)
	require.True(t, ok)
	require.Len(t, selectors, 1)
	sel, ok := selectors[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, false, sel["IncludeManagementEvents"])
	assert.Equal(t, "ReadOnly", sel["ReadWriteType"])
}
