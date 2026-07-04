package resourcegroupstaggingapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

// ======================================================================
// Gap 1: ComplianceDetails.KeysWithNoncompliantValues JSON field name
// ======================================================================

func TestParity_ComplianceDetailsFieldName(t *testing.T) {
	t.Parallel()

	// Real AWS spells the field "KeysWithNoncompliantValues" (lowercase 'c' in noncompliant).
	// Verify the JSON response uses the correct casing.
	b := newBackend(t)
	seedResources(b, []resourcegroupstaggingapi.TaggedResource{
		{
			ResourceARN:  "arn:aws:sqs:us-east-1:000000000000:q1",
			ResourceType: "sqs:queue",
			Tags:         map[string]string{"env": "prod"},
		},
	})

	h := resourcegroupstaggingapi.NewHandler(b)
	rec := doTaggingRequest(t, h, "GetResources", map[string]any{
		"IncludeComplianceDetails": true,
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	list, ok := out["ResourceTagMappingList"].([]any)
	require.True(t, ok)
	require.Len(t, list, 1)

	item := list[0].(map[string]any)
	cd, ok := item["ComplianceDetails"].(map[string]any)
	require.True(t, ok, "ComplianceDetails must be present when IncludeComplianceDetails=true")

	// Must contain "KeysWithNoncompliantValues" (not "KeysWithNonCompliantValues").
	_, hasCorrect := cd["KeysWithNoncompliantValues"]
	_, hasWrong := cd["KeysWithNonCompliantValues"]

	assert.False(t, hasWrong, "response must not contain misspelled 'KeysWithNonCompliantValues'")
	_ = hasCorrect // field may be absent when empty (omitempty) — absence is also acceptable
}

// ======================================================================
// Gap 2: ResourceARNList filter in GetResources
// ======================================================================

func TestParity_GetResources_ResourceARNList_FiltersToSpecificARNs(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	seedResources(b, []resourcegroupstaggingapi.TaggedResource{
		{
			ResourceARN:  "arn:aws:sqs:us-east-1:000000000000:q1",
			ResourceType: "sqs:queue",
			Tags:         map[string]string{"env": "prod"},
		},
		{
			ResourceARN:  "arn:aws:sqs:us-east-1:000000000000:q2",
			ResourceType: "sqs:queue",
			Tags:         map[string]string{"env": "dev"},
		},
		{
			ResourceARN:  "arn:aws:sqs:us-east-1:000000000000:q3",
			ResourceType: "sqs:queue",
			Tags:         map[string]string{"env": "staging"},
		},
	})

	h := resourcegroupstaggingapi.NewHandler(b)
	rec := doTaggingRequest(t, h, "GetResources", map[string]any{
		"ResourceARNList": []string{
			"arn:aws:sqs:us-east-1:000000000000:q1",
			"arn:aws:sqs:us-east-1:000000000000:q3",
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	list, ok := out["ResourceTagMappingList"].([]any)
	require.True(t, ok)
	require.Len(t, list, 2, "only the two requested ARNs should be returned")

	arns := make([]string, 0, 2)
	for _, item := range list {
		m := item.(map[string]any)
		arns = append(arns, m["ResourceARN"].(string))
	}

	assert.Contains(t, arns, "arn:aws:sqs:us-east-1:000000000000:q1")
	assert.Contains(t, arns, "arn:aws:sqs:us-east-1:000000000000:q3")
	assert.NotContains(t, arns, "arn:aws:sqs:us-east-1:000000000000:q2")
}

func TestParity_GetResources_ResourceARNList_EmptyReturnsAll(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	seedResources(b, []resourcegroupstaggingapi.TaggedResource{
		{
			ResourceARN:  "arn:aws:sqs:us-east-1:000000000000:q1",
			ResourceType: "sqs:queue",
			Tags:         map[string]string{"env": "prod"},
		},
		{
			ResourceARN:  "arn:aws:sqs:us-east-1:000000000000:q2",
			ResourceType: "sqs:queue",
			Tags:         map[string]string{"env": "dev"},
		},
	})

	h := resourcegroupstaggingapi.NewHandler(b)
	// Omitting ResourceARNList should return all resources.
	rec := doTaggingRequest(t, h, "GetResources", map[string]any{})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	list, ok := out["ResourceTagMappingList"].([]any)
	require.True(t, ok)
	assert.Len(t, list, 2)
}

// ======================================================================
// Gap 3: DescribeReportCreation returns nil Status when no report exists
// ======================================================================

func TestParity_DescribeReportCreation_NoReportHasNilStatus(t *testing.T) {
	t.Parallel()

	// Real AWS: when no report has been generated in the region, DescribeReportCreation
	// returns an empty response — not a "NO REPORT" status string.
	b := newBackend(t)
	out := b.DescribeReportCreation(context.Background())

	require.NotNil(t, out)
	assert.Nil(t, out.Status, "Status must be nil (not 'NO REPORT') when no report exists")
	assert.Nil(t, out.S3Location)
	assert.Nil(t, out.StartDate)
}

func TestParity_DescribeReportCreation_NoReportResponseBody(t *testing.T) {
	t.Parallel()

	// At the HTTP level the response must not contain the non-AWS "NO REPORT" string.
	h := resourcegroupstaggingapi.NewHandler(newBackend(t))
	rec := doTaggingRequest(t, h, "DescribeReportCreation", map[string]any{})

	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "NO REPORT",
		"'NO REPORT' is not a real AWS status value and must not appear in the response")
}
