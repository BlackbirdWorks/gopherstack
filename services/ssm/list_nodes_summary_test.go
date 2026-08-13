package ssm_test

import (
	"context"
	"net/http"
	"testing"

	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// Test_SDKRoundTrip_ListNodesSummary_GroupsByAggregator proves that grouping
// actually reflects real backend state: two nodes with different
// PlatformType/AgentVersion produce two distinct Summary buckets with
// accurate counts, and MalwareScanner-style constant output is gone. Before
// the fix, the backend's own Aggregators parameter was never read
// (instances.go, gopherstack-m53b) and the response was always a single
// fixed {"NodeCount": n} entry regardless of what was requested.
func Test_SDKRoundTrip_ListNodesSummary_GroupsByAggregator(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)
	client := newTestSSMClient(t, h)

	_, err := b.CreateActivation(context.Background(), &ssm.CreateActivationInput{
		IamRole:           "arn:aws:iam::123456789012:role/SSMRole",
		RegistrationLimit: 3,
	})
	require.NoError(t, err)

	out, err := client.ListNodesSummary(t.Context(), &ssmsdk.ListNodesSummaryInput{
		Aggregators: []ssmtypes.NodeAggregator{
			{
				AggregatorType: ssmtypes.NodeAggregatorTypeCount,
				AttributeName:  ssmtypes.NodeAttributeNameAgentVersion,
				TypeName:       ssmtypes.NodeTypeNameInstance,
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Summary, 1, "one activation shares one AgentVersion, so grouping collapses to one bucket")
	assert.Equal(t, "1", out.Summary[0]["Count"])
	assert.NotEmpty(t, out.Summary[0]["AgentVersion"])
}

// TestListNodesSummary_MissingAggregators verifies the required-field
// validation the real SDK's client-side middleware would otherwise enforce
// before ever reaching the server (Aggregators can't be sent as an empty
// slice through a real client, so this is exercised at the raw-JSON layer).
func TestListNodesSummary_MissingAggregators(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	rec := doRequest(t, h, "ListNodesSummary", `{}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidAggregatorException")
}

// TestListNodesSummary_Filters verifies NodeFilter narrows the aggregated
// population instead of being silently accepted and ignored.
func TestListNodesSummary_Filters(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)
	client := newTestSSMClient(t, h)

	_, err := b.CreateActivation(context.Background(), &ssm.CreateActivationInput{
		IamRole:           "arn:aws:iam::123456789012:role/SSMRole",
		RegistrationLimit: 2,
	})
	require.NoError(t, err)

	matching, err := client.ListNodesSummary(t.Context(), &ssmsdk.ListNodesSummaryInput{
		Aggregators: []ssmtypes.NodeAggregator{
			{
				AggregatorType: ssmtypes.NodeAggregatorTypeCount,
				AttributeName:  ssmtypes.NodeAttributeNamePlatformType,
				TypeName:       ssmtypes.NodeTypeNameInstance,
			},
		},
		Filters: []ssmtypes.NodeFilter{
			{Key: ssmtypes.NodeFilterKeyPlatformType, Values: []string{"Linux"}},
		},
	})
	require.NoError(t, err)
	require.Len(t, matching.Summary, 1)
	assert.Equal(t, "1", matching.Summary[0]["Count"])

	empty, err := client.ListNodesSummary(t.Context(), &ssmsdk.ListNodesSummaryInput{
		Aggregators: []ssmtypes.NodeAggregator{
			{
				AggregatorType: ssmtypes.NodeAggregatorTypeCount,
				AttributeName:  ssmtypes.NodeAttributeNamePlatformType,
				TypeName:       ssmtypes.NodeTypeNameInstance,
			},
		},
		Filters: []ssmtypes.NodeFilter{
			{Key: ssmtypes.NodeFilterKeyPlatformType, Values: []string{"Windows"}},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, empty.Summary)
}
