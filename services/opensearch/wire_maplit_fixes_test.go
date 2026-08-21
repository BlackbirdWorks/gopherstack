package opensearch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	opensearchsdk "github.com/aws/aws-sdk-go-v2/service/opensearch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

// TestDescribeDomainHealth_CountsDecodeAsStrings drives DescribeDomainHealth
// through the real opensearch client. TotalShards/DataNodeCount/
// WarmNodeCount/ActiveAvailabilityZoneCount/TotalUnAssignedShards are all
// NumberOfShards/NumberOfNodes/NumberOfAZs shapes, which deserialize as JSON
// strings (deserializers.go,
// awsRestjson1_deserializeOpDocumentDescribeDomainHealthOutput) --
// gopherstack previously emitted raw numbers there.
func TestDescribeDomainHealth_CountsDecodeAsStrings(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
	h := opensearch.NewHandler(b)
	client := newTestOpenSearchClient(t, h)

	_, err := b.CreateDomain(opensearch.CreateDomainInput{Name: "health-domain"})
	require.NoError(t, err)

	out, err := client.DescribeDomainHealth(t.Context(), &opensearchsdk.DescribeDomainHealthInput{
		DomainName: aws.String("health-domain"),
	})
	require.NoError(t, err, "real SDK client must decode DescribeDomainHealth without error")
	require.NotNil(t, out.TotalShards)
	assert.NotEmpty(t, *out.TotalShards)
	require.NotNil(t, out.DataNodeCount)
	require.NotNil(t, out.ActiveAvailabilityZoneCount)
}

// TestGetChangeProgress_TimestampsDecodeAsEpoch drives
// DescribeDomainChangeProgress through the real opensearch client.
// StartTime/LastUpdatedTime deserialize from a json.Number via
// smithytime.ParseEpochSeconds (deserializers.go,
// awsRestjson1_deserializeDocumentChangeProgressStatusDetails) --
// gopherstack previously emitted RFC3339 strings there.
func TestGetChangeProgress_TimestampsDecodeAsEpoch(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
	h := opensearch.NewHandler(b)
	client := newTestOpenSearchClient(t, h)

	_, err := b.CreateDomain(opensearch.CreateDomainInput{Name: "progress-domain"})
	require.NoError(t, err)

	out, err := client.DescribeDomainChangeProgress(
		t.Context(), &opensearchsdk.DescribeDomainChangeProgressInput{DomainName: aws.String("progress-domain")},
	)
	require.NoError(t, err, "real SDK client must decode DescribeDomainChangeProgress without error")
	require.NotNil(t, out.ChangeProgressStatus)
	assert.NotNil(t, out.ChangeProgressStatus.StartTime)
	assert.NotNil(t, out.ChangeProgressStatus.LastUpdatedTime)
}

// TestDescribeInstanceTypeLimits_MinimumCountDecodesAsNumber drives
// DescribeInstanceTypeLimits through the real opensearch client.
// MinimumInstanceCount deserializes from a json.Number
// (deserializers.go) -- gopherstack previously emitted the string "1".
func TestDescribeInstanceTypeLimits_MinimumCountDecodesAsNumber(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
	h := opensearch.NewHandler(b)
	client := newTestOpenSearchClient(t, h)

	out, err := client.DescribeInstanceTypeLimits(t.Context(), &opensearchsdk.DescribeInstanceTypeLimitsInput{
		InstanceType:  "r6g.large.search",
		EngineVersion: aws.String("OpenSearch_2.11"),
	})
	require.NoError(t, err, "real SDK client must decode DescribeInstanceTypeLimits without error")
	require.NotNil(t, out.LimitsByRole)
}
