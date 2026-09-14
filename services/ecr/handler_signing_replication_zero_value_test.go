package ecr_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecrsdk "github.com/aws/aws-sdk-go-v2/service/ecr"
	"github.com/aws/aws-sdk-go-v2/service/ecr/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSigningConfiguration_EmptyRules_RealClient drives
// PutSigningConfiguration then GetSigningConfiguration through the real SDK
// client with an empty (non-nil) rules list. types.SigningConfiguration.
// Rules is "This member is required" (ecr@v1.64.0 types/types.go:1110),
// but as a []SigningRule the client-side required check (validators.go:
// 1415-1416) only rejects nil, not an empty slice -- a conformant client
// can legitimately send Rules: []types.SigningRule{}. The pre-fix handler
// tagged Rules `omitempty`, silently dropping it instead of echoing []
// -- gopherstack-mven required-output nested-domain-struct sweep.
func TestSigningConfiguration_EmptyRules_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestECRClient(t, newTestHandler(t))
	ctx := t.Context()

	_, err := client.PutSigningConfiguration(ctx, &ecrsdk.PutSigningConfigurationInput{
		SigningConfiguration: &types.SigningConfiguration{Rules: []types.SigningRule{}},
	})
	require.NoError(t, err)

	got, err := client.GetSigningConfiguration(ctx, &ecrsdk.GetSigningConfigurationInput{})
	require.NoError(t, err)
	require.NotNil(t, got.SigningConfiguration)
	assert.NotNil(t, got.SigningConfiguration.Rules,
		"an empty (non-nil) Rules must round-trip as [], not vanish")
	assert.Empty(t, got.SigningConfiguration.Rules)
}

// TestReplicationConfiguration_EmptyRules_RealClient drives
// PutReplicationConfiguration then DescribeRegistry through the real SDK
// client with an empty (non-nil) rules list. Same required-but-nil-checked
// pattern as SigningConfiguration.Rules above (ecr@v1.64.0 types/types.go:
// 846, validators.go:1250-1251).
func TestReplicationConfiguration_EmptyRules_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestECRClient(t, newTestHandler(t))
	ctx := t.Context()

	_, err := client.PutReplicationConfiguration(ctx, &ecrsdk.PutReplicationConfigurationInput{
		ReplicationConfiguration: &types.ReplicationConfiguration{Rules: []types.ReplicationRule{}},
	})
	require.NoError(t, err)

	got, err := client.DescribeRegistry(ctx, &ecrsdk.DescribeRegistryInput{})
	require.NoError(t, err)
	require.NotNil(t, got.ReplicationConfiguration)
	assert.NotNil(t, got.ReplicationConfiguration.Rules,
		"an empty (non-nil) Rules must round-trip as [], not vanish")
	assert.Empty(t, got.ReplicationConfiguration.Rules)
}

// TestPutImageTagMutability_ExclusionFilterEmptyFilter_RealClient drives
// CreateRepository then PutImageTagMutability through the real SDK client
// with ImageTagMutabilityExclusionFilter.Filter set to a legitimate empty
// string. Filter is "This member is required" (ecr@v1.64.0 types/types.go:
// 577) but *string on the wire, so the client-side check (validators.go)
// only rejects nil.
func TestPutImageTagMutability_ExclusionFilterEmptyFilter_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestECRClient(t, newTestHandler(t))
	ctx := t.Context()

	_, err := client.CreateRepository(ctx, &ecrsdk.CreateRepositoryInput{
		RepositoryName: aws.String("filter-repo"),
	})
	require.NoError(t, err)

	out, err := client.PutImageTagMutability(ctx, &ecrsdk.PutImageTagMutabilityInput{
		RepositoryName:     aws.String("filter-repo"),
		ImageTagMutability: types.ImageTagMutabilityImmutable,
		ImageTagMutabilityExclusionFilters: []types.ImageTagMutabilityExclusionFilter{
			{Filter: aws.String(""), FilterType: types.ImageTagMutabilityExclusionFilterTypeWildcard},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.ImageTagMutabilityExclusionFilters, 1)
	require.NotNil(t, out.ImageTagMutabilityExclusionFilters[0].Filter,
		"Filter is required and must round-trip even when empty")
	assert.Empty(t, aws.ToString(out.ImageTagMutabilityExclusionFilters[0].Filter))
}
