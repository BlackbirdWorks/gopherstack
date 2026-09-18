package opensearch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	opensearchsdk "github.com/aws/aws-sdk-go-v2/service/opensearch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

// TestUpdateDomainConfig_PreservesOmittedAccessPolicies proves the
// zeroguard fix (cmd/zeroguard): UpdateDomainConfigInput.AccessPolicies is a
// plain string in the real SDK's request type. Before the fix this backend
// decoded it as string guarded by `!= ""`, so an omitted UpdateDomainConfig
// call and an explicit empty-string AccessPolicies (which real AWS allows,
// to grant the domain's default account-only access) were indistinguishable
// and a client could never observe an omitted call preserve the prior value.
func TestUpdateDomainConfig_PreservesOmittedAccessPolicies(t *testing.T) {
	t.Parallel()

	backend := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestOpenSearchClient(t, opensearch.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateDomain(ctx, &opensearchsdk.CreateDomainInput{DomainName: aws.String("uds-dom")})
	require.NoError(t, err)

	policy := `{"Version":"2012-10-17","Statement":[]}`

	_, err = client.UpdateDomainConfig(ctx, &opensearchsdk.UpdateDomainConfigInput{
		DomainName:     aws.String("uds-dom"),
		AccessPolicies: aws.String(policy),
	})
	require.NoError(t, err)

	desc, err := client.DescribeDomain(ctx, &opensearchsdk.DescribeDomainInput{DomainName: aws.String("uds-dom")})
	require.NoError(t, err)
	assert.JSONEq(t, policy, aws.ToString(desc.DomainStatus.AccessPolicies))

	// Omitted AccessPolicies must preserve the prior value.
	_, err = client.UpdateDomainConfig(ctx, &opensearchsdk.UpdateDomainConfigInput{
		DomainName: aws.String("uds-dom"),
	})
	require.NoError(t, err)

	desc, err = client.DescribeDomain(ctx, &opensearchsdk.DescribeDomainInput{DomainName: aws.String("uds-dom")})
	require.NoError(t, err)
	assert.JSONEq(t, policy, aws.ToString(desc.DomainStatus.AccessPolicies), "omitted AccessPolicies must survive")

	// Explicit empty string clears it.
	_, err = client.UpdateDomainConfig(ctx, &opensearchsdk.UpdateDomainConfigInput{
		DomainName:     aws.String("uds-dom"),
		AccessPolicies: aws.String(""),
	})
	require.NoError(t, err)

	desc, err = client.DescribeDomain(ctx, &opensearchsdk.DescribeDomainInput{DomainName: aws.String("uds-dom")})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(desc.DomainStatus.AccessPolicies))
}
