package cognitoidentity_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidentitysdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentity"
	sdktypes "github.com/aws/aws-sdk-go-v2/service/cognitoidentity/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLookupDeveloperIdentity_CrossPoolIdentityID_RealClient proves
// LookupDeveloperIdentity must scope IdentityId to the given IdentityPoolId,
// matching UnlinkDeveloperIdentity's own identity.IdentityPoolID != poolID
// check a few dozen lines below it in identities.go. Without the check, any
// caller who knows another pool's identity ID and supplies their own,
// unrelated pool ID can read that identity's developer user identifiers --
// a cross-principal information disclosure.
func TestLookupDeveloperIdentity_CrossPoolIdentityID_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	poolA, err := client.CreateIdentityPool(ctx, &cognitoidentitysdk.CreateIdentityPoolInput{
		IdentityPoolName:               aws.String("cross-pool-a"),
		AllowUnauthenticatedIdentities: true,
	})
	require.NoError(t, err)

	poolB, err := client.CreateIdentityPool(ctx, &cognitoidentitysdk.CreateIdentityPoolInput{
		IdentityPoolName:               aws.String("cross-pool-b"),
		AllowUnauthenticatedIdentities: true,
	})
	require.NoError(t, err)

	token, err := client.GetOpenIdTokenForDeveloperIdentity(
		ctx, &cognitoidentitysdk.GetOpenIdTokenForDeveloperIdentityInput{
			IdentityPoolId: poolA.IdentityPoolId,
			Logins:         map[string]string{"developer.example.com": "victim-user-001"},
		},
	)
	require.NoError(t, err)

	_, err = client.LookupDeveloperIdentity(ctx, &cognitoidentitysdk.LookupDeveloperIdentityInput{
		IdentityPoolId: poolB.IdentityPoolId,
		IdentityId:     token.IdentityId,
	})

	require.Error(t, err, "identity %q belongs to pool %q, not %q; "+
		"LookupDeveloperIdentity must refuse it instead of leaking the developer user identifier",
		aws.ToString(token.IdentityId), aws.ToString(poolA.IdentityPoolId), aws.ToString(poolB.IdentityPoolId))

	var nf *sdktypes.ResourceNotFoundException
	assert.ErrorAs(t, err, &nf, "want ResourceNotFoundException, got %T: %v", err, err)
}
