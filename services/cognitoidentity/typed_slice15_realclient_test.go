package cognitoidentity_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidentitysdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTypedSlice15RealClient drives cognitoidentity's typed-coverage-blind
// ops (gopherstack-n3zi slice 15) through the real aws-sdk-go-v2 client.
func TestTypedSlice15RealClient(t *testing.T) {
	t.Parallel()

	t.Run("tags", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)
		ctx := t.Context()

		pool, err := client.CreateIdentityPool(ctx, &cognitoidentitysdk.CreateIdentityPoolInput{
			IdentityPoolName:               aws.String("s15-tags-pool"),
			AllowUnauthenticatedIdentities: true,
			IdentityPoolTags:               map[string]string{"env": "test"},
		})
		require.NoError(t, err)
		poolARN := "arn:aws:cognito-identity:us-east-1:000000000000:identitypool/" + aws.ToString(pool.IdentityPoolId)

		_, err = client.TagResource(ctx, &cognitoidentitysdk.TagResourceInput{
			ResourceArn: aws.String(poolARN),
			Tags:        map[string]string{"team": "platform"},
		})
		require.NoError(t, err)

		listOut, err := client.ListTagsForResource(ctx, &cognitoidentitysdk.ListTagsForResourceInput{
			ResourceArn: aws.String(poolARN),
		})
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"env": "test", "team": "platform"}, listOut.Tags)

		_, err = client.UntagResource(ctx, &cognitoidentitysdk.UntagResourceInput{
			ResourceArn: aws.String(poolARN),
			TagKeys:     []string{"team"},
		})
		require.NoError(t, err)

		afterOut, err := client.ListTagsForResource(ctx, &cognitoidentitysdk.ListTagsForResourceInput{
			ResourceArn: aws.String(poolARN),
		})
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"env": "test"}, afterOut.Tags)
	})

	t.Run("principal tag attribute map", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)
		ctx := t.Context()

		pool, err := client.CreateIdentityPool(ctx, &cognitoidentitysdk.CreateIdentityPoolInput{
			IdentityPoolName:               aws.String("s15-tagmap-pool"),
			AllowUnauthenticatedIdentities: true,
		})
		require.NoError(t, err)
		poolID := pool.IdentityPoolId

		defaultOut, err := client.GetPrincipalTagAttributeMap(ctx, &cognitoidentitysdk.GetPrincipalTagAttributeMapInput{
			IdentityPoolId:       poolID,
			IdentityProviderName: aws.String("cognito-idp.us-east-1.amazonaws.com/us-east-1_s15Pool"),
		})
		require.NoError(t, err)
		assert.True(t, aws.ToBool(defaultOut.UseDefaults))

		setOut, err := client.SetPrincipalTagAttributeMap(ctx, &cognitoidentitysdk.SetPrincipalTagAttributeMapInput{
			IdentityPoolId:       poolID,
			IdentityProviderName: aws.String("cognito-idp.us-east-1.amazonaws.com/us-east-1_s15Pool"),
			UseDefaults:          aws.Bool(false),
			PrincipalTags:        map[string]string{"department": "sub:custom:department"},
		})
		require.NoError(t, err)
		assert.False(t, aws.ToBool(setOut.UseDefaults))
		assert.Equal(t, "sub:custom:department", setOut.PrincipalTags["department"])

		getOut, err := client.GetPrincipalTagAttributeMap(ctx, &cognitoidentitysdk.GetPrincipalTagAttributeMapInput{
			IdentityPoolId:       poolID,
			IdentityProviderName: aws.String("cognito-idp.us-east-1.amazonaws.com/us-east-1_s15Pool"),
		})
		require.NoError(t, err)
		assert.False(t, aws.ToBool(getOut.UseDefaults))
		assert.Equal(t, "sub:custom:department", getOut.PrincipalTags["department"])
	})

	t.Run("identities lifecycle", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)
		ctx := t.Context()

		pool, err := client.CreateIdentityPool(ctx, &cognitoidentitysdk.CreateIdentityPoolInput{
			IdentityPoolName:               aws.String("s15-identities-pool"),
			AllowUnauthenticatedIdentities: true,
		})
		require.NoError(t, err)
		poolID := pool.IdentityPoolId

		idOut, err := client.GetId(ctx, &cognitoidentitysdk.GetIdInput{IdentityPoolId: poolID})
		require.NoError(t, err)
		identityID := aws.ToString(idOut.IdentityId)
		require.NotEmpty(t, identityID)

		listOut, err := client.ListIdentities(ctx, &cognitoidentitysdk.ListIdentitiesInput{
			IdentityPoolId: poolID,
			MaxResults:     aws.Int32(10),
		})
		require.NoError(t, err)
		found := false
		for _, ident := range listOut.Identities {
			if aws.ToString(ident.IdentityId) == identityID {
				found = true
			}
		}
		assert.True(t, found, "GetId's identity must appear in ListIdentities")
		assert.Equal(t, poolID, listOut.IdentityPoolId)

		delOut, err := client.DeleteIdentities(ctx, &cognitoidentitysdk.DeleteIdentitiesInput{
			IdentityIdsToDelete: []string{identityID},
		})
		require.NoError(t, err)
		assert.Empty(t, delOut.UnprocessedIdentityIds)

		listAfter, err := client.ListIdentities(ctx, &cognitoidentitysdk.ListIdentitiesInput{
			IdentityPoolId: poolID,
			MaxResults:     aws.Int32(10),
		})
		require.NoError(t, err)
		for _, ident := range listAfter.Identities {
			assert.NotEqual(t, identityID, aws.ToString(ident.IdentityId))
		}
	})

	t.Run("merge developer identities", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)
		ctx := t.Context()

		pool, err := client.CreateIdentityPool(ctx, &cognitoidentitysdk.CreateIdentityPoolInput{
			IdentityPoolName:               aws.String("s15-merge-pool"),
			AllowUnauthenticatedIdentities: true,
			DeveloperProviderName:          aws.String("s15.developer.example.com"),
		})
		require.NoError(t, err)
		poolID := pool.IdentityPoolId

		srcTok, err := client.GetOpenIdTokenForDeveloperIdentity(
			ctx, &cognitoidentitysdk.GetOpenIdTokenForDeveloperIdentityInput{
				IdentityPoolId: poolID,
				Logins:         map[string]string{"s15.developer.example.com": "s15-source-user"},
			},
		)
		require.NoError(t, err)

		_, err = client.GetOpenIdTokenForDeveloperIdentity(
			ctx, &cognitoidentitysdk.GetOpenIdTokenForDeveloperIdentityInput{
				IdentityPoolId: poolID,
				Logins:         map[string]string{"s15.developer.example.com": "s15-destination-user"},
			},
		)
		require.NoError(t, err)

		mergeOut, err := client.MergeDeveloperIdentities(ctx, &cognitoidentitysdk.MergeDeveloperIdentitiesInput{
			SourceUserIdentifier:      aws.String("s15-source-user"),
			DestinationUserIdentifier: aws.String("s15-destination-user"),
			DeveloperProviderName:     aws.String("s15.developer.example.com"),
			IdentityPoolId:            poolID,
		})
		require.NoError(t, err)
		assert.NotEqual(t, aws.ToString(srcTok.IdentityId), aws.ToString(mergeOut.IdentityId))
	})
}
