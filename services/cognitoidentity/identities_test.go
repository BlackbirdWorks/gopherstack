package cognitoidentity_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidentity"
)

func TestInMemoryBackend_GetID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget error
		name      string
		wantErr   bool
	}{
		{name: "success_new_identity"},
		{name: "success_existing_identity"},
		{
			name:      "pool_not_found",
			wantErr:   true,
			errTarget: cognitoidentity.ErrIdentityPoolNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			var poolID string

			if tt.name != "pool_not_found" {
				pool, setupErr := b.CreateIdentityPool(
					context.Background(),
					"get-id-pool",
					true,
					false,
					"",
					nil,
					nil,
					nil,
				)
				require.NoError(t, setupErr)
				poolID = pool.IdentityPoolID
			} else {
				poolID = "nonexistent"
			}

			logins := map[string]string{
				"cognito-idp.us-east-1.amazonaws.com/us-east-1_xxx": "token123",
			}
			identity, err := b.GetID(context.Background(), poolID, "000000000000", logins)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, identity.IdentityID)
			assert.Contains(t, identity.IdentityID, "us-east-1:")

			if tt.name == "success_existing_identity" {
				identity2, err2 := b.GetID(context.Background(), poolID, "000000000000", logins)
				require.NoError(t, err2)
				assert.Equal(t, identity.IdentityID, identity2.IdentityID)
			}
		})
	}
}

func TestInMemoryBackend_DeleteIdentities_UnprocessedNil(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	pool, err := b.CreateIdentityPool(
		context.Background(),
		"del-id-pool",
		true,
		false,
		"",
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)

	identity, err := b.GetID(context.Background(), pool.IdentityPoolID, "000000000000", nil)
	require.NoError(t, err)

	unprocessed, err := b.DeleteIdentities(context.Background(), []string{identity.IdentityID})
	require.NoError(t, err)
	assert.Empty(t, unprocessed)

	_, descErr := b.GetCredentialsForIdentity(context.Background(), identity.IdentityID, nil)
	require.Error(t, descErr)
}

func TestInMemoryBackend_ListIdentities_EmptyPoolID(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	_, err := b.ListIdentities(context.Background(), "", 10, false, "")
	require.Error(t, err)
	assert.ErrorIs(t, err, cognitoidentity.ErrInvalidParameter)
}

func TestInMemoryBackend_ListIdentities_LastModifiedDate(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateIdentityPool(
		context.Background(),
		"lmd-pool",
		true,
		false,
		"",
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)

	_, err = b.GetID(
		context.Background(),
		pool.IdentityPoolID,
		"",
		map[string]string{"accounts.google.com": "tok1"},
	)
	require.NoError(t, err)

	result, err := b.ListIdentities(context.Background(), pool.IdentityPoolID, 10, false, "")
	require.NoError(t, err)
	require.Len(t, result.Identities, 1)

	id := result.Identities[0]
	// LastModifiedDate must equal CreatedDate when first created, not be zero.
	assert.False(t, id.LastModifiedDate.IsZero(), "LastModifiedDate must not be zero")
}

func TestInMemoryBackend_GetID_ProviderMatching(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateIdentityPool(
		context.Background(),
		"pm-pool",
		true,
		false,
		"",
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)

	// Create an identity with only google.
	id1, err := b.GetID(
		context.Background(),
		pool.IdentityPoolID,
		"",
		map[string]string{"accounts.google.com": "g-token"},
	)
	require.NoError(t, err)

	// Call GetId again with google + facebook: should return the same identity (not create new).
	id2, err := b.GetID(context.Background(), pool.IdentityPoolID, "", map[string]string{
		"accounts.google.com": "g-token",
		"graph.facebook.com":  "fb-token",
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		id1.IdentityID,
		id2.IdentityID,
		"should find existing identity by provider match",
	)

	// And the facebook login should now be merged in.
	desc, err := b.DescribeIdentity(context.Background(), id2.IdentityID)
	require.NoError(t, err)
	assert.Contains(
		t,
		desc.Logins,
		"graph.facebook.com",
		"new provider should be merged into existing identity",
	)
}

func TestInMemoryBackend_GetID_EmptyPoolID(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	_, err := b.GetID(context.Background(), "", "", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, cognitoidentity.ErrInvalidParameter)
}

func TestInMemoryBackend_ListIdentities_NextToken(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateIdentityPool(
		context.Background(),
		"page-pool",
		true,
		false,
		"",
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)

	for i := range 3 {
		_, err = b.GetID(context.Background(), pool.IdentityPoolID, "", map[string]string{
			"accounts.google.com": fmt.Sprintf("tok-%d", i),
		})
		require.NoError(t, err)
	}

	page1, err := b.ListIdentities(context.Background(), pool.IdentityPoolID, 2, false, "")
	require.NoError(t, err)
	require.Len(t, page1.Identities, 2)
	require.NotEmpty(t, page1.NextToken, "nextToken must be populated when more pages exist")

	page2, err := b.ListIdentities(
		context.Background(),
		pool.IdentityPoolID,
		2,
		false,
		page1.NextToken,
	)
	require.NoError(t, err)
	require.Len(t, page2.Identities, 1)
	assert.Empty(t, page2.NextToken)
}

func TestGetID_UnauthDisabled_EmptyLogins(t *testing.T) {
	t.Parallel()

	b := cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")

	pool, err := b.CreateIdentityPool(context.Background(), "no-unauth-pool", false, false, "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.GetID(context.Background(), pool.IdentityPoolID, "000000000000", nil)
	require.Error(t, err)
	assert.ErrorIs(
		t,
		err,
		cognitoidentity.ErrNotAuthorized,
		"empty logins + AllowUnauthenticated=false must return NotAuthorizedException",
	)
}

func TestGetID_UnauthEnabled_EmptyLogins(t *testing.T) {
	t.Parallel()

	b := cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")

	pool, err := b.CreateIdentityPool(context.Background(), "unauth-pool", true, false, "", nil, nil, nil)
	require.NoError(t, err)

	identity, err := b.GetID(context.Background(), pool.IdentityPoolID, "000000000000", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, identity.IdentityID)
}

func TestGetID_UnauthDisabled_WithLogins(t *testing.T) {
	t.Parallel()

	b := cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")

	pool, err := b.CreateIdentityPool(context.Background(), "no-unauth-with-logins", false, false, "", nil, nil, nil)
	require.NoError(t, err)

	logins := map[string]string{"accounts.google.com": "google-token-abc"}
	identity, err := b.GetID(context.Background(), pool.IdentityPoolID, "000000000000", logins)
	require.NoError(t, err, "authenticated GetId must succeed even when AllowUnauthenticated=false")
	assert.NotEmpty(t, identity.IdentityID)
}

func TestListIdentities_HideDisabled(t *testing.T) {
	t.Parallel()

	b := cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")

	pool, err := b.CreateIdentityPool(context.Background(), "hide-disabled-pool", true, false, "", nil, nil, nil)
	require.NoError(t, err)

	id1, err := b.GetID(context.Background(), pool.IdentityPoolID, "", map[string]string{"p1": "t1"})
	require.NoError(t, err)

	id2, err := b.GetID(context.Background(), pool.IdentityPoolID, "", map[string]string{"p2": "t2"})
	require.NoError(t, err)

	b.SetIdentityEnabled(id2.IdentityID, false)

	result, err := b.ListIdentities(context.Background(), pool.IdentityPoolID, 10, true, "")
	require.NoError(t, err)
	require.Len(t, result.Identities, 1)
	assert.Equal(
		t,
		id1.IdentityID,
		result.Identities[0].IdentityID,
		"disabled identity must be hidden",
	)
}

func TestListIdentities_ShowDisabled(t *testing.T) {
	t.Parallel()

	b := cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")

	pool, err := b.CreateIdentityPool(context.Background(), "show-disabled-pool", true, false, "", nil, nil, nil)
	require.NoError(t, err)

	id1, err := b.GetID(context.Background(), pool.IdentityPoolID, "", map[string]string{"p1": "t1"})
	require.NoError(t, err)

	_, err = b.GetID(context.Background(), pool.IdentityPoolID, "", map[string]string{"p2": "t2"})
	require.NoError(t, err)

	b.SetIdentityEnabled(id1.IdentityID, false)

	result, err := b.ListIdentities(context.Background(), pool.IdentityPoolID, 10, false, "")
	require.NoError(t, err)
	assert.Len(t, result.Identities, 2, "HideDisabled=false must include disabled identities")
}

func TestNewIdentity_EnabledByDefault(t *testing.T) {
	t.Parallel()

	b := cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")

	pool, err := b.CreateIdentityPool(context.Background(), "enabled-default-pool", true, false, "", nil, nil, nil)
	require.NoError(t, err)

	identity, err := b.GetID(context.Background(), pool.IdentityPoolID, "", nil)
	require.NoError(t, err)

	result, err := b.ListIdentities(context.Background(), pool.IdentityPoolID, 10, true, "")
	require.NoError(t, err)
	require.Len(t, result.Identities, 1, "new identity must be enabled by default")
	assert.Equal(t, identity.IdentityID, result.Identities[0].IdentityID)
}

func TestListIdentities_MaxResultsZero(t *testing.T) {
	t.Parallel()

	b := cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")

	pool, err := b.CreateIdentityPool(context.Background(), "maxresults-pool", true, false, "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.ListIdentities(context.Background(), pool.IdentityPoolID, 0, false, "")
	require.Error(t, err)
	assert.ErrorIs(
		t,
		err,
		cognitoidentity.ErrInvalidParameter,
		"MaxResults=0 must return InvalidParameterException",
	)
}
