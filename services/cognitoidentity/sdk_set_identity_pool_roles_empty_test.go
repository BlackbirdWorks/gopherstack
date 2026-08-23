package cognitoidentity_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidentitysdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentity"
	"github.com/stretchr/testify/require"
)

// TestSetIdentityPoolRoles_EmptyRolesMap_RealClient covers gopherstack-jodk
// bug 1: the real client-side validator (cognitoidentity@v1.36.4
// validators.go:929, validateOpSetIdentityPoolRolesInput) only null-checks
// Roles, never its length, so a real client can legally send a non-nil,
// zero-length Roles map. Terraform sends exactly that on
// aws_cognito_identity_pool_roles_attachment destroy, to clear the
// association. gopherstack previously rejected it with "Roles must contain
// at least one of authenticated or unauthenticated", so a real terraform
// destroy could never tear the resource down.
func TestSetIdentityPoolRoles_EmptyRolesMap_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	pool, err := client.CreateIdentityPool(ctx, &cognitoidentitysdk.CreateIdentityPoolInput{
		IdentityPoolName:               aws.String("jodk-roles-pool"),
		AllowUnauthenticatedIdentities: true,
	})
	require.NoError(t, err)

	_, err = client.SetIdentityPoolRoles(ctx, &cognitoidentitysdk.SetIdentityPoolRolesInput{
		IdentityPoolId: pool.IdentityPoolId,
		Roles:          map[string]string{},
	})
	require.NoError(t, err,
		"SetIdentityPoolRoles must accept a non-nil, empty Roles map: the real validator only null-checks it")
}
