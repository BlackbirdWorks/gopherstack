package cognitoidp_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUserPool_UpdateResetsOmittedToDefaults(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name           string
		update         types.VerifiedAttributeType
		wantProtection types.DeletionProtectionType
		wantAutoVerify []types.VerifiedAttributeType
	}{
		{name: "omitted resets", wantProtection: types.DeletionProtectionTypeInactive},
		{
			name: "supplied kept", update: types.VerifiedAttributeTypeEmail,
			wantAutoVerify: []types.VerifiedAttributeType{types.VerifiedAttributeTypeEmail},
			wantProtection: types.DeletionProtectionTypeInactive,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestCognitoIDPClient(t, newTestHandler(t))
			ctx := t.Context()

			pool, err := c.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{
				PoolName:               aws.String("p"),
				AutoVerifiedAttributes: []types.VerifiedAttributeType{types.VerifiedAttributeTypePhoneNumber},
				DeletionProtection:     types.DeletionProtectionTypeActive,
			})
			require.NoError(t, err)

			in := &cognitoidpsdk.UpdateUserPoolInput{UserPoolId: pool.UserPool.Id}
			if tc.update != "" {
				in.AutoVerifiedAttributes = []types.VerifiedAttributeType{tc.update}
			}

			_, err = c.UpdateUserPool(ctx, in)
			require.NoError(t, err)

			got, err := c.DescribeUserPool(ctx, &cognitoidpsdk.DescribeUserPoolInput{UserPoolId: pool.UserPool.Id})
			require.NoError(t, err)
			assert.Equal(t, tc.wantAutoVerify, got.UserPool.AutoVerifiedAttributes)
			assert.Equal(t, tc.wantProtection, got.UserPool.DeletionProtection)
		})
	}
}
