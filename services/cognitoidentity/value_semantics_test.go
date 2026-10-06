package cognitoidentity_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidentitysdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateIdentityPool_OmittedMembersResetToDefaults(t *testing.T) {
	t.Parallel()

	cases := []struct {
		update        *cognitoidentitysdk.UpdateIdentityPoolInput
		name          string
		wantLoginKeys int
		wantUnauth    bool
		wantClassic   bool
	}{
		{name: "all omitted", update: &cognitoidentitysdk.UpdateIdentityPoolInput{}},
		{
			name: "classic only", update: &cognitoidentitysdk.UpdateIdentityPoolInput{AllowClassicFlow: aws.Bool(true)},
			wantClassic: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestHandlerAndClient(t)
			created, err := c.CreateIdentityPool(t.Context(), &cognitoidentitysdk.CreateIdentityPoolInput{
				IdentityPoolName:               aws.String("p"),
				AllowUnauthenticatedIdentities: true,
				SupportedLoginProviders:        map[string]string{"graph.facebook.com": "123"},
			})
			require.NoError(t, err)

			tc.update.IdentityPoolId = created.IdentityPoolId
			tc.update.IdentityPoolName = aws.String("p")
			_, err = c.UpdateIdentityPool(t.Context(), tc.update)
			require.NoError(t, err)

			got, err := c.DescribeIdentityPool(
				t.Context(),
				&cognitoidentitysdk.DescribeIdentityPoolInput{IdentityPoolId: created.IdentityPoolId},
			)
			require.NoError(t, err)
			assert.Equal(t, tc.wantUnauth, got.AllowUnauthenticatedIdentities)
			assert.Equal(t, tc.wantClassic, aws.ToBool(got.AllowClassicFlow))
			assert.Len(t, got.SupportedLoginProviders, tc.wantLoginKeys)
		})
	}
}
