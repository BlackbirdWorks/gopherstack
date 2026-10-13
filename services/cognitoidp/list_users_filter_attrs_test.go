package cognitoidp_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListUsersFilterAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		filter  string
		wantErr bool
		want    int
	}{
		{name: "email", filter: `email = "a@b.com"`, want: 1},
		{name: "username prefix", filter: `username ^= "a"`, want: 1},
		{name: "unknown attribute", filter: `bogus = "x"`, wantErr: true},
		{name: "custom attribute", filter: `custom:x = "y"`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			client := newTestCognitoIDPClient(t, h)
			poolID, _ := setupHandlerPoolAndClient(t, h, "filter-pool")

			_, err := client.AdminCreateUser(t.Context(), &cognitoidpsdk.AdminCreateUserInput{
				UserPoolId: aws.String(poolID), Username: aws.String("alice"),
				UserAttributes: []types.AttributeType{{Name: aws.String("email"), Value: aws.String("a@b.com")}},
			})
			require.NoError(t, err)

			out, err := client.ListUsers(t.Context(), &cognitoidpsdk.ListUsersInput{
				UserPoolId: aws.String(poolID), Filter: aws.String(tt.filter),
			})
			if tt.wantErr {
				var ipe *types.InvalidParameterException
				require.ErrorAs(t, err, &ipe)

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.Users, tt.want)
		})
	}
}
