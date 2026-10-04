package cognitoidp_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListUsers_FilterOperators(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		filter  string
		want    []string
		wantErr bool
	}{
		{name: "username_exact_is_not_prefix", filter: `username = "bob"`, want: []string{"bob"}},
		{name: "username_star_is_literal", filter: `username = "bo*"`, want: nil},
		{name: "username_prefix", filter: `username ^= "bo"`, want: []string{"bob", "bobby"}},
		{name: "username_case_sensitive", filter: `username = "BOB"`, want: nil},
		{name: "attr_exact", filter: `given_name = "Jon"`, want: []string{"bob"}},
		{name: "attr_prefix", filter: `given_name ^= "Jo"`, want: []string{"bob", "bobby"}},
		{name: "attr_exact_no_prefix_match", filter: `given_name = "Jo"`, want: nil},
		{name: "status_prefix", filter: `status ^= "Enab"`, want: []string{"bob", "bobby"}},
		{
			name:   "user_status_case_insensitive",
			filter: `cognito:user_status = "force_change_password"`,
			want:   []string{"bob", "bobby"},
		},
		{name: "user_status_prefix", filter: `cognito:user_status ^= "FORCE"`, want: []string{"bob", "bobby"}},
		{name: "escaped_quote_value", filter: `family_name = "O\"Neil"`, want: []string{"bobby"}},
		{name: "malformed_rejected", filter: `username bob`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCognitoIDPClient(t, newTestHandler(t))

			pool, err := client.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{
				PoolName: aws.String("filter-pool"),
			})
			require.NoError(t, err)

			poolID := pool.UserPool.Id

			for name, attrs := range map[string][]types.AttributeType{
				"bob": {{Name: aws.String("given_name"), Value: aws.String("Jon")}},
				"bobby": {
					{Name: aws.String("given_name"), Value: aws.String("Jonas")},
					{Name: aws.String("family_name"), Value: aws.String(`O"Neil`)},
				},
			} {
				_, err = client.AdminCreateUser(t.Context(), &cognitoidpsdk.AdminCreateUserInput{
					UserPoolId: poolID, Username: aws.String(name), UserAttributes: attrs,
				})
				require.NoError(t, err)
			}

			out, err := client.ListUsers(t.Context(), &cognitoidpsdk.ListUsersInput{
				UserPoolId: poolID, Filter: aws.String(tt.filter),
			})
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "InvalidParameterException")

				return
			}

			require.NoError(t, err)

			var got []string
			for _, u := range out.Users {
				got = append(got, aws.ToString(u.Username))
			}

			assert.Equal(t, tt.want, got)
		})
	}
}
