package cognitoidp_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDescribeTermsByClient(t *testing.T) {
	t.Parallel()

	client := newTestCognitoIDPClient(t, newTestHandler(t))

	pool, err := client.CreateUserPool(
		t.Context(),
		&cognitoidpsdk.CreateUserPoolInput{PoolName: aws.String("tbc-pool")},
	)
	require.NoError(t, err)

	poolID := aws.ToString(pool.UserPool.Id)

	mkClient := func(name string) string {
		c, cerr := client.CreateUserPoolClient(t.Context(), &cognitoidpsdk.CreateUserPoolClientInput{
			UserPoolId: aws.String(poolID), ClientName: aws.String(name),
		})
		require.NoError(t, cerr)

		return aws.ToString(c.UserPoolClient.ClientId)
	}

	clientA, clientB := mkClient("a"), mkClient("b")

	created, err := client.CreateTerms(t.Context(), &cognitoidpsdk.CreateTermsInput{
		UserPoolId: aws.String(poolID), ClientId: aws.String(clientA), TermsName: aws.String("tos"),
		TermsSource: types.TermsSourceTypeLink, Enforcement: types.TermsEnforcementTypeNone,
		Links: map[string]string{"cognito:default": "https://example.com/tos"},
	})
	require.NoError(t, err)

	tests := []struct {
		name     string
		clientID string
		terms    string
		pool     string
		wantErr  string
	}{
		{name: "found", clientID: clientA, terms: "tos", pool: poolID},
		{name: "other client", clientID: clientB, terms: "tos", pool: poolID, wantErr: "ResourceNotFoundException"},
		{name: "unknown name", clientID: clientA, terms: "nope", pool: poolID, wantErr: "ResourceNotFoundException"},
		{name: "unknown client", clientID: "nope", terms: "tos", pool: poolID, wantErr: "ResourceNotFoundException"},
		{
			name:     "unknown pool",
			clientID: clientA,
			terms:    "tos",
			pool:     "us-east-1_nope",
			wantErr:  "ResourceNotFoundException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, derr := client.DescribeTermsByClient(t.Context(), &cognitoidpsdk.DescribeTermsByClientInput{
				UserPoolId: aws.String(tc.pool), ClientId: aws.String(tc.clientID), TermsName: aws.String(tc.terms),
			})
			if tc.wantErr != "" {
				require.Error(t, derr)
				assert.Contains(t, derr.Error(), tc.wantErr)

				return
			}

			require.NoError(t, derr)
			require.NotNil(t, out.Terms)
			assert.Equal(t, aws.ToString(created.Terms.TermsId), aws.ToString(out.Terms.TermsId))
			assert.Equal(t, "tos", aws.ToString(out.Terms.TermsName))
			assert.Equal(t, clientA, aws.ToString(out.Terms.ClientId))
			assert.Equal(t, types.TermsEnforcementTypeNone, out.Terms.Enforcement)
			assert.Equal(t, types.TermsSourceTypeLink, out.Terms.TermsSource)
			assert.Equal(t, "https://example.com/tos", out.Terms.Links["cognito:default"])
			require.NotNil(t, out.Terms.CreationDate)
			assert.False(t, out.Terms.CreationDate.IsZero())
		})
	}
}
