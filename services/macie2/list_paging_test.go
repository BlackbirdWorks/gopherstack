package macie2_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	macie2sdk "github.com/aws/aws-sdk-go-v2/service/macie2"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/macie2"
)

// TestListOps_PageAndRejectBadTokens covers maxResults/nextToken (serializers.go@v1.54.4 query members).
func TestListOps_PageAndRejectBadTokens(t *testing.T) {
	t.Parallel()

	tests := []struct {
		list func(ctx context.Context, c *macie2sdk.Client, size int32, tok *string) (int, *string, error)
		name string
	}{
		{
			name: "automated_discovery_accounts",
			list: func(ctx context.Context, c *macie2sdk.Client, sz int32, tok *string) (int, *string, error) {
				out, err := c.ListAutomatedDiscoveryAccounts(ctx, &macie2sdk.ListAutomatedDiscoveryAccountsInput{
					MaxResults: aws.Int32(sz), NextToken: tok,
				})
				if err != nil {
					return 0, nil, err
				}

				return len(out.Items), out.NextToken, nil
			},
		},
		{
			name: "organization_admin_accounts",
			list: func(ctx context.Context, c *macie2sdk.Client, sz int32, tok *string) (int, *string, error) {
				out, err := c.ListOrganizationAdminAccounts(ctx, &macie2sdk.ListOrganizationAdminAccountsInput{
					MaxResults: aws.Int32(sz), NextToken: tok,
				})
				if err != nil {
					return 0, nil, err
				}

				return len(out.AdminAccounts), out.NextToken, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, c := newMacie2Client(t)

			updates := make([]macie2.AutoDiscoveryAccountUpdate, 0, 3)

			for _, id := range []string{"111111111111", "222222222222", "333333333333"} {
				require.NoError(t, b.EnableOrganizationAdminAccount(id))

				updates = append(updates, macie2.AutoDiscoveryAccountUpdate{AccountID: id, Status: "ENABLED"})
			}

			require.NoError(t, b.BatchUpdateAutomatedDiscoveryAccounts(updates))

			total, next, err := tt.list(t.Context(), c, 0, nil)
			require.NoError(t, err)
			assert.Equal(t, 3, total)
			assert.Nil(t, next)

			n, next, err := tt.list(t.Context(), c, 2, nil)
			require.NoError(t, err)
			assert.Equal(t, 2, n)
			require.NotNil(t, next)

			rest, _, err := tt.list(t.Context(), c, 2, next)
			require.NoError(t, err)
			assert.Equal(t, 1, rest)

			_, _, err = tt.list(t.Context(), c, 0, aws.String("bogus"))
			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "ValidationException", apiErr.ErrorCode())
		})
	}
}
