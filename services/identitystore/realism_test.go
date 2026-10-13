package identitystore_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	identitystoresdk "github.com/aws/aws-sdk-go-v2/service/identitystore"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrorMessagesDropCodePrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call     func(t *testing.T, c *identitystoresdk.Client) error
		name     string
		wantCode string
	}{
		{
			name:     "conflict_duplicate_user",
			wantCode: "ConflictException",
			call: func(t *testing.T, c *identitystoresdk.Client) error {
				t.Helper()

				in := &identitystoresdk.CreateUserInput{
					IdentityStoreId: aws.String("d-1234567890"),
					UserName:        aws.String("dup"),
				}
				_, err := c.CreateUser(t.Context(), in)
				require.NoError(t, err)
				_, err = c.CreateUser(t.Context(), in)

				return err
			},
		},
		{
			name:     "user_not_found",
			wantCode: "ResourceNotFoundException",
			call: func(t *testing.T, c *identitystoresdk.Client) error {
				t.Helper()

				_, err := c.DescribeUser(t.Context(), &identitystoresdk.DescribeUserInput{
					IdentityStoreId: aws.String("d-1234567890"),
					UserId:          aws.String("11111111-1111-1111-1111-111111111111"),
				})

				return err
			},
		},
		{
			name:     "validation_user_name_length",
			wantCode: "ValidationException",
			call: func(t *testing.T, c *identitystoresdk.Client) error {
				t.Helper()

				_, err := c.CreateUser(t.Context(), &identitystoresdk.CreateUserInput{
					IdentityStoreId: aws.String("d-1234567890"),
					UserName:        aws.String(strings.Repeat("x", 130)),
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.call(t, newRealClient(t))
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
			assert.NotEmpty(t, apiErr.ErrorMessage())
			assert.NotContains(t, apiErr.ErrorMessage(), "Exception:")
		})
	}
}

func TestListRejectsBadNextToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(ctx context.Context, c *identitystoresdk.Client, tok *string) error
		name string
	}{
		{
			name: "users",
			call: func(ctx context.Context, c *identitystoresdk.Client, tok *string) error {
				_, err := c.ListUsers(ctx, &identitystoresdk.ListUsersInput{
					IdentityStoreId: aws.String("d-1234567890"), NextToken: tok,
				})

				return err
			},
		},
		{
			name: "groups",
			call: func(ctx context.Context, c *identitystoresdk.Client, tok *string) error {
				_, err := c.ListGroups(ctx, &identitystoresdk.ListGroupsInput{
					IdentityStoreId: aws.String("d-1234567890"), NextToken: tok,
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newRealClient(t)

			for _, tok := range []string{"bogus!", "YWJj"} {
				var apiErr smithy.APIError
				require.ErrorAs(t, tt.call(t.Context(), c, aws.String(tok)), &apiErr, tok)
				assert.Equal(t, "ValidationException", apiErr.ErrorCode())
			}

			require.NoError(t, tt.call(t.Context(), c, nil))
		})
	}
}
