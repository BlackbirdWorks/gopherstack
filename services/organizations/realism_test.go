package organizations_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	organizationssdk "github.com/aws/aws-sdk-go-v2/service/organizations"
	organizationstypes "github.com/aws/aws-sdk-go-v2/service/organizations/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newRealismClient(t *testing.T) *organizationssdk.Client {
	t.Helper()

	client := newTestOrganizationsClient(t, newTestHandler(t))
	_, err := client.CreateOrganization(t.Context(), &organizationssdk.CreateOrganizationInput{
		FeatureSet: organizationstypes.OrganizationFeatureSetAll,
	})
	require.NoError(t, err)

	return client
}

func rootID(t *testing.T, client *organizationssdk.Client) string {
	t.Helper()

	out, err := client.ListRoots(t.Context(), &organizationssdk.ListRootsInput{})
	require.NoError(t, err)
	require.NotEmpty(t, out.Roots)

	return aws.ToString(out.Roots[0].Id)
}

func TestErrorsCarryMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call     func(t *testing.T, c *organizationssdk.Client) error
		name     string
		wantCode string
	}{
		{
			name:     "account_not_found",
			wantCode: "AccountNotFoundException",
			call: func(t *testing.T, c *organizationssdk.Client) error {
				t.Helper()

				_, err := c.DescribeAccount(t.Context(), &organizationssdk.DescribeAccountInput{
					AccountId: aws.String("999999999999"),
				})

				return err
			},
		},
		{
			name:     "ou_not_found",
			wantCode: "OrganizationalUnitNotFoundException",
			call: func(t *testing.T, c *organizationssdk.Client) error {
				t.Helper()

				_, err := c.DescribeOrganizationalUnit(t.Context(), &organizationssdk.DescribeOrganizationalUnitInput{
					OrganizationalUnitId: aws.String("ou-abcd-12345678"),
				})

				return err
			},
		},
		{
			name:     "policy_not_found",
			wantCode: "PolicyNotFoundException",
			call: func(t *testing.T, c *organizationssdk.Client) error {
				t.Helper()

				_, err := c.DescribePolicy(t.Context(), &organizationssdk.DescribePolicyInput{
					PolicyId: aws.String("p-abcdefgh"),
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.call(t, newRealismClient(t))
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
			assert.NotEmpty(t, apiErr.ErrorMessage())
			assert.NotContains(t, apiErr.ErrorMessage(), tt.wantCode)
		})
	}
}

func TestCreateAccountInputValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		acct    string
		email   string
		role    string
		wantErr bool
	}{
		{name: "valid", acct: "ok", email: "ok@example.com"},
		{name: "name_too_long", acct: strings.Repeat("x", 51), email: "a@example.com", wantErr: true},
		{name: "name_max", acct: strings.Repeat("x", 50), email: "b@example.com"},
		{name: "email_too_short", acct: "n", email: "a@b.c", wantErr: true},
		{name: "email_no_at", acct: "n", email: "notanemail.example.com", wantErr: true},
		{name: "email_too_long", acct: "n", email: strings.Repeat("a", 60) + "@e.com", wantErr: true},
		{name: "role_bad_chars", acct: "n", email: "c@example.com", role: "bad role!", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealismClient(t)
			in := &organizationssdk.CreateAccountInput{
				AccountName: aws.String(tt.acct),
				Email:       aws.String(tt.email),
			}
			if tt.role != "" {
				in.RoleName = aws.String(tt.role)
			}

			_, err := client.CreateAccount(t.Context(), in)
			if !tt.wantErr {
				require.NoError(t, err)

				return
			}

			var invalid *organizationstypes.InvalidInputException
			require.ErrorAs(t, err, &invalid)
			assert.NotEmpty(t, aws.ToString(invalid.Message))
		})
	}
}

func TestCreateAccountDuplicateEmailFails(t *testing.T) {
	t.Parallel()

	client := newRealismClient(t)
	in := &organizationssdk.CreateAccountInput{
		AccountName: aws.String("dup"),
		Email:       aws.String("dup@example.com"),
	}

	first, err := client.CreateAccount(t.Context(), in)
	require.NoError(t, err)
	assert.Equal(t, organizationstypes.CreateAccountStateSucceeded, first.CreateAccountStatus.State)

	second, err := client.CreateAccount(t.Context(), in)
	require.NoError(t, err)
	assert.Equal(t, organizationstypes.CreateAccountStateFailed, second.CreateAccountStatus.State)
	assert.Equal(t, organizationstypes.CreateAccountFailureReasonEmailAlreadyExists,
		second.CreateAccountStatus.FailureReason)

	desc, err := client.DescribeCreateAccountStatus(t.Context(), &organizationssdk.DescribeCreateAccountStatusInput{
		CreateAccountRequestId: second.CreateAccountStatus.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, organizationstypes.CreateAccountStateFailed, desc.CreateAccountStatus.State)
}

func TestOrganizationalUnitParentAndNameErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		parent       string
		ouName       string
		wantNotFound bool
		wantInvalid  bool
	}{
		{name: "missing_parent", parent: "ou-abcd-12345678", ouName: "x", wantNotFound: true},
		{name: "malformed_parent", parent: "bogus", ouName: "x", wantInvalid: true},
		{name: "name_too_long", ouName: strings.Repeat("n", 129), wantInvalid: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealismClient(t)
			parent := tt.parent
			if parent == "" {
				parent = rootID(t, client)
			}

			_, err := client.CreateOrganizationalUnit(t.Context(), &organizationssdk.CreateOrganizationalUnitInput{
				ParentId: aws.String(parent),
				Name:     aws.String(tt.ouName),
			})

			if tt.wantNotFound {
				var nf *organizationstypes.ParentNotFoundException
				require.ErrorAs(t, err, &nf)
			}

			if tt.wantInvalid {
				var inv *organizationstypes.InvalidInputException
				require.ErrorAs(t, err, &inv)
			}

			_, err = client.ListOrganizationalUnitsForParent(
				t.Context(),
				&organizationssdk.ListOrganizationalUnitsForParentInput{ParentId: aws.String(parent)},
			)
			if tt.wantNotFound {
				var nf *organizationstypes.ParentNotFoundException
				require.ErrorAs(t, err, &nf)
			}
		})
	}
}

func TestListPaginationBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(ctx context.Context, c *organizationssdk.Client, tok *string, maxResults *int32) error
		name string
	}{
		{
			name: "list_accounts",
			call: func(ctx context.Context, c *organizationssdk.Client, tok *string, maxResults *int32) error {
				_, err := c.ListAccounts(
					ctx, &organizationssdk.ListAccountsInput{NextToken: tok, MaxResults: maxResults},
				)

				return err
			},
		},
		{
			name: "list_policies",
			call: func(ctx context.Context, c *organizationssdk.Client, tok *string, maxResults *int32) error {
				_, err := c.ListPolicies(ctx, &organizationssdk.ListPoliciesInput{
					Filter:     organizationstypes.PolicyTypeServiceControlPolicy,
					NextToken:  tok,
					MaxResults: maxResults,
				})

				return err
			},
		},
		{
			name: "list_handshakes_for_organization",
			call: func(ctx context.Context, c *organizationssdk.Client, tok *string, maxResults *int32) error {
				_, err := c.ListHandshakesForOrganization(
					ctx, &organizationssdk.ListHandshakesForOrganizationInput{NextToken: tok, MaxResults: maxResults},
				)

				return err
			},
		},
		{
			name: "list_create_account_status",
			call: func(ctx context.Context, c *organizationssdk.Client, tok *string, maxResults *int32) error {
				_, err := c.ListCreateAccountStatus(
					ctx, &organizationssdk.ListCreateAccountStatusInput{NextToken: tok, MaxResults: maxResults},
				)

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealismClient(t)

			var inv *organizationstypes.InvalidInputException
			require.ErrorAs(t, tt.call(t.Context(), client, aws.String("not-a-token!"), nil), &inv)
			assert.Equal(t, organizationstypes.InvalidInputExceptionReasonInvalidPaginationToken, inv.Reason)
			require.ErrorAs(t, tt.call(t.Context(), client, nil, aws.Int32(21)), &inv)
			require.NoError(t, tt.call(t.Context(), client, nil, aws.Int32(20)))
		})
	}
}

func TestConstraintViolationReasons(t *testing.T) {
	t.Parallel()

	client := newRealismClient(t)

	org, err := client.DescribeOrganization(t.Context(), &organizationssdk.DescribeOrganizationInput{})
	require.NoError(t, err)

	_, err = client.CloseAccount(t.Context(), &organizationssdk.CloseAccountInput{
		AccountId: org.Organization.MasterAccountId,
	})

	var cv *organizationstypes.ConstraintViolationException
	require.ErrorAs(t, err, &cv)
	assert.Equal(t, organizationstypes.ConstraintViolationExceptionReasonCannotCloseManagementAccount, cv.Reason)
	assert.NotEmpty(t, aws.ToString(cv.Message))
}
