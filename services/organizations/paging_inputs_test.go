package organizations_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	organizationssdk "github.com/aws/aws-sdk-go-v2/service/organizations"
	"github.com/aws/aws-sdk-go-v2/service/organizations/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSingletonLists_PagingInputs checks MaxResults and NextToken validation on single-item and always-empty lists.
func TestSingletonLists_PagingInputs(t *testing.T) {
	t.Parallel()

	type callFn func(ctx context.Context, c *organizationssdk.Client, size *int32, token *string) (int, error)

	roots := func(ctx context.Context, c *organizationssdk.Client, s *int32, tok *string) (int, error) {
		o, err := c.ListRoots(ctx, &organizationssdk.ListRootsInput{MaxResults: s, NextToken: tok})
		if err != nil {
			return 0, err
		}

		assert.Nil(t, o.NextToken)

		return len(o.Roots), nil
	}
	parents := func(ctx context.Context, c *organizationssdk.Client, s *int32, tok *string) (int, error) {
		o, err := c.ListParents(ctx, &organizationssdk.ListParentsInput{
			ChildId: aws.String("000000000000"), MaxResults: s, NextToken: tok,
		})
		if err != nil {
			return 0, err
		}

		return len(o.Parents), nil
	}
	invalidAccounts := func(ctx context.Context, c *organizationssdk.Client, s *int32, tok *string) (int, error) {
		in := &organizationssdk.ListAccountsWithInvalidEffectivePolicyInput{
			PolicyType: types.EffectivePolicyTypeTagPolicy, MaxResults: s, NextToken: tok,
		}

		o, err := c.ListAccountsWithInvalidEffectivePolicy(ctx, in)
		if err != nil {
			return 0, err
		}

		return len(o.Accounts), nil
	}
	validationErrors := func(ctx context.Context, c *organizationssdk.Client, s *int32, tok *string) (int, error) {
		in := &organizationssdk.ListEffectivePolicyValidationErrorsInput{
			PolicyType: types.EffectivePolicyTypeTagPolicy, AccountId: aws.String("000000000000"),
			MaxResults: s, NextToken: tok,
		}

		o, err := c.ListEffectivePolicyValidationErrors(ctx, in)
		if err != nil {
			return 0, err
		}

		return len(o.EffectivePolicyValidationErrors), nil
	}

	tests := []struct {
		call callFn
		name string
		want int
	}{
		{name: "roots", call: roots, want: 1},
		{name: "parents", call: parents, want: 1},
		{name: "invalid_accounts", call: invalidAccounts, want: 0},
		{name: "validation_errors", call: validationErrors, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, _ := newRealClient(t)
			ctx := t.Context()

			n, err := tt.call(ctx, client, aws.Int32(20), nil)
			require.NoError(t, err)
			assert.Equal(t, tt.want, n)

			_, err = tt.call(ctx, client, aws.Int32(21), nil)
			require.ErrorContains(t, err, "InvalidInputException")

			_, err = tt.call(ctx, client, nil, aws.String("!!bad"))
			require.ErrorContains(t, err, "InvalidInputException")
		})
	}
}
