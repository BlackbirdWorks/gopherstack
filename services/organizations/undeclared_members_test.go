package organizations_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	organizationssdk "github.com/aws/aws-sdk-go-v2/service/organizations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_DescribeAccountAfterTrim(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		role string
	}{
		{name: "defaults"},
		{name: "explicit", role: "CustomRole"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client, _ := newRealClient(t)
			ctx := t.Context()

			in := &organizationssdk.CreateAccountInput{
				AccountName: aws.String("member"),
				Email:       aws.String("member@example.com"),
			}
			if tc.role != "" {
				in.RoleName = aws.String(tc.role)
			}

			created, err := client.CreateAccount(ctx, in)
			require.NoError(t, err)

			got, err := client.DescribeAccount(ctx, &organizationssdk.DescribeAccountInput{
				AccountId: created.CreateAccountStatus.AccountId,
			})
			require.NoError(t, err)
			assert.Equal(t, "member", aws.ToString(got.Account.Name))
			assert.Equal(t, "member@example.com", aws.ToString(got.Account.Email))
			assert.NotEmpty(t, aws.ToString(got.Account.Arn))
			assert.NotEmpty(t, string(got.Account.Status))
		})
	}
}
