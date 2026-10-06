package ec2_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestRealClient_ApplicationStatusOpsPaginate(t *testing.T) {
	t.Parallel()

	const total = 3

	tests := []struct {
		page func(ctx context.Context, c *ec2sdk.Client, token *string) (n int, next *string, err error)
		name string
	}{
		{
			name: "describe checks",
			page: func(ctx context.Context, c *ec2sdk.Client, token *string) (int, *string, error) {
				out, err := c.DescribeApplicationStatusChecks(ctx, &ec2sdk.DescribeApplicationStatusChecksInput{
					MaxResults: aws.Int32(1), NextToken: token,
				})
				if err != nil {
					return 0, nil, err
				}

				return len(out.ApplicationStatusChecks), out.NextToken, nil
			},
		},
		{
			name: "describe associations",
			page: func(ctx context.Context, c *ec2sdk.Client, token *string) (int, *string, error) {
				out, err := c.DescribeApplicationStatusCheckAssociations(
					ctx, &ec2sdk.DescribeApplicationStatusCheckAssociationsInput{
						MaxResults: aws.Int32(1), NextToken: token,
					})
				if err != nil {
					return 0, nil, err
				}

				return len(out.Associations), out.NextToken, nil
			},
		},
		{
			name: "describe status",
			page: func(ctx context.Context, c *ec2sdk.Client, token *string) (int, *string, error) {
				out, err := c.DescribeApplicationStatus(ctx, &ec2sdk.DescribeApplicationStatusInput{
					MaxResults: aws.Int32(1), NextToken: token,
				})
				if err != nil {
					return 0, nil, err
				}

				return len(out.ApplicationStatuses.Instances), out.NextToken, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, client := newMiscClient(t)

			insts, err := b.RunInstances("ami-12345678", "t3.micro", "", total)
			require.NoError(t, err)

			for i, inst := range insts {
				check, cerr := b.CreateApplicationStatusCheck(ec2.ApplicationStatusCheckParams{
					Protocol: aws.String("http"), Port: aws.Int(80 + i),
				})
				require.NoError(t, cerr)
				_, _, err = b.AssociateApplicationStatusCheck(check.ApplicationStatusCheckID, []string{inst.ID}, nil)
				require.NoError(t, err)
			}

			var (
				seen  int
				pages int
				token *string
			)

			for {
				n, next, perr := tt.page(t.Context(), client, token)
				require.NoError(t, perr)
				assert.LessOrEqual(t, n, 1)

				seen += n
				pages++

				if next == nil || aws.ToString(next) == "" {
					break
				}

				token = next

				require.LessOrEqual(t, pages, total+1, "pagination must terminate")
			}

			assert.Equal(t, total, seen)
			assert.Equal(t, total, pages)
		})
	}
}
