package appstream_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appstreamsdk "github.com/aws/aws-sdk-go-v2/service/appstream"
	"github.com/aws/aws-sdk-go-v2/service/appstream/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appstream"
)

func seedPagedAppStream(t *testing.T, c *appstreamsdk.Client, n int) {
	t.Helper()

	for i := range n {
		name := fmt.Sprintf("paged-%03d", i)
		_, err := c.CreateStack(t.Context(), &appstreamsdk.CreateStackInput{Name: aws.String(name)})
		require.NoError(t, err)
		_, err = c.CreateUser(t.Context(), &appstreamsdk.CreateUserInput{
			UserName: aws.String(name + "@example.com"), AuthenticationType: types.AuthenticationTypeUserpool,
		})
		require.NoError(t, err)
		_, err = c.CreateFleet(t.Context(), &appstreamsdk.CreateFleetInput{
			Name: aws.String(name), InstanceType: aws.String("stream.standard.medium"),
			ImageName: aws.String("img"), ComputeCapacity: &types.ComputeCapacity{DesiredInstances: aws.Int32(1)},
		})
		require.NoError(t, err)
		_, err = c.AssociateFleet(t.Context(), &appstreamsdk.AssociateFleetInput{
			FleetName: aws.String(name), StackName: aws.String("paged-000"),
		})
		require.NoError(t, err)
		_, err = c.BatchAssociateUserStack(t.Context(), &appstreamsdk.BatchAssociateUserStackInput{
			UserStackAssociations: []types.UserStackAssociation{{
				UserName: aws.String(name + "@example.com"), StackName: aws.String("paged-000"),
				AuthenticationType: types.AuthenticationTypeUserpool,
			}},
		})
		require.NoError(t, err)
	}
}

// TestRealClient_DescribeOpsHonourPaging pages ops whose body-bound
// MaxResults/NextToken were ignored (rpcv2cbor serializers, appstream@v1.64.5).
func TestRealClient_DescribeOpsHonourPaging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fetch func(t *testing.T, c *appstreamsdk.Client, token *string) (int, *string)
		name  string
		total int
	}{
		{
			name:  "describe_users",
			total: 5,
			fetch: func(t *testing.T, c *appstreamsdk.Client, tok *string) (int, *string) {
				t.Helper()

				out, err := c.DescribeUsers(t.Context(), &appstreamsdk.DescribeUsersInput{
					AuthenticationType: types.AuthenticationTypeUserpool, MaxResults: aws.Int32(2), NextToken: tok,
				})
				require.NoError(t, err)

				return len(out.Users), out.NextToken
			},
		},
		{
			name:  "user_stack_associations",
			total: 5,
			fetch: func(t *testing.T, c *appstreamsdk.Client, tok *string) (int, *string) {
				t.Helper()

				out, err := c.DescribeUserStackAssociations(
					t.Context(),
					&appstreamsdk.DescribeUserStackAssociationsInput{
						StackName: aws.String("paged-000"), MaxResults: aws.Int32(2), NextToken: tok,
					},
				)
				require.NoError(t, err)

				return len(out.UserStackAssociations), out.NextToken
			},
		},
		{
			name:  "describe_fleets_default_page",
			total: 55,
			fetch: func(t *testing.T, c *appstreamsdk.Client, tok *string) (int, *string) {
				t.Helper()

				out, err := c.DescribeFleets(t.Context(), &appstreamsdk.DescribeFleetsInput{NextToken: tok})
				require.NoError(t, err)

				return len(out.Fleets), out.NextToken
			},
		},
		{
			name:  "describe_stacks_default_page",
			total: 55,
			fetch: func(t *testing.T, c *appstreamsdk.Client, tok *string) (int, *string) {
				t.Helper()

				out, err := c.DescribeStacks(t.Context(), &appstreamsdk.DescribeStacksInput{NextToken: tok})
				require.NoError(t, err)

				return len(out.Stacks), out.NextToken
			},
		},
		{
			name:  "list_associated_fleets",
			total: 55,
			fetch: func(t *testing.T, c *appstreamsdk.Client, tok *string) (int, *string) {
				t.Helper()

				out, err := c.ListAssociatedFleets(t.Context(), &appstreamsdk.ListAssociatedFleetsInput{
					StackName: aws.String("paged-000"), NextToken: tok,
				})
				require.NoError(t, err)

				return len(out.Names), out.NextToken
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestAppStreamClient(
				t,
				appstream.NewHandler(appstream.NewInMemoryBackend("000000000000", "us-east-1")),
			)
			seedPagedAppStream(t, c, tt.total)

			var token *string

			got, pages := 0, 0

			for {
				n, next := tt.fetch(t, c, token)
				got += n
				pages++

				if next == nil {
					break
				}

				token = next

				require.LessOrEqual(t, pages, tt.total)
			}

			require.Equal(t, tt.total, got)
			require.Greater(t, pages, 1)
		})
	}
}

func TestRealClient_DescribeOpsRejectBadToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(c *appstreamsdk.Client) error
		name string
	}{
		{name: "describe_users", call: func(c *appstreamsdk.Client) error {
			_, err := c.DescribeUsers(context.Background(), &appstreamsdk.DescribeUsersInput{
				AuthenticationType: types.AuthenticationTypeUserpool, NextToken: aws.String("%%%"),
			})

			return err
		}},
		{name: "describe_stacks", call: func(c *appstreamsdk.Client) error {
			_, err := c.DescribeStacks(
				context.Background(),
				&appstreamsdk.DescribeStacksInput{NextToken: aws.String("%%%")},
			)

			return err
		}},
		{name: "list_export_image_tasks_max", call: func(c *appstreamsdk.Client) error {
			_, err := c.ListExportImageTasks(
				context.Background(),
				&appstreamsdk.ListExportImageTasksInput{MaxResults: aws.Int32(501)},
			)

			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestAppStreamClient(
				t,
				appstream.NewHandler(appstream.NewInMemoryBackend("000000000000", "us-east-1")),
			)

			var apiErr smithy.APIError

			require.ErrorAs(t, tt.call(c), &apiErr)
			require.Equal(t, "InvalidParameterCombinationException", apiErr.ErrorCode())
		})
	}
}
