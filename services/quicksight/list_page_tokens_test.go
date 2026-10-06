package quicksight_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	quicksightsdk "github.com/aws/aws-sdk-go-v2/service/quicksight"
	"github.com/aws/aws-sdk-go-v2/service/quicksight/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type qsPager func(ctx context.Context, c *quicksightsdk.Client, size int32, token *string) (int, *string, error)

// TestListOps_PageAndRejectBadTokens covers max-results/next-token (quicksight@v1.129.0 serializers.go).
func TestListOps_PageAndRejectBadTokens(t *testing.T) {
	t.Parallel()

	tests := []struct {
		list     qsPager
		name     string
		wantCode string
	}{
		{
			name: "folder_permissions", wantCode: "InvalidNextTokenException",
			list: func(ctx context.Context, c *quicksightsdk.Client, size int32, tok *string) (int, *string, error) {
				out, err := c.DescribeFolderPermissions(ctx, &quicksightsdk.DescribeFolderPermissionsInput{
					AwsAccountId: aws.String(qsTestAccountID), FolderId: aws.String("pf"),
					MaxResults: aws.Int32(size), NextToken: tok,
				})
				if err != nil {
					return 0, nil, err
				}

				return len(out.Permissions), out.NextToken, nil
			},
		},
		{
			name: "folder_resolved_permissions", wantCode: "InvalidNextTokenException",
			list: func(ctx context.Context, c *quicksightsdk.Client, size int32, tok *string) (int, *string, error) {
				out, err := c.DescribeFolderResolvedPermissions(
					ctx,
					&quicksightsdk.DescribeFolderResolvedPermissionsInput{
						AwsAccountId: aws.String(qsTestAccountID), FolderId: aws.String("pf"),
						MaxResults: aws.Int32(size), NextToken: tok,
					},
				)
				if err != nil {
					return 0, nil, err
				}

				return len(out.Permissions), out.NextToken, nil
			},
		},
		{
			name: "identity_propagation_configs", wantCode: "InvalidParameterValueException",
			list: func(ctx context.Context, c *quicksightsdk.Client, size int32, tok *string) (int, *string, error) {
				out, err := c.ListIdentityPropagationConfigs(ctx, &quicksightsdk.ListIdentityPropagationConfigsInput{
					AwsAccountId: aws.String(qsTestAccountID), MaxResults: aws.Int32(size), NextToken: tok,
				})
				if err != nil {
					return 0, nil, err
				}

				return len(out.Services), out.NextToken, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newQuickSightTestClient(t)
			seedPagedQuickSightState(t, client)

			seen := 0

			var token *string

			for range 4 {
				n, next, err := tt.list(t.Context(), client, 2, token)
				require.NoError(t, err)

				seen += n
				if token = next; token == nil {
					break
				}
			}

			assert.Equal(t, 3, seen)
			assert.Nil(t, token)

			_, _, err := tt.list(t.Context(), client, 2, aws.String("%%%not-base64"))

			var apiErr smithy.APIError

			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
		})
	}
}

func seedPagedQuickSightState(t *testing.T, c *quicksightsdk.Client) {
	t.Helper()

	perms := make([]types.ResourcePermission, 0, 3)
	for i := range 3 {
		perms = append(perms, types.ResourcePermission{
			Principal: aws.String(fmt.Sprintf("arn:aws:quicksight:us-east-1:000000000000:user/default/u%d", i)),
			Actions:   []string{"quicksight:DescribeFolder"},
		})
	}

	_, err := c.CreateFolder(t.Context(), &quicksightsdk.CreateFolderInput{
		AwsAccountId: aws.String(qsTestAccountID), FolderId: aws.String("pf"),
		Name: aws.String("paged"), FolderType: types.FolderTypeShared, Permissions: perms,
	})
	require.NoError(t, err)

	for _, svc := range []types.ServiceType{
		types.ServiceTypeRedshift, types.ServiceTypeQbusiness, types.ServiceTypeAthena,
	} {
		_, err = c.UpdateIdentityPropagationConfig(t.Context(), &quicksightsdk.UpdateIdentityPropagationConfigInput{
			AwsAccountId: aws.String(qsTestAccountID), Service: svc, AuthorizedTargets: []string{"t1"},
		})
		require.NoError(t, err)
	}
}
