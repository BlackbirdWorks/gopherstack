package transfer_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	transfersdk "github.com/aws/aws-sdk-go-v2/service/transfer"
	"github.com/aws/aws-sdk-go-v2/service/transfer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateUser_HomeDirectoryRules(t *testing.T) {
	t.Parallel()

	mapping := []types.HomeDirectoryMapEntry{{Entry: aws.String("/"), Target: aws.String("/bucket/u")}}

	tests := []struct {
		name    string
		in      transfersdk.CreateUserInput
		wantErr bool
	}{
		{name: "logical_with_mappings", in: transfersdk.CreateUserInput{
			HomeDirectoryType: types.HomeDirectoryTypeLogical, HomeDirectoryMappings: mapping,
		}},
		{name: "logical_without_mappings", wantErr: true, in: transfersdk.CreateUserInput{
			HomeDirectoryType: types.HomeDirectoryTypeLogical,
		}},
		{name: "path_with_mappings", wantErr: true, in: transfersdk.CreateUserInput{
			HomeDirectoryType: types.HomeDirectoryTypePath, HomeDirectoryMappings: mapping,
		}},
		{name: "logical_with_both", wantErr: true, in: transfersdk.CreateUserInput{
			HomeDirectoryType: types.HomeDirectoryTypeLogical, HomeDirectoryMappings: mapping,
			HomeDirectory: aws.String("/bucket/u"),
		}},
		{name: "path_with_home", in: transfersdk.CreateUserInput{HomeDirectory: aws.String("/bucket/u")}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestTransferClient(t, newTestHandler(t))
			srv, err := c.CreateServer(t.Context(), &transfersdk.CreateServerInput{})
			require.NoError(t, err)

			in := tt.in
			in.ServerId = srv.ServerId
			in.UserName = aws.String("user1")
			in.Role = aws.String("arn:aws:iam::123456789012:role/TransferRole")

			_, err = c.CreateUser(t.Context(), &in)
			if tt.wantErr {
				var ire *types.InvalidRequestException
				require.ErrorAs(t, err, &ire)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestUser_LogicalHomeOmitsHomeDirectory(t *testing.T) {
	t.Parallel()

	c := newTestTransferClient(t, newTestHandler(t))
	srv, err := c.CreateServer(t.Context(), &transfersdk.CreateServerInput{})
	require.NoError(t, err)

	_, err = c.CreateUser(t.Context(), &transfersdk.CreateUserInput{
		ServerId:          srv.ServerId,
		UserName:          aws.String("user1"),
		Role:              aws.String("arn:aws:iam::123456789012:role/TransferRole"),
		HomeDirectoryType: types.HomeDirectoryTypeLogical,
		HomeDirectoryMappings: []types.HomeDirectoryMapEntry{
			{Entry: aws.String("/"), Target: aws.String("/bucket/u")},
		},
	})
	require.NoError(t, err)

	desc, err := c.DescribeUser(t.Context(), &transfersdk.DescribeUserInput{
		ServerId: srv.ServerId, UserName: aws.String("user1"),
	})
	require.NoError(t, err)
	assert.Nil(t, desc.User.HomeDirectory)

	list, err := c.ListUsers(t.Context(), &transfersdk.ListUsersInput{ServerId: srv.ServerId})
	require.NoError(t, err)
	require.Len(t, list.Users, 1)
	assert.Nil(t, list.Users[0].HomeDirectory)
}

func TestCreateServer_IdentityProviderDetailsRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		details *types.IdentityProviderDetails
		name    string
		idpType types.IdentityProviderType
		wantErr bool
	}{
		{name: "service_managed", idpType: types.IdentityProviderTypeServiceManaged},
		{
			name: "service_managed_with_details", idpType: types.IdentityProviderTypeServiceManaged, wantErr: true,
			details: &types.IdentityProviderDetails{Url: aws.String("https://x")},
		},
		{name: "api_gateway_missing_url", idpType: types.IdentityProviderTypeApiGateway, wantErr: true},
		{
			name: "api_gateway", idpType: types.IdentityProviderTypeApiGateway,
			details: &types.IdentityProviderDetails{Url: aws.String("https://x"), InvocationRole: aws.String("arn:r")},
		},
		{name: "directory_missing_id", idpType: types.IdentityProviderTypeAwsDirectoryService, wantErr: true},
		{
			name: "directory", idpType: types.IdentityProviderTypeAwsDirectoryService,
			details: &types.IdentityProviderDetails{DirectoryId: aws.String("d-1234567890")},
		},
		{name: "lambda_missing_function", idpType: types.IdentityProviderTypeAwsLambda, wantErr: true},
		{
			name: "lambda", idpType: types.IdentityProviderTypeAwsLambda,
			details: &types.IdentityProviderDetails{
				Function: aws.String("arn:aws:lambda:us-east-1:123456789012:function:f"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestTransferClient(t, newTestHandler(t))
			_, err := c.CreateServer(t.Context(), &transfersdk.CreateServerInput{
				IdentityProviderType: tt.idpType, IdentityProviderDetails: tt.details,
			})

			if tt.wantErr {
				var ire *types.InvalidRequestException
				require.ErrorAs(t, err, &ire)

				return
			}

			require.NoError(t, err)
		})
	}
}
