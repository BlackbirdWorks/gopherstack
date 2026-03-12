//go:build integration
// +build integration

package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	lakeformationsdk "github.com/aws/aws-sdk-go-v2/service/lakeformation"
	"github.com/aws/aws-sdk-go-v2/service/lakeformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createLakeFormationClient returns a Lake Formation client pointed at the shared test container.
func createLakeFormationClient(t *testing.T) *lakeformationsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return lakeformationsdk.NewFromConfig(cfg, func(o *lakeformationsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_LakeFormation_DataLakeSettings tests get and put data lake settings.
func TestIntegration_LakeFormation_DataLakeSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		admins  []string
	}{
		{
			name:   "set_and_get_admins",
			admins: []string{"arn:aws:iam::123456789012:user/admin"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createLakeFormationClient(t)

			admins := make([]types.DataLakePrincipal, 0, len(tt.admins))
			for _, a := range tt.admins {
				admins = append(admins, types.DataLakePrincipal{
					DataLakePrincipalIdentifier: aws.String(a),
				})
			}

			_, err := client.PutDataLakeSettings(ctx, &lakeformationsdk.PutDataLakeSettingsInput{
				DataLakeSettings: &types.DataLakeSettings{
					DataLakeAdmins: admins,
				},
			})
			require.NoError(t, err, "PutDataLakeSettings should succeed")

			out, err := client.GetDataLakeSettings(ctx, &lakeformationsdk.GetDataLakeSettingsInput{})
			require.NoError(t, err, "GetDataLakeSettings should succeed")
			require.NotNil(t, out.DataLakeSettings)
			require.Len(t, out.DataLakeSettings.DataLakeAdmins, len(tt.admins))
			assert.Equal(t, tt.admins[0], aws.ToString(out.DataLakeSettings.DataLakeAdmins[0].DataLakePrincipalIdentifier))
		})
	}
}

// TestIntegration_LakeFormation_ResourceLifecycle tests register, describe, list, deregister.
func TestIntegration_LakeFormation_ResourceLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		resourceArn string
		roleArn     string
	}{
		{
			name:        "register_describe_deregister",
			resourceArn: "arn:aws:s3:::integration-test-bucket",
			roleArn:     "arn:aws:iam::123456789012:role/LFRole",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createLakeFormationClient(t)

			_, err := client.RegisterResource(ctx, &lakeformationsdk.RegisterResourceInput{
				ResourceArn: aws.String(tt.resourceArn),
				RoleArn:     aws.String(tt.roleArn),
			})
			require.NoError(t, err, "RegisterResource should succeed")

			descOut, err := client.DescribeResource(ctx, &lakeformationsdk.DescribeResourceInput{
				ResourceArn: aws.String(tt.resourceArn),
			})
			require.NoError(t, err, "DescribeResource should succeed")
			require.NotNil(t, descOut.ResourceInfo)
			assert.Equal(t, tt.resourceArn, aws.ToString(descOut.ResourceInfo.ResourceArn))

			listOut, err := client.ListResources(ctx, &lakeformationsdk.ListResourcesInput{})
			require.NoError(t, err, "ListResources should succeed")
			assert.NotEmpty(t, listOut.ResourceInfoList)

			_, err = client.DeregisterResource(ctx, &lakeformationsdk.DeregisterResourceInput{
				ResourceArn: aws.String(tt.resourceArn),
			})
			require.NoError(t, err, "DeregisterResource should succeed")
		})
	}
}

// TestIntegration_LakeFormation_LFTagLifecycle tests create, get, update, list, delete LF tags.
func TestIntegration_LakeFormation_LFTagLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tagKey    string
		tagValues []string
	}{
		{
			name:      "full_lftag_lifecycle",
			tagKey:    "integration-env",
			tagValues: []string{"dev", "prod"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createLakeFormationClient(t)

			_, err := client.CreateLFTag(ctx, &lakeformationsdk.CreateLFTagInput{
				TagKey:    aws.String(tt.tagKey),
				TagValues: tt.tagValues,
			})
			require.NoError(t, err, "CreateLFTag should succeed")

			getOut, err := client.GetLFTag(ctx, &lakeformationsdk.GetLFTagInput{
				TagKey: aws.String(tt.tagKey),
			})
			require.NoError(t, err, "GetLFTag should succeed")
			assert.Equal(t, tt.tagKey, aws.ToString(getOut.TagKey))
			assert.ElementsMatch(t, tt.tagValues, getOut.TagValues)

			_, err = client.UpdateLFTag(ctx, &lakeformationsdk.UpdateLFTagInput{
				TagKey:            aws.String(tt.tagKey),
				TagValuesToAdd:    []string{"staging"},
				TagValuesToDelete: []string{"dev"},
			})
			require.NoError(t, err, "UpdateLFTag should succeed")

			listOut, err := client.ListLFTags(ctx, &lakeformationsdk.ListLFTagsInput{})
			require.NoError(t, err, "ListLFTags should succeed")
			assert.NotEmpty(t, listOut.LFTags)

			_, err = client.DeleteLFTag(ctx, &lakeformationsdk.DeleteLFTagInput{
				TagKey: aws.String(tt.tagKey),
			})
			require.NoError(t, err, "DeleteLFTag should succeed")
		})
	}
}

// TestIntegration_LakeFormation_Permissions tests grant, list, revoke permissions.
func TestIntegration_LakeFormation_Permissions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		principal   string
		resourceArn string
	}{
		{
			name:        "grant_list_revoke",
			principal:   "arn:aws:iam::123456789012:user/alice",
			resourceArn: "arn:aws:s3:::perm-test-bucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createLakeFormationClient(t)

			_, err := client.GrantPermissions(ctx, &lakeformationsdk.GrantPermissionsInput{
				Principal: &types.DataLakePrincipal{
					DataLakePrincipalIdentifier: aws.String(tt.principal),
				},
				Resource: &types.Resource{
					DataLocation: &types.DataLocationResource{
						ResourceArn: aws.String(tt.resourceArn),
					},
				},
				Permissions: []types.Permission{types.PermissionDataLocationAccess},
			})
			require.NoError(t, err, "GrantPermissions should succeed")

			listOut, err := client.ListPermissions(ctx, &lakeformationsdk.ListPermissionsInput{})
			require.NoError(t, err, "ListPermissions should succeed")
			assert.NotEmpty(t, listOut.PrincipalResourcePermissions)

			_, err = client.RevokePermissions(ctx, &lakeformationsdk.RevokePermissionsInput{
				Principal: &types.DataLakePrincipal{
					DataLakePrincipalIdentifier: aws.String(tt.principal),
				},
				Resource: &types.Resource{
					DataLocation: &types.DataLocationResource{
						ResourceArn: aws.String(tt.resourceArn),
					},
				},
				Permissions: []types.Permission{types.PermissionDataLocationAccess},
			})
			require.NoError(t, err, "RevokePermissions should succeed")
		})
	}
}
