package lakeformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lakeformationsdk "github.com/aws/aws-sdk-go-v2/service/lakeformation"
	"github.com/aws/aws-sdk-go-v2/service/lakeformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lakeformation"
)

// TestGetTemporaryDataLocationCredentials_RealSDKClient_RoundTrip drives the
// op through a real, unmodified aws-sdk-go-v2 client. gopherstack's request
// struct previously modeled a fabricated ResourceArn field copied from the
// GetTemporaryGlue*Credentials sibling family; the real
// GetTemporaryDataLocationCredentialsInput has no such member at all -- it
// serializes DataLocations/CredentialsScope instead. A real client's
// request body therefore never matched the old handler's required-field
// check, so this op was unreachable by any typed client. Using the real SDK
// type here (not a hand-built JSON body) is itself proof of the fix: the
// compiler enforces the real field names.
func TestGetTemporaryDataLocationCredentials_RealSDKClient_RoundTrip(t *testing.T) {
	t.Parallel()

	h := lakeformation.NewHandler(lakeformation.NewInMemoryBackend())
	client := newTestLakeFormationClient(t, h)

	out, err := client.GetTemporaryDataLocationCredentials(
		t.Context(),
		&lakeformationsdk.GetTemporaryDataLocationCredentialsInput{
			DataLocations:    []string{"s3://my-bucket/path"},
			CredentialsScope: types.CredentialsScopeReadwrite,
			DurationSeconds:  aws.Int32(900),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, out.Credentials)
	assert.NotEmpty(t, aws.ToString(out.Credentials.AccessKeyId))
	assert.NotEmpty(t, aws.ToString(out.Credentials.SecretAccessKey))
	assert.NotEmpty(t, aws.ToString(out.Credentials.SessionToken))
	assert.Equal(t, types.CredentialsScopeReadwrite, out.CredentialsScope)
	assert.Equal(t, []string{"s3://my-bucket/path"}, out.AccessibleDataLocations)
}

// TestGetTemporaryDataLocationCredentials_RealSDKClient_DataLocationsOptional
// proves a request omitting DataLocations entirely succeeds.
// GetTemporaryDataLocationCredentialsInput marks no member required
// (api_op_GetTemporaryDataLocationCredentials.go, lakeformation@v1.50.4) --
// the handler previously rejected this with a 400 (gopherstack-4ly2).
func TestGetTemporaryDataLocationCredentials_RealSDKClient_DataLocationsOptional(t *testing.T) {
	t.Parallel()

	h := lakeformation.NewHandler(lakeformation.NewInMemoryBackend())
	client := newTestLakeFormationClient(t, h)

	out, err := client.GetTemporaryDataLocationCredentials(
		t.Context(),
		&lakeformationsdk.GetTemporaryDataLocationCredentialsInput{},
	)
	require.NoError(t, err)
	require.NotNil(t, out.Credentials)
}

// TestListDataCellsFilter_RealSDKClient_TableOptional proves a request
// omitting Table entirely succeeds. ListDataCellsFilterInput marks no
// member required (api_op_ListDataCellsFilter.go, lakeformation@v1.50.4) --
// the handler previously rejected this with a 400 (gopherstack-4ly2).
func TestListDataCellsFilter_RealSDKClient_TableOptional(t *testing.T) {
	t.Parallel()

	h := lakeformation.NewHandler(lakeformation.NewInMemoryBackend())
	client := newTestLakeFormationClient(t, h)

	out, err := client.ListDataCellsFilter(
		t.Context(),
		&lakeformationsdk.ListDataCellsFilterInput{},
	)
	require.NoError(t, err)
	assert.Empty(t, out.DataCellsFilters)
}

// TestDeleteDataCellsFilter_RealSDKClient_AllFieldsOptional proves a real
// SDK client can send a DeleteDataCellsFilter request with every field
// omitted -- DeleteDataCellsFilterInput marks no member required
// (api_op_DeleteDataCellsFilter.go, lakeformation@v1.50.4). The real
// client's own client-side validator would reject the call before it ever
// reached the wire if any of these were required, so a successful dial-out
// here is itself proof none are: gopherstack-i8lo.
func TestDeleteDataCellsFilter_RealSDKClient_AllFieldsOptional(t *testing.T) {
	t.Parallel()

	h := lakeformation.NewHandler(lakeformation.NewInMemoryBackend())
	client := newTestLakeFormationClient(t, h)

	_, err := client.DeleteDataCellsFilter(t.Context(), &lakeformationsdk.DeleteDataCellsFilterInput{})
	require.Error(t, err)
	// No matching filter exists, so the request reaches gopherstack's
	// ordinary not-found path -- proof it was never rejected client-side
	// (InvalidParamsError) or by a server-side 400 (InvalidInputException).
	var notFound *types.EntityNotFoundException
	require.ErrorAs(t, err, &notFound)
}

// TestGetDataCellsFilter_RealSDKClient_RequiredFieldsRejectedClientSide
// proves GetDataCellsFilterInput's four required members (TableCatalogId,
// DatabaseName, TableName, Name -- api_op_GetDataCellsFilter.go,
// lakeformation@v1.50.4) are enforced by the real SDK client itself: a
// call omitting TableCatalogId never reaches gopherstack at all. This is
// exactly why the missing server-side check on gopherstack's own backend
// (three of the four were previously unvalidated: gopherstack-i8lo) went
// unnoticed by any typed-client caller for so long -- the gap is only
// reachable by a raw HTTP request, which is what the table-driven
// TestGetDataCellsFilter_RequiredFields test exercises instead.
func TestGetDataCellsFilter_RealSDKClient_RequiredFieldsRejectedClientSide(t *testing.T) {
	t.Parallel()

	h := lakeformation.NewHandler(lakeformation.NewInMemoryBackend())
	client := newTestLakeFormationClient(t, h)

	_, err := client.GetDataCellsFilter(t.Context(), &lakeformationsdk.GetDataCellsFilterInput{
		DatabaseName: aws.String("db"),
		TableName:    aws.String("tbl"),
		Name:         aws.String("f"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TableCatalogId")
}

// TestGetTemporaryGlueTableCredentials_RealSDKClient_VendedS3Path proves
// S3Path (a real request member that was previously parsed nowhere) reaches
// the backend and comes back as VendedS3Path (a real response member that
// was previously entirely absent from the wire struct).
func TestGetTemporaryGlueTableCredentials_RealSDKClient_VendedS3Path(t *testing.T) {
	t.Parallel()

	h := lakeformation.NewHandler(lakeformation.NewInMemoryBackend())
	client := newTestLakeFormationClient(t, h)

	out, err := client.GetTemporaryGlueTableCredentials(
		t.Context(),
		&lakeformationsdk.GetTemporaryGlueTableCredentialsInput{
			TableArn: aws.String("arn:aws:glue:us-east-1:123456789012:table/db1/tbl1"),
			S3Path:   aws.String("s3://my-bucket/db1/tbl1/"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, []string{"s3://my-bucket/db1/tbl1/"}, out.VendedS3Path)
}

// TestIdentityCenterConfiguration_RealSDKClient_ServiceIntegrationsAndShareRecipients
// proves three previously-discarded real request members reach the
// backend, and one fabricated response member (ApplicationStatus, real
// only as Update's request field) does not leak onto Describe:
//  1. Create's ServiceIntegrations (a real member; previously unparsed).
//  2. Update's ShareRecipients (a real member; previously not even a field
//     on gopherstack's update input struct, so silently discarded).
//  3. Update's ServiceIntegrations (same class as #1).
//
// Using the real SDK's DescribeLakeFormationIdentityCenterConfigurationOutput
// type is itself proof ApplicationStatus can't leak: the real type has no
// such field, so there is nothing to even attempt to read.
func TestIdentityCenterConfiguration_RealSDKClient_ServiceIntegrationsAndShareRecipients(t *testing.T) {
	t.Parallel()

	h := lakeformation.NewHandler(lakeformation.NewInMemoryBackend())
	h.AccountID = "123456789012"
	client := newTestLakeFormationClient(t, h)

	redshiftIntegration := []types.ServiceIntegrationUnion{
		&types.ServiceIntegrationUnionMemberRedshift{
			Value: []types.RedshiftScopeUnion{
				&types.RedshiftScopeUnionMemberRedshiftConnect{
					Value: types.RedshiftConnect{Authorization: types.ServiceAuthorizationEnabled},
				},
			},
		},
	}

	_, err := client.CreateLakeFormationIdentityCenterConfiguration(
		t.Context(),
		&lakeformationsdk.CreateLakeFormationIdentityCenterConfigurationInput{
			CatalogId:           aws.String("123456789012"),
			InstanceArn:         aws.String("arn:aws:sso:::instance/ssoins-0000000000000000"),
			ServiceIntegrations: redshiftIntegration,
		},
	)
	require.NoError(t, err)

	newRecipients := []types.DataLakePrincipal{
		{DataLakePrincipalIdentifier: aws.String("arn:aws:iam::999999999999:root")},
	}

	_, err = client.UpdateLakeFormationIdentityCenterConfiguration(
		t.Context(),
		&lakeformationsdk.UpdateLakeFormationIdentityCenterConfigurationInput{
			CatalogId:           aws.String("123456789012"),
			ShareRecipients:     newRecipients,
			ServiceIntegrations: redshiftIntegration,
		},
	)
	require.NoError(t, err)

	desc, err := client.DescribeLakeFormationIdentityCenterConfiguration(
		t.Context(),
		&lakeformationsdk.DescribeLakeFormationIdentityCenterConfigurationInput{
			CatalogId: aws.String("123456789012"),
		},
	)
	require.NoError(t, err)

	require.Len(t, desc.ServiceIntegrations, 1)
	redshiftMember, ok := desc.ServiceIntegrations[0].(*types.ServiceIntegrationUnionMemberRedshift)
	require.True(t, ok)
	require.Len(t, redshiftMember.Value, 1)
	connectMember, ok := redshiftMember.Value[0].(*types.RedshiftScopeUnionMemberRedshiftConnect)
	require.True(t, ok)
	assert.Equal(t, types.ServiceAuthorizationEnabled, connectMember.Value.Authorization)

	require.Len(t, desc.ShareRecipients, 1)
	assert.Equal(t, "arn:aws:iam::999999999999:root", aws.ToString(desc.ShareRecipients[0].DataLakePrincipalIdentifier))
}

// TestUpdateIdentityCenterConfiguration_ShareRecipients_EmptyListClears
// proves the nil-vs-empty-list distinction: an Update call that omits
// ShareRecipients entirely leaves the existing value untouched, while one
// that explicitly sends an empty list clears it (matching AWS's documented
// "If the ShareRecipients value is an empty list, then the existing share
// recipients list will be cleared" behavior). Each subtest gets its own
// backend/catalog to avoid racing on shared state under t.Parallel().
func TestUpdateIdentityCenterConfiguration_ShareRecipients_EmptyListClears(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T) *lakeformationsdk.Client {
		t.Helper()

		h := lakeformation.NewHandler(lakeformation.NewInMemoryBackend())
		h.AccountID = "123456789012"
		client := newTestLakeFormationClient(t, h)

		_, err := client.CreateLakeFormationIdentityCenterConfiguration(
			t.Context(),
			&lakeformationsdk.CreateLakeFormationIdentityCenterConfigurationInput{
				CatalogId:   aws.String("123456789012"),
				InstanceArn: aws.String("arn:aws:sso:::instance/ssoins-0000000000000000"),
				ShareRecipients: []types.DataLakePrincipal{
					{DataLakePrincipalIdentifier: aws.String("arn:aws:iam::111111111111:root")},
				},
			},
		)
		require.NoError(t, err)

		return client
	}

	t.Run("unspecified_leaves_unchanged", func(t *testing.T) {
		t.Parallel()

		client := setup(t)

		_, err := client.UpdateLakeFormationIdentityCenterConfiguration(
			t.Context(),
			&lakeformationsdk.UpdateLakeFormationIdentityCenterConfigurationInput{
				CatalogId: aws.String("123456789012"),
				ExternalFiltering: &types.ExternalFilteringConfiguration{
					Status:            types.EnableStatusEnabled,
					AuthorizedTargets: []string{"arn:aws:s3:::bucket"},
				},
			},
		)
		require.NoError(t, err)

		desc, err := client.DescribeLakeFormationIdentityCenterConfiguration(
			t.Context(),
			&lakeformationsdk.DescribeLakeFormationIdentityCenterConfigurationInput{
				CatalogId: aws.String("123456789012"),
			},
		)
		require.NoError(t, err)
		require.Len(t, desc.ShareRecipients, 1)
	})

	t.Run("explicit_empty_clears", func(t *testing.T) {
		t.Parallel()

		client := setup(t)

		_, err := client.UpdateLakeFormationIdentityCenterConfiguration(
			t.Context(),
			&lakeformationsdk.UpdateLakeFormationIdentityCenterConfigurationInput{
				CatalogId:       aws.String("123456789012"),
				ShareRecipients: []types.DataLakePrincipal{},
			},
		)
		require.NoError(t, err)

		desc, err := client.DescribeLakeFormationIdentityCenterConfiguration(
			t.Context(),
			&lakeformationsdk.DescribeLakeFormationIdentityCenterConfigurationInput{
				CatalogId: aws.String("123456789012"),
			},
		)
		require.NoError(t, err)
		assert.Empty(t, desc.ShareRecipients)
	})
}

// TestGetEffectivePermissionsForPath_RealSDKClient_PermissionsKey proves
// gopherstack-zquj: getEffectivePermissionsForPathOutput tagged its grant
// list json:"PrincipalResourcePermissions" (correct for ListPermissions'
// sibling type) but the real GetEffectivePermissionsForPathOutput
// deserializer switches on "Permissions"
// (deserializers.go: awsRestjson1_deserializeOpDocumentGetEffectivePermissionsForPathOutput,
// lakeformation@v1.50.4) -- every real client decoded Permissions as nil.
func TestGetEffectivePermissionsForPath_RealSDKClient_PermissionsKey(t *testing.T) {
	t.Parallel()

	h := lakeformation.NewHandler(lakeformation.NewInMemoryBackend())
	client := newTestLakeFormationClient(t, h)

	_, err := client.GrantPermissions(t.Context(), &lakeformationsdk.GrantPermissionsInput{
		Principal: &types.DataLakePrincipal{
			DataLakePrincipalIdentifier: aws.String("arn:aws:iam::123456789012:user/alice"),
		},
		Resource: &types.Resource{
			Database: &types.DatabaseResource{Name: aws.String("salesdb")},
		},
		Permissions: []types.Permission{types.PermissionDescribe},
	})
	require.NoError(t, err)

	out, err := client.GetEffectivePermissionsForPath(
		t.Context(),
		&lakeformationsdk.GetEffectivePermissionsForPathInput{
			ResourceArn: aws.String("arn:aws:glue:us-east-1:123456789012:database/salesdb"),
		},
	)
	require.NoError(t, err)
	require.Len(t, out.Permissions, 1)
	assert.Equal(t, "arn:aws:iam::123456789012:user/alice",
		aws.ToString(out.Permissions[0].Principal.DataLakePrincipalIdentifier))
}

// TestListLFTags_ResourceShareType_Foreign proves ListLFTags honours
// ResourceShareType. ListLFTagsInput.ResourceShareType (api_op_ListLFTags.go,
// lakeformation@v1.50.4) is FOREIGN|ALL: "If resource share type is FOREIGN,
// returns all share LF-tags that the requester can view." This backend
// models a single account with no RAM cross-account sharing, so no LF-tag is
// ever foreign -- FOREIGN must return none of the account's own tags.
func TestListLFTags_ResourceShareType_Foreign(t *testing.T) {
	t.Parallel()

	h := lakeformation.NewHandler(lakeformation.NewInMemoryBackend())
	client := newTestLakeFormationClient(t, h)

	_, err := client.CreateLFTag(t.Context(), &lakeformationsdk.CreateLFTagInput{
		TagKey:    aws.String("env"),
		TagValues: []string{"dev"},
	})
	require.NoError(t, err)

	out, err := client.ListLFTags(t.Context(), &lakeformationsdk.ListLFTagsInput{
		ResourceShareType: types.ResourceShareTypeForeign,
	})
	require.NoError(t, err)
	assert.Empty(t, out.LFTags)
}

// TestListResources_FilterConditionList proves ListResources honours
// FilterConditionList. ListResourcesInput.FilterConditionList
// (api_op_ListResources.go, lakeformation@v1.50.4) filters on RESOURCE_ARN,
// ROLE_ARN or LAST_MODIFIED via a ComparisonOperator -- previously not even
// parsed into the wire request struct, so every registered resource always
// came back regardless of the filter.
func TestListResources_FilterConditionList(t *testing.T) {
	t.Parallel()

	h := lakeformation.NewHandler(lakeformation.NewInMemoryBackend())
	client := newTestLakeFormationClient(t, h)

	_, err := client.RegisterResource(t.Context(), &lakeformationsdk.RegisterResourceInput{
		ResourceArn: aws.String("arn:aws:s3:::bucket-a"),
		RoleArn:     aws.String("arn:aws:iam::123456789012:role/role-a"),
	})
	require.NoError(t, err)

	_, err = client.RegisterResource(t.Context(), &lakeformationsdk.RegisterResourceInput{
		ResourceArn: aws.String("arn:aws:s3:::bucket-b"),
		RoleArn:     aws.String("arn:aws:iam::123456789012:role/role-b"),
	})
	require.NoError(t, err)

	out, err := client.ListResources(t.Context(), &lakeformationsdk.ListResourcesInput{
		FilterConditionList: []types.FilterCondition{
			{
				Field:              types.FieldNameStringResourceArn,
				ComparisonOperator: types.ComparisonOperatorEq,
				StringValueList:    []string{"arn:aws:s3:::bucket-a"},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.ResourceInfoList, 1)
	assert.Equal(t, "arn:aws:s3:::bucket-a", aws.ToString(out.ResourceInfoList[0].ResourceArn))
}
