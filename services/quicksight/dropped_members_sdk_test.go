package quicksight_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	quicksightsdk "github.com/aws/aws-sdk-go-v2/service/quicksight"
	"github.com/aws/aws-sdk-go-v2/service/quicksight/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

type qsDroppedEnv struct {
	backend *quicksight.InMemoryBackend
	client  *quicksightsdk.Client
}

func newQSDroppedEnv(t *testing.T) qsDroppedEnv {
	t.Helper()

	backend := quicksight.NewInMemoryBackend(qsTestAccountID, rtQSTestRegion)

	return qsDroppedEnv{backend: backend, client: newTestQuickSightClient(t, quicksight.NewHandler(backend))}
}

func (e qsDroppedEnv) createFolder(t *testing.T, id string) string {
	t.Helper()

	out, err := e.client.CreateFolder(t.Context(), &quicksightsdk.CreateFolderInput{
		AwsAccountId: aws.String(
			qsTestAccountID,
		), FolderId: aws.String(id), Name: aws.String(id), FolderType: types.FolderTypeShared,
	})
	require.NoError(t, err)

	return aws.ToString(out.Arn)
}

func (e qsDroppedEnv) folderMembers(t *testing.T, id string) []string {
	t.Helper()

	out, err := e.client.ListFolderMembers(t.Context(), &quicksightsdk.ListFolderMembersInput{
		AwsAccountId: aws.String(qsTestAccountID), FolderId: aws.String(id),
	})
	require.NoError(t, err)

	ids := make([]string, 0, len(out.FolderMemberList))
	for _, m := range out.FolderMemberList {
		ids = append(ids, aws.ToString(m.MemberId))
	}

	return ids
}

func s3DataSourceParams() types.DataSourceParameters {
	return &types.DataSourceParametersMemberS3Parameters{Value: types.S3Parameters{
		ManifestFileLocation: &types.ManifestFileLocation{Bucket: aws.String("bkt"), Key: aws.String("manifest.json")},
	}}
}

func TestDroppedMembers_DataSourceAndDataSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, e qsDroppedEnv)
		name string
	}{
		{
			name: "data source parameters, ssl, vpc and secret arn round trip and update",
			run: func(t *testing.T, e qsDroppedEnv) {
				t.Helper()

				folderArn := e.createFolder(t, "ds-folder")
				_, err := e.client.CreateDataSource(t.Context(), &quicksightsdk.CreateDataSourceInput{
					AwsAccountId:         aws.String(qsTestAccountID),
					DataSourceId:         aws.String("src"),
					Name:                 aws.String("src"),
					Type:                 types.DataSourceTypeS3,
					DataSourceParameters: s3DataSourceParams(),
					SslProperties:        &types.SslProperties{DisableSsl: true},
					Credentials: &types.DataSourceCredentials{
						SecretArn: aws.String("arn:aws:secretsmanager:us-east-1:000000000000:secret:s"),
					},
					VpcConnectionProperties: &types.VpcConnectionProperties{
						VpcConnectionArn: aws.String("arn:aws:quicksight:us-east-1:000000000000:vpcConnection/vpc1"),
					},
					FolderArns: []string{folderArn},
				})
				require.NoError(t, err)

				got, err := e.client.DescribeDataSource(t.Context(), &quicksightsdk.DescribeDataSourceInput{
					AwsAccountId: aws.String(qsTestAccountID), DataSourceId: aws.String("src"),
				})
				require.NoError(t, err)

				ds := got.DataSource
				s3, ok := ds.DataSourceParameters.(*types.DataSourceParametersMemberS3Parameters)
				require.True(t, ok)
				assert.Equal(t, "bkt", aws.ToString(s3.Value.ManifestFileLocation.Bucket))
				assert.True(t, ds.SslProperties.DisableSsl)
				assert.Contains(t, aws.ToString(ds.VpcConnectionProperties.VpcConnectionArn), "vpc1")
				assert.Contains(t, aws.ToString(ds.SecretArn), "secret:s")
				assert.Equal(t, []string{"src"}, e.folderMembers(t, "ds-folder"))

				_, err = e.client.UpdateDataSource(t.Context(), &quicksightsdk.UpdateDataSourceInput{
					AwsAccountId: aws.String(
						qsTestAccountID,
					), DataSourceId: aws.String("src"), Name: aws.String("renamed"),
					SslProperties: &types.SslProperties{DisableSsl: false},
				})
				require.NoError(t, err)

				got, err = e.client.DescribeDataSource(t.Context(), &quicksightsdk.DescribeDataSourceInput{
					AwsAccountId: aws.String(qsTestAccountID), DataSourceId: aws.String("src"),
				})
				require.NoError(t, err)
				assert.Equal(t, "renamed", aws.ToString(got.DataSource.Name))
				assert.False(t, got.DataSource.SslProperties.DisableSsl)
				assert.NotNil(t, got.DataSource.DataSourceParameters, "an update that omits parameters keeps them")

				listed, err := e.client.ListDataSources(
					t.Context(),
					&quicksightsdk.ListDataSourcesInput{AwsAccountId: aws.String(qsTestAccountID)},
				)
				require.NoError(t, err)
				require.Len(t, listed.DataSources, 1)
				assert.NotNil(t, listed.DataSources[0].DataSourceParameters)
				assert.Equal(t, types.ResourceStatusUpdateSuccessful, listed.DataSources[0].Status)
			},
		},
		{
			name: "unknown data source type and missing folder are rejected",
			run: func(t *testing.T, e qsDroppedEnv) {
				t.Helper()

				_, err := e.client.CreateDataSource(t.Context(), &quicksightsdk.CreateDataSourceInput{
					AwsAccountId: aws.String(qsTestAccountID), DataSourceId: aws.String("bad"), Name: aws.String("bad"),
					Type: types.DataSourceType("NOT_A_SOURCE"),
				})
				var invalid *types.InvalidParameterValueException
				require.ErrorAs(t, err, &invalid)

				_, err = e.client.CreateDataSource(t.Context(), &quicksightsdk.CreateDataSourceInput{
					AwsAccountId: aws.String(
						qsTestAccountID,
					), DataSourceId: aws.String("nofolder"), Name: aws.String("nofolder"),
					Type: types.DataSourceTypeS3, DataSourceParameters: s3DataSourceParams(),
					FolderArns: []string{"arn:aws:quicksight:us-east-1:000000000000:folder/missing"},
				})
				var nf *types.ResourceNotFoundException
				require.ErrorAs(t, err, &nf)

				_, err = e.client.DescribeDataSource(t.Context(), &quicksightsdk.DescribeDataSourceInput{
					AwsAccountId: aws.String(qsTestAccountID), DataSourceId: aws.String("nofolder"),
				})
				require.ErrorAs(t, err, &nf, "a failed create leaves no data source behind")
			},
		},
		{
			name: "data set configuration members round trip, update replaces and folders receive the data set",
			run: func(t *testing.T, e qsDroppedEnv) {
				t.Helper()

				folderArn := e.createFolder(t, "set-folder")
				_, err := e.client.CreateDataSet(t.Context(), &quicksightsdk.CreateDataSetInput{
					AwsAccountId:     aws.String(qsTestAccountID),
					DataSetId:        aws.String("set"),
					Name:             aws.String("set"),
					ImportMode:       types.DataSetImportModeDirectQuery,
					PhysicalTableMap: securityTestTables(),
					FolderArns:       []string{folderArn},
					DataSetUsageConfiguration: &types.DataSetUsageConfiguration{
						DisableUseAsDirectQuerySource: true,
					},
					FieldFolders: map[string]types.FieldFolder{
						"f1": {Description: aws.String("folder"), Columns: []string{"id"}},
					},
					PerformanceConfiguration: &types.PerformanceConfiguration{
						UniqueKeys: []types.UniqueKey{{ColumnNames: []string{"id"}}},
					},
					ColumnGroups: []types.ColumnGroup{{GeoSpatialColumnGroup: &types.GeoSpatialColumnGroup{
						Name: aws.String("geo"), CountryCode: types.GeoSpatialCountryCodeUs, Columns: []string{"id"},
					}}},
				})
				require.NoError(t, err)

				got, err := e.client.DescribeDataSet(t.Context(), &quicksightsdk.DescribeDataSetInput{
					AwsAccountId: aws.String(qsTestAccountID), DataSetId: aws.String("set"),
				})
				require.NoError(t, err)

				ds := got.DataSet
				require.NotNil(t, ds.DataSetUsageConfiguration)
				assert.True(t, ds.DataSetUsageConfiguration.DisableUseAsDirectQuerySource)
				assert.Equal(t, "folder", aws.ToString(ds.FieldFolders["f1"].Description))
				require.Len(t, ds.PerformanceConfiguration.UniqueKeys, 1)
				require.Len(t, ds.ColumnGroups, 1)
				require.NotNil(t, ds.ColumnGroups[0].GeoSpatialColumnGroup)
				assert.Equal(t, "geo", aws.ToString(ds.ColumnGroups[0].GeoSpatialColumnGroup.Name))
				assert.Equal(t, []string{"set"}, e.folderMembers(t, "set-folder"))

				_, err = e.client.UpdateDataSet(t.Context(), &quicksightsdk.UpdateDataSetInput{
					AwsAccountId: aws.String(qsTestAccountID), DataSetId: aws.String("set"), Name: aws.String("set"),
					ImportMode: types.DataSetImportModeDirectQuery, PhysicalTableMap: securityTestTables(),
					DataSetUsageConfiguration: &types.DataSetUsageConfiguration{DisableUseAsImportedSource: true},
				})
				require.NoError(t, err)

				got, err = e.client.DescribeDataSet(t.Context(), &quicksightsdk.DescribeDataSetInput{
					AwsAccountId: aws.String(qsTestAccountID), DataSetId: aws.String("set"),
				})
				require.NoError(t, err)
				assert.True(t, got.DataSet.DataSetUsageConfiguration.DisableUseAsImportedSource)
				assert.False(t, got.DataSet.DataSetUsageConfiguration.DisableUseAsDirectQuerySource)
				assert.Empty(t, got.DataSet.FieldFolders, "update is a full replace of the optional members")
			},
		},
		{
			name: "analysis, dashboard and topic join the folders they name",
			run: func(t *testing.T, e qsDroppedEnv) {
				t.Helper()

				folderArn := e.createFolder(t, "multi-folder")

				_, err := e.client.CreateAnalysis(t.Context(), &quicksightsdk.CreateAnalysisInput{
					AwsAccountId: aws.String(qsTestAccountID), AnalysisId: aws.String("an"), Name: aws.String("an"),
					Definition: &types.AnalysisDefinition{
						DataSetIdentifierDeclarations: minimalDataSetIdentifierDeclarations(),
					},
					FolderArns: []string{folderArn},
				})
				require.NoError(t, err)

				_, err = e.client.CreateDashboard(t.Context(), &quicksightsdk.CreateDashboardInput{
					AwsAccountId: aws.String(
						qsTestAccountID,
					), DashboardId: aws.String("dash"), Name: aws.String("dash"),
					Definition: &types.DashboardVersionDefinition{
						DataSetIdentifierDeclarations: minimalDataSetIdentifierDeclarations(),
					},
					FolderArns: []string{folderArn},
				})
				require.NoError(t, err)

				_, err = e.client.CreateTopic(t.Context(), &quicksightsdk.CreateTopicInput{
					AwsAccountId: aws.String(qsTestAccountID), TopicId: aws.String("topic"),
					Topic: &types.TopicDetails{Name: aws.String("topic")}, FolderArns: []string{folderArn},
				})
				require.NoError(t, err)

				assert.ElementsMatch(t, []string{"an", "dash", "topic"}, e.folderMembers(t, "multi-folder"))

				_, err = e.client.CreateTopic(t.Context(), &quicksightsdk.CreateTopicInput{
					AwsAccountId: aws.String(qsTestAccountID), TopicId: aws.String("orphan"),
					Topic:      &types.TopicDetails{Name: aws.String("orphan")},
					FolderArns: []string{"arn:aws:quicksight:us-east-1:000000000000:folder/missing"},
				})
				var nf *types.ResourceNotFoundException
				require.ErrorAs(t, err, &nf)
			},
		},
		{
			name: "restore analysis re-joins folders only with RestoreToFolders",
			run: func(t *testing.T, e qsDroppedEnv) {
				t.Helper()

				folderArn := e.createFolder(t, "restore-folder")

				for _, id := range []string{"keep", "drop"} {
					_, err := e.client.CreateAnalysis(t.Context(), &quicksightsdk.CreateAnalysisInput{
						AwsAccountId: aws.String(qsTestAccountID), AnalysisId: aws.String(id), Name: aws.String(id),
						Definition: &types.AnalysisDefinition{
							DataSetIdentifierDeclarations: minimalDataSetIdentifierDeclarations(),
						},
						FolderArns: []string{folderArn},
					})
					require.NoError(t, err)

					_, err = e.client.DeleteAnalysis(t.Context(), &quicksightsdk.DeleteAnalysisInput{
						AwsAccountId: aws.String(qsTestAccountID), AnalysisId: aws.String(id),
					})
					require.NoError(t, err)
				}

				_, err := e.client.RestoreAnalysis(t.Context(), &quicksightsdk.RestoreAnalysisInput{
					AwsAccountId: aws.String(qsTestAccountID), AnalysisId: aws.String("keep"), RestoreToFolders: true,
				})
				require.NoError(t, err)

				_, err = e.client.RestoreAnalysis(t.Context(), &quicksightsdk.RestoreAnalysisInput{
					AwsAccountId: aws.String(qsTestAccountID), AnalysisId: aws.String("drop"),
				})
				require.NoError(t, err)

				assert.Equal(t, []string{"keep"}, e.folderMembers(t, "restore-folder"))
			},
		},
		{
			name: "ingestion type is echoed as request type",
			run: func(t *testing.T, e qsDroppedEnv) {
				t.Helper()

				_, err := e.client.CreateDataSet(t.Context(), &quicksightsdk.CreateDataSetInput{
					AwsAccountId: aws.String(qsTestAccountID), DataSetId: aws.String("ing"), Name: aws.String("ing"),
					ImportMode: types.DataSetImportModeDirectQuery, PhysicalTableMap: securityTestTables(),
				})
				require.NoError(t, err)

				_, err = e.client.CreateIngestion(t.Context(), &quicksightsdk.CreateIngestionInput{
					AwsAccountId: aws.String(
						qsTestAccountID,
					), DataSetId: aws.String("ing"), IngestionId: aws.String("i1"),
					IngestionType: types.IngestionTypeIncrementalRefresh,
				})
				require.NoError(t, err)

				got, err := e.client.DescribeIngestion(t.Context(), &quicksightsdk.DescribeIngestionInput{
					AwsAccountId: aws.String(
						qsTestAccountID,
					), DataSetId: aws.String("ing"), IngestionId: aws.String("i1"),
				})
				require.NoError(t, err)
				assert.Equal(t, types.IngestionRequestTypeIncrementalRefresh, got.Ingestion.RequestType)
				assert.Equal(t, types.IngestionRequestSourceManual, got.Ingestion.RequestSource)

				_, err = e.client.CreateIngestion(t.Context(), &quicksightsdk.CreateIngestionInput{
					AwsAccountId: aws.String(
						qsTestAccountID,
					), DataSetId: aws.String("ing"), IngestionId: aws.String("i2"),
					IngestionType: types.IngestionType("SOMETIMES"),
				})
				var invalid *types.InvalidParameterValueException
				require.ErrorAs(t, err, &invalid)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t, newQSDroppedEnv(t))
		})
	}
}

func TestDroppedMembers_UsersEmbedAndSubscription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, e qsDroppedEnv)
		name string
	}{
		{
			name: "user federation settings round trip, clear with NONE and custom permissions unapply",
			run: func(t *testing.T, e qsDroppedEnv) {
				t.Helper()

				_, err := e.client.CreateCustomPermissions(t.Context(), &quicksightsdk.CreateCustomPermissionsInput{
					AwsAccountId: aws.String(qsTestAccountID), CustomPermissionsName: aws.String("cp"),
					Capabilities: &types.Capabilities{ExportToCsv: types.CapabilityStateDeny},
				})
				require.NoError(t, err)

				reg, err := e.client.RegisterUser(t.Context(), &quicksightsdk.RegisterUserInput{
					AwsAccountId: aws.String(
						qsTestAccountID,
					), Namespace: aws.String("default"), IdentityType: types.IdentityTypeQuicksight,
					Email: aws.String("fed@example.com"), UserRole: types.UserRoleReader, UserName: aws.String("fed"),
					ExternalLoginFederationProviderType: aws.String("CUSTOM_OIDC"),
					CustomFederationProviderUrl:         aws.String("https://idp.example.com"),
					ExternalLoginId:                     aws.String("ext-1"),
					CustomPermissionsName:               aws.String("cp"),
				})
				require.NoError(t, err)
				assert.Equal(t, "CUSTOM_OIDC", aws.ToString(reg.User.ExternalLoginFederationProviderType))
				assert.Equal(t, "https://idp.example.com", aws.ToString(reg.User.ExternalLoginFederationProviderUrl))
				assert.Equal(t, "ext-1", aws.ToString(reg.User.ExternalLoginId))

				updated, err := e.client.UpdateUser(t.Context(), &quicksightsdk.UpdateUserInput{
					AwsAccountId: aws.String(
						qsTestAccountID,
					), Namespace: aws.String("default"), UserName: aws.String("fed"),
					Email: aws.String("fed@example.com"), Role: types.UserRoleReader,
					ExternalLoginFederationProviderType: aws.String("COGNITO"), UnapplyCustomPermissions: true,
				})
				require.NoError(t, err)
				assert.Equal(t, "COGNITO", aws.ToString(updated.User.ExternalLoginFederationProviderType))
				assert.Empty(t, aws.ToString(updated.User.ExternalLoginFederationProviderUrl))
				assert.Empty(t, aws.ToString(updated.User.CustomPermissionsName))

				cleared, err := e.client.UpdateUser(t.Context(), &quicksightsdk.UpdateUserInput{
					AwsAccountId: aws.String(
						qsTestAccountID,
					), Namespace: aws.String("default"), UserName: aws.String("fed"),
					Email: aws.String("fed@example.com"), Role: types.UserRoleReader,
					ExternalLoginFederationProviderType: aws.String("NONE"),
				})
				require.NoError(t, err)
				assert.Empty(t, aws.ToString(cleared.User.ExternalLoginFederationProviderType))
				assert.Empty(t, aws.ToString(cleared.User.ExternalLoginId))

				_, err = e.client.RegisterUser(t.Context(), &quicksightsdk.RegisterUserInput{
					AwsAccountId: aws.String(
						qsTestAccountID,
					), Namespace: aws.String("default"), IdentityType: types.IdentityTypeQuicksight,
					Email: aws.String("bad@example.com"), UserRole: types.UserRoleReader, UserName: aws.String("bad"),
					ExternalLoginFederationProviderType: aws.String("COGNITO"),
					CustomFederationProviderUrl:         aws.String("https://idp.example.com"),
				})
				var invalid *types.InvalidParameterValueException
				require.ErrorAs(t, err, &invalid)
			},
		},
		{
			name: "embed url session lifetime and allowed domains are validated",
			run: func(t *testing.T, e qsDroppedEnv) {
				t.Helper()

				anon := func() *quicksightsdk.GenerateEmbedUrlForAnonymousUserInput {
					return &quicksightsdk.GenerateEmbedUrlForAnonymousUserInput{
						AwsAccountId:           aws.String(qsTestAccountID),
						Namespace:              aws.String("default"),
						AuthorizedResourceArns: []string{"arn:aws:quicksight:us-east-1:000000000000:dashboard/d"},
						ExperienceConfiguration: &types.AnonymousUserEmbeddingExperienceConfiguration{
							Dashboard: &types.AnonymousUserDashboardEmbeddingConfiguration{
								InitialDashboardId: aws.String("d"),
							},
						},
					}
				}

				tooShort := anon()
				tooShort.SessionLifetimeInMinutes = aws.Int64(14)
				_, err := e.client.GenerateEmbedUrlForAnonymousUser(t.Context(), tooShort)
				var badLifetime *types.SessionLifetimeInMinutesInvalidException
				require.ErrorAs(t, err, &badLifetime)
				var invalid *types.InvalidParameterValueException

				tooMany := anon()
				tooMany.AllowedDomains = []string{
					"https://a.example",
					"https://b.example",
					"https://c.example",
					"https://d.example",
				}
				_, err = e.client.GenerateEmbedUrlForAnonymousUser(t.Context(), tooMany)
				require.ErrorAs(t, err, &invalid)

				_, err = e.client.GetSessionEmbedUrl(t.Context(), &quicksightsdk.GetSessionEmbedUrlInput{
					AwsAccountId: aws.String(qsTestAccountID), SessionLifetimeInMinutes: aws.Int64(601),
				})
				require.ErrorAs(t, err, &badLifetime)

				ok, err := e.client.GetSessionEmbedUrl(t.Context(), &quicksightsdk.GetSessionEmbedUrlInput{
					AwsAccountId: aws.String(qsTestAccountID), SessionLifetimeInMinutes: aws.Int64(600),
				})
				require.NoError(t, err)
				assert.NotEmpty(t, aws.ToString(ok.EmbedUrl))
			},
		},
		{
			name: "account subscription requires documented members and echoes the identity center instance",
			run: func(t *testing.T, e qsDroppedEnv) {
				t.Helper()

				subscription := func(
					method types.AuthenticationMethodOption, edition types.Edition,
				) *quicksightsdk.CreateAccountSubscriptionInput {
					return &quicksightsdk.CreateAccountSubscriptionInput{
						AwsAccountId:         aws.String(qsTestAccountID),
						AccountName:          aws.String("acct"),
						AuthenticationMethod: method,
						NotificationEmail:    aws.String("n@example.com"),
						Edition:              edition,
					}
				}

				_, err := e.client.CreateAccountSubscription(t.Context(),
					subscription(types.AuthenticationMethodOptionActiveDirectory, types.EditionEnterprise))
				var invalid *types.InvalidParameterValueException
				require.ErrorAs(t, err, &invalid)
				assert.Contains(t, err.Error(), "ActiveDirectoryName")

				_, err = e.client.CreateAccountSubscription(t.Context(),
					subscription(types.AuthenticationMethodOptionIamAndQuicksight, types.EditionEnterpriseAndQ))
				require.ErrorAs(t, err, &invalid)
				assert.Contains(t, err.Error(), "ContactNumber")

				center := subscription(types.AuthenticationMethodOptionIamIdentityCenter, types.EditionEnterprise)
				center.AdminGroup = []string{"admins"}
				center.IAMIdentityCenterInstanceArn = aws.String("arn:aws:sso:::instance/ssoins-1")
				_, err = e.client.CreateAccountSubscription(t.Context(), center)
				require.NoError(t, err)

				got, err := e.client.DescribeAccountSubscription(
					t.Context(),
					&quicksightsdk.DescribeAccountSubscriptionInput{
						AwsAccountId: aws.String(qsTestAccountID),
					},
				)
				require.NoError(t, err)
				assert.Equal(
					t,
					"arn:aws:sso:::instance/ssoins-1",
					aws.ToString(got.AccountInfo.IAMIdentityCenterInstanceArn),
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t, newQSDroppedEnv(t))
		})
	}
}
