package dms_test

import (
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	dmssdk "github.com/aws/aws-sdk-go-v2/service/databasemigrationservice"
	"github.com/aws/aws-sdk-go-v2/service/databasemigrationservice/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/dms"
)

const tagsRTRegion = "us-east-1"

const tagsRTAccountID = "000000000000"

// newTestDMSClient stands up the real aws-sdk-go-v2 DMS client against an
// httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production.
func newTestDMSClient(t *testing.T, h *dms.Handler) *dmssdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(tagsRTRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return dmssdk.NewFromConfig(cfg, func(o *dmssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOpsWithTags_RoundTrip drives every dms Create* op whose real
// Input struct accepts Tags (databasemigrationservice@v1.66.4:
// api_op_CreateReplicationInstance.go:157, CreateEndpoint.go:242,
// CreateReplicationTask.go:127, CreateDataMigration.go:73,
// CreateDataProvider.go:56, CreateInstanceProfile.go:69,
// CreateMigrationProject.go:69, CreateReplicationSubnetGroup.go:67,
// CreateReplicationConfig.go:106) through the real SDK client and asserts
// ListTagsForResource sees what was supplied at creation (gopherstack-2mwl).
// CreateFleetAdvisorCollector takes no Tags in the real SDK and is excluded.
//
// CreateEventSubscription also accepts Tags in the real SDK
// (CreateEventSubscription.go:87) but is deliberately NOT covered here:
// gopherstack decodes and stores the tags (event_subscriptions.go:40-43) but
// findResourceTags (tags.go) has no case that can ever find them again,
// because types.EventSubscription carries no ARN field in the pinned SDK, so
// there is no SDK-verifiable ARN to construct TagResource/ListTagsForResource
// calls against -- the same "cannot verify without live AWS" gap sesv2's
// CreateMultiRegionEndpoint hit (gopherstack-uljk). Tracked, not fixed here.
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, client *dmssdk.Client) string
		name  string
	}{
		{
			name: "replication instance",
			setup: func(t *testing.T, client *dmssdk.Client) string {
				t.Helper()
				out, err := client.CreateReplicationInstance(t.Context(), &dmssdk.CreateReplicationInstanceInput{
					ReplicationInstanceIdentifier: aws.String("tagged-repl-instance"),
					ReplicationInstanceClass:      aws.String("dms.t3.micro"),
					Tags:                          []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.ReplicationInstance.ReplicationInstanceArn)
			},
		},
		{
			name: "endpoint",
			setup: func(t *testing.T, client *dmssdk.Client) string {
				t.Helper()
				out, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
					EndpointIdentifier: aws.String("tagged-endpoint"),
					EndpointType:       types.ReplicationEndpointTypeValueSource,
					EngineName:         aws.String("mysql"),
					Tags:               []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.Endpoint.EndpointArn)
			},
		},
		{
			name: "replication task",
			setup: func(t *testing.T, client *dmssdk.Client) string {
				t.Helper()
				src, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
					EndpointIdentifier: aws.String("rt-source-endpoint"),
					EndpointType:       types.ReplicationEndpointTypeValueSource,
					EngineName:         aws.String("mysql"),
				})
				require.NoError(t, err)
				tgt, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
					EndpointIdentifier: aws.String("rt-target-endpoint"),
					EndpointType:       types.ReplicationEndpointTypeValueTarget,
					EngineName:         aws.String("mysql"),
				})
				require.NoError(t, err)
				inst, err := client.CreateReplicationInstance(t.Context(), &dmssdk.CreateReplicationInstanceInput{
					ReplicationInstanceIdentifier: aws.String("rt-source-instance"),
					ReplicationInstanceClass:      aws.String("dms.t3.micro"),
				})
				require.NoError(t, err)

				out, err := client.CreateReplicationTask(t.Context(), &dmssdk.CreateReplicationTaskInput{
					ReplicationTaskIdentifier: aws.String("tagged-repl-task"),
					SourceEndpointArn:         src.Endpoint.EndpointArn,
					TargetEndpointArn:         tgt.Endpoint.EndpointArn,
					ReplicationInstanceArn:    inst.ReplicationInstance.ReplicationInstanceArn,
					MigrationType:             types.MigrationTypeValueFullLoad,
					TableMappings:             aws.String(`{"rules":[]}`),
					Tags:                      []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.ReplicationTask.ReplicationTaskArn)
			},
		},
		{
			name: "data migration",
			setup: func(t *testing.T, client *dmssdk.Client) string {
				t.Helper()
				out, err := client.CreateDataMigration(t.Context(), &dmssdk.CreateDataMigrationInput{
					DataMigrationName:          aws.String("tagged-data-migration"),
					DataMigrationType:          types.MigrationTypeValueFullLoad,
					MigrationProjectIdentifier: aws.String("dummy-migration-project"),
					ServiceAccessRoleArn:       aws.String("arn:aws:iam::000000000000:role/dms-access"),
					Tags:                       []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.DataMigration.DataMigrationArn)
			},
		},
		{
			name: "data provider",
			setup: func(t *testing.T, client *dmssdk.Client) string {
				t.Helper()
				out, err := client.CreateDataProvider(t.Context(), &dmssdk.CreateDataProviderInput{
					DataProviderName: aws.String("tagged-data-provider"),
					Engine:           aws.String("mysql"),
					Settings: &types.DataProviderSettingsMemberMySqlSettings{
						Value: types.MySqlDataProviderSettings{},
					},
					Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.DataProvider.DataProviderArn)
			},
		},
		{
			name: "instance profile",
			setup: func(t *testing.T, client *dmssdk.Client) string {
				t.Helper()
				out, err := client.CreateInstanceProfile(t.Context(), &dmssdk.CreateInstanceProfileInput{
					InstanceProfileName: aws.String("tagged-instance-profile"),
					Tags:                []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.InstanceProfile.InstanceProfileArn)
			},
		},
		{
			name: "migration project",
			setup: func(t *testing.T, client *dmssdk.Client) string {
				t.Helper()
				out, err := client.CreateMigrationProject(t.Context(), &dmssdk.CreateMigrationProjectInput{
					MigrationProjectName: aws.String("tagged-migration-project"),
					InstanceProfileIdentifier: aws.String(
						"arn:aws:dms:us-east-1:000000000000:instance-profile:dummy",
					),
					SourceDataProviderDescriptors: []types.DataProviderDescriptorDefinition{
						{DataProviderIdentifier: aws.String("dummy-source-provider")},
					},
					TargetDataProviderDescriptors: []types.DataProviderDescriptorDefinition{
						{DataProviderIdentifier: aws.String("dummy-target-provider")},
					},
					Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.MigrationProject.MigrationProjectArn)
			},
		},
		{
			// ReplicationSubnetGroup has no Arn field on the real SDK response
			// type (types.ReplicationSubnetGroup), so it can't be read back from
			// the Create response the way the others are. gopherstack tags it
			// under arn.Build("dms", region, account, "subgrp:"+identifier)
			// (replication_subnet_groups.go), the same "subgrp:" convention RDS/
			// Neptune/DocDB use for their subnet groups; reconstructing that same
			// ARN here tests the create-write/list-read symmetry that matters for
			// gopherstack-2mwl even though the SDK gives no field to read it from.
			name: "replication subnet group",
			setup: func(t *testing.T, client *dmssdk.Client) string {
				t.Helper()
				_, err := client.CreateReplicationSubnetGroup(t.Context(), &dmssdk.CreateReplicationSubnetGroupInput{
					ReplicationSubnetGroupIdentifier:  aws.String("tagged-subnet-group"),
					ReplicationSubnetGroupDescription: aws.String("test"),
					SubnetIds:                         []string{"subnet-1"},
					Tags:                              []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				return fmt.Sprintf(
					"arn:aws:dms:%s:%s:subgrp:tagged-subnet-group",
					tagsRTRegion, tagsRTAccountID,
				)
			},
		},
		{
			name: "replication config",
			setup: func(t *testing.T, client *dmssdk.Client) string {
				t.Helper()
				out, err := client.CreateReplicationConfig(t.Context(), &dmssdk.CreateReplicationConfigInput{
					ReplicationConfigIdentifier: aws.String("tagged-repl-config"),
					ReplicationType:             types.MigrationTypeValueFullLoad,
					SourceEndpointArn:           aws.String("arn:aws:dms:us-east-1:000000000000:endpoint:src"),
					TargetEndpointArn:           aws.String("arn:aws:dms:us-east-1:000000000000:endpoint:tgt"),
					ComputeConfig:               &types.ComputeConfig{},
					TableMappings:               aws.String(`{"rules":[]}`),
					Tags:                        []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.ReplicationConfig.ReplicationConfigArn)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := dms.NewInMemoryBackend(tagsRTAccountID, tagsRTRegion)
			h := dms.NewHandler(backend)
			client := newTestDMSClient(t, h)

			resourceARN := tt.setup(t, client)
			require.NotEmpty(t, resourceARN)

			out, err := client.ListTagsForResource(t.Context(), &dmssdk.ListTagsForResourceInput{
				ResourceArn: aws.String(resourceARN),
			})
			require.NoError(t, err)

			require.Len(t, out.TagList, 1)
			assert.Equal(t, "env", aws.ToString(out.TagList[0].Key))
			assert.Equal(t, "prod", aws.ToString(out.TagList[0].Value))
		})
	}
}
