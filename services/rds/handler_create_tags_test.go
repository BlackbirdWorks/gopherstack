package rds_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/rds"
)

// newTestRDSClient stands up the real aws-sdk-go-v2 RDS client against an
// httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production.
func newTestRDSClient(t *testing.T, h *rds.Handler) *rdssdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(config.DefaultRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return rdssdk.NewFromConfig(cfg, func(o *rdssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

func newTestRDSHandler() *rds.Handler {
	return rds.NewHandler(rds.NewInMemoryBackend("123456789012", config.DefaultRegion))
}

// TestCreateOps_TagsRoundTrip drives every RDS Create op whose real Input
// struct accepts Tags (rds@v1.124.1) through the real SDK client and asserts
// ListTagsForResource sees what was supplied at creation (gopherstack-2mwl).
// None of these 20 ops applied Tags at creation before this fix -- a pure
// decode-drop identical in shape to the forecast bug: Tags was accepted and
// silently discarded, and 8 of the 20 resource kinds (DBSubnetGroup,
// OptionGroup, DBSecurityGroup, DBClusterEndpoint, GlobalCluster,
// CustomDBEngineVersion, EventSubscription, BlueGreenDeployment) additionally
// never exposed their own ARN in the wire response at all, so even a fixed
// decode would have been unreachable by any real client (the docdb
// DBClusterSnapshot-ARN shape from the same issue).
func TestCreateOps_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	tags := []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}}

	requireTags := func(t *testing.T, client *rdssdk.Client, resourceName *string) {
		t.Helper()
		out, err := client.ListTagsForResource(t.Context(), &rdssdk.ListTagsForResourceInput{
			ResourceName: resourceName,
		})
		require.NoError(t, err)
		require.Len(t, out.TagList, 1)
		assert.Equal(t, "env", aws.ToString(out.TagList[0].Key))
		assert.Equal(t, "prod", aws.ToString(out.TagList[0].Value))
	}

	t.Run("createdbinstance", func(t *testing.T) {
		t.Parallel()

		client := newTestRDSClient(t, newTestRDSHandler())

		out, err := client.CreateDBInstance(t.Context(), &rdssdk.CreateDBInstanceInput{
			DBInstanceIdentifier: aws.String("tagged-db"),
			DBInstanceClass:      aws.String("db.t3.micro"),
			Engine:               aws.String("postgres"),
			Tags:                 tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.DBInstance.DBInstanceArn)
	})

	t.Run("createdbinstancereadreplica", func(t *testing.T) {
		t.Parallel()

		h := newTestRDSHandler()
		client := newTestRDSClient(t, h)

		_, err := client.CreateDBInstance(t.Context(), &rdssdk.CreateDBInstanceInput{
			DBInstanceIdentifier: aws.String("source-db"),
			DBInstanceClass:      aws.String("db.t3.micro"),
			Engine:               aws.String("postgres"),
		})
		require.NoError(t, err)

		out, err := client.CreateDBInstanceReadReplica(t.Context(), &rdssdk.CreateDBInstanceReadReplicaInput{
			DBInstanceIdentifier:       aws.String("tagged-replica"),
			SourceDBInstanceIdentifier: aws.String("source-db"),
			Tags:                       tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.DBInstance.DBInstanceArn)
	})

	t.Run("createdbsnapshot", func(t *testing.T) {
		t.Parallel()

		h := newTestRDSHandler()
		client := newTestRDSClient(t, h)

		_, err := client.CreateDBInstance(t.Context(), &rdssdk.CreateDBInstanceInput{
			DBInstanceIdentifier: aws.String("snap-source-db"),
			DBInstanceClass:      aws.String("db.t3.micro"),
			Engine:               aws.String("postgres"),
		})
		require.NoError(t, err)

		out, err := client.CreateDBSnapshot(t.Context(), &rdssdk.CreateDBSnapshotInput{
			DBSnapshotIdentifier: aws.String("tagged-snap"),
			DBInstanceIdentifier: aws.String("snap-source-db"),
			Tags:                 tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.DBSnapshot.DBSnapshotArn)
	})

	t.Run("createdbsubnetgroup", func(t *testing.T) {
		t.Parallel()

		client := newTestRDSClient(t, newTestRDSHandler())

		out, err := client.CreateDBSubnetGroup(t.Context(), &rdssdk.CreateDBSubnetGroupInput{
			DBSubnetGroupName:        aws.String("tagged-subgrp"),
			DBSubnetGroupDescription: aws.String("test"),
			SubnetIds:                []string{"subnet-1", "subnet-2"},
			Tags:                     tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.DBSubnetGroup.DBSubnetGroupArn)
	})

	t.Run("createdbparametergroup", func(t *testing.T) {
		t.Parallel()

		client := newTestRDSClient(t, newTestRDSHandler())

		out, err := client.CreateDBParameterGroup(t.Context(), &rdssdk.CreateDBParameterGroupInput{
			DBParameterGroupName:   aws.String("tagged-pg"),
			DBParameterGroupFamily: aws.String("postgres16"),
			Description:            aws.String("test"),
			Tags:                   tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.DBParameterGroup.DBParameterGroupArn)
	})

	t.Run("createoptiongroup", func(t *testing.T) {
		t.Parallel()

		client := newTestRDSClient(t, newTestRDSHandler())

		out, err := client.CreateOptionGroup(t.Context(), &rdssdk.CreateOptionGroupInput{
			OptionGroupName:        aws.String("tagged-og"),
			EngineName:             aws.String("mysql"),
			MajorEngineVersion:     aws.String("8.0"),
			OptionGroupDescription: aws.String("test"),
			Tags:                   tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.OptionGroup.OptionGroupArn)
	})

	t.Run("createdbcluster", func(t *testing.T) {
		t.Parallel()

		client := newTestRDSClient(t, newTestRDSHandler())

		out, err := client.CreateDBCluster(t.Context(), &rdssdk.CreateDBClusterInput{
			DBClusterIdentifier: aws.String("tagged-cluster"),
			Engine:              aws.String("aurora-postgresql"),
			Tags:                tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.DBCluster.DBClusterArn)
	})

	t.Run("createdbclusterparametergroup", func(t *testing.T) {
		t.Parallel()

		client := newTestRDSClient(t, newTestRDSHandler())

		out, err := client.CreateDBClusterParameterGroup(t.Context(), &rdssdk.CreateDBClusterParameterGroupInput{
			DBClusterParameterGroupName: aws.String("tagged-cluster-pg"),
			DBParameterGroupFamily:      aws.String("aurora-postgresql16"),
			Description:                 aws.String("test"),
			Tags:                        tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.DBClusterParameterGroup.DBClusterParameterGroupArn)
	})

	t.Run("createdbclustersnapshot", func(t *testing.T) {
		t.Parallel()

		h := newTestRDSHandler()
		client := newTestRDSClient(t, h)

		_, err := client.CreateDBCluster(t.Context(), &rdssdk.CreateDBClusterInput{
			DBClusterIdentifier: aws.String("snap-source-cluster"),
			Engine:              aws.String("aurora-postgresql"),
		})
		require.NoError(t, err)

		out, err := client.CreateDBClusterSnapshot(t.Context(), &rdssdk.CreateDBClusterSnapshotInput{
			DBClusterSnapshotIdentifier: aws.String("tagged-cluster-snap"),
			DBClusterIdentifier:         aws.String("snap-source-cluster"),
			Tags:                        tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.DBClusterSnapshot.DBClusterSnapshotArn)
	})

	t.Run("createdbclusterendpoint", func(t *testing.T) {
		t.Parallel()

		h := newTestRDSHandler()
		client := newTestRDSClient(t, h)

		_, err := client.CreateDBCluster(t.Context(), &rdssdk.CreateDBClusterInput{
			DBClusterIdentifier: aws.String("endpoint-source-cluster"),
			Engine:              aws.String("aurora-postgresql"),
		})
		require.NoError(t, err)

		out, err := client.CreateDBClusterEndpoint(t.Context(), &rdssdk.CreateDBClusterEndpointInput{
			DBClusterEndpointIdentifier: aws.String("tagged-endpoint"),
			DBClusterIdentifier:         aws.String("endpoint-source-cluster"),
			EndpointType:                aws.String("ANY"),
			Tags:                        tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.DBClusterEndpointArn)
	})

	t.Run("createglobalcluster", func(t *testing.T) {
		t.Parallel()

		client := newTestRDSClient(t, newTestRDSHandler())

		out, err := client.CreateGlobalCluster(t.Context(), &rdssdk.CreateGlobalClusterInput{
			GlobalClusterIdentifier: aws.String("tagged-global-cluster"),
			Engine:                  aws.String("aurora-postgresql"),
			Tags:                    tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.GlobalCluster.GlobalClusterArn)
	})

	t.Run("createdbsecuritygroup", func(t *testing.T) {
		t.Parallel()

		client := newTestRDSClient(t, newTestRDSHandler())

		out, err := client.CreateDBSecurityGroup(t.Context(), &rdssdk.CreateDBSecurityGroupInput{
			DBSecurityGroupName:        aws.String("tagged-secgrp"),
			DBSecurityGroupDescription: aws.String("test"),
			Tags:                       tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.DBSecurityGroup.DBSecurityGroupArn)
	})

	t.Run("createeventsubscription", func(t *testing.T) {
		t.Parallel()

		client := newTestRDSClient(t, newTestRDSHandler())

		out, err := client.CreateEventSubscription(t.Context(), &rdssdk.CreateEventSubscriptionInput{
			SubscriptionName: aws.String("tagged-sub"),
			SnsTopicArn:      aws.String("arn:aws:sns:us-east-1:123456789012:topic"),
			Tags:             tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.EventSubscription.EventSubscriptionArn)
	})

	t.Run("createdbproxy", func(t *testing.T) {
		t.Parallel()

		client := newTestRDSClient(t, newTestRDSHandler())

		out, err := client.CreateDBProxy(t.Context(), &rdssdk.CreateDBProxyInput{
			DBProxyName:  aws.String("tagged-proxy"),
			EngineFamily: types.EngineFamilyMysql,
			RoleArn:      aws.String("arn:aws:iam::123456789012:role/proxy-role"),
			VpcSubnetIds: []string{"subnet-1"},
			Tags:         tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.DBProxy.DBProxyArn)
	})

	t.Run("createdbproxyendpoint", func(t *testing.T) {
		t.Parallel()

		h := newTestRDSHandler()
		client := newTestRDSClient(t, h)

		_, err := client.CreateDBProxy(t.Context(), &rdssdk.CreateDBProxyInput{
			DBProxyName:  aws.String("endpoint-source-proxy"),
			EngineFamily: types.EngineFamilyMysql,
			RoleArn:      aws.String("arn:aws:iam::123456789012:role/proxy-role"),
			VpcSubnetIds: []string{"subnet-1"},
		})
		require.NoError(t, err)

		out, err := client.CreateDBProxyEndpoint(t.Context(), &rdssdk.CreateDBProxyEndpointInput{
			DBProxyEndpointName: aws.String("tagged-proxy-endpoint"),
			DBProxyName:         aws.String("endpoint-source-proxy"),
			VpcSubnetIds:        []string{"subnet-1"},
			Tags:                tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.DBProxyEndpoint.DBProxyEndpointArn)
	})

	t.Run("createdbshardgroup", func(t *testing.T) {
		t.Parallel()

		h := newTestRDSHandler()
		client := newTestRDSClient(t, h)

		_, err := client.CreateDBCluster(t.Context(), &rdssdk.CreateDBClusterInput{
			DBClusterIdentifier: aws.String("shard-source-cluster"),
			Engine:              aws.String("aurora-postgresql"),
		})
		require.NoError(t, err)

		out, err := client.CreateDBShardGroup(t.Context(), &rdssdk.CreateDBShardGroupInput{
			DBShardGroupIdentifier: aws.String("tagged-shard-group"),
			DBClusterIdentifier:    aws.String("shard-source-cluster"),
			MaxACU:                 aws.Float64(4),
			Tags:                   tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.DBShardGroupArn)
	})

	t.Run("createcustomdbengineversion", func(t *testing.T) {
		t.Parallel()

		client := newTestRDSClient(t, newTestRDSHandler())

		out, err := client.CreateCustomDBEngineVersion(t.Context(), &rdssdk.CreateCustomDBEngineVersionInput{
			Engine:        aws.String("custom-oracle-ee"),
			EngineVersion: aws.String("19.cdbev1"),
			Tags:          tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.DBEngineVersionArn)
	})

	t.Run("createbluegreendeployment", func(t *testing.T) {
		t.Parallel()

		h := newTestRDSHandler()
		client := newTestRDSClient(t, h)

		_, err := client.CreateDBInstance(t.Context(), &rdssdk.CreateDBInstanceInput{
			DBInstanceIdentifier: aws.String("bgd-source-db"),
			DBInstanceClass:      aws.String("db.t3.micro"),
			Engine:               aws.String("mysql"),
		})
		require.NoError(t, err)

		out, err := client.CreateBlueGreenDeployment(t.Context(), &rdssdk.CreateBlueGreenDeploymentInput{
			BlueGreenDeploymentName: aws.String("tagged-bgd"),
			Source:                  aws.String("bgd-source-db"),
			Tags:                    tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.BlueGreenDeployment.BlueGreenDeploymentIdentifier)
	})

	t.Run("createintegration", func(t *testing.T) {
		t.Parallel()

		client := newTestRDSClient(t, newTestRDSHandler())

		out, err := client.CreateIntegration(t.Context(), &rdssdk.CreateIntegrationInput{
			IntegrationName: aws.String("tagged-integration"),
			SourceArn:       aws.String("arn:aws:rds:us-east-1:123456789012:cluster:source-cluster"),
			TargetArn:       aws.String("arn:aws:redshift-serverless:us-east-1:123456789012:namespace/target"),
			Tags:            tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.IntegrationArn)
	})

	t.Run("createtenantdatabase", func(t *testing.T) {
		t.Parallel()

		h := newTestRDSHandler()
		client := newTestRDSClient(t, h)

		_, err := client.CreateDBInstance(t.Context(), &rdssdk.CreateDBInstanceInput{
			DBInstanceIdentifier: aws.String("tenant-source-db"),
			DBInstanceClass:      aws.String("db.t3.micro"),
			Engine:               aws.String("oracle-ee-cdb"),
		})
		require.NoError(t, err)

		out, err := client.CreateTenantDatabase(t.Context(), &rdssdk.CreateTenantDatabaseInput{
			DBInstanceIdentifier: aws.String("tenant-source-db"),
			TenantDBName:         aws.String("tagged_tenant"),
			MasterUsername:       aws.String("admin"),
			Tags:                 tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.TenantDatabase.TenantDatabaseARN)
	})
}
