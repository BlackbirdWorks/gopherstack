package terraform_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssvc "github.com/aws/aws-sdk-go-v2/service/rds"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// megaRDSRedshiftProviderBlock returns an OpenTofu provider block covering the
// RDS and Redshift endpoints plus the cross-service dependencies (IAM, KMS,
// S3, SNS, Secrets Manager) mega-batch-16 (RDS) and mega-batch-18 (Redshift)
// need for parameter groups, proxies, export tasks, and zero-ETL
// integrations. Shared by both files since they're in the same package.
func megaRDSRedshiftProviderBlock(addr string) string {
	return fmt.Sprintf(`terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  required_version = ">= 1.0"
}

provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    rds             = %[1]q
    redshift        = %[1]q
    redshiftdata    = %[1]q
    iam             = %[1]q
    kms             = %[1]q
    s3              = %[1]q
    sns             = %[1]q
    secretsmanager  = %[1]q
    sts             = %[1]q
  }
}
`, addr)
}

// TestTerraform_MegaBatch16 provisions RDS resources without prior terraform
// coverage: DB subnet group, parameter group, option group, snapshot and
// snapshot copy, instance role association, event subscription, instance
// state, certificate override, and the DB proxy family (proxy, default
// target group, endpoint, target).
func TestTerraform_MegaBatch16(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:       "success",
			fixture:    "mega-batch-16",
			providerFn: megaRDSRedshiftProviderBlock,
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{"Suffix": uuid.NewString()[:8]}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				suffix := vars["Suffix"].(string)
				client := createRDSClient(t)
				verifyMegaBatch16Instances(ctx, t, client, suffix)
				verifyMegaBatch16Proxy(ctx, t, client, suffix)
				verifyMegaBatch16Cluster(ctx, t, client, suffix)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

func verifyMegaBatch16Instances(ctx context.Context, t *testing.T, client *rdssvc.Client, suffix string) {
	t.Helper()

	snapOut, err := client.DescribeDBSnapshots(ctx, &rdssvc.DescribeDBSnapshotsInput{
		DBSnapshotIdentifier: aws.String("mega-batch-16-mysql-snap-" + suffix),
	})
	require.NoError(t, err, "DescribeDBSnapshots should succeed")
	require.Len(t, snapOut.DBSnapshots, 1)

	copyOut, err := client.DescribeDBSnapshots(ctx, &rdssvc.DescribeDBSnapshotsInput{
		DBSnapshotIdentifier: aws.String("mega-batch-16-mysql-snap-copy-" + suffix),
	})
	require.NoError(t, err, "DescribeDBSnapshots(copy) should succeed")
	require.Len(t, copyOut.DBSnapshots, 1)

	subOut, err := client.DescribeEventSubscriptions(ctx, &rdssvc.DescribeEventSubscriptionsInput{
		SubscriptionName: aws.String("mega-batch-16-events-sub-" + suffix),
	})
	require.NoError(t, err, "DescribeEventSubscriptions should succeed")
	require.Len(t, subOut.EventSubscriptionsList, 1)
	assert.Equal(t, "db-instance", aws.ToString(subOut.EventSubscriptionsList[0].SourceType))

	pgOut, err := client.DescribeDBInstances(ctx, &rdssvc.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String("mega-batch-16-pg-" + suffix),
	})
	require.NoError(t, err, "DescribeDBInstances(pg) should succeed")
	require.Len(t, pgOut.DBInstances, 1)
	require.Len(t, pgOut.DBInstances[0].AssociatedRoles, 1)
	assert.Equal(t, "s3Import", aws.ToString(pgOut.DBInstances[0].AssociatedRoles[0].FeatureName))

	certOut, err := client.DescribeCertificates(ctx, &rdssvc.DescribeCertificatesInput{
		CertificateIdentifier: aws.String("rds-ca-rsa2048-g1"),
	})
	require.NoError(t, err, "DescribeCertificates should succeed")
	require.Len(t, certOut.Certificates, 1)
	assert.True(t, aws.ToBool(certOut.Certificates[0].CustomerOverride),
		"rds_ca_rsa2048_g1 should be the customer override after aws_rds_certificate applies")
}

func verifyMegaBatch16Cluster(ctx context.Context, t *testing.T, client *rdssvc.Client, suffix string) {
	t.Helper()

	clusterID := "mega-batch-16-aurora-" + suffix

	clOut, err := client.DescribeDBClusters(ctx, &rdssvc.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String(clusterID),
	})
	require.NoError(t, err, "DescribeDBClusters should succeed")
	require.Len(t, clOut.DBClusters, 1)
	require.Len(t, clOut.DBClusters[0].AssociatedRoles, 1, "aws_rds_cluster_role_association should attach one role")
	assert.Equal(t, "started", string(clOut.DBClusters[0].ActivityStreamStatus))

	epOut, err := client.DescribeDBClusterEndpoints(ctx, &rdssvc.DescribeDBClusterEndpointsInput{
		DBClusterIdentifier: aws.String(clusterID),
	})
	require.NoError(t, err, "DescribeDBClusterEndpoints should succeed")
	require.Len(t, epOut.DBClusterEndpoints, 1)

	snapOut, err := client.DescribeDBClusterSnapshots(ctx, &rdssvc.DescribeDBClusterSnapshotsInput{
		DBClusterSnapshotIdentifier: aws.String("mega-batch-16-aurora-snap-" + suffix),
	})
	require.NoError(t, err, "DescribeDBClusterSnapshots should succeed")
	require.Len(t, snapOut.DBClusterSnapshots, 1)

	globalOut, err := client.DescribeGlobalClusters(ctx, &rdssvc.DescribeGlobalClustersInput{
		GlobalClusterIdentifier: aws.String("mega-batch-16-global-" + suffix),
	})
	require.NoError(t, err, "DescribeGlobalClusters should succeed")
	require.Len(t, globalOut.GlobalClusters, 1)

	exportOut, err := client.DescribeExportTasks(ctx, &rdssvc.DescribeExportTasksInput{
		ExportTaskIdentifier: aws.String("mega-batch-16-export-" + suffix),
	})
	require.NoError(t, err, "DescribeExportTasks should succeed")
	require.Len(t, exportOut.ExportTasks, 1)

	backupsOut, err := client.DescribeDBInstanceAutomatedBackups(
		ctx, &rdssvc.DescribeDBInstanceAutomatedBackupsInput{
			DBInstanceIdentifier: aws.String("mega-batch-16-mysql-" + suffix),
		},
	)
	require.NoError(t, err, "DescribeDBInstanceAutomatedBackups should succeed")
	require.NotEmpty(t, backupsOut.DBInstanceAutomatedBackups)

	shardOut, err := client.DescribeDBShardGroups(ctx, &rdssvc.DescribeDBShardGroupsInput{
		DBShardGroupIdentifier: aws.String("mega-batch-16-shard-group-" + suffix),
	})
	require.NoError(t, err, "DescribeDBShardGroups should succeed")
	require.Len(t, shardOut.DBShardGroups, 1)

	integOut, err := client.DescribeIntegrations(ctx, &rdssvc.DescribeIntegrationsInput{
		IntegrationIdentifier: aws.String("mega-batch-16-integration-" + suffix),
	})
	require.NoError(t, err, "DescribeIntegrations should succeed")
	require.Len(t, integOut.Integrations, 1)
}

func verifyMegaBatch16Proxy(ctx context.Context, t *testing.T, client *rdssvc.Client, suffix string) {
	t.Helper()

	proxyName := "mega-batch-16-proxy-" + suffix

	proxyOut, err := client.DescribeDBProxies(ctx, &rdssvc.DescribeDBProxiesInput{
		DBProxyName: aws.String(proxyName),
	})
	require.NoError(t, err, "DescribeDBProxies should succeed")
	require.Len(t, proxyOut.DBProxies, 1)
	assert.Equal(t, string(rdstypes.EngineFamilyPostgresql), aws.ToString(proxyOut.DBProxies[0].EngineFamily))

	tgOut, err := client.DescribeDBProxyTargetGroups(ctx, &rdssvc.DescribeDBProxyTargetGroupsInput{
		DBProxyName: aws.String(proxyName),
	})
	require.NoError(t, err, "DescribeDBProxyTargetGroups should succeed")
	require.Len(t, tgOut.TargetGroups, 1)

	epOut, err := client.DescribeDBProxyEndpoints(ctx, &rdssvc.DescribeDBProxyEndpointsInput{
		DBProxyName: aws.String(proxyName),
	})
	require.NoError(t, err, "DescribeDBProxyEndpoints should succeed")
	require.Len(t, epOut.DBProxyEndpoints, 1)

	targetOut, err := client.DescribeDBProxyTargets(ctx, &rdssvc.DescribeDBProxyTargetsInput{
		DBProxyName: aws.String(proxyName),
	})
	require.NoError(t, err, "DescribeDBProxyTargets should succeed")
	require.Len(t, targetOut.Targets, 1)
}
