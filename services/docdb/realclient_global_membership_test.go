package docdb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	docdbsdk "github.com/aws/aws-sdk-go-v2/service/docdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_CreateDBClusterJoinsGlobalCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		globalC string
		errCode string
	}{
		{name: "joins as secondary", globalC: "g1"},
		{name: "unknown global cluster", globalC: "nope", errCode: "GlobalClusterNotFoundFault"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			ctx := t.Context()
			primary, err := client.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
				DBClusterIdentifier: aws.String("primary"), Engine: aws.String("docdb"),
			})
			require.NoError(t, err)
			_, err = client.CreateGlobalCluster(ctx, &docdbsdk.CreateGlobalClusterInput{
				GlobalClusterIdentifier:   aws.String("g1"),
				SourceDBClusterIdentifier: primary.DBCluster.DBClusterArn,
			})
			require.NoError(t, err)

			sec, err := client.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
				DBClusterIdentifier: aws.String("secondary"), Engine: aws.String("docdb"),
				GlobalClusterIdentifier: aws.String(tt.globalC),
			})
			if tt.errCode != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errCode)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(primary.DBCluster.DBClusterArn),
				aws.ToString(sec.DBCluster.ReplicationSourceIdentifier))

			got, err := client.DescribeDBClusters(ctx, &docdbsdk.DescribeDBClustersInput{
				DBClusterIdentifier: aws.String("primary"),
			})
			require.NoError(t, err)
			require.Len(t, got.DBClusters, 1)
			assert.Equal(t, []string{aws.ToString(sec.DBCluster.DBClusterArn)},
				got.DBClusters[0].ReadReplicaIdentifiers)

			gcs, err := client.DescribeGlobalClusters(ctx, &docdbsdk.DescribeGlobalClustersInput{
				GlobalClusterIdentifier: aws.String("g1"),
			})
			require.NoError(t, err)
			require.Len(t, gcs.GlobalClusters, 1)
			require.Len(t, gcs.GlobalClusters[0].GlobalClusterMembers, 2)
			assert.Equal(t, []string{aws.ToString(sec.DBCluster.DBClusterArn)},
				gcs.GlobalClusters[0].GlobalClusterMembers[0].Readers)

			_, err = client.FailoverGlobalCluster(ctx, &docdbsdk.FailoverGlobalClusterInput{
				GlobalClusterIdentifier:   aws.String("g1"),
				TargetDbClusterIdentifier: sec.DBCluster.DBClusterArn,
			})
			require.NoError(t, err)
			got, err = client.DescribeDBClusters(ctx, &docdbsdk.DescribeDBClustersInput{
				DBClusterIdentifier: aws.String("secondary"),
			})
			require.NoError(t, err)
			assert.Empty(t, aws.ToString(got.DBClusters[0].ReplicationSourceIdentifier))
			assert.Equal(t, []string{aws.ToString(primary.DBCluster.DBClusterArn)},
				got.DBClusters[0].ReadReplicaIdentifiers)

			_, err = client.RemoveFromGlobalCluster(ctx, &docdbsdk.RemoveFromGlobalClusterInput{
				GlobalClusterIdentifier: aws.String("g1"),
				DbClusterIdentifier:     primary.DBCluster.DBClusterArn,
			})
			require.NoError(t, err)
			got, err = client.DescribeDBClusters(ctx, &docdbsdk.DescribeDBClustersInput{
				DBClusterIdentifier: aws.String("primary"),
			})
			require.NoError(t, err)
			assert.Empty(t, aws.ToString(got.DBClusters[0].ReplicationSourceIdentifier))
			assert.Empty(t, got.DBClusters[0].ReadReplicaIdentifiers)
		})
	}
}

func TestRealClient_InstanceCertificateDetailsAndOrderableVpc(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vpcFilter *bool
		name      string
		caID      string
		wantTill  bool
		wantOpts  bool
	}{
		{name: "known ca", caID: "rds-ca-rsa2048-g1", wantTill: true, vpcFilter: aws.Bool(true), wantOpts: true},
		{name: "no ca", vpcFilter: aws.Bool(false), wantOpts: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			ctx := t.Context()
			_, err := client.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
				DBClusterIdentifier: aws.String("c1"), Engine: aws.String("docdb"),
			})
			require.NoError(t, err)
			in := &docdbsdk.CreateDBInstanceInput{
				DBInstanceIdentifier: aws.String("i1"), DBInstanceClass: aws.String("db.r5.large"),
				Engine: aws.String("docdb"), DBClusterIdentifier: aws.String("c1"),
			}
			if tt.caID != "" {
				in.CACertificateIdentifier = aws.String(tt.caID)
			}
			out, err := client.CreateDBInstance(ctx, in)
			require.NoError(t, err)
			cd := out.DBInstance.CertificateDetails
			if tt.caID == "" {
				assert.Nil(t, cd)
			} else {
				require.NotNil(t, cd)
				assert.Equal(t, tt.caID, aws.ToString(cd.CAIdentifier))
				assert.Equal(t, tt.wantTill, cd.ValidTill != nil)
			}

			optsIn := &docdbsdk.DescribeOrderableDBInstanceOptionsInput{
				Engine: aws.String("docdb"), Vpc: tt.vpcFilter,
			}
			opts, err := client.DescribeOrderableDBInstanceOptions(ctx, optsIn)
			require.NoError(t, err)
			assert.Equal(t, tt.wantOpts, len(opts.OrderableDBInstanceOptions) > 0)
			for _, o := range opts.OrderableDBInstanceOptions {
				assert.True(t, aws.ToBool(o.Vpc))
			}
		})
	}
}
