package neptune_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	neptunesdk "github.com/aws/aws-sdk-go-v2/service/neptune"
	"github.com/aws/aws-sdk-go-v2/service/neptune/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune"
)

func TestRestoreDBCluster_HonorsRequestOptions_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		restore func(t *testing.T, c *neptunesdk.Client, target string) (*types.DBCluster, error)
		name    string
	}{
		{
			name: "from_snapshot",
			restore: func(t *testing.T, c *neptunesdk.Client, target string) (*types.DBCluster, error) {
				t.Helper()
				out, err := c.RestoreDBClusterFromSnapshot(t.Context(), &neptunesdk.RestoreDBClusterFromSnapshotInput{
					DBClusterIdentifier:             aws.String(target),
					SnapshotIdentifier:              aws.String("src-snap"),
					Engine:                          aws.String("neptune"),
					DBSubnetGroupName:               aws.String("restore-sg"),
					Port:                            aws.Int32(9999),
					NetworkType:                     aws.String("DUAL"),
					DeletionProtection:              aws.Bool(true),
					EnableIAMDatabaseAuthentication: aws.Bool(true),
					VpcSecurityGroupIds:             []string{"sg-1234"},
					Tags:                            []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
				})
				if err != nil {
					return nil, err
				}

				return out.DBCluster, nil
			},
		},
		{
			name: "to_point_in_time",
			restore: func(t *testing.T, c *neptunesdk.Client, target string) (*types.DBCluster, error) {
				t.Helper()
				out, err := c.RestoreDBClusterToPointInTime(t.Context(), &neptunesdk.RestoreDBClusterToPointInTimeInput{
					DBClusterIdentifier:             aws.String(target),
					SourceDBClusterIdentifier:       aws.String("src"),
					UseLatestRestorableTime:         aws.Bool(true),
					DBSubnetGroupName:               aws.String("restore-sg"),
					Port:                            aws.Int32(9999),
					NetworkType:                     aws.String("DUAL"),
					DeletionProtection:              aws.Bool(true),
					EnableIAMDatabaseAuthentication: aws.Bool(true),
					VpcSecurityGroupIds:             []string{"sg-1234"},
					Tags:                            []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
				})
				if err != nil {
					return nil, err
				}

				return out.DBCluster, nil
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := neptune.NewInMemoryBackend("000000000000", testRegion)
			client := newTestNeptuneClient(t, neptune.NewHandler(backend))
			ctx := t.Context()

			_, err := client.CreateDBSubnetGroup(ctx, &neptunesdk.CreateDBSubnetGroupInput{
				DBSubnetGroupName:        aws.String("restore-sg"),
				DBSubnetGroupDescription: aws.String("d"),
				SubnetIds:                []string{"subnet-a", "subnet-b"},
			})
			require.NoError(t, err)
			_, err = client.CreateDBCluster(ctx, &neptunesdk.CreateDBClusterInput{
				DBClusterIdentifier: aws.String("src"), Engine: aws.String("neptune"),
			})
			require.NoError(t, err)
			_, err = client.CreateDBClusterSnapshot(ctx, &neptunesdk.CreateDBClusterSnapshotInput{
				DBClusterSnapshotIdentifier: aws.String("src-snap"),
				DBClusterIdentifier:         aws.String("src"),
			})
			require.NoError(t, err)

			got, err := tc.restore(t, client, "restored")
			require.NoError(t, err)
			require.NotNil(t, got)
			assert.Equal(t, "restore-sg", aws.ToString(got.DBSubnetGroup))
			assert.Equal(t, int32(9999), aws.ToInt32(got.Port))
			assert.Equal(t, "DUAL", aws.ToString(got.NetworkType))
			assert.True(t, aws.ToBool(got.DeletionProtection))
			assert.True(t, aws.ToBool(got.IAMDatabaseAuthenticationEnabled))
			require.Len(t, got.VpcSecurityGroups, 1)
			assert.Equal(t, "sg-1234", aws.ToString(got.VpcSecurityGroups[0].VpcSecurityGroupId))

			tags, err := client.ListTagsForResource(ctx, &neptunesdk.ListTagsForResourceInput{
				ResourceName: got.DBClusterArn,
			})
			require.NoError(t, err)
			require.Len(t, tags.TagList, 1)
			assert.Equal(t, "k", aws.ToString(tags.TagList[0].Key))

			_, err = client.RestoreDBClusterToPointInTime(ctx, &neptunesdk.RestoreDBClusterToPointInTimeInput{
				DBClusterIdentifier:       aws.String("bad-sg"),
				SourceDBClusterIdentifier: aws.String("src"),
				UseLatestRestorableTime:   aws.Bool(true),
				DBSubnetGroupName:         aws.String("missing"),
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "DBSubnetGroupNotFound")
		})
	}
}

func TestCreateGlobalCluster_HonorsRequestOptions_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name           string
		wantVersion    string
		withSource     bool
		wantEncrypted  bool
		wantProtection bool
	}{
		{name: "standalone_uses_request", wantVersion: "1.2.1.0", wantEncrypted: true, wantProtection: true},
		{name: "source_cluster_wins", withSource: true, wantVersion: "", wantEncrypted: false, wantProtection: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := neptune.NewInMemoryBackend("000000000000", testRegion)
			client := newTestNeptuneClient(t, neptune.NewHandler(backend))
			ctx := t.Context()

			in := &neptunesdk.CreateGlobalClusterInput{
				GlobalClusterIdentifier: aws.String("gc"),
				EngineVersion:           aws.String("1.2.1.0"),
				StorageEncrypted:        aws.Bool(true),
				DeletionProtection:      aws.Bool(true),
			}
			if tc.withSource {
				src, err := client.CreateDBCluster(ctx, &neptunesdk.CreateDBClusterInput{
					DBClusterIdentifier: aws.String("src"), Engine: aws.String("neptune"),
				})
				require.NoError(t, err)
				in.SourceDBClusterIdentifier = src.DBCluster.DBClusterIdentifier
				tc.wantVersion = aws.ToString(src.DBCluster.EngineVersion)
			}
			out, err := client.CreateGlobalCluster(ctx, in)
			require.NoError(t, err)

			got := out.GlobalCluster
			assert.Equal(t, tc.wantVersion, aws.ToString(got.EngineVersion))
			assert.Equal(t, tc.wantEncrypted, aws.ToBool(got.StorageEncrypted))
			assert.Equal(t, tc.wantProtection, aws.ToBool(got.DeletionProtection))

			_, err = client.DeleteGlobalCluster(ctx, &neptunesdk.DeleteGlobalClusterInput{
				GlobalClusterIdentifier: aws.String("gc"),
			})
			require.Error(t, err)
		})
	}
}
