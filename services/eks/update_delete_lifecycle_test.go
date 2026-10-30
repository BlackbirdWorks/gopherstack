package eks_test

import (
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ekssdk "github.com/aws/aws-sdk-go-v2/service/eks"
	ekstypes "github.com/aws/aws-sdk-go-v2/service/eks/types"
	"github.com/aws/smithy-go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eks"
)

const lifecycleTestDelay = 50 * time.Millisecond

func newLifecycleClient(t *testing.T, delay time.Duration) *ekssdk.Client {
	t.Helper()

	backend := eks.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	backend.SetLifecycleDelay(delay)

	return newTestEKSClient(t, eks.NewHandler(backend))
}

func waitClusterStatus(t *testing.T, c *ekssdk.Client, name string, want ekstypes.ClusterStatus) {
	t.Helper()

	require.Eventually(t, func() bool {
		out, err := c.DescribeCluster(t.Context(), &ekssdk.DescribeClusterInput{Name: aws.String(name)})

		return err == nil && out.Cluster.Status == want
	}, 5*time.Second, 5*time.Millisecond)
}

func TestClusterUpdate_UpdatingThenActive(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		c := newLifecycleClient(t, lifecycleTestDelay)
		createTestCluster(t, c, "upd")
		waitClusterStatus(t, c, "upd", ekstypes.ClusterStatusActive)

		out, err := c.UpdateClusterVersion(t.Context(), &ekssdk.UpdateClusterVersionInput{
			Name: aws.String("upd"), Version: aws.String("1.33"),
		})
		require.NoError(t, err)
		assert.NoError(t, uuid.Validate(aws.ToString(out.Update.Id)), "update ids are UUIDs")
		assert.Equal(t, ekstypes.UpdateStatusInProgress, out.Update.Status)

		desc, err := c.DescribeCluster(t.Context(), &ekssdk.DescribeClusterInput{Name: aws.String("upd")})
		require.NoError(t, err)
		assert.Equal(t, ekstypes.ClusterStatusUpdating, desc.Cluster.Status)
		assert.Equal(t, "1.32", aws.ToString(desc.Cluster.Version), "version flips only when the update completes")

		waitClusterStatus(t, c, "upd", ekstypes.ClusterStatusActive)

		desc, err = c.DescribeCluster(t.Context(), &ekssdk.DescribeClusterInput{Name: aws.String("upd")})
		require.NoError(t, err)
		assert.Equal(t, "1.33", aws.ToString(desc.Cluster.Version))

		upd, err := c.DescribeUpdate(t.Context(), &ekssdk.DescribeUpdateInput{
			Name: aws.String("upd"), UpdateId: out.Update.Id,
		})
		require.NoError(t, err)
		assert.Equal(t, ekstypes.UpdateStatusSuccessful, upd.Update.Status)
	})
}

func TestDeletes_DwellInDeletingWhenDelayed(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		c := newLifecycleClient(t, lifecycleTestDelay)
		createTestCluster(t, c, "del")
		waitClusterStatus(t, c, "del", ekstypes.ClusterStatusActive)

		_, err := c.CreateNodegroup(t.Context(), &ekssdk.CreateNodegroupInput{
			ClusterName: aws.String("del"), NodegroupName: aws.String("ng"),
			NodeRole: aws.String("arn:aws:iam::123456789012:role/ng"), Subnets: []string{"subnet-abc123"},
		})
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			out, descErr := c.DescribeNodegroup(t.Context(), &ekssdk.DescribeNodegroupInput{
				ClusterName: aws.String("del"), NodegroupName: aws.String("ng"),
			})

			return descErr == nil && out.Nodegroup.Status == ekstypes.NodegroupStatusActive
		}, 5*time.Second, 5*time.Millisecond)

		delNG, err := c.DeleteNodegroup(t.Context(), &ekssdk.DeleteNodegroupInput{
			ClusterName: aws.String("del"), NodegroupName: aws.String("ng"),
		})
		require.NoError(t, err)
		assert.Equal(t, ekstypes.NodegroupStatusDeleting, delNG.Nodegroup.Status)

		desc, err := c.DescribeNodegroup(t.Context(), &ekssdk.DescribeNodegroupInput{
			ClusterName: aws.String("del"), NodegroupName: aws.String("ng"),
		})
		require.NoError(t, err)
		assert.Equal(t, ekstypes.NodegroupStatusDeleting, desc.Nodegroup.Status)

		_, err = c.DeleteCluster(t.Context(), &ekssdk.DeleteClusterInput{Name: aws.String("del")})
		var inUse *ekstypes.ResourceInUseException
		require.ErrorAs(t, err, &inUse, "cluster with a deleting nodegroup is still in use")

		require.Eventually(t, func() bool {
			_, descErr := c.DescribeNodegroup(t.Context(), &ekssdk.DescribeNodegroupInput{
				ClusterName: aws.String("del"), NodegroupName: aws.String("ng"),
			})
			var nf *ekstypes.ResourceNotFoundException

			return errors.As(descErr, &nf)
		}, 5*time.Second, 5*time.Millisecond)

		delCluster, err := c.DeleteCluster(t.Context(), &ekssdk.DeleteClusterInput{Name: aws.String("del")})
		require.NoError(t, err)
		assert.Equal(t, ekstypes.ClusterStatusDeleting, delCluster.Cluster.Status)

		waitClusterStatus(t, c, "del", ekstypes.ClusterStatusDeleting)

		require.Eventually(t, func() bool {
			_, descErr := c.DescribeCluster(t.Context(), &ekssdk.DescribeClusterInput{Name: aws.String("del")})
			var nf *ekstypes.ResourceNotFoundException

			return errors.As(descErr, &nf)
		}, 5*time.Second, 5*time.Millisecond)
	})
}

func TestCreateValidation(t *testing.T) {
	t.Parallel()

	const role = "arn:aws:iam::123456789012:role/eks"

	tests := []struct {
		call    func(*ekssdk.Client) error
		name    string
		wantMsg string
	}{
		{
			name: "cluster_name_charset", wantMsg: "Cluster name can contain only alphanumeric characters",
			call: func(c *ekssdk.Client) error {
				_, err := c.CreateCluster(t.Context(), &ekssdk.CreateClusterInput{
					Name: aws.String("bad name!"), RoleArn: aws.String(role),
					ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
				})

				return err
			},
		},
		{
			name: "cluster_version_malformed", wantMsg: `unsupported Kubernetes version "0.1"`,
			call: func(c *ekssdk.Client) error {
				_, err := c.CreateCluster(t.Context(), &ekssdk.CreateClusterInput{
					Name: aws.String("v"), RoleArn: aws.String(role), Version: aws.String("0.1"),
					ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
				})

				return err
			},
		},
		{
			name: "cluster_role_arn", wantMsg: `Role ARN "badarn" is not a valid IAM role ARN`,
			call: func(c *ekssdk.Client) error {
				_, err := c.CreateCluster(t.Context(), &ekssdk.CreateClusterInput{
					Name: aws.String("r"), RoleArn: aws.String("badarn"),
					ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
				})

				return err
			},
		},
		{
			name: "nodegroup_min_above_max", wantMsg: "Invalid scaling config",
			call: func(c *ekssdk.Client) error {
				createTestCluster(t, c, "sc")
				_, err := c.CreateNodegroup(t.Context(), &ekssdk.CreateNodegroupInput{
					ClusterName: aws.String("sc"), NodegroupName: aws.String("ng"), NodeRole: aws.String(role),
					Subnets: []string{"subnet-1"},
					ScalingConfig: &ekstypes.NodegroupScalingConfig{
						MinSize: aws.Int32(3), MaxSize: aws.Int32(2), DesiredSize: aws.Int32(2),
					},
				})

				return err
			},
		},
		{
			name:    "update_skips_minor_version",
			wantMsg: "Unsupported Kubernetes minor version update from 1.32 to 1.34",
			call: func(c *ekssdk.Client) error {
				createTestCluster(t, c, "up")
				_, err := c.UpdateClusterVersion(t.Context(), &ekssdk.UpdateClusterVersionInput{
					Name: aws.String("up"), Version: aws.String("1.34"),
				})

				return err
			},
		},
		{
			name: "update_downgrade", wantMsg: "Unsupported Kubernetes minor version update from 1.32 to 1.31",
			call: func(c *ekssdk.Client) error {
				createTestCluster(t, c, "dg")
				_, err := c.UpdateClusterVersion(t.Context(), &ekssdk.UpdateClusterVersionInput{
					Name: aws.String("dg"), Version: aws.String("1.31"),
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestEKSClient(t, newRealClientHandler(t))
			err := tt.call(c)

			var invalid *ekstypes.InvalidParameterException
			require.ErrorAs(t, err, &invalid)
			assert.Contains(t, invalid.ErrorMessage(), tt.wantMsg)
			assert.NotContains(t, invalid.ErrorMessage(), "InvalidParameterException:")
		})
	}
}

func TestNotFoundWording(t *testing.T) {
	t.Parallel()

	c := newTestEKSClient(t, newRealClientHandler(t))
	createTestCluster(t, c, "nf")

	tests := []struct {
		call    func() error
		name    string
		wantMsg string
	}{
		{
			name: "cluster", wantMsg: "No cluster found for name: nope.",
			call: func() error {
				_, err := c.DescribeCluster(t.Context(), &ekssdk.DescribeClusterInput{Name: aws.String("nope")})

				return err
			},
		},
		{
			name: "nodegroup", wantMsg: "No node group found for name: nope.",
			call: func() error {
				_, err := c.DescribeNodegroup(t.Context(), &ekssdk.DescribeNodegroupInput{
					ClusterName: aws.String("nf"), NodegroupName: aws.String("nope"),
				})

				return err
			},
		},
		{
			name: "duplicate_cluster", wantMsg: "Cluster already exists with name: nf",
			call: func() error {
				_, err := c.CreateCluster(t.Context(), &ekssdk.CreateClusterInput{
					Name: aws.String("nf"), RoleArn: aws.String("arn:aws:iam::123456789012:role/eks"),
					ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var apiErr smithy.APIError
			require.ErrorAs(t, tt.call(), &apiErr)
			assert.Equal(t, tt.wantMsg, apiErr.ErrorMessage())
		})
	}
}

func TestListClusters_RejectsBadPagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input ekssdk.ListClustersInput
	}{
		{name: "garbage_token", input: ekssdk.ListClustersInput{NextToken: aws.String("garbage")}},
		{name: "non_numeric_token", input: ekssdk.ListClustersInput{NextToken: aws.String("YWJj")}},
		{name: "max_results_too_large", input: ekssdk.ListClustersInput{MaxResults: aws.Int32(101)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestEKSClient(t, newRealClientHandler(t))
			_, err := c.ListClusters(t.Context(), &tt.input)

			var invalid *ekstypes.InvalidParameterException
			require.ErrorAs(t, err, &invalid)
		})
	}
}
