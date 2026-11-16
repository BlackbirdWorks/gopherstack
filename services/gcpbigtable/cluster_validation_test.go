package gcpbigtable_test

import (
	"fmt"
	"testing"

	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/blackbirdworks/gopherstack/pkgs/cmek"
	"github.com/blackbirdworks/gopherstack/services/gcpbigtable"
)

type fakeKeys struct {
	got map[string]string
	err map[string]error
}

func (f *fakeKeys) CheckKey(key, location string) error {
	f.got[key] = location

	return f.err[key]
}

func createInstanceReq(id string, c *adminpb.Cluster) *adminpb.CreateInstanceRequest {
	return &adminpb.CreateInstanceRequest{
		Parent: "projects/" + project, InstanceId: id,
		Instance: &adminpb.Instance{DisplayName: id, Type: adminpb.Instance_PRODUCTION},
		Clusters: map[string]*adminpb.Cluster{"c": c},
	}
}

func baseCluster() *adminpb.Cluster {
	return &adminpb.Cluster{Location: "projects/" + project + "/locations/" + zone, ServeNodes: 2}
}

func autoscaled(minNodes, maxNodes, storage int32) *adminpb.Cluster {
	c := baseCluster()
	c.ServeNodes = 0
	c.Config = &adminpb.Cluster_ClusterConfig_{ClusterConfig: &adminpb.Cluster_ClusterConfig{
		ClusterAutoscalingConfig: &adminpb.Cluster_ClusterAutoscalingConfig{
			AutoscalingLimits: &adminpb.AutoscalingLimits{MinServeNodes: minNodes, MaxServeNodes: maxNodes},
			AutoscalingTargets: &adminpb.AutoscalingTargets{
				CpuUtilizationPercent:        50,
				StorageUtilizationGibPerNode: storage,
			},
		},
	}}

	return c
}

func TestClusterScalingValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cluster func() *adminpb.Cluster
		name    string
		want    codes.Code
	}{
		{func() *adminpb.Cluster { return autoscaled(1, 3, 3000) }, "ssd target", codes.OK},
		{func() *adminpb.Cluster { return autoscaled(1, 3, 0) }, "default target", codes.OK},
		{func() *adminpb.Cluster { return autoscaled(1, 3, 2559) }, "ssd target low", codes.InvalidArgument},
		{func() *adminpb.Cluster { return autoscaled(1, 3, 5121) }, "ssd target high", codes.InvalidArgument},
		{func() *adminpb.Cluster {
			c := autoscaled(1, 3, 9000)
			c.DefaultStorageType = adminpb.StorageType_HDD

			return c
		}, "hdd target", codes.OK},
		{func() *adminpb.Cluster {
			c := autoscaled(1, 3, 5000)
			c.DefaultStorageType = adminpb.StorageType_HDD

			return c
		}, "hdd target low", codes.InvalidArgument},
		{func() *adminpb.Cluster {
			c := autoscaled(1, 3, 20000)
			c.DefaultStorageType = adminpb.StorageType_HDD

			return c
		}, "hdd target high", codes.InvalidArgument},
		{func() *adminpb.Cluster {
			c := baseCluster()
			c.NodeScalingFactor = adminpb.Cluster_NODE_SCALING_FACTOR_2X
			c.ServeNodes = 4

			return c
		}, "2x even", codes.OK},
		{func() *adminpb.Cluster {
			c := baseCluster()
			c.NodeScalingFactor = adminpb.Cluster_NODE_SCALING_FACTOR_2X
			c.ServeNodes = 3

			return c
		}, "2x odd", codes.InvalidArgument},
		{func() *adminpb.Cluster {
			c := autoscaled(2, 6, 0)
			c.NodeScalingFactor = adminpb.Cluster_NODE_SCALING_FACTOR_2X

			return c
		}, "2x autoscaling even", codes.OK},
		{func() *adminpb.Cluster {
			c := autoscaled(2, 5, 0)
			c.NodeScalingFactor = adminpb.Cluster_NODE_SCALING_FACTOR_2X

			return c
		}, "2x autoscaling odd max", codes.InvalidArgument},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			_, err := adminpb.NewBigtableInstanceAdminClient(e.conn).
				CreateInstance(t.Context(), createInstanceReq(fmt.Sprintf("scale-%d", i), tt.cluster()))
			assert.Equal(t, tt.want, code(err))
		})
	}
}

func TestClusterScalingFactorOnUpdate(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ic := adminpb.NewBigtableInstanceAdminClient(e.conn)

	c := baseCluster()
	c.NodeScalingFactor = adminpb.Cluster_NODE_SCALING_FACTOR_2X
	c.ServeNodes = 4

	_, err := ic.CreateInstance(t.Context(), createInstanceReq("nsf", c))
	require.NoError(t, err)

	name := "projects/" + project + "/instances/nsf/clusters/c"

	_, err = ic.UpdateCluster(t.Context(), &adminpb.Cluster{Name: name, ServeNodes: 5})
	assert.Equal(t, codes.InvalidArgument, code(err))

	_, err = ic.UpdateCluster(t.Context(), &adminpb.Cluster{Name: name, ServeNodes: 6})
	require.NoError(t, err)
}

func TestClusterEncryptionKey(t *testing.T) {
	t.Parallel()

	const key = "projects/test-project/locations/us-central1/keyRings/r/cryptoKeys/k"

	tests := []struct {
		err  error
		name string
		key  string
		want codes.Code
	}{
		{nil, "ok", key, codes.OK},
		{nil, "no key", "", codes.OK},
		{cmek.ErrNotFound, "missing", key, codes.InvalidArgument},
		{cmek.ErrLocation, "wrong region", key, codes.InvalidArgument},
		{cmek.ErrPurpose, "wrong purpose", key, codes.InvalidArgument},
		{cmek.ErrDisabled, "disabled", key, codes.FailedPrecondition},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fk := &fakeKeys{got: map[string]string{}, err: map[string]error{key: tt.err}}
			b := gcpbigtable.NewInMemoryBackend()
			b.SetKeyChecker(fk)

			e := newEnvWithBackend(t, b)
			c := baseCluster()
			c.EncryptionConfig = &adminpb.Cluster_EncryptionConfig{KmsKeyName: tt.key}

			_, err := adminpb.NewBigtableInstanceAdminClient(e.conn).
				CreateInstance(t.Context(), createInstanceReq(fmt.Sprintf("kms-%d", i), c))
			require.Equal(t, tt.want, code(err))

			if tt.key != "" {
				assert.Equal(t, "us-central1", fk.got[key], "the key is checked against the cluster's region")
			}
		})
	}
}

func TestClusterEncryptionKeyOnCreateCluster(t *testing.T) {
	t.Parallel()

	const key = "projects/test-project/locations/us-east1/keyRings/r/cryptoKeys/k"

	fk := &fakeKeys{got: map[string]string{}, err: map[string]error{key: cmek.ErrNotFound}}
	b := gcpbigtable.NewInMemoryBackend()
	b.SetKeyChecker(fk)

	e := newEnvWithBackend(t, b)
	ic := adminpb.NewBigtableInstanceAdminClient(e.conn)

	_, err := ic.CreateInstance(t.Context(), createInstanceReq("kms-add", baseCluster()))
	require.NoError(t, err)

	_, err = ic.CreateCluster(t.Context(), &adminpb.CreateClusterRequest{
		Parent: "projects/" + project + "/instances/kms-add", ClusterId: "second",
		Cluster: &adminpb.Cluster{
			Location: "projects/" + project + "/locations/us-east1-b", ServeNodes: 1,
			EncryptionConfig: &adminpb.Cluster_EncryptionConfig{KmsKeyName: key},
		},
	})
	assert.Equal(t, codes.InvalidArgument, code(err))
	assert.Equal(t, "us-east1", fk.got[key])
}

func TestUndeleteTableRejectsCMEK(t *testing.T) {
	t.Parallel()

	const key = "projects/test-project/locations/us-central1/keyRings/r/cryptoKeys/k"

	b := gcpbigtable.NewInMemoryBackend()
	b.SetKeyChecker(&fakeKeys{got: map[string]string{}, err: map[string]error{}})

	e := newEnvWithBackend(t, b)
	c := baseCluster()
	c.EncryptionConfig = &adminpb.Cluster_EncryptionConfig{KmsKeyName: key}

	_, err := adminpb.NewBigtableInstanceAdminClient(e.conn).CreateInstance(t.Context(), createInstanceReq(instance, c))
	require.NoError(t, err)
	require.NoError(t, e.ac.CreateTableFromConf(t.Context(), tableConf("t", "cf")))
	require.NoError(t, e.ac.DeleteTable(t.Context(), "t"))

	_, err = adminpb.NewBigtableTableAdminClient(e.conn).UndeleteTable(t.Context(), &adminpb.UndeleteTableRequest{
		Name: "projects/" + project + "/instances/" + instance + "/tables/t",
	})
	assert.Equal(t, codes.FailedPrecondition, code(err))
}
