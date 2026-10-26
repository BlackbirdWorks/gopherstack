package gcpbigtable_test

import (
	"testing"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/iam"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
)

func TestInstanceLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()

	require.NoError(t, e.iac.CreateInstance(ctx, &bigtable.InstanceConf{
		InstanceId: "inst-one", DisplayName: "First", ClusterId: "c-one", Zone: zone, NumNodes: 3,
		InstanceType: bigtable.PRODUCTION, Labels: map[string]string{"env": "dev"},
	}))

	info, err := e.iac.InstanceInfo(ctx, "inst-one")
	require.NoError(t, err)
	assert.Equal(t, "inst-one", info.Name)
	assert.Equal(t, "First", info.DisplayName)
	assert.Equal(t, bigtable.Ready, info.InstanceState)
	assert.Equal(t, bigtable.PRODUCTION, info.InstanceType)
	assert.Equal(t, map[string]string{"env": "dev"}, info.Labels)

	cl, err := e.iac.GetCluster(ctx, "inst-one", "c-one")
	require.NoError(t, err)
	assert.Equal(t, 3, cl.ServeNodes)
	assert.Equal(t, zone, cl.Zone)
	assert.Equal(t, bigtable.SSD, cl.StorageType, "unspecified storage defaults to SSD")

	require.NoError(t, e.iac.UpdateInstanceWithClusters(ctx, &bigtable.InstanceWithClustersConfig{
		InstanceID: "inst-one", DisplayName: "Renamed", Labels: map[string]string{"env": "prod", "team": "x"},
	}))

	info, err = e.iac.InstanceInfo(ctx, "inst-one")
	require.NoError(t, err)
	assert.Equal(t, "Renamed", info.DisplayName)
	assert.Equal(t, map[string]string{"env": "prod", "team": "x"}, info.Labels)

	require.NoError(t, e.iac.CreateInstance(ctx, &bigtable.InstanceConf{
		InstanceId: "inst-two", DisplayName: "Second", ClusterId: "c-two", Zone: zone, NumNodes: 1,
	}))

	all, err := e.iac.Instances(ctx)
	require.NoError(t, err)
	require.Len(t, all, 2)
	assert.Equal(t, "inst-one", all[0].Name)
	assert.Equal(t, bigtable.PRODUCTION, all[1].InstanceType, "unspecified type becomes PRODUCTION")

	require.NoError(t, e.iac.DeleteInstance(ctx, "inst-one"))

	_, err = e.iac.InstanceInfo(ctx, "inst-one")
	assert.Equal(t, codes.NotFound, code(err))
	assert.Equal(t, codes.NotFound, code(e.iac.DeleteInstance(ctx, "inst-one")))
}

func TestCreateInstanceErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, "exists-already")

	base := func() *bigtable.InstanceConf {
		return &bigtable.InstanceConf{
			InstanceId: "fresh-one", DisplayName: "d", ClusterId: "c-one", Zone: zone, NumNodes: 1,
		}
	}

	tests := []struct {
		mutate func(c *bigtable.InstanceConf)
		name   string
		want   codes.Code
	}{
		{func(c *bigtable.InstanceConf) { c.InstanceId = "exists-already" }, "duplicate", codes.AlreadyExists},
		{func(c *bigtable.InstanceConf) { c.InstanceId = "Bad_ID" }, "bad instance id", codes.InvalidArgument},
		{func(c *bigtable.InstanceConf) { c.InstanceId = "a" }, "instance id too short", codes.InvalidArgument},
		{func(c *bigtable.InstanceConf) { c.DisplayName = "" }, "missing display name", codes.InvalidArgument},
		{func(c *bigtable.InstanceConf) { c.ClusterId = "Bad_Cluster" }, "bad cluster id", codes.InvalidArgument},
		{func(c *bigtable.InstanceConf) { c.NumNodes = 0 }, "production needs nodes", codes.InvalidArgument},
		{
			func(c *bigtable.InstanceConf) { c.Labels = map[string]string{"UPPER": "x"} },
			"bad label key",
			codes.InvalidArgument,
		},
		{func(c *bigtable.InstanceConf) {
			c.AutoscalingConfig = &bigtable.AutoscalingConfig{MinNodes: 3, MaxNodes: 1, CPUTargetPercent: 50}
		}, "autoscaling min above max", codes.InvalidArgument},
		{func(c *bigtable.InstanceConf) {
			c.AutoscalingConfig = &bigtable.AutoscalingConfig{MinNodes: 1, MaxNodes: 3, CPUTargetPercent: 5}
		}, "autoscaling cpu below range", codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := base()
			tt.mutate(c)
			assert.Equal(t, tt.want, code(e.iac.CreateInstance(t.Context(), c)))
		})
	}
}

func TestInstanceTypeUpgrade(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()

	require.NoError(t, e.iac.CreateInstance(ctx, &bigtable.InstanceConf{
		InstanceId: "dev-inst", DisplayName: "d", ClusterId: "c-dev", Zone: zone, InstanceType: bigtable.DEVELOPMENT,
	}))

	require.NoError(t, e.iac.UpdateInstanceWithClusters(ctx, &bigtable.InstanceWithClustersConfig{
		InstanceID: "dev-inst", InstanceType: bigtable.PRODUCTION,
	}))

	info, err := e.iac.InstanceInfo(ctx, "dev-inst")
	require.NoError(t, err)
	assert.Equal(t, bigtable.PRODUCTION, info.InstanceType)

	err = e.iac.UpdateInstanceWithClusters(ctx, &bigtable.InstanceWithClustersConfig{
		InstanceID: "dev-inst", InstanceType: bigtable.DEVELOPMENT,
	})
	assert.Equal(t, codes.InvalidArgument, code(err), "no downgrade")
}

func TestClusters(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, instance)
	ctx := t.Context()

	require.NoError(t, e.iac.CreateCluster(ctx, &bigtable.ClusterConfig{
		InstanceID: instance, ClusterID: "second", Zone: "us-east1-b", NumNodes: 2, StorageType: bigtable.HDD,
	}))

	cls, err := e.iac.Clusters(ctx, instance)
	require.NoError(t, err)
	require.Len(t, cls, 2)
	assert.Equal(t, "second", cls[0].Name)
	assert.Equal(t, cluster, cls[1].Name)
	assert.Equal(t, bigtable.HDD, cls[0].StorageType)
	assert.Equal(t, "READY", cls[0].State)

	require.NoError(t, e.iac.UpdateCluster(ctx, instance, "second", 5))

	got, err := e.iac.GetCluster(ctx, instance, "second")
	require.NoError(t, err)
	assert.Equal(t, 5, got.ServeNodes)

	require.NoError(t, e.iac.SetAutoscaling(ctx, instance, "second", bigtable.AutoscalingConfig{
		MinNodes: 2, MaxNodes: 8, CPUTargetPercent: 60,
	}))

	got, err = e.iac.GetCluster(ctx, instance, "second")
	require.NoError(t, err)
	require.NotNil(t, got.AutoscalingConfig)
	assert.Equal(t, 2, got.AutoscalingConfig.MinNodes)
	assert.Equal(t, 8, got.AutoscalingConfig.MaxNodes)
	assert.Equal(t, 5, got.ServeNodes, "serve nodes are kept inside the autoscaling limits")

	require.NoError(t, e.iac.UpdateCluster(ctx, instance, "second", 3))

	got, err = e.iac.GetCluster(ctx, instance, "second")
	require.NoError(t, err)
	assert.Nil(t, got.AutoscalingConfig, "a manual size removes autoscaling")
	assert.Equal(t, 3, got.ServeNodes)

	assert.Equal(t, codes.AlreadyExists, code(e.iac.CreateCluster(ctx, &bigtable.ClusterConfig{
		InstanceID: instance, ClusterID: "second", Zone: "europe-west1-b", NumNodes: 1, StorageType: bigtable.SSD,
	})))
	assert.Equal(t, codes.InvalidArgument, code(e.iac.CreateCluster(ctx, &bigtable.ClusterConfig{
		InstanceID: instance, ClusterID: "third", Zone: zone, NumNodes: 1, StorageType: bigtable.SSD,
	})), "one cluster per location")
	assert.Equal(t, codes.NotFound, code(e.iac.CreateCluster(ctx, &bigtable.ClusterConfig{
		InstanceID: "nope-inst", ClusterID: "third", Zone: zone, NumNodes: 1, StorageType: bigtable.SSD,
	})))
	assert.Equal(t, codes.NotFound, code(e.iac.UpdateCluster(ctx, instance, "ghost", 1)))
	assert.Equal(t, codes.InvalidArgument, code(e.iac.UpdateCluster(ctx, instance, "second", 0)))

	require.NoError(t, e.iac.DeleteCluster(ctx, instance, "second"))
	assert.Equal(t, codes.FailedPrecondition, code(e.iac.DeleteCluster(ctx, instance, cluster)), "last cluster stays")
	assert.Equal(t, codes.NotFound, code(e.iac.DeleteCluster(ctx, instance, "second")))
}

func TestAppProfiles(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, instance)
	ctx := t.Context()

	created, err := e.iac.CreateAppProfile(ctx, bigtable.ProfileConf{
		ProfileID: "p1", InstanceID: instance, Description: "first",
		RoutingConfig: &bigtable.SingleClusterRoutingConfig{ClusterID: cluster, AllowTransactionalWrites: true},
	})
	require.NoError(t, err)
	assert.Contains(t, created.GetName(), "/appProfiles/p1")
	assert.NotEmpty(t, created.GetEtag())

	got, err := e.iac.GetAppProfile(ctx, instance, "p1")
	require.NoError(t, err)
	assert.Equal(t, "first", got.GetDescription())
	assert.True(t, got.GetSingleClusterRouting().GetAllowTransactionalWrites())

	_, err = e.iac.CreateAppProfile(ctx, bigtable.ProfileConf{
		ProfileID: "p2", InstanceID: instance, RoutingConfig: &bigtable.MultiClusterRoutingUseAnyConfig{},
	})
	require.NoError(t, err)

	var names []string

	it := e.iac.ListAppProfiles(ctx, instance)
	for {
		p, nerr := it.Next()
		if nerr != nil {
			break
		}

		names = append(names, p.GetName())
	}

	assert.Len(t, names, 2)

	desc := "updated"
	require.NoError(t, e.iac.UpdateAppProfile(ctx, instance, "p1", bigtable.ProfileAttrsToUpdate{Description: desc}))

	got, err = e.iac.GetAppProfile(ctx, instance, "p1")
	require.NoError(t, err)
	assert.Equal(t, "updated", got.GetDescription())
	assert.NotEqual(t, created.GetEtag(), got.GetEtag(), "an update changes the etag")
	assert.NotNil(t, got.GetSingleClusterRouting(), "unmasked routing is untouched")

	require.NoError(t, e.iac.UpdateAppProfile(ctx, instance, "p1", bigtable.ProfileAttrsToUpdate{
		RoutingConfig: &bigtable.MultiClusterRoutingUseAnyConfig{ClusterIDs: []string{cluster}},
	}))

	got, err = e.iac.GetAppProfile(ctx, instance, "p1")
	require.NoError(t, err)
	assert.NotNil(t, got.GetMultiClusterRoutingUseAny())

	_, cerr := e.iac.CreateAppProfile(ctx, bigtable.ProfileConf{
		ProfileID: "p1", InstanceID: instance, RoutingConfig: &bigtable.MultiClusterRoutingUseAnyConfig{},
	})
	assert.Equal(t, codes.AlreadyExists, code(cerr))

	_, cerr = e.iac.CreateAppProfile(ctx, bigtable.ProfileConf{
		ProfileID: "p3", InstanceID: instance, RoutingConfig: &bigtable.SingleClusterRoutingConfig{ClusterID: "ghost"},
	})
	assert.Equal(t, codes.InvalidArgument, code(cerr))

	_, cerr = e.iac.GetAppProfile(ctx, instance, "ghost")
	assert.Equal(t, codes.NotFound, code(cerr))

	require.NoError(t, e.iac.DeleteAppProfile(ctx, instance, "p2"))
	assert.Equal(t, codes.NotFound, code(e.iac.DeleteAppProfile(ctx, instance, "p2")))
}

func TestAppProfileRouting(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, instance)
	ctx := t.Context()

	for id, rc := range map[string]bigtable.RoutingPolicyConfig{
		"single-tx":     &bigtable.SingleClusterRoutingConfig{ClusterID: cluster, AllowTransactionalWrites: true},
		"single-notx":   &bigtable.SingleClusterRoutingConfig{ClusterID: cluster},
		"multi-cluster": &bigtable.MultiClusterRoutingUseAnyConfig{},
	} {
		_, err := e.iac.CreateAppProfile(
			ctx,
			bigtable.ProfileConf{ProfileID: id, InstanceID: instance, RoutingConfig: rc},
		)
		require.NoError(t, err)
	}

	require.NoError(t, e.ac.CreateTableFromConf(ctx, tableConf("t", "cf")))

	tests := []struct {
		profile string
		name    string
		want    codes.Code
	}{
		{"single-tx", "single cluster with transactions", codes.OK},
		{"single-notx", "single cluster without transactions", codes.FailedPrecondition},
		{"multi-cluster", "multi cluster", codes.FailedPrecondition},
		{"ghost", "unknown profile", codes.NotFound},
		{"", "default profile", codes.OK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := bigtable.ClientConfig{AppProfile: tt.profile, MetricsProvider: bigtable.NoopMetricsProvider{}}
			c, err := bigtable.NewClientWithConfig(t.Context(), project, instance, cfg, option.WithGRPCConn(e.conn))
			require.NoError(t, err)

			rmw := bigtable.NewReadModifyWrite()
			rmw.Increment("cf", "n", 1)
			_, err = c.Open("t").ApplyReadModifyWrite(t.Context(), "r", rmw)
			assert.Equal(t, tt.want, code(err))
		})
	}
}

func TestInstanceIAM(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, instance)
	ctx := t.Context()
	h := e.iac.InstanceIAM(instance)

	pol, err := h.Policy(ctx)
	require.NoError(t, err)
	assert.Empty(t, pol.Roles())

	pol.Add("user:a@example.com", "roles/bigtable.reader")
	require.NoError(t, h.SetPolicy(ctx, pol))

	got, err := h.Policy(ctx)
	require.NoError(t, err)
	assert.Equal(t, []iam.RoleName{"roles/bigtable.reader"}, got.Roles())
	assert.Equal(t, []string{"user:a@example.com"}, got.Members("roles/bigtable.reader"))

	perms, err := h.TestPermissions(ctx, []string{"bigtable.instances.get"})
	require.NoError(t, err)
	assert.Equal(t, []string{"bigtable.instances.get"}, perms)

	stale := *pol
	stale.Add("user:b@example.com", "roles/bigtable.user")
	assert.Equal(t, codes.Aborted, code(h.SetPolicy(ctx, &stale)), "the old etag no longer matches")

	_, err = e.iac.InstanceIAM("ghost-instance").Policy(ctx)
	assert.Equal(t, codes.NotFound, code(err))
}
