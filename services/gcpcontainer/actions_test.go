//nolint:staticcheck // the API still serves fields the proto marks deprecated
package gcpcontainer_test

import (
	"testing"
	"time"

	"cloud.google.com/go/container/apiv1/containerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSetLabels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		labels  map[string]string
		name    string
		wantMsg string
		want    codes.Code
		stale   bool
		noFP    bool
	}{
		{name: "ok", labels: map[string]string{"team": "a"}},
		{name: "clear", labels: map[string]string{}},
		{
			name:    "stale fingerprint",
			labels:  map[string]string{"x": "y"},
			stale:   true,
			want:    codes.FailedPrecondition,
			wantMsg: "stale",
		},
		{
			name:    "missing fingerprint",
			labels:  map[string]string{"x": "y"},
			noFP:    true,
			want:    codes.InvalidArgument,
			wantMsg: "fingerprint",
		},
		{name: "bad key", labels: map[string]string{"Bad": "y"}, want: codes.InvalidArgument, wantMsg: "label key"},
		{name: "bad value", labels: map[string]string{"k": "UPPER"}, want: codes.InvalidArgument, wantMsg: "label"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			before := e.ready(
				t,
				zone,
				&containerpb.Cluster{Name: "l", InitialNodeCount: 1, ResourceLabels: map[string]string{"a": "b"}},
			)

			fp := before.GetLabelFingerprint()

			switch {
			case tt.stale:
				fp = "AAAA"
			case tt.noFP:
				fp = ""
			}

			op, err := e.gc.SetLabels(t.Context(), &containerpb.SetLabelsRequest{
				Name: clusterPath(zone, "l"), ResourceLabels: tt.labels, LabelFingerprint: fp,
			})
			if tt.want != codes.OK {
				assertCode(t, tt.want, err, tt.wantMsg)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, containerpb.Operation_SET_LABELS, op.GetOperationType())
			assert.Equal(t, containerpb.Cluster_RECONCILING, e.getCluster(t, zone, "l").GetStatus())

			e.settle()

			after := e.getCluster(t, zone, "l")
			assert.Equal(t, containerpb.Cluster_RUNNING, after.GetStatus())
			assert.Len(t, after.GetResourceLabels(), len(tt.labels))
			assert.NotEqual(t, before.GetLabelFingerprint(), after.GetLabelFingerprint())
		})
	}
}

func TestSetMasterAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		update  *containerpb.MasterAuth
		check   func(t *testing.T, ma *containerpb.MasterAuth)
		name    string
		wantMsg string
		action  containerpb.SetMasterAuthRequest_Action
		want    codes.Code
	}{
		{
			name: "set password", action: containerpb.SetMasterAuthRequest_SET_PASSWORD,
			update: &containerpb.MasterAuth{Password: "0123456789abcdef"},
			check: func(t *testing.T, ma *containerpb.MasterAuth) {
				t.Helper()
				assert.Equal(t, "0123456789abcdef", ma.GetPassword())
				assert.Equal(t, "admin", ma.GetUsername())
			},
		},
		{
			name: "short password", action: containerpb.SetMasterAuthRequest_SET_PASSWORD,
			update: &containerpb.MasterAuth{Password: "short"}, want: codes.InvalidArgument, wantMsg: "Password",
		},
		{
			name: "generate", action: containerpb.SetMasterAuthRequest_GENERATE_PASSWORD,
			check: func(t *testing.T, ma *containerpb.MasterAuth) {
				t.Helper()
				assert.Len(t, ma.GetPassword(), 24)
			},
		},
		{
			name: "set username", action: containerpb.SetMasterAuthRequest_SET_USERNAME,
			update: &containerpb.MasterAuth{Username: "root"},
			check: func(t *testing.T, ma *containerpb.MasterAuth) {
				t.Helper()
				assert.Equal(t, "root", ma.GetUsername())
			},
		},
		{
			name: "empty username", action: containerpb.SetMasterAuthRequest_SET_USERNAME,
			want: codes.InvalidArgument, wantMsg: "Username",
		},
		{
			name:    "unknown action",
			action:  containerpb.SetMasterAuthRequest_UNKNOWN,
			want:    codes.InvalidArgument,
			wantMsg: "Action",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.ready(t, zone, stdCluster("m"))

			op, err := e.gc.SetMasterAuth(t.Context(), &containerpb.SetMasterAuthRequest{
				Name: clusterPath(zone, "m"), Action: tt.action, Update: tt.update,
			})
			if tt.want != codes.OK {
				assertCode(t, tt.want, err, tt.wantMsg)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, containerpb.Operation_SET_MASTER_AUTH, op.GetOperationType())

			e.settle()
			tt.check(t, e.getCluster(t, zone, "m").GetMasterAuth())
		})
	}
}

func TestLegacyAbacAndNetworkPolicy(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.ready(t, zone, stdCluster("n"))
	name := clusterPath(zone, "n")

	_, err := e.gc.SetLegacyAbac(t.Context(), &containerpb.SetLegacyAbacRequest{Name: name, Enabled: true})
	require.NoError(t, err)
	e.settle()
	assert.True(t, e.getCluster(t, zone, "n").GetLegacyAbac().GetEnabled())

	_, err = e.gc.SetNetworkPolicy(t.Context(), &containerpb.SetNetworkPolicyRequest{
		Name:          name,
		NetworkPolicy: &containerpb.NetworkPolicy{Enabled: true, Provider: containerpb.NetworkPolicy_CALICO},
	})
	assertCode(t, codes.FailedPrecondition, err, "addon must be enabled")

	_, err = e.gc.SetAddonsConfig(t.Context(), &containerpb.SetAddonsConfigRequest{
		Name:         name,
		AddonsConfig: &containerpb.AddonsConfig{NetworkPolicyConfig: &containerpb.NetworkPolicyConfig{Disabled: false}},
	})
	require.NoError(t, err)
	e.settle()

	_, err = e.gc.SetNetworkPolicy(t.Context(), &containerpb.SetNetworkPolicyRequest{
		Name: name, NetworkPolicy: &containerpb.NetworkPolicy{Enabled: true},
	})
	assertCode(t, codes.InvalidArgument, err, "CALICO")

	_, err = e.gc.SetNetworkPolicy(t.Context(), &containerpb.SetNetworkPolicyRequest{Name: name})
	assertCode(t, codes.InvalidArgument, err, "must be specified")

	op, err := e.gc.SetNetworkPolicy(t.Context(), &containerpb.SetNetworkPolicyRequest{
		Name:          name,
		NetworkPolicy: &containerpb.NetworkPolicy{Enabled: true, Provider: containerpb.NetworkPolicy_CALICO},
	})
	require.NoError(t, err)
	assert.Equal(t, containerpb.Operation_SET_NETWORK_POLICY, op.GetOperationType())
	e.settle()
	assert.True(t, e.getCluster(t, zone, "n").GetNetworkPolicy().GetEnabled())

	_, err = e.gc.SetAddonsConfig(t.Context(), &containerpb.SetAddonsConfigRequest{
		Name:         name,
		AddonsConfig: &containerpb.AddonsConfig{NetworkPolicyConfig: &containerpb.NetworkPolicyConfig{Disabled: true}},
	})
	assertCode(t, codes.FailedPrecondition, err, "cannot be disabled")

	cl := e.getCluster(t, zone, "n")
	assert.False(t, cl.GetAddonsConfig().GetHttpLoadBalancing().GetDisabled())
}

func TestSetMaintenancePolicy(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 11, 1, 3, 0, 0, 0, time.UTC)
	recurring := &containerpb.MaintenanceWindow{Policy: &containerpb.MaintenanceWindow_RecurringWindow{
		RecurringWindow: &containerpb.RecurringTimeWindow{
			Window: &containerpb.TimeWindow{
				StartTime: timestamppb.New(start),
				EndTime:   timestamppb.New(start.Add(4 * time.Hour)),
			},
			Recurrence: "FREQ=WEEKLY;BYDAY=SA",
		},
	}}

	tests := []struct {
		window  *containerpb.MaintenanceWindow
		name    string
		version string
		wantMsg string
		want    codes.Code
	}{
		{
			name: "daily",
			window: &containerpb.MaintenanceWindow{Policy: &containerpb.MaintenanceWindow_DailyMaintenanceWindow{
				DailyMaintenanceWindow: &containerpb.DailyMaintenanceWindow{StartTime: "03:00"},
			}},
		},
		{name: "recurring", window: recurring},
		{
			name: "bad daily",
			window: &containerpb.MaintenanceWindow{Policy: &containerpb.MaintenanceWindow_DailyMaintenanceWindow{
				DailyMaintenanceWindow: &containerpb.DailyMaintenanceWindow{StartTime: "25:99"},
			}},
			want:    codes.InvalidArgument,
			wantMsg: "HH:MM",
		},
		{
			name: "recurring without rule", want: codes.InvalidArgument, wantMsg: "RRULE",
			window: &containerpb.MaintenanceWindow{Policy: &containerpb.MaintenanceWindow_RecurringWindow{
				RecurringWindow: &containerpb.RecurringTimeWindow{
					Window: &containerpb.TimeWindow{
						StartTime: timestamppb.New(start),
						EndTime:   timestamppb.New(start.Add(time.Hour)),
					},
				},
			}},
		},
		{
			name:    "stale version",
			window:  recurring,
			version: "deadbeef",
			want:    codes.FailedPrecondition,
			wantMsg: "stale",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			before := e.ready(t, zone, stdCluster("mw"))
			rv := before.GetMaintenancePolicy().GetResourceVersion()

			if tt.version != "" {
				rv = tt.version
			}

			op, err := e.gc.SetMaintenancePolicy(t.Context(), &containerpb.SetMaintenancePolicyRequest{
				Name: clusterPath(
					zone,
					"mw",
				),
				MaintenancePolicy: &containerpb.MaintenancePolicy{Window: tt.window, ResourceVersion: rv},
			})
			if tt.want != codes.OK {
				assertCode(t, tt.want, err, tt.wantMsg)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, containerpb.Operation_SET_MAINTENANCE_POLICY, op.GetOperationType())
			e.settle()

			after := e.getCluster(t, zone, "mw").GetMaintenancePolicy()
			assert.NotNil(t, after.GetWindow())
			assert.NotEqual(t, before.GetMaintenancePolicy().GetResourceVersion(), after.GetResourceVersion())
		})
	}

	t.Run("missing version", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		e.ready(t, zone, stdCluster("mw"))

		_, err := e.gc.SetMaintenancePolicy(t.Context(), &containerpb.SetMaintenancePolicyRequest{
			Name: clusterPath(zone, "mw"), MaintenancePolicy: &containerpb.MaintenancePolicy{Window: recurring},
		})
		assertCode(t, codes.InvalidArgument, err, "Resource version")
	})
}

func TestIPRotation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		credentials bool
	}{
		{"ip only", false},
		{"with credentials", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			before := e.ready(t, zone, stdCluster("rot"))
			name := clusterPath(zone, "rot")

			_, err := e.gc.CompleteIPRotation(t.Context(), &containerpb.CompleteIPRotationRequest{Name: name})
			assertCode(t, codes.FailedPrecondition, err, "not in progress")

			_, err = e.gc.StartIPRotation(
				t.Context(),
				&containerpb.StartIPRotationRequest{Name: name, RotateCredentials: tt.credentials},
			)
			require.NoError(t, err)
			e.settle()

			mid := e.getCluster(t, zone, "rot")
			assert.NotEqual(t, before.GetEndpoint(), mid.GetEndpoint())
			assert.Equal(
				t,
				tt.credentials,
				before.GetMasterAuth().GetClusterCaCertificate() != mid.GetMasterAuth().GetClusterCaCertificate(),
			)

			_, err = e.gc.StartIPRotation(t.Context(), &containerpb.StartIPRotationRequest{Name: name})
			assertCode(t, codes.FailedPrecondition, err, "already in progress")

			op, err := e.gc.CompleteIPRotation(t.Context(), &containerpb.CompleteIPRotationRequest{Name: name})
			require.NoError(t, err)
			assert.Equal(t, containerpb.Operation_UPDATE_CLUSTER, op.GetOperationType())
			e.settle()

			_, err = e.gc.CompleteIPRotation(t.Context(), &containerpb.CompleteIPRotationRequest{Name: name})
			assertCode(t, codes.FailedPrecondition, err, "not in progress")
			assert.Equal(t, mid.GetEndpoint(), e.getCluster(t, zone, "rot").GetEndpoint())
		})
	}
}

func TestLoggingMonitoringLocations(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.ready(
		t,
		region,
		&containerpb.Cluster{Name: "svc", NodePools: []*containerpb.NodePool{{Name: "p", InitialNodeCount: 1}}},
	)
	name := clusterPath(region, "svc")

	_, err := e.gc.SetLoggingService(
		t.Context(),
		&containerpb.SetLoggingServiceRequest{Name: name, LoggingService: "bogus"},
	)
	assertCode(t, codes.InvalidArgument, err, "Invalid service")

	_, err = e.gc.SetLoggingService(
		t.Context(),
		&containerpb.SetLoggingServiceRequest{Name: name, LoggingService: "none"},
	)
	require.NoError(t, err)
	e.settle()

	_, err = e.gc.SetMonitoringService(
		t.Context(),
		&containerpb.SetMonitoringServiceRequest{Name: name, MonitoringService: "monitoring.googleapis.com"},
	)
	require.NoError(t, err)
	e.settle()

	got := e.getCluster(t, region, "svc")
	assert.Equal(t, "none", got.GetLoggingService())
	assert.Equal(t, "monitoring.googleapis.com", got.GetMonitoringService())

	_, err = e.gc.SetLocations(
		t.Context(),
		&containerpb.SetLocationsRequest{Name: name, Locations: []string{"europe-west1-b"}},
	)
	assertCode(t, codes.InvalidArgument, err, "not a zone of")

	_, err = e.gc.SetLocations(t.Context(), &containerpb.SetLocationsRequest{Name: name})
	assertCode(t, codes.InvalidArgument, err, "At least one")

	_, err = e.gc.SetLocations(
		t.Context(),
		&containerpb.SetLocationsRequest{Name: name, Locations: []string{"us-central1-b", "us-central1-f"}},
	)
	require.NoError(t, err)
	e.settle()

	got = e.getCluster(t, region, "svc")
	assert.Equal(t, []string{"us-central1-b", "us-central1-f"}, got.GetLocations())
	assert.Equal(t, got.GetLocations(), got.GetNodePools()[0].GetLocations())
	assert.Equal(t, int32(2), got.GetCurrentNodeCount())
}

func TestUpdateCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		update  *containerpb.ClusterUpdate
		check   func(t *testing.T, c *containerpb.Cluster)
		name    string
		wantMsg string
		wantOp  containerpb.Operation_Type
		want    codes.Code
	}{
		{
			name: "monitoring", update: &containerpb.ClusterUpdate{DesiredMonitoringService: "none"},
			wantOp: containerpb.Operation_UPDATE_CLUSTER,
			check: func(t *testing.T, c *containerpb.Cluster) {
				t.Helper()
				assert.Equal(t, "none", c.GetMonitoringService())
			},
		},
		{
			name: "generic message field", wantOp: containerpb.Operation_UPDATE_CLUSTER,
			update: &containerpb.ClusterUpdate{DesiredBinaryAuthorization: &containerpb.BinaryAuthorization{
				EvaluationMode: containerpb.BinaryAuthorization_PROJECT_SINGLETON_POLICY_ENFORCE,
			}},
			check: func(t *testing.T, c *containerpb.Cluster) {
				t.Helper()
				assert.Equal(
					t,
					containerpb.BinaryAuthorization_PROJECT_SINGLETON_POLICY_ENFORCE,
					c.GetBinaryAuthorization().GetEvaluationMode(),
				)
			},
		},
		{
			name: "network config enum", wantOp: containerpb.Operation_UPDATE_CLUSTER,
			update: &containerpb.ClusterUpdate{DesiredDatapathProvider: containerpb.DatapathProvider_ADVANCED_DATAPATH},
			check: func(t *testing.T, c *containerpb.Cluster) {
				t.Helper()
				assert.Equal(
					t,
					containerpb.DatapathProvider_ADVANCED_DATAPATH,
					c.GetNetworkConfig().GetDatapathProvider(),
				)
			},
		},
		{
			name:   "intra node visibility",
			wantOp: containerpb.Operation_UPDATE_CLUSTER,
			update: &containerpb.ClusterUpdate{
				DesiredIntraNodeVisibilityConfig: &containerpb.IntraNodeVisibilityConfig{Enabled: true},
			},
			check: func(t *testing.T, c *containerpb.Cluster) {
				t.Helper()
				assert.True(t, c.GetNetworkConfig().GetEnableIntraNodeVisibility())
			},
		},
		{
			name:   "release channel",
			wantOp: containerpb.Operation_UPDATE_CLUSTER,
			update: &containerpb.ClusterUpdate{
				DesiredReleaseChannel: &containerpb.ReleaseChannel{Channel: containerpb.ReleaseChannel_RAPID},
			},
			check: func(t *testing.T, c *containerpb.Cluster) {
				t.Helper()
				assert.Equal(t, containerpb.ReleaseChannel_RAPID, c.GetReleaseChannel().GetChannel())
			},
		},
		{
			name:   "authorized networks",
			wantOp: containerpb.Operation_UPDATE_CLUSTER,
			update: &containerpb.ClusterUpdate{
				DesiredMasterAuthorizedNetworksConfig: &containerpb.MasterAuthorizedNetworksConfig{
					Enabled: true,
					CidrBlocks: []*containerpb.MasterAuthorizedNetworksConfig_CidrBlock{
						{DisplayName: "o", CidrBlock: "203.0.113.0/24"},
					},
				},
			},
			check: func(t *testing.T, c *containerpb.Cluster) {
				t.Helper()
				assert.Equal(
					t,
					"203.0.113.0/24",
					c.GetMasterAuthorizedNetworksConfig().GetCidrBlocks()[0].GetCidrBlock(),
				)
			},
		},
		{
			name:    "bad authorized network",
			want:    codes.InvalidArgument,
			wantMsg: "Invalid CIDR",
			update: &containerpb.ClusterUpdate{
				DesiredMasterAuthorizedNetworksConfig: &containerpb.MasterAuthorizedNetworksConfig{
					CidrBlocks: []*containerpb.MasterAuthorizedNetworksConfig_CidrBlock{{CidrBlock: "nope"}},
				},
			},
		},
		{
			name: "master upgrade", wantOp: containerpb.Operation_UPGRADE_MASTER,
			update: &containerpb.ClusterUpdate{DesiredMasterVersion: "latest"},
			check: func(t *testing.T, c *containerpb.Cluster) {
				t.Helper()
				assert.Equal(t, "1.35.1-gke.1000000", c.GetCurrentMasterVersion())
				assert.NotEqual(t, c.GetInitialClusterVersion(), c.GetCurrentMasterVersion())
			},
		},
		{
			name: "master downgrade", want: codes.InvalidArgument, wantMsg: "downgrades",
			update: &containerpb.ClusterUpdate{DesiredMasterVersion: "1.33.7-gke.1044000"},
		},
		{
			name: "unknown master version", want: codes.InvalidArgument, wantMsg: "unsupported",
			update: &containerpb.ClusterUpdate{DesiredMasterVersion: "0.1"},
		},
		{
			name:    "unsupported field",
			want:    codes.InvalidArgument,
			wantMsg: "not supported by the emulator",
			update: &containerpb.ClusterUpdate{
				DesiredGcfsConfig: &containerpb.GcfsConfig{Enabled: true},
				DesiredImage:      "x",
			},
		},
		{name: "empty", update: &containerpb.ClusterUpdate{}, want: codes.InvalidArgument, wantMsg: "No update"},
		{
			name: "stale etag", want: codes.FailedPrecondition, wantMsg: "etag",
			update: &containerpb.ClusterUpdate{Etag: "stale", DesiredMonitoringService: "none"},
		},
		{
			name: "private endpoint without private nodes", want: codes.FailedPrecondition, wantMsg: "private cluster",
			update: &containerpb.ClusterUpdate{DesiredEnablePrivateEndpoint: new(true)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.ready(
				t,
				zone,
				&containerpb.Cluster{Name: "u", InitialNodeCount: 1, InitialClusterVersion: "1.34.2-gke.1008000"},
			)

			op, err := e.gc.UpdateCluster(
				t.Context(),
				&containerpb.UpdateClusterRequest{Name: clusterPath(zone, "u"), Update: tt.update},
			)
			if tt.want != codes.OK {
				assertCode(t, tt.want, err, tt.wantMsg)

				assert.Equal(t, containerpb.Cluster_RUNNING, e.getCluster(t, zone, "u").GetStatus())

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantOp, op.GetOperationType())
			assert.Equal(t, containerpb.Cluster_RECONCILING, e.getCluster(t, zone, "u").GetStatus())
			e.settle()
			tt.check(t, e.getCluster(t, zone, "u"))
		})
	}
}

func TestUpdateClusterCurrentEtag(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	c := e.ready(t, zone, stdCluster("et"))

	_, err := e.gc.UpdateCluster(t.Context(), &containerpb.UpdateClusterRequest{
		Name: clusterPath(
			zone,
			"et",
		),
		Update: &containerpb.ClusterUpdate{Etag: c.GetEtag(), DesiredMonitoringService: "none"},
	})
	require.NoError(t, err)
	e.settle()
	assert.NotEqual(t, c.GetEtag(), e.getCluster(t, zone, "et").GetEtag())
}
