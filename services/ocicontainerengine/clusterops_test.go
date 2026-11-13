package ocicontainerengine_test

import (
	"net/http"
	"testing"
	"time"

	ke "github.com/oracle/oci-go-sdk/v65/containerengine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (e *env) mkEnhanced(t *testing.T, name string) ke.Cluster {
	t.Helper()

	return e.mkCluster(t, name, func(d *ke.CreateClusterDetails) { d.Type = ke.ClusterTypeEnhancedCluster })
}

func (e *env) wrType(t *testing.T, id *string) ke.WorkRequestOperationTypeEnum {
	t.Helper()

	wr, err := e.ce.GetWorkRequest(t.Context(), ke.GetWorkRequestRequest{WorkRequestId: id})
	require.NoError(t, err)

	return wr.OperationType
}

func TestAddonLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	c := e.mkEnhanced(t, "addons")

	opts, err := e.ce.ListAddonOptions(ctx, ke.ListAddonOptionsRequest{KubernetesVersion: new(testVersion)})
	require.NoError(t, err)
	require.NotEmpty(t, opts.Items)

	var kubeProxy ke.AddonOptionSummary

	for _, o := range opts.Items {
		if *o.Name == "kube-proxy" {
			kubeProxy = o
		}
	}

	require.NotNil(t, kubeProxy.Name)
	assert.True(t, *kubeProxy.IsEssential)
	require.Len(t, kubeProxy.Versions, 1)
	assert.Equal(t, "1.34.10", *kubeProxy.Versions[0].VersionNumber)

	one, err := e.ce.ListAddonOptions(ctx, ke.ListAddonOptionsRequest{
		KubernetesVersion: new(testVersion), AddonName: new("CoreDNS"),
	})
	require.NoError(t, err)
	require.Len(t, one.Items, 1)

	inst, err := e.ce.InstallAddon(ctx, ke.InstallAddonRequest{
		ClusterId:      c.Id,
		AddonName:      new("Certificate Manager"),
		Configurations: []ke.AddonConfiguration{{Key: new("replicas"), Value: new("2")}},
	})
	require.NoError(t, err)
	assert.Equal(t, ke.WorkRequestOperationTypeEnableAddon, e.wrType(t, inst.OpcWorkRequestId))

	mid, err := e.ce.GetAddon(ctx, ke.GetAddonRequest{ClusterId: c.Id, AddonName: new("Certificate Manager")})
	require.NoError(t, err)
	assert.Equal(t, ke.AddonLifecycleStateCreating, mid.LifecycleState)

	e.settle()

	got, err := e.ce.GetAddon(ctx, ke.GetAddonRequest{ClusterId: c.Id, AddonName: new("certificatemanager")})
	require.NoError(t, err)
	assert.Equal(t, ke.AddonLifecycleStateActive, got.LifecycleState)
	assert.Equal(t, "1.17.1", *got.Version)
	assert.Equal(t, "1.17.1", *got.CurrentInstalledVersion)
	require.Len(t, got.Configurations, 1)
	assert.NotEmpty(t, *got.Etag)

	list, err := e.ce.ListAddons(ctx, ke.ListAddonsRequest{ClusterId: c.Id})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)

	upd, err := e.ce.UpdateAddon(ctx, ke.UpdateAddonRequest{
		ClusterId: c.Id, AddonName: new("Certificate Manager"), IfMatch: got.Etag,
		Configurations: []ke.AddonConfiguration{{Key: new("replicas"), Value: new("3")}},
	})
	require.NoError(t, err)
	assert.Equal(t, ke.WorkRequestOperationTypeUpdateAddon, e.wrType(t, upd.OpcWorkRequestId))
	e.settle()

	got, err = e.ce.GetAddon(ctx, ke.GetAddonRequest{ClusterId: c.Id, AddonName: new("Certificate Manager")})
	require.NoError(t, err)
	assert.Equal(t, "3", *got.Configurations[0].Value)

	snap := e.backend.Snapshot(ctx)
	fresh := newEnv(t, envOpts{})
	require.NoError(t, fresh.backend.Restore(ctx, snap))

	_, err = fresh.ce.GetAddon(ctx, ke.GetAddonRequest{ClusterId: c.Id, AddonName: new("Certificate Manager")})
	require.NoError(t, err)

	dis, err := e.ce.DisableAddon(ctx, ke.DisableAddonRequest{
		ClusterId: c.Id, AddonName: new("Certificate Manager"), IsRemoveExistingAddOn: new(true),
	})
	require.NoError(t, err)
	assert.Equal(t, ke.WorkRequestOperationTypeDisableAddon, e.wrType(t, dis.OpcWorkRequestId))
	e.settle()

	_, err = e.ce.GetAddon(ctx, ke.GetAddonRequest{ClusterId: c.Id, AddonName: new("Certificate Manager")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	after, err := e.ce.ListAddons(ctx, ke.ListAddonsRequest{ClusterId: c.Id})
	require.NoError(t, err)
	assert.Empty(t, after.Items)
}

func TestAddonErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	basic := e.mkCluster(t, "basic", nil)
	enh := e.mkEnhanced(t, "enh")

	install := func(id *string, d ke.InstallAddonDetails) error {
		_, err := e.ce.InstallAddon(ctx, ke.InstallAddonRequest{ClusterId: id, InstallAddonDetails: d})

		return err
	}

	require.NoError(t, install(enh.Id, ke.InstallAddonDetails{AddonName: new("CoreDNS")}))

	tests := []struct {
		err    error
		name   string
		code   string
		status int
	}{
		{name: "basic cluster", status: 400, code: "InvalidParameter",
			err: install(basic.Id, ke.InstallAddonDetails{AddonName: new("CoreDNS")})},
		{name: "unknown addon", status: 400, code: "InvalidParameter",
			err: install(enh.Id, ke.InstallAddonDetails{AddonName: new("NoSuchAddon")})},
		{name: "bad version", status: 400, code: "InvalidParameter",
			err: install(enh.Id, ke.InstallAddonDetails{AddonName: new("Istio"), Version: new("9.9.9")})},
		{name: "duplicate keys", status: 400, code: "InvalidParameter", err: install(enh.Id, ke.InstallAddonDetails{
			AddonName:      new("Istio"),
			Configurations: []ke.AddonConfiguration{{Key: new("a"), Value: new("1")}, {Key: new("a"), Value: new("2")}},
		})},
		{name: "already installed", status: 409, code: "NotAuthorizedOrResourceAlreadyExists",
			err: install(enh.Id, ke.InstallAddonDetails{AddonName: new("CoreDNS")})},
		{name: "unknown cluster", status: 404, code: "NotAuthorizedOrNotFound",
			err: install(new("ocid1.cluster.oc1.iad.nope"), ke.InstallAddonDetails{AddonName: new("CoreDNS")})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			se := svcErr(t, tt.err)
			assert.Equal(t, tt.status, se.GetHTTPStatusCode())
			assert.Equal(t, tt.code, se.GetCode())
		})
	}

	require.NoError(
		t,
		install(enh.Id, ke.InstallAddonDetails{AddonName: new("CoreDNS"), IsOverrideExisting: new(true)}),
	)

	_, err := e.ce.ListAddonOptions(ctx, ke.ListAddonOptionsRequest{KubernetesVersion: new("v1.20.0")})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.ce.DisableAddon(ctx, ke.DisableAddonRequest{
		ClusterId: enh.Id, AddonName: new("Istio"), IsRemoveExistingAddOn: new(false),
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestWorkloadMappingLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	c := e.mkEnhanced(t, "wm")

	created, err := e.ce.CreateWorkloadMapping(ctx, ke.CreateWorkloadMappingRequest{
		ClusterId: c.Id,
		Namespace: new("payments"), MappedCompartmentId: new(testCompartment),
		FreeformTags: map[string]string{"team": "a"},
	})
	require.NoError(t, err)
	assert.Equal(t, ke.WorkloadMappingLifecycleStateCreating, created.LifecycleState)
	assert.NotEmpty(t, *created.Etag)

	id := created.Id
	e.settle()

	got, err := e.ce.GetWorkloadMapping(ctx, ke.GetWorkloadMappingRequest{ClusterId: c.Id, WorkloadMappingId: id})
	require.NoError(t, err)
	assert.Equal(t, ke.WorkloadMappingLifecycleStateActive, got.LifecycleState)
	assert.Equal(t, "payments", *got.Namespace)
	assert.Equal(t, "a", got.FreeformTags["team"])

	list, err := e.ce.ListWorkloadMappings(ctx, ke.ListWorkloadMappingsRequest{ClusterId: c.Id})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)

	upd, err := e.ce.UpdateWorkloadMapping(ctx, ke.UpdateWorkloadMappingRequest{
		ClusterId: c.Id, WorkloadMappingId: id, IfMatch: got.Etag,
		MappedCompartmentId: new(testCompartment), FreeformTags: map[string]string{"team": "b"},
	})
	require.NoError(t, err)
	assert.Equal(t, ke.WorkloadMappingLifecycleStateUpdating, upd.LifecycleState)
	e.settle()

	snap := e.backend.Snapshot(ctx)
	fresh := newEnv(t, envOpts{})
	require.NoError(t, fresh.backend.Restore(ctx, snap))

	restored, err := fresh.ce.GetWorkloadMapping(
		ctx,
		ke.GetWorkloadMappingRequest{ClusterId: c.Id, WorkloadMappingId: id},
	)
	require.NoError(t, err)
	assert.Equal(t, "b", restored.FreeformTags["team"])

	_, err = e.ce.DeleteWorkloadMapping(ctx, ke.DeleteWorkloadMappingRequest{ClusterId: c.Id, WorkloadMappingId: id})
	require.NoError(t, err)

	mid, err := e.ce.GetWorkloadMapping(ctx, ke.GetWorkloadMappingRequest{ClusterId: c.Id, WorkloadMappingId: id})
	require.NoError(t, err)
	assert.Equal(t, ke.WorkloadMappingLifecycleStateDeleting, mid.LifecycleState)
	e.settle()

	gone, err := e.ce.GetWorkloadMapping(ctx, ke.GetWorkloadMappingRequest{ClusterId: c.Id, WorkloadMappingId: id})
	require.NoError(t, err)
	assert.Equal(t, ke.WorkloadMappingLifecycleStateDeleted, gone.LifecycleState)

	after, err := e.ce.ListWorkloadMappings(ctx, ke.ListWorkloadMappingsRequest{ClusterId: c.Id})
	require.NoError(t, err)
	assert.Empty(t, after.Items)
}

func TestWorkloadMappingErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	c := e.mkEnhanced(t, "wmerr")

	create := func(clusterID *string, ns string, token *string) error {
		_, err := e.ce.CreateWorkloadMapping(ctx, ke.CreateWorkloadMappingRequest{
			ClusterId: clusterID, OpcRetryToken: token,
			Namespace: new(ns), MappedCompartmentId: new(testCompartment),
		})

		return err
	}

	require.NoError(t, create(c.Id, "dup", nil))

	tests := []struct {
		err    error
		name   string
		status int
	}{
		{create(c.Id, "Bad_NS", nil), "bad namespace", http.StatusBadRequest},
		{create(c.Id, "dup", nil), "duplicate", http.StatusConflict},
		{create(new("ocid1.cluster.oc1.iad.nope"), "x", nil), "unknown cluster", http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.status, svcErr(t, tt.err).GetHTTPStatusCode())
		})
	}

	require.NoError(t, create(c.Id, "tokened", new("tok")))
	require.NoError(t, create(c.Id, "tokened", new("tok")))

	list, err := e.ce.ListWorkloadMappings(ctx, ke.ListWorkloadMappingsRequest{ClusterId: c.Id})
	require.NoError(t, err)
	assert.Len(t, list.Items, 2)

	_, err = e.ce.GetWorkloadMapping(ctx, ke.GetWorkloadMappingRequest{ClusterId: c.Id, WorkloadMappingId: new("nope")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestCredentialRotation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	c := e.mkCluster(t, "rot", nil)

	idle, err := e.ce.GetCredentialRotationStatus(ctx, ke.GetCredentialRotationStatusRequest{ClusterId: c.Id})
	require.NoError(t, err)
	assert.Equal(t, ke.CredentialRotationStatusStatusCompleted, idle.Status)

	_, err = e.ce.CompleteCredentialRotation(ctx, ke.CompleteCredentialRotationRequest{ClusterId: c.Id})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.ce.StartCredentialRotation(ctx, ke.StartCredentialRotationRequest{
		ClusterId:                   c.Id,
		AutoCompletionDelayDuration: new("P91D"),
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	start, err := e.ce.StartCredentialRotation(ctx, ke.StartCredentialRotationRequest{
		ClusterId:                   c.Id,
		AutoCompletionDelayDuration: new("P5D"),
	})
	require.NoError(t, err)
	assert.Equal(t, ke.WorkRequestOperationTypeClusterUpdate, e.wrType(t, start.OpcWorkRequestId))

	st, err := e.ce.GetCredentialRotationStatus(ctx, ke.GetCredentialRotationStatusRequest{ClusterId: c.Id})
	require.NoError(t, err)
	assert.Equal(t, ke.CredentialRotationStatusStatusInProgress, st.Status)
	assert.Equal(t, ke.CredentialRotationStatusStatusDetailsIssuingNewCredentials, st.StatusDetails)

	e.settle()

	st, err = e.ce.GetCredentialRotationStatus(ctx, ke.GetCredentialRotationStatusRequest{ClusterId: c.Id})
	require.NoError(t, err)
	assert.Equal(t, ke.CredentialRotationStatusStatusWaiting, st.Status)
	assert.Equal(t, ke.CredentialRotationStatusStatusDetailsNewCredentialsIssued, st.StatusDetails)
	require.NotNil(t, st.TimeAutoCompletionScheduled)

	_, err = e.ce.StartCredentialRotation(ctx, ke.StartCredentialRotationRequest{
		ClusterId:                   c.Id,
		AutoCompletionDelayDuration: new("P5D"),
	})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	snap := e.backend.Snapshot(ctx)
	fresh := newEnv(t, envOpts{})
	require.NoError(t, fresh.backend.Restore(ctx, snap))

	restored, err := fresh.ce.GetCredentialRotationStatus(ctx, ke.GetCredentialRotationStatusRequest{ClusterId: c.Id})
	require.NoError(t, err)
	assert.Equal(t, ke.CredentialRotationStatusStatusWaiting, restored.Status)

	done, err := e.ce.CompleteCredentialRotation(ctx, ke.CompleteCredentialRotationRequest{ClusterId: c.Id})
	require.NoError(t, err)
	assert.NotEmpty(t, *done.OpcWorkRequestId)

	st, err = e.ce.GetCredentialRotationStatus(ctx, ke.GetCredentialRotationStatusRequest{ClusterId: c.Id})
	require.NoError(t, err)
	assert.Equal(t, ke.CredentialRotationStatusStatusDetailsRetiringOldCredentials, st.StatusDetails)
	e.settle()

	st, err = e.ce.GetCredentialRotationStatus(ctx, ke.GetCredentialRotationStatusRequest{ClusterId: c.Id})
	require.NoError(t, err)
	assert.Equal(t, ke.CredentialRotationStatusStatusCompleted, st.Status)
	assert.Equal(t, ke.CredentialRotationStatusStatusDetailsCompleted, st.StatusDetails)
}

func TestCredentialRotationAutoCompletes(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	c := e.mkCluster(t, "auto", nil)

	_, err := e.ce.StartCredentialRotation(ctx, ke.StartCredentialRotationRequest{
		ClusterId:                   c.Id,
		AutoCompletionDelayDuration: new("P1D"),
	})
	require.NoError(t, err)
	e.settle()

	e.clock.Advance(25 * time.Hour)

	st, err := e.ce.GetCredentialRotationStatus(ctx, ke.GetCredentialRotationStatusRequest{ClusterId: c.Id})
	require.NoError(t, err)
	assert.Equal(t, ke.CredentialRotationStatusStatusCompleted, st.Status)
}

func (e *env) startDecom(t *testing.T, id *string) (ke.StartPublicApiEndpointDecommissionResponse, error) {
	t.Helper()

	return e.ce.StartPublicApiEndpointDecommission(t.Context(),
		ke.StartPublicApiEndpointDecommissionRequest{ClusterId: id})
}

func TestPublicApiEndpointDecommission(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	c := e.mkCluster(t, "legacy", func(d *ke.CreateClusterDetails) { d.EndpointConfig = nil; d.VcnId = new(vcnA) })

	_, err := e.ce.StartPublicApiEndpointDecommission(
		ctx,
		ke.StartPublicApiEndpointDecommissionRequest{ClusterId: c.Id},
	)
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "not migrated yet")

	_, err = e.ce.ClusterMigrateToNativeVcn(ctx, ke.ClusterMigrateToNativeVcnRequest{
		ClusterId:      c.Id,
		EndpointConfig: &ke.ClusterEndpointConfig{SubnetId: new(subRegional)},
	})
	require.NoError(t, err)
	e.settle()

	pre, err := e.ce.GetPublicApiEndpointDecommissionStatus(
		ctx,
		ke.GetPublicApiEndpointDecommissionStatusRequest{ClusterId: c.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, ke.PublicApiEndpointDecommissionStatusStatusPending, pre.Status)

	start, err := e.ce.StartPublicApiEndpointDecommission(
		ctx,
		ke.StartPublicApiEndpointDecommissionRequest{ClusterId: c.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, ke.WorkRequestOperationTypeStartPublicApiEndpointDecommission, e.wrType(t, start.OpcWorkRequestId))

	st, err := e.ce.GetPublicApiEndpointDecommissionStatus(
		ctx,
		ke.GetPublicApiEndpointDecommissionStatusRequest{ClusterId: c.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, ke.PublicApiEndpointDecommissionStatusStatusInProgress, st.Status)
	e.settle()

	st, err = e.ce.GetPublicApiEndpointDecommissionStatus(
		ctx,
		ke.GetPublicApiEndpointDecommissionStatusRequest{ClusterId: c.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, ke.PublicApiEndpointDecommissionStatusStatusDecommissioned, st.Status)
	require.NotNil(t, st.TimeDecommissionRollbackDeadline)

	deadline := st.TimeDecommissionRollbackDeadline.Time

	cl, err := e.ce.GetCluster(ctx, ke.GetClusterRequest{ClusterId: c.Id})
	require.NoError(t, err)
	assert.Empty(t, cl.Endpoints.Kubernetes)

	_, err = e.ce.ExtendEndpointDecommissionRollbackDeadline(ctx, ke.ExtendEndpointDecommissionRollbackDeadlineRequest{
		ClusterId:             c.Id,
		RollbackDeadlineDelay: new("P31D"),
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	ext, err := e.ce.ExtendEndpointDecommissionRollbackDeadline(
		ctx,
		ke.ExtendEndpointDecommissionRollbackDeadlineRequest{
			ClusterId:             c.Id,
			RollbackDeadlineDelay: new("P2D"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, deadline.Add(48*time.Hour), ext.TimeDecommissionRollbackDeadline.Time)

	rb, err := e.ce.RollbackPublicApiEndpointDecommission(
		ctx,
		ke.RollbackPublicApiEndpointDecommissionRequest{ClusterId: c.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, ke.WorkRequestOperationTypeRollbackPublicApiEndpointDecommission, e.wrType(t, rb.OpcWorkRequestId))
	e.settle()

	st, err = e.ce.GetPublicApiEndpointDecommissionStatus(
		ctx,
		ke.GetPublicApiEndpointDecommissionStatusRequest{ClusterId: c.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, ke.PublicApiEndpointDecommissionStatusStatusPending, st.Status)

	cl, err = e.ce.GetCluster(ctx, ke.GetClusterRequest{ClusterId: c.Id})
	require.NoError(t, err)
	assert.NotEmpty(t, cl.Endpoints.Kubernetes, "the legacy endpoint is back")

	_, err = e.ce.RollbackPublicApiEndpointDecommission(
		ctx,
		ke.RollbackPublicApiEndpointDecommissionRequest{ClusterId: c.Id},
	)
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.startDecom(t, c.Id)
	require.NoError(t, err)
	e.settle()

	e.clock.Advance(31 * 24 * time.Hour)

	st, err = e.ce.GetPublicApiEndpointDecommissionStatus(
		ctx,
		ke.GetPublicApiEndpointDecommissionStatusRequest{ClusterId: c.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, ke.PublicApiEndpointDecommissionStatusStatusFinalized, st.Status)

	_, err = e.ce.RollbackPublicApiEndpointDecommission(
		ctx,
		ke.RollbackPublicApiEndpointDecommissionRequest{ClusterId: c.Id},
	)
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "past the deadline")
}

func TestUpdateClusterEndpointConfig(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	c := e.mkCluster(t, "ep", nil)
	require.NotEmpty(t, c.Endpoints.PublicEndpoint)

	upd, err := e.ce.UpdateClusterEndpointConfig(ctx, ke.UpdateClusterEndpointConfigRequest{
		ClusterId:         c.Id,
		IsPublicIpEnabled: new(false), NsgIds: []string{"ocid1.networksecuritygroup.oc1.iad.nsg"},
	})
	require.NoError(t, err)
	assert.Equal(t, ke.WorkRequestOperationTypeClusterUpdate, e.wrType(t, upd.OpcWorkRequestId))
	e.settle()

	got, err := e.ce.GetCluster(ctx, ke.GetClusterRequest{ClusterId: c.Id})
	require.NoError(t, err)
	assert.Empty(t, got.Endpoints.PublicEndpoint)
	assert.False(t, *got.EndpointConfig.IsPublicIpEnabled)
	assert.Equal(t, []string{"ocid1.networksecuritygroup.oc1.iad.nsg"}, got.EndpointConfig.NsgIds)

	_, err = e.ce.UpdateClusterEndpointConfig(ctx, ke.UpdateClusterEndpointConfigRequest{
		ClusterId:         c.Id,
		IsPublicIpEnabled: new(true),
	})
	require.NoError(t, err)
	e.settle()

	got, err = e.ce.GetCluster(ctx, ke.GetClusterRequest{ClusterId: c.Id})
	require.NoError(t, err)
	assert.NotEmpty(t, got.Endpoints.PublicEndpoint)

	legacy := e.mkCluster(
		t,
		"legacyep",
		func(d *ke.CreateClusterDetails) { d.EndpointConfig = nil; d.VcnId = new(vcnA) },
	)

	_, err = e.ce.UpdateClusterEndpointConfig(ctx, ke.UpdateClusterEndpointConfigRequest{
		ClusterId:         legacy.Id,
		IsPublicIpEnabled: new(true),
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.ce.UpdateClusterEndpointConfig(ctx, ke.UpdateClusterEndpointConfigRequest{
		ClusterId: c.Id, IfMatch: new("stale"),
	})
	assert.Error(t, err)
}

func TestClusterNodeActions(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	c := e.mkCluster(t, "nodes", nil)
	np := e.mkPool(t, c.Id, "workers", nil)
	node := np.Nodes[0]

	reboot, err := e.ce.RebootClusterNode(ctx, ke.RebootClusterNodeRequest{
		ClusterId: c.Id, NodeId: node.Id,
		NodeEvictionSettings: &ke.NodeEvictionSettings{EvictionGraceDuration: new("PT30M")},
	})
	require.NoError(t, err)
	assert.Equal(t, ke.WorkRequestOperationTypeClusterNodeReboot, e.wrType(t, reboot.OpcWorkRequestId))

	mid, err := e.ce.GetNodePool(ctx, ke.GetNodePoolRequest{NodePoolId: np.Id})
	require.NoError(t, err)

	states := map[string]ke.NodeLifecycleStateEnum{}
	for _, n := range mid.Nodes {
		states[*n.Id] = n.LifecycleState
	}

	assert.Equal(t, ke.NodeLifecycleStateUpdating, states[*node.Id])

	_, err = e.ce.RebootClusterNode(ctx, ke.RebootClusterNodeRequest{ClusterId: c.Id, NodeId: node.Id})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "node is still UPDATING")
	e.settle()

	replace, err := e.ce.ReplaceBootVolumeClusterNode(ctx, ke.ReplaceBootVolumeClusterNodeRequest{
		ClusterId: c.Id, NodeId: node.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, ke.WorkRequestOperationTypeClusterNodeReplaceBootVolume, e.wrType(t, replace.OpcWorkRequestId))
	e.settle()

	for _, call := range []func() error{
		func() error {
			_, cerr := e.ce.RebootClusterNode(ctx, ke.RebootClusterNodeRequest{ClusterId: c.Id, NodeId: new("nope")})

			return cerr
		},
		func() error {
			_, cerr := e.ce.RebootClusterNode(ctx, ke.RebootClusterNodeRequest{
				ClusterId: c.Id, NodeId: node.Id,
				NodeEvictionSettings: &ke.NodeEvictionSettings{EvictionGraceDuration: new("PT61M")},
			})

			return cerr
		},
	} {
		assert.Contains(t, []int{400, 404}, svcErr(t, call()).GetHTTPStatusCode())
	}
}

func TestDeleteWorkRequest(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()

	resp, err := e.ce.CreateCluster(ctx, ke.CreateClusterRequest{CreateClusterDetails: clusterDetails("wr")})
	require.NoError(t, err)

	_, err = e.ce.DeleteWorkRequest(ctx, ke.DeleteWorkRequestRequest{WorkRequestId: resp.OpcWorkRequestId})
	require.NoError(t, err)

	wr, err := e.ce.GetWorkRequest(ctx, ke.GetWorkRequestRequest{WorkRequestId: resp.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, ke.WorkRequestStatusCanceled, wr.Status)

	_, err = e.ce.DeleteWorkRequest(ctx, ke.DeleteWorkRequestRequest{WorkRequestId: resp.OpcWorkRequestId})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.ce.DeleteWorkRequest(ctx, ke.DeleteWorkRequestRequest{WorkRequestId: new("nope")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	second, err := e.ce.CreateCluster(ctx, ke.CreateClusterRequest{CreateClusterDetails: clusterDetails("wr2")})
	require.NoError(t, err)
	e.settle()

	_, err = e.ce.DeleteWorkRequest(ctx, ke.DeleteWorkRequestRequest{WorkRequestId: second.OpcWorkRequestId})
	assert.Equal(
		t,
		http.StatusConflict,
		svcErr(t, err).GetHTTPStatusCode(),
		"finished work requests cannot be canceled",
	)
}
