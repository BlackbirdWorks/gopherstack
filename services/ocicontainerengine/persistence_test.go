package ocicontainerengine_test

import (
	"net/http"
	"testing"

	ke "github.com/oracle/oci-go-sdk/v65/containerengine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oke "github.com/blackbirdworks/gopherstack/services/ocicontainerengine"
)

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	c := e.mkCluster(t, "persist", nil)
	np := e.mkPool(t, c.Id, "pool", nil)

	kc, err := e.ce.CreateKubeconfig(t.Context(), ke.CreateKubeconfigRequest{ClusterId: c.Id})
	require.NoError(t, err)

	want := readKubeconfig(t, kc)

	snap := e.backend.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	restored := newEnv(t, envOpts{})
	require.NoError(t, restored.backend.Restore(t.Context(), snap))
	restored.clock.ns.Store(e.clock.ns.Load())

	gotC, err := restored.ce.GetCluster(t.Context(), ke.GetClusterRequest{ClusterId: c.Id})
	require.NoError(t, err)
	assert.Equal(t, "persist", *gotC.Name)
	assert.Equal(t, ke.ClusterLifecycleStateActive, gotC.LifecycleState)
	assert.Equal(t, *c.Endpoints.PrivateEndpoint, *gotC.Endpoints.PrivateEndpoint)

	gotP, err := restored.ce.GetNodePool(t.Context(), ke.GetNodePoolRequest{NodePoolId: np.Id})
	require.NoError(t, err)
	assert.Len(t, gotP.Nodes, 3)

	wr, err := restored.ce.GetWorkRequest(
		t.Context(),
		ke.GetWorkRequestRequest{WorkRequestId: c.Metadata.CreatedByWorkRequestId},
	)
	require.NoError(t, err)
	assert.Equal(t, ke.WorkRequestStatusSucceeded, wr.Status)

	again, err := restored.ce.CreateKubeconfig(t.Context(), ke.CreateKubeconfigRequest{ClusterId: c.Id})
	require.NoError(t, err)
	assert.Equal(t, want, readKubeconfig(t, again), "CA and token survive a restore")

	restored.backend.Reset()

	_, err = restored.ce.GetCluster(t.Context(), ke.GetClusterRequest{ClusterId: c.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestRestoreRejectsBadSnapshots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    string
		wantErr bool
	}{
		{"null cluster", `{"version":1,"clusters":{"a":null}}`, true},
		{"null node pool", `{"version":1,"nodePools":{"a":null}}`, true},
		{"null work request", `{"version":1,"workRequests":{"a":null}}`, true},
		{"old version starts empty", `{"version":99}`, false},
		{"empty maps initialised", `{"version":1}`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := oke.NewInMemoryBackend()
			err := b.Restore(t.Context(), []byte(tt.data))

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			_, _, err = b.GetCluster("ocid1.cluster.oc1.iad.x")
			require.Error(t, err)
		})
	}
}

func TestErrorEnvelope(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	tests := []struct {
		name   string
		method string
		path   string
		body   string
		code   string
		status int
	}{
		{
			"missing cluster",
			http.MethodGet,
			"/20180222/clusters/ocid1.cluster.oc1.iad.nope",
			"",
			"NotAuthorizedOrNotFound",
			404,
		},
		{"bad json", http.MethodPost, "/20180222/clusters", "{", "CannotParseRequest", 400},
		{"no body", http.MethodPost, "/20180222/clusters", "", "MissingParameter", 400},
		{
			"no compartment",
			http.MethodPost,
			"/20180222/clusters",
			`{"name":"x","kubernetesVersion":"v1.34.2"}`,
			"MissingParameter",
			400,
		},
		{"list needs compartment", http.MethodGet, "/20180222/clusters", "", "MissingParameter", 400},
		{"bad sort", http.MethodGet, "/20180222/clusters?compartmentId=c&sortBy=BOGUS", "", "InvalidParameter", 400},
		{
			"bad sort order",
			http.MethodGet,
			"/20180222/nodePools?compartmentId=c&sortOrder=UP",
			"",
			"InvalidParameter",
			400,
		},
		{"unknown route", http.MethodGet, "/20180222/nothing", "", "NotFound", 404},
		{"missing work request", http.MethodGet, "/20180222/workRequests/nope", "", "NotAuthorizedOrNotFound", 404},
		{
			"missing work request errors",
			http.MethodGet,
			"/20180222/workRequests/nope/errors",
			"",
			"NotAuthorizedOrNotFound",
			404,
		},
		{
			"missing work request logs",
			http.MethodGet,
			"/20180222/workRequests/nope/logs",
			"",
			"NotAuthorizedOrNotFound",
			404,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status, body := rawDo(t, tt.method, e.base+tt.path, tt.body)
			assert.Equal(t, tt.status, status)
			assert.Contains(t, body, `"code":"`+tt.code+`"`)
			assert.Contains(t, body, `"message"`)
		})
	}
}

func TestAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		opts     envOpts
		name     string
		wantCode int
		raw      bool
	}{
		{envOpts{enforce: true, register: true}, "signed with registered key", http.StatusOK, false},
		{envOpts{enforce: true}, "unregistered key rejected", http.StatusUnauthorized, false},
		{envOpts{enforce: true, register: true}, "unsigned rejected", http.StatusUnauthorized, true},
		{envOpts{}, "unsigned accepted when not enforcing", http.StatusBadRequest, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, tt.opts)

			if tt.raw {
				status, _ := rawDo(t, http.MethodGet, e.base+"/20180222/clusters", "")
				assert.Equal(t, tt.wantCode, status)

				return
			}

			_, err := e.ce.ListClusters(
				t.Context(),
				ke.ListClustersRequest{CompartmentId: new(testCompartment)},
			)
			if tt.wantCode == http.StatusOK {
				require.NoError(t, err)

				return
			}

			assert.Equal(t, tt.wantCode, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

func TestAdminKeyEndpoint(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	status, _ := rawDo(t, http.MethodPost, e.base+"/_gopherstack/oci/keys",
		`{"tenancy":"`+testTenancy+`","user":"`+testUser+`","publicKey":`+quote(e.kp.pubPEM)+`}`)
	assert.Equal(t, http.StatusCreated, status)

	status, body := rawDo(t, http.MethodGet, e.base+"/_gopherstack/oci/keys", "")
	assert.Equal(t, http.StatusOK, status)
	assert.Contains(t, body, testTenancy)
}

func TestWorkRequestSurface(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()

	created, err := e.ce.CreateCluster(
		ctx,
		ke.CreateClusterRequest{CreateClusterDetails: clusterDetails("wr")},
	)
	require.NoError(t, err)

	logs, err := e.ce.ListWorkRequestLogs(
		ctx,
		ke.ListWorkRequestLogsRequest{
			CompartmentId: new(testCompartment),
			WorkRequestId: created.OpcWorkRequestId,
		},
	)
	require.NoError(t, err)
	assert.Len(t, logs.Items, 1)

	e.settle()

	logs, err = e.ce.ListWorkRequestLogs(
		ctx,
		ke.ListWorkRequestLogsRequest{
			CompartmentId: new(testCompartment),
			WorkRequestId: created.OpcWorkRequestId,
		},
	)
	require.NoError(t, err)
	require.Len(t, logs.Items, 2)
	assert.Contains(t, *logs.Items[1].Message, "CLUSTER_CREATE succeeded")

	errs, err := e.ce.ListWorkRequestErrors(
		ctx,
		ke.ListWorkRequestErrorsRequest{
			CompartmentId: new(testCompartment),
			WorkRequestId: created.OpcWorkRequestId,
		},
	)
	require.NoError(t, err)
	assert.Empty(t, errs.Items)

	tests := []struct {
		name   string
		status []string
		want   int
	}{
		{"succeeded", []string{"SUCCEEDED"}, 1},
		{"failed", []string{"FAILED"}, 0},
		{"either", []string{"FAILED", "succeeded"}, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp, rerr := e.ce.ListWorkRequests(ctx, ke.ListWorkRequestsRequest{
				CompartmentId: new(testCompartment), Status: tt.status,
			})
			require.NoError(t, rerr)
			assert.Len(t, resp.Items, tt.want)
		})
	}

	status, _ := rawDo(t, http.MethodGet, e.base+"/20180222/workRequests", "")
	assert.Equal(t, http.StatusBadRequest, status)
}
