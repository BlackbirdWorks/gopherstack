package gcpapigateway_test

import (
	"net/http"
	"testing"
	"time"

	"cloud.google.com/go/apigateway/apiv1/apigatewaypb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpapigateway"
)

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	be := echoBackend(t)
	src := newEnv(t)
	gw := src.deploy(t, opSpec(be.URL, "", "  /hello:\n"+op("get", "")))
	apiName := parent + "/apis/" + apiID

	iam := iampb.NewIAMPolicyClient(src.conn)
	_, err := iam.SetIamPolicy(t.Context(), &iampb.SetIamPolicyRequest{
		Resource: gw.GetName(), Policy: &iampb.Policy{Bindings: []*iampb.Binding{
			{Role: "roles/apigateway.admin", Members: []string{"user:a@example.com"}},
		}},
	})
	require.NoError(t, err)

	src.handler.Backend.SetTiming(gcpapigateway.Timing{Create: time.Minute})

	pending, err := src.client.CreateApi(t.Context(), &apigatewaypb.CreateApiRequest{Parent: parent, ApiId: "pending"})
	require.NoError(t, err)

	snap := src.handler.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	dst := newEnv(t)
	require.NoError(t, dst.handler.Restore(t.Context(), snap))

	got, err := dst.client.GetApi(t.Context(), &apigatewaypb.GetApiRequest{Name: apiName})
	require.NoError(t, err)
	assert.Equal(t, src.managedService(t), got.GetManagedService())

	gotGW, err := dst.client.GetGateway(t.Context(), &apigatewaypb.GetGatewayRequest{Name: gw.GetName()})
	require.NoError(t, err)
	assert.Equal(t, gw.GetDefaultHostname(), gotGW.GetDefaultHostname())

	full, err := dst.client.GetApiConfig(t.Context(), &apigatewaypb.GetApiConfigRequest{
		Name: gw.GetApiConfig(), View: apigatewaypb.GetApiConfigRequest_FULL,
	})
	require.NoError(t, err)
	assert.Len(t, full.GetOpenapiDocuments(), 1)

	pol, err := iampb.NewIAMPolicyClient(dst.conn).
		GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: gw.GetName()})
	require.NoError(t, err)
	require.Len(t, pol.GetBindings(), 1)

	served := dst.call(t, gotGW, http.MethodGet, "/hello", nil)
	assert.Equal(t, http.StatusOK, served.status, "the restored config routes again")

	op, err := dst.client.LROClient.GetOperation(t.Context(), &longrunningpb.GetOperationRequest{Name: pending.Name()})
	require.NoError(t, err)
	assert.False(t, op.GetDone(), "a running operation stays running")

	dst.clock.Advance(2 * time.Minute)

	op, err = dst.client.LROClient.GetOperation(t.Context(), &longrunningpb.GetOperationRequest{Name: pending.Name()})
	require.NoError(t, err)
	assert.True(t, op.GetDone())
}

func TestRestoreRejectsCorruptAndStaleSnapshots(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createAPI(t, apiID)

	require.Error(t, e.handler.Restore(t.Context(), []byte("not json")))

	snap := e.handler.Snapshot(t.Context())
	stale := []byte(string(snap))
	stale = replaceOnce(stale, `"version":1`, `"version":999`)

	require.NoError(t, e.handler.Restore(t.Context(), stale))

	it := e.client.ListApis(t.Context(), &apigatewaypb.ListApisRequest{Parent: parent})
	_, err := it.Next()
	assert.Error(t, err, "an incompatible snapshot starts empty")
}

func replaceOnce(b []byte, old, repl string) []byte {
	s := string(b)

	for i := 0; i+len(old) <= len(s); i++ {
		if s[i:i+len(old)] == old {
			return []byte(s[:i] + repl + s[i+len(old):])
		}
	}

	return b
}
