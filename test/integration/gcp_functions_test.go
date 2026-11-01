package integration_test

import (
	"archive/zip"
	"bytes"
	"net/http"
	"net/url"
	"testing"

	functions "cloud.google.com/go/functions/apiv2"
	"cloud.google.com/go/functions/apiv2/functionspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

func newFunctionsClient(t *testing.T) *functions.FunctionClient {
	t.Helper()

	client, err := functions.NewFunctionClient(t.Context(), gcpGRPCOpts(t, "8128")...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return client
}

func nodeSourceZip(t *testing.T) []byte {
	t.Helper()

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)
	w, err := zw.Create("index.js")
	require.NoError(t, err)
	_, err = w.Write([]byte("exports.hello = (req, res) => res.send('hi');"))
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	return buf.Bytes()
}

// uploadFunctionSource PUTs the archive to the issued upload URL via the host-mapped ingress port.
func uploadFunctionSource(
	t *testing.T, client *functions.FunctionClient, parent string,
) *functionspb.StorageSource {
	t.Helper()

	resp, err := client.GenerateUploadUrl(t.Context(), &functionspb.GenerateUploadUrlRequest{Parent: parent})
	require.NoError(t, err)

	u, err := url.Parse(resp.GetUploadUrl())
	require.NoError(t, err)

	u.Host = gcpAddr(t, "8129")

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, u.String(), bytes.NewReader(nodeSourceZip(t)))
	require.NoError(t, err)

	res, err := testclient.New(t).Do(req)
	require.NoError(t, err)
	_ = res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)

	return resp.GetStorageSource()
}

func TestIntegration_GCP_Functions_Lifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := newFunctionsClient(t)
	ctx := t.Context()
	parent := "projects/" + gcpProject + "/locations/us-central1"
	id := gcpName("it-fn")
	src := uploadFunctionSource(t, client, parent)

	op, err := client.CreateFunction(ctx, &functionspb.CreateFunctionRequest{
		Parent: parent, FunctionId: id,
		Function: &functionspb.Function{
			BuildConfig: &functionspb.BuildConfig{
				Runtime: "nodejs22", EntryPoint: "hello",
				Source: &functionspb.Source{Source: &functionspb.Source_StorageSource{StorageSource: src}},
			},
		},
	})
	require.NoError(t, err)

	fn, err := op.Wait(ctx)
	if err != nil {
		t.Skipf("Cloud Functions deploy needs a container runtime inside the test container: %v", err)
	}

	assert.Equal(t, parent+"/functions/"+id, fn.GetName())
	assert.Equal(t, functionspb.Function_ACTIVE, fn.GetState())

	got, err := client.GetFunction(ctx, &functionspb.GetFunctionRequest{Name: fn.GetName()})
	require.NoError(t, err)
	assert.Equal(t, fn.GetUrl(), got.GetUrl())

	del, err := client.DeleteFunction(ctx, &functionspb.DeleteFunctionRequest{Name: fn.GetName()})
	require.NoError(t, err)
	require.NoError(t, del.Wait(ctx))
}

func TestIntegration_GCP_Functions_NotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := newFunctionsClient(t)

	_, err := client.GetFunction(t.Context(), &functionspb.GetFunctionRequest{
		Name: "projects/" + gcpProject + "/locations/us-central1/functions/" + gcpName("absent"),
	})
	assert.Equal(t, codes.NotFound, status.Code(err))
}
