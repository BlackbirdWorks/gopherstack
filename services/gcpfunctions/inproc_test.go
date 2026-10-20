package gcpfunctions_test

import (
	"net/http"
	"testing"
	"time"

	"cloud.google.com/go/functions/apiv2/functionspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpfunctions"
)

func TestAPI_UploadAndInvoke(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	api := e.handler.API()

	slot, err := api.Functions.GenerateUploadUrl(t.Context(), &functionspb.GenerateUploadUrlRequest{Parent: parent})
	require.NoError(t, err)
	require.NoError(t, api.Upload(t.Context(), slot.GetUploadUrl(), zipOf(t, nodeFilesFn())))

	op, err := api.Functions.CreateFunction(t.Context(), &functionspb.CreateFunctionRequest{
		Parent: parent, FunctionId: "inproc", Function: fnSpec(slot.GetStorageSource()),
	})
	require.NoError(t, err)
	require.NotNil(t, op)

	name := parent + "/functions/inproc"

	require.Eventually(t, func() bool {
		fn, gerr := api.Functions.GetFunction(t.Context(), &functionspb.GetFunctionRequest{Name: name})

		return gerr == nil && fn.GetState() == functionspb.Function_ACTIVE
	}, 10*time.Second, 20*time.Millisecond)

	res, err := api.Invoke(t.Context(), name, gcpfunctions.InvokeRequest{
		Method: http.MethodPost, Path: "/x?a=b", Body: []byte("payload"), Header: http.Header{"X-T": {"1"}},
	})
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, res.Status)
	assert.NotEmpty(t, res.Body)
}

func TestAPI_Rejects(t *testing.T) {
	t.Parallel()

	api := newEnv(t, true).handler.API()

	tests := []struct {
		call func() error
		name string
	}{
		{func() error { return api.Upload(t.Context(), "http://x/other", nil) }, "upload foreign url"},
		{func() error { return api.Upload(t.Context(), "http://x/_gcf/upload/b/o", nil) }, "upload unknown slot"},
		{func() error {
			_, err := api.Invoke(t.Context(), parent+"/functions/none", gcpfunctions.InvokeRequest{Method: "GET"})

			return err
		}, "invoke unknown"},
		{func() error {
			_, err := api.Invoke(t.Context(), "bad", gcpfunctions.InvokeRequest{Method: "GET"})

			return err
		}, "invoke bad name"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.call())
		})
	}
}
