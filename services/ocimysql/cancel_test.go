package ocimysql_test

import (
	"net/http"
	"testing"

	my "github.com/oracle/oci-go-sdk/v65/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCancelWorkRequest(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	created := e.createDB(t, dbDetails("cancel"))

	_, err := e.wr.CancelWorkRequest(ctx, my.CancelWorkRequestRequest{WorkRequestId: created.OpcWorkRequestId})
	require.NoError(t, err)

	got, err := e.wr.GetWorkRequest(ctx, my.GetWorkRequestRequest{WorkRequestId: created.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, my.WorkRequestOperationStatusCanceled, got.Status)
	assert.NotNil(t, got.TimeFinished)

	logs, err := e.wr.ListWorkRequestLogs(ctx, my.ListWorkRequestLogsRequest{WorkRequestId: created.OpcWorkRequestId})
	require.NoError(t, err)
	require.Len(t, logs.Items, 2)
	assert.Contains(t, *logs.Items[1].Message, "canceled")

	e.settle()
	assert.Equal(t, my.DbSystemLifecycleStateActive, e.getDB(t, created.Id).LifecycleState, "the transition still runs")
}

func TestCancelWorkRequestErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	created := e.createDB(t, dbDetails("done"))
	e.settle()

	_, err := e.wr.CancelWorkRequest(t.Context(), my.CancelWorkRequestRequest{WorkRequestId: created.OpcWorkRequestId})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.wr.CancelWorkRequest(t.Context(), my.CancelWorkRequestRequest{
		WorkRequestId: new("ocid1.mysqlworkrequest.oc1.iad.nope"),
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}
