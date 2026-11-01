package ociqueue_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueueLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{register: true, enforce: true})
	ctx := t.Context()

	created, err := e.admin.CreateQueue(ctx, queue.CreateQueueRequest{
		CompartmentId: new(testCompartment), DisplayName: new("lifecycle"),
		FreeformTags: map[string]string{"env": "test"}})
	require.NoError(t, err)

	wr, err := e.admin.GetWorkRequest(ctx, queue.GetWorkRequestRequest{WorkRequestId: created.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, queue.OperationStatusInProgress, wr.Status)
	assert.Equal(t, queue.OperationTypeCreateQueue, wr.OperationType)

	list, err := e.admin.ListQueues(ctx, queue.ListQueuesRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, queue.QueueLifecycleStateCreating, list.Items[0].LifecycleState)

	_, err = e.data(t, queue.Queue{MessagesEndpoint: &e.base}).PutMessages(ctx, queue.PutMessagesRequest{
		QueueId:  list.Items[0].Id,
		Messages: []queue.PutMessagesDetailsEntry{{Content: new("x")}},
	})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	e.settle()

	got, err := e.admin.GetQueue(ctx, queue.GetQueueRequest{QueueId: list.Items[0].Id})
	require.NoError(t, err)
	q := got.Queue
	assert.Equal(t, queue.QueueLifecycleStateActive, q.LifecycleState)
	assert.Equal(t, 86400, *q.RetentionInSeconds)
	assert.Equal(t, 30, *q.VisibilityInSeconds)
	assert.Equal(t, 30, *q.TimeoutInSeconds)
	assert.Equal(t, 5, *q.DeadLetterQueueDeliveryCount)
	assert.Equal(t, e.base, *q.MessagesEndpoint)
	assert.Equal(t, "test", q.FreeformTags["env"])
	require.NotNil(t, got.Etag)

	wr, err = e.admin.GetWorkRequest(ctx, queue.GetWorkRequestRequest{WorkRequestId: created.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, queue.OperationStatusSucceeded, wr.Status)
	assert.Equal(t, queue.ActionTypeCreated, wr.Resources[0].ActionType)

	_, err = e.admin.UpdateQueue(ctx, queue.UpdateQueueRequest{
		QueueId: q.Id, IfMatch: new("stale"),
		DisplayName: new("renamed"),
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.admin.UpdateQueue(ctx, queue.UpdateQueueRequest{
		QueueId: q.Id, IfMatch: got.Etag,
		DisplayName: new("renamed"), VisibilityInSeconds: new(60),
	})
	require.NoError(t, err)

	mid, err := e.admin.GetQueue(ctx, queue.GetQueueRequest{QueueId: q.Id})
	require.NoError(t, err)
	assert.Equal(t, queue.QueueLifecycleStateUpdating, mid.LifecycleState)
	assert.Equal(t, "renamed", *mid.DisplayName)
	assert.Equal(t, 60, *mid.VisibilityInSeconds)
	e.settle()

	_, err = e.admin.ChangeQueueCompartment(ctx, queue.ChangeQueueCompartmentRequest{
		QueueId:       q.Id,
		CompartmentId: new("ocid1.compartment.oc1..other"),
	})
	require.NoError(t, err)

	none, err := e.admin.ListQueues(ctx, queue.ListQueuesRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
	assert.Empty(t, none.Items)

	_, err = e.admin.DeleteQueue(ctx, queue.DeleteQueueRequest{QueueId: q.Id})
	require.NoError(t, err)

	del, err := e.admin.GetQueue(ctx, queue.GetQueueRequest{QueueId: q.Id})
	require.NoError(t, err)
	assert.Equal(t, queue.QueueLifecycleStateDeleting, del.LifecycleState)
	e.settle()

	del, err = e.admin.GetQueue(ctx, queue.GetQueueRequest{QueueId: q.Id})
	require.NoError(t, err)
	assert.Equal(t, queue.QueueLifecycleStateDeleted, del.LifecycleState)

	_, err = e.data(t, q).PutMessages(ctx, queue.PutMessagesRequest{
		QueueId:  q.Id,
		Messages: []queue.PutMessagesDetailsEntry{{Content: new("x")}},
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestCreateQueueValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mod  func(*queue.CreateQueueDetails)
		name string
		code string
	}{
		{func(d *queue.CreateQueueDetails) { d.RetentionInSeconds = new(5) }, "retention low", "InvalidParameter"},
		{
			func(d *queue.CreateQueueDetails) { d.RetentionInSeconds = new(604801) },
			"retention high",
			"InvalidParameter",
		},
		{
			func(d *queue.CreateQueueDetails) { d.VisibilityInSeconds = new(43201) },
			"visibility high",
			"InvalidParameter",
		},
		{func(d *queue.CreateQueueDetails) { d.TimeoutInSeconds = new(31) }, "timeout high", "InvalidParameter"},
		{
			func(d *queue.CreateQueueDetails) { d.DeadLetterQueueDeliveryCount = new(21) },
			"dlq high",
			"InvalidParameter",
		},
		{func(d *queue.CreateQueueDetails) { d.DisplayName = new(" ") }, "blank name", "MissingParameter"},
		{func(d *queue.CreateQueueDetails) { d.CustomEncryptionKeyId = new("nope") }, "bad key", "InvalidParameter"},
		{
			func(d *queue.CreateQueueDetails) {
				d.Capabilities = []queue.CapabilityDetails{queue.LargeMessagesCapabilityDetails{}}
			},
			"capabilities", "InvalidParameter",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			d := queue.CreateQueueDetails{CompartmentId: new(testCompartment), DisplayName: new("v")}
			tt.mod(&d)

			_, err := e.admin.CreateQueue(t.Context(), queue.CreateQueueRequest{CreateQueueDetails: d})
			se := svcErr(t, err)
			assert.Equal(t, http.StatusBadRequest, se.GetHTTPStatusCode())
			assert.Equal(t, tt.code, se.GetCode())
		})
	}
}

func TestListQueuesFilters(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	for _, n := range []string{"b", "a", "c"} {
		e.mkQueue(t, n, nil)
	}

	tests := []struct {
		req  queue.ListQueuesRequest
		name string
		want []string
	}{
		{
			queue.ListQueuesRequest{SortBy: queue.ListQueuesSortByDisplayname, SortOrder: queue.ListQueuesSortOrderAsc},
			"by name asc",
			[]string{"a", "b", "c"},
		},
		{
			queue.ListQueuesRequest{
				SortBy:    queue.ListQueuesSortByDisplayname,
				SortOrder: queue.ListQueuesSortOrderDesc,
			},
			"by name desc",
			[]string{"c", "b", "a"},
		},
		{queue.ListQueuesRequest{DisplayName: new("b")}, "name filter", []string{"b"}},
		{queue.ListQueuesRequest{LifecycleState: queue.QueueLifecycleStateDeleted}, "state filter", nil},
		{
			queue.ListQueuesRequest{
				Limit:     new(2),
				SortBy:    queue.ListQueuesSortByDisplayname,
				SortOrder: queue.ListQueuesSortOrderAsc,
			},
			"limit",
			[]string{"a", "b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := tt.req
			r.CompartmentId = new(testCompartment)

			resp, err := e.admin.ListQueues(t.Context(), r)
			require.NoError(t, err)

			var got []string
			for _, q := range resp.Items {
				got = append(got, *q.DisplayName)
			}

			assert.Equal(t, tt.want, got)

			if tt.name == "limit" {
				require.NotNil(t, resp.OpcNextPage)

				r.Page = resp.OpcNextPage
				next, nerr := e.admin.ListQueues(t.Context(), r)
				require.NoError(t, nerr)
				require.Len(t, next.Items, 1)
				assert.Equal(t, "c", *next.Items[0].DisplayName)
			}
		})
	}
}

func TestListQueuesRequiresCompartment(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	_, err := e.admin.ListQueues(t.Context(), queue.ListQueuesRequest{})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
}

func TestWorkRequests(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	q := e.mkQueue(t, "wr", nil)

	list, err := e.admin.ListWorkRequests(
		t.Context(),
		queue.ListWorkRequestsRequest{CompartmentId: new(testCompartment)},
	)
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, queue.OperationStatusSucceeded, list.Items[0].Status)
	assert.Equal(t, *q.Id, *list.Items[0].Resources[0].Identifier)

	logs, err := e.admin.ListWorkRequestLogs(
		t.Context(),
		queue.ListWorkRequestLogsRequest{WorkRequestId: list.Items[0].Id},
	)
	require.NoError(t, err)
	assert.Len(t, logs.Items, 2)

	errs, err := e.admin.ListWorkRequestErrors(
		t.Context(),
		queue.ListWorkRequestErrorsRequest{WorkRequestId: list.Items[0].Id},
	)
	require.NoError(t, err)
	assert.Empty(t, errs.Items)

	_, err = e.admin.GetWorkRequest(t.Context(), queue.GetWorkRequestRequest{WorkRequestId: new("missing")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestAuthEnforced(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{enforce: true, register: true})

	status, body := rawDo(t, http.MethodGet, e.base+"/20210201/queues?compartmentId="+testCompartment, "")
	assert.Equal(t, http.StatusUnauthorized, status)
	assert.Contains(t, body, "NotAuthenticated")

	_, err := e.admin.ListQueues(t.Context(), queue.ListQueuesRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
}

func TestUnknownRoute(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	status, body := rawDo(t, http.MethodGet, e.base+"/20210201/nothing", "")
	assert.Equal(t, http.StatusNotFound, status)
	assert.Contains(t, body, "NotFound")
}
