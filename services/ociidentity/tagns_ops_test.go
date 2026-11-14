package ociidentity_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/identity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChangeTagNamespaceCompartment(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	comp := e.mkCompartment(t, testTenancy, "dest")

	ns, err := e.client.CreateTagNamespace(ctx, identity.CreateTagNamespaceRequest{
		CompartmentId: new(testTenancy), Name: new("mv"), Description: new("d"),
	})
	require.NoError(t, err)

	tag, err := e.client.CreateTag(ctx, identity.CreateTagRequest{
		TagNamespaceId: ns.Id, Name: new("t"), Description: new("d"),
	})
	require.NoError(t, err)

	_, err = e.client.ChangeTagNamespaceCompartment(ctx, identity.ChangeTagNamespaceCompartmentRequest{
		TagNamespaceId: ns.Id,
		CompartmentId:  new("ocid1.compartment.oc1..nope"),
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.ChangeTagNamespaceCompartment(ctx, identity.ChangeTagNamespaceCompartmentRequest{
		TagNamespaceId: ns.Id,
		CompartmentId:  comp.Id,
	})
	require.NoError(t, err)

	got, err := e.client.GetTagNamespace(ctx, identity.GetTagNamespaceRequest{TagNamespaceId: ns.Id})
	require.NoError(t, err)
	assert.Equal(t, *comp.Id, *got.CompartmentId)

	gt, err := e.client.GetTag(ctx, identity.GetTagRequest{TagNamespaceId: ns.Id, TagName: tag.Name})
	require.NoError(t, err)
	assert.Equal(t, *comp.Id, *gt.CompartmentId)
}

func TestCascadeDeleteTagNamespace(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()

	ns, err := e.client.CreateTagNamespace(ctx, identity.CreateTagNamespaceRequest{
		CompartmentId: new(testTenancy), Name: new("cd"), Description: new("d"),
	})
	require.NoError(t, err)

	tag, err := e.client.CreateTag(ctx, identity.CreateTagRequest{
		TagNamespaceId: ns.Id, Name: new("t"), Description: new("d"),
	})
	require.NoError(t, err)

	_, err = e.client.CascadeDeleteTagNamespace(ctx, identity.CascadeDeleteTagNamespaceRequest{TagNamespaceId: ns.Id})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "must be retired first")

	_, err = e.client.UpdateTagNamespace(ctx, identity.UpdateTagNamespaceRequest{
		TagNamespaceId: ns.Id,
		IsRetired:      new(true),
	})
	require.NoError(t, err)

	del, err := e.client.CascadeDeleteTagNamespace(
		ctx,
		identity.CascadeDeleteTagNamespaceRequest{TagNamespaceId: ns.Id},
	)
	require.NoError(t, err)
	require.NotNil(t, del.OpcWorkRequestId)

	wr, err := e.client.GetTaggingWorkRequest(
		ctx,
		identity.GetTaggingWorkRequestRequest{WorkRequestId: del.OpcWorkRequestId},
	)
	require.NoError(t, err)
	assert.Equal(t, identity.TaggingWorkRequestOperationTypeDeleteNonEmptyTagNamespace, wr.OperationType)
	assert.Equal(t, identity.TaggingWorkRequestStatusInProgress, wr.Status)

	list, err := e.client.ListTaggingWorkRequests(
		ctx,
		identity.ListTaggingWorkRequestsRequest{CompartmentId: new(testTenancy)},
	)
	require.NoError(t, err)
	require.Len(t, list.Items, 1)

	e.clock.Advance(10 * time.Second)

	wr, err = e.client.GetTaggingWorkRequest(
		ctx,
		identity.GetTaggingWorkRequestRequest{WorkRequestId: del.OpcWorkRequestId},
	)
	require.NoError(t, err)
	assert.Equal(t, identity.TaggingWorkRequestStatusSucceeded, wr.Status)

	logs, err := e.client.ListTaggingWorkRequestLogs(
		ctx,
		identity.ListTaggingWorkRequestLogsRequest{WorkRequestId: del.OpcWorkRequestId},
	)
	require.NoError(t, err)
	assert.Len(t, logs.Items, 2)

	errs, err := e.client.ListTaggingWorkRequestErrors(
		ctx,
		identity.ListTaggingWorkRequestErrorsRequest{WorkRequestId: del.OpcWorkRequestId},
	)
	require.NoError(t, err)
	assert.Empty(t, errs.Items)

	e.backend.Sweep()

	_, err = e.client.GetTagNamespace(ctx, identity.GetTagNamespaceRequest{TagNamespaceId: ns.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.GetTag(ctx, identity.GetTagRequest{TagNamespaceId: ns.Id, TagName: tag.Name})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.GetTaggingWorkRequest(
		ctx,
		identity.GetTaggingWorkRequestRequest{WorkRequestId: new("ocid1.workrequest.oc1..x")},
	)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}
