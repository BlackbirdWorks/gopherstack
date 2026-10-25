package ociidentity_test

import (
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/identity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ociidentity"
)

func TestCompartmentLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	c := e.mkCompartment(t, testTenancy, "prod")

	assert.Equal(t, identity.CompartmentLifecycleStateActive, c.LifecycleState)
	assert.Equal(t, testTenancy, *c.CompartmentId)

	g, err := e.client.GetCompartment(ctx, identity.GetCompartmentRequest{CompartmentId: c.Id})
	require.NoError(t, err)
	assert.Equal(t, "prod", *g.Name)
	assert.NotNil(t, g.Etag)

	root, err := e.client.GetCompartment(ctx, identity.GetCompartmentRequest{CompartmentId: new(testTenancy)})
	require.NoError(t, err)
	assert.Equal(t, testTenancy, *root.Id)

	u, err := e.client.UpdateCompartment(ctx, identity.UpdateCompartmentRequest{
		CompartmentId: c.Id, IfMatch: g.Etag,
		UpdateCompartmentDetails: identity.UpdateCompartmentDetails{Description: new("new"), Name: new("prod2")},
	})
	require.NoError(t, err)
	assert.Equal(t, "prod2", *u.Name)

	_, err = e.client.UpdateCompartment(ctx, identity.UpdateCompartmentRequest{
		CompartmentId: c.Id, IfMatch: g.Etag,
		UpdateCompartmentDetails: identity.UpdateCompartmentDetails{Description: new("stale")},
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	d, err := e.client.DeleteCompartment(ctx, identity.DeleteCompartmentRequest{CompartmentId: c.Id})
	require.NoError(t, err)
	require.NotNil(t, d.OpcWorkRequestId)

	wr, err := e.client.GetWorkRequest(ctx, identity.GetWorkRequestRequest{WorkRequestId: d.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, identity.WorkRequestStatusInProgress, wr.Status)
	assert.Equal(t, identity.WorkRequestOperationTypeCompartment, wr.OperationType)

	mid, err := e.client.GetCompartment(ctx, identity.GetCompartmentRequest{CompartmentId: c.Id})
	require.NoError(t, err)
	assert.Equal(t, identity.CompartmentLifecycleStateDeleting, mid.LifecycleState)

	e.clock.Advance(3 * time.Second)
	e.backend.Sweep()

	wr, err = e.client.GetWorkRequest(ctx, identity.GetWorkRequestRequest{WorkRequestId: d.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, identity.WorkRequestStatusSucceeded, wr.Status)
	assert.Equal(t, identity.WorkRequestResourceActionTypeDeleted, wr.Resources[0].ActionType)

	after, err := e.client.GetCompartment(ctx, identity.GetCompartmentRequest{CompartmentId: c.Id})
	require.NoError(t, err)
	assert.Equal(t, identity.CompartmentLifecycleStateDeleted, after.LifecycleState)

	wrs, err := e.client.ListWorkRequests(ctx, identity.ListWorkRequestsRequest{
		CompartmentId: new(testTenancy), ResourceIdentifier: c.Id,
	})
	require.NoError(t, err)
	require.Len(t, wrs.Items, 1)

	rec, err := e.client.RecoverCompartment(ctx, identity.RecoverCompartmentRequest{CompartmentId: c.Id})
	require.NoError(t, err)
	assert.Equal(t, identity.CompartmentLifecycleStateActive, rec.LifecycleState)

	_, err = e.client.RecoverCompartment(ctx, identity.RecoverCompartmentRequest{CompartmentId: c.Id})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	d2, err := e.client.DeleteCompartment(ctx, identity.DeleteCompartmentRequest{CompartmentId: c.Id})
	require.NoError(t, err)
	require.NotNil(t, d2.OpcWorkRequestId)

	e.clock.Advance(91 * 24 * time.Hour)
	e.backend.Sweep()

	_, err = e.client.RecoverCompartment(ctx, identity.RecoverCompartmentRequest{CompartmentId: c.Id})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())
}

func TestCompartmentErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	parent := e.mkCompartment(t, testTenancy, "parent")
	child := e.mkCompartment(t, *parent.Id, "child")
	_ = child

	tests := []struct {
		name     string
		call     func() error
		wantCode string
		status   int
	}{
		{"duplicate name", func() error {
			_, err := e.client.CreateCompartment(t.Context(), identity.CreateCompartmentRequest{
				CreateCompartmentDetails: identity.CreateCompartmentDetails{
					CompartmentId: parent.Id, Name: new("CHILD"), Description: new("d"),
				},
			})

			return err
		}, "CompartmentAlreadyExists", http.StatusConflict},
		{"missing parent", func() error {
			_, err := e.client.CreateCompartment(t.Context(), identity.CreateCompartmentRequest{
				CreateCompartmentDetails: identity.CreateCompartmentDetails{
					CompartmentId: new("ocid1.compartment.oc1..nope"), Name: new("x"), Description: new("d"),
				},
			})

			return err
		}, "NotAuthorizedOrNotFound", http.StatusNotFound},
		{"empty description", func() error {
			_, err := e.client.CreateCompartment(t.Context(), identity.CreateCompartmentRequest{
				CreateCompartmentDetails: identity.CreateCompartmentDetails{
					CompartmentId: parent.Id, Name: new("x"), Description: new(" "),
				},
			})

			return err
		}, "MissingParameter", http.StatusBadRequest},
		{"delete non-empty", func() error {
			_, err := e.client.DeleteCompartment(t.Context(), identity.DeleteCompartmentRequest{CompartmentId: parent.Id})

			return err
		}, "Conflict", http.StatusConflict},
		{"delete root", func() error {
			_, err := e.client.DeleteCompartment(t.Context(), identity.DeleteCompartmentRequest{CompartmentId: new(testTenancy)})

			return err
		}, "InvalidParameter", http.StatusBadRequest},
		{"subtree below root", func() error {
			_, err := e.client.ListCompartments(t.Context(), identity.ListCompartmentsRequest{
				CompartmentId: parent.Id, CompartmentIdInSubtree: new(true),
			})

			return err
		}, "InvalidParameter", http.StatusBadRequest},
		{"move into descendant", func() error {
			_, err := e.client.MoveCompartment(t.Context(), identity.MoveCompartmentRequest{
				CompartmentId:          parent.Id,
				MoveCompartmentDetails: identity.MoveCompartmentDetails{TargetCompartmentId: child.Id},
			})

			return err
		}, "InvalidParameter", http.StatusBadRequest},
		{"recover active", func() error {
			_, err := e.client.RecoverCompartment(t.Context(), identity.RecoverCompartmentRequest{CompartmentId: parent.Id})

			return err
		}, "IncorrectState", http.StatusConflict},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			se := svcErr(t, tt.call())
			assert.Equal(t, tt.status, se.GetHTTPStatusCode())
			assert.Equal(t, tt.wantCode, se.GetCode())
		})
	}
}

func TestCompartmentDepthAndListing(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	parent := testTenancy
	ids := make([]string, 0, 6)

	for i := range 6 {
		c := e.mkCompartment(t, parent, "level"+strconv.Itoa(i+1))
		ids = append(ids, *c.Id)
		parent = *c.Id
	}

	_, err := e.client.CreateCompartment(ctx, identity.CreateCompartmentRequest{
		CreateCompartmentDetails: identity.CreateCompartmentDetails{
			CompartmentId: new(parent), Name: new("level7"), Description: new("too deep"),
		},
	})
	se := svcErr(t, err)
	assert.Equal(t, "LimitExceeded", se.GetCode())

	tests := []struct {
		name    string
		req     identity.ListCompartmentsRequest
		wantLen int
	}{
		{"direct children", identity.ListCompartmentsRequest{CompartmentId: new(testTenancy)}, 1},
		{"whole subtree", identity.ListCompartmentsRequest{CompartmentId: new(testTenancy), CompartmentIdInSubtree: new(true)}, 6},
		{"by name", identity.ListCompartmentsRequest{
			CompartmentId: new(testTenancy), CompartmentIdInSubtree: new(true), Name: new("level3"),
		}, 1},
		{"active only", identity.ListCompartmentsRequest{
			CompartmentId: new(testTenancy), CompartmentIdInSubtree: new(true),
			LifecycleState: identity.CompartmentLifecycleStateActive, AccessLevel: identity.ListCompartmentsAccessLevelAny,
		}, 6},
		{"deleted only", identity.ListCompartmentsRequest{
			CompartmentId: new(testTenancy), CompartmentIdInSubtree: new(true),
			LifecycleState: identity.CompartmentLifecycleStateDeleted,
		}, 0},
		{"paged", identity.ListCompartmentsRequest{
			CompartmentId: new(testTenancy), CompartmentIdInSubtree: new(true), Limit: new(2),
		}, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r, lerr := e.client.ListCompartments(ctx, tt.req)
			require.NoError(t, lerr)
			assert.Len(t, r.Items, tt.wantLen)
		})
	}

	page1, err := e.client.ListCompartments(ctx, identity.ListCompartmentsRequest{
		CompartmentId: new(testTenancy), CompartmentIdInSubtree: new(true), Limit: new(4), SortBy: identity.ListCompartmentsSortByName,
	})
	require.NoError(t, err)
	require.NotNil(t, page1.OpcNextPage)

	page2, err := e.client.ListCompartments(ctx, identity.ListCompartmentsRequest{
		CompartmentId: new(testTenancy), CompartmentIdInSubtree: new(true), Page: page1.OpcNextPage, Limit: new(4),
		SortBy: identity.ListCompartmentsSortByName,
	})
	require.NoError(t, err)
	assert.Len(t, page2.Items, 2)
	assert.Nil(t, page2.OpcNextPage)
	assert.Equal(t, "level1", *page1.Items[0].Name)
	assert.Equal(t, ids[5], *page2.Items[1].Id)
}

func TestCompartmentMove(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	a := e.mkCompartment(t, testTenancy, "a")
	b := e.mkCompartment(t, testTenancy, "b")
	leaf := e.mkCompartment(t, *a.Id, "leaf")
	e.mkCompartment(t, *b.Id, "leaf")

	_, err := e.client.MoveCompartment(ctx, identity.MoveCompartmentRequest{
		CompartmentId: leaf.Id, MoveCompartmentDetails: identity.MoveCompartmentDetails{TargetCompartmentId: b.Id},
	})
	assert.Equal(t, "CompartmentAlreadyExists", svcErr(t, err).GetCode())

	_, err = e.client.UpdateCompartment(ctx, identity.UpdateCompartmentRequest{
		CompartmentId: leaf.Id, UpdateCompartmentDetails: identity.UpdateCompartmentDetails{Name: new("moved")},
	})
	require.NoError(t, err)

	mv, err := e.client.MoveCompartment(ctx, identity.MoveCompartmentRequest{
		CompartmentId: leaf.Id, MoveCompartmentDetails: identity.MoveCompartmentDetails{TargetCompartmentId: b.Id},
	})
	require.NoError(t, err)
	assert.NotNil(t, mv.OpcWorkRequestId)

	got, err := e.client.GetCompartment(ctx, identity.GetCompartmentRequest{CompartmentId: leaf.Id})
	require.NoError(t, err)
	assert.Equal(t, *b.Id, *got.CompartmentId)

	_, err = e.client.MoveCompartment(ctx, identity.MoveCompartmentRequest{
		CompartmentId: leaf.Id, MoveCompartmentDetails: identity.MoveCompartmentDetails{TargetCompartmentId: new(oci.TenancyID)},
	})
	require.NoError(t, err)
}
