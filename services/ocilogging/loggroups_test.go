package ocilogging_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogGroupLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	resp, err := e.mgmt.CreateLogGroup(t.Context(), logging.CreateLogGroupRequest{
		CompartmentId: new(testCompartment), DisplayName: new("lg"), Description: new("first"),
		FreeformTags: map[string]string{"k": "v"},
	})
	require.NoError(t, err)

	list, err := e.mgmt.ListLogGroups(t.Context(), logging.ListLogGroupsRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, logging.LogGroupLifecycleStateCreating, list.Items[0].LifecycleState)

	wr, err := e.mgmt.GetWorkRequest(t.Context(), logging.GetWorkRequestRequest{WorkRequestId: resp.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, logging.OperationStatusInProgress, wr.Status)
	assert.Equal(t, logging.OperationTypesCreateLogGroup, wr.OperationType)

	e.settle()

	got, err := e.mgmt.GetLogGroup(t.Context(), logging.GetLogGroupRequest{LogGroupId: list.Items[0].Id})
	require.NoError(t, err)
	assert.Equal(t, logging.LogGroupLifecycleStateActive, got.LifecycleState)
	assert.Equal(t, "first", *got.Description)
	assert.Equal(t, map[string]string{"k": "v"}, got.FreeformTags)
	assert.NotEmpty(t, *got.Etag)

	wr, err = e.mgmt.GetWorkRequest(t.Context(), logging.GetWorkRequestRequest{WorkRequestId: resp.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, logging.OperationStatusSucceeded, wr.Status)

	_, err = e.mgmt.UpdateLogGroup(t.Context(), logging.UpdateLogGroupRequest{
		LogGroupId: got.Id, IfMatch: new("stale"),
		Description: new("x"),
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.mgmt.UpdateLogGroup(t.Context(), logging.UpdateLogGroupRequest{
		LogGroupId: got.Id, IfMatch: got.Etag,
		DisplayName: new("renamed"), Description: new("second"),
	})
	require.NoError(t, err)
	e.settle()

	got, err = e.mgmt.GetLogGroup(t.Context(), logging.GetLogGroupRequest{LogGroupId: got.Id})
	require.NoError(t, err)
	assert.Equal(t, "renamed", *got.DisplayName)
	assert.Equal(t, "second", *got.Description)

	move, err := e.mgmt.ChangeLogGroupCompartment(t.Context(), logging.ChangeLogGroupCompartmentRequest{
		LogGroupId:    got.Id,
		CompartmentId: new("ocid1.compartment.oc1..other"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, *move.OpcWorkRequestId)

	del, err := e.mgmt.DeleteLogGroup(t.Context(), logging.DeleteLogGroupRequest{LogGroupId: got.Id})
	require.NoError(t, err)

	_, err = e.mgmt.UpdateLogGroup(t.Context(), logging.UpdateLogGroupRequest{LogGroupId: got.Id})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	e.settle()

	_, err = e.mgmt.GetLogGroup(t.Context(), logging.GetLogGroupRequest{LogGroupId: got.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	wrs, err := e.mgmt.ListWorkRequests(t.Context(), logging.ListWorkRequestsRequest{
		CompartmentId: new("ocid1.compartment.oc1..other"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, wrs.Items)

	_, err = e.mgmt.DeleteWorkRequest(
		t.Context(),
		logging.DeleteWorkRequestRequest{WorkRequestId: del.OpcWorkRequestId},
	)
	require.NoError(t, err)

	_, err = e.mgmt.GetWorkRequest(t.Context(), logging.GetWorkRequestRequest{WorkRequestId: del.OpcWorkRequestId})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestLogGroupValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mod    func(*logging.CreateLogGroupDetails)
		name   string
		code   string
		status int
	}{
		{
			func(d *logging.CreateLogGroupDetails) { d.CompartmentId = new("") },
			"no compartment",
			"MissingParameter",
			400,
		},
		{func(d *logging.CreateLogGroupDetails) { d.DisplayName = new(" ") }, "blank name", "MissingParameter", 400},
		{func(d *logging.CreateLogGroupDetails) {
			n := make([]byte, 300)
			for i := range n {
				n[i] = 'a'
			}
			d.DisplayName = new(string(n))
		}, "long name", "InvalidParameter", 400},
		{func(d *logging.CreateLogGroupDetails) {
			n := make([]byte, 401)
			for i := range n {
				n[i] = 'a'
			}
			d.Description = new(string(n))
		}, "long description", "InvalidParameter", 400},
		{func(d *logging.CreateLogGroupDetails) { d.DisplayName = new("dup") }, "duplicate name", "Conflict", 409},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			e.mkGroup(t, "dup")

			d := logging.CreateLogGroupDetails{CompartmentId: new(testCompartment), DisplayName: new("ok")}
			tt.mod(&d)

			_, err := e.mgmt.CreateLogGroup(t.Context(), logging.CreateLogGroupRequest{CreateLogGroupDetails: d})
			se := svcErr(t, err)
			assert.Equal(t, tt.status, se.GetHTTPStatusCode())
			assert.Equal(t, tt.code, se.GetCode())
		})
	}
}

func TestLogGroupListFilters(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	for _, n := range []string{"b", "a", "c"} {
		e.mkGroup(t, n)
	}

	tests := []struct {
		name  string
		req   logging.ListLogGroupsRequest
		want  []string
		pages int
	}{
		{
			"by name asc",
			logging.ListLogGroupsRequest{SortBy: logging.ListLogGroupsSortByDisplayname},
			[]string{"a", "b", "c"},
			1,
		},
		{"by name desc", logging.ListLogGroupsRequest{
			SortBy: logging.ListLogGroupsSortByDisplayname, SortOrder: logging.ListLogGroupsSortOrderDesc,
		}, []string{"c", "b", "a"}, 1},
		{"filter", logging.ListLogGroupsRequest{DisplayName: new("b")}, []string{"b"}, 1},
		{"page of two", logging.ListLogGroupsRequest{
			SortBy: logging.ListLogGroupsSortByDisplayname, Limit: new(2),
		}, []string{"a", "b", "c"}, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var got []string

			tt.req.CompartmentId = new(testCompartment)

			pages := 0

			for {
				resp, err := e.mgmt.ListLogGroups(t.Context(), tt.req)
				require.NoError(t, err)

				pages++

				for _, g := range resp.Items {
					got = append(got, *g.DisplayName)
				}

				if resp.OpcNextPage == nil {
					break
				}

				tt.req.Page = resp.OpcNextPage
			}

			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.pages, pages)
		})
	}

	status, body := rawDo(t, http.MethodGet, e.base+"/20200531/logGroups", "")
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, body, "MissingParameter")
}

func TestAuthEnforcement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		enforce  bool
		register bool
		wantErr  bool
	}{
		{"registered key", true, true, false},
		{"unknown key rejected", true, false, true},
		{"open by default", false, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{enforce: tt.enforce, register: tt.register})

			_, err := e.mgmt.ListLogGroups(
				t.Context(),
				logging.ListLogGroupsRequest{CompartmentId: new(testCompartment)},
			)
			if tt.wantErr {
				assert.Equal(t, http.StatusUnauthorized, svcErr(t, err).GetHTTPStatusCode())

				return
			}

			require.NoError(t, err)
		})
	}

	e := newEnv(t, envOpts{enforce: true, register: true})
	status, body := rawDo(t, http.MethodGet, e.base+"/20200531/logGroups?compartmentId=x", "")
	assert.Equal(t, http.StatusUnauthorized, status)
	assert.Contains(t, body, "NotAuthenticated")

	status, _ = rawDo(t, http.MethodGet, e.base+"/20200531/nope", "")
	assert.Equal(t, http.StatusNotFound, status)
}
