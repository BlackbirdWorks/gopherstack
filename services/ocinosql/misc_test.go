package ocinosql_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/nosql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	ocinosql "github.com/blackbirdworks/gopherstack/services/ocinosql"
)

func TestWorkRequests(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	created := e.createTable(t, "t", usersDDL("t"), nil)

	wr := e.workRequest(t, created.OpcWorkRequestId)
	require.Len(t, wr.Resources, 1)
	assert.Equal(t, "table", *wr.Resources[0].EntityType)
	assert.Equal(t, nosql.WorkRequestResourceActionTypeInProgress, wr.Resources[0].ActionType)
	assert.InDelta(t, 50, *wr.PercentComplete, 0)

	e.settle()

	wr = e.workRequest(t, created.OpcWorkRequestId)
	assert.Equal(t, nosql.WorkRequestStatusSucceeded, wr.Status)
	assert.Equal(t, nosql.WorkRequestResourceActionTypeCreated, wr.Resources[0].ActionType)
	assert.InDelta(t, 100, *wr.PercentComplete, 0)
	assert.NotNil(t, wr.TimeFinished)

	logs, err := e.client.ListWorkRequestLogs(
		t.Context(),
		nosql.ListWorkRequestLogsRequest{WorkRequestId: created.OpcWorkRequestId},
	)
	require.NoError(t, err)
	assert.Len(t, logs.Items, 2)

	errs, err := e.client.ListWorkRequestErrors(
		t.Context(),
		nosql.ListWorkRequestErrorsRequest{WorkRequestId: created.OpcWorkRequestId},
	)
	require.NoError(t, err)
	assert.Empty(t, errs.Items)

	list, err := e.client.ListWorkRequests(
		t.Context(),
		nosql.ListWorkRequestsRequest{CompartmentId: new(testCompartment)},
	)
	require.NoError(t, err)
	assert.Len(t, list.Items, 1)

	other, err := e.client.ListWorkRequests(
		t.Context(),
		nosql.ListWorkRequestsRequest{CompartmentId: new(otherCompart)},
	)
	require.NoError(t, err)
	assert.Empty(t, other.Items)

	for _, fn := range []func() error{
		func() error {
			_, gerr := e.client.GetWorkRequest(t.Context(),
				nosql.GetWorkRequestRequest{WorkRequestId: new("ocid1.nosqlworkrequest.oc1..x")})

			return gerr
		},
		func() error {
			_, gerr := e.client.ListWorkRequestLogs(t.Context(), nosql.ListWorkRequestLogsRequest{WorkRequestId: new("nope")})

			return gerr
		},
		func() error {
			_, gerr := e.client.ListWorkRequestErrors(t.Context(),
				nosql.ListWorkRequestErrorsRequest{WorkRequestId: new("nope")})

			return gerr
		},
	} {
		assert.Equal(t, http.StatusNotFound, svcErr(t, fn()).GetHTTPStatusCode())
	}
}

func TestErrorShapes(t *testing.T) {
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
		{name: "no route", method: http.MethodGet, path: "/20190828/nothing", status: 404, code: "NotFound"},
		{name: "wrong method", method: http.MethodPatch, path: "/20190828/tables", status: 404, code: "NotFound"},
		{
			name:   "bad json",
			method: http.MethodPost,
			path:   "/20190828/tables",
			body:   "{not json",
			status: 400,
			code:   "CannotParseRequest",
		},
		{name: "empty body", method: http.MethodPost, path: "/20190828/tables", status: 400, code: "MissingParameter"},
		{
			name:   "list needs compartment",
			method: http.MethodGet,
			path:   "/20190828/tables",
			status: 400,
			code:   "MissingParameter",
		},
		{
			name:   "missing table",
			method: http.MethodGet,
			path:   "/20190828/tables/ocid1.nosqltable.oc1..nope",
			status: 404,
			code:   "NotAuthorizedOrNotFound",
		},
		{
			name:   "row table missing",
			method: http.MethodPut,
			path:   "/20190828/tables/x/rows",
			body:   `{"compartmentId":"c"}`,
			status: 404,
			code:   "NotAuthorizedOrNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status, body, hdr := rawDo(t, tt.method, e.base+tt.path, tt.body)
			assert.Equal(t, tt.status, status)
			assert.Contains(t, body, `"code":"`+tt.code+`"`)
			assert.NotEmpty(t, hdr.Get("Opc-Request-Id"))
		})
	}

	e.mkTable(t, "t", usersDDL("t"))

	status, body, _ := rawDo(
		t,
		http.MethodPut,
		e.base+"/20190828/tables/t/rows",
		`{"compartmentId":"`+testCompartment+`"}`,
	)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, body, "MissingParameter")
}

func TestAuthEnforced(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{register: true, enforce: true})

	status, body, _ := rawDo(t, http.MethodGet, e.base+"/20190828/tables?compartmentId="+testCompartment, "")
	assert.Equal(t, http.StatusUnauthorized, status, "unsigned request rejected")
	assert.Contains(t, body, "NotAuthenticated")

	_, err := e.client.ListTables(t.Context(), nosql.ListTablesRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err, "registered key is accepted")

	stranger := newKeyPair(t)
	prov := common.NewRawConfigurationProvider(
		testTenancy,
		testUser,
		ocinosql.Region,
		stranger.fprint,
		stranger.privPEM,
		nil,
	)
	c, err := nosql.NewNosqlClientWithConfigurationProvider(prov)
	require.NoError(t, err)

	c.Host = e.base

	_, err = c.ListTables(t.Context(), nosql.ListTablesRequest{CompartmentId: new(testCompartment)})
	assert.Equal(t, http.StatusUnauthorized, svcErr(t, err).GetHTTPStatusCode(), "unregistered key rejected")
}

func TestAdminKeys(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{enforce: true})

	status, _, _ := rawDo(t, http.MethodGet, e.base+"/20190828/tables?compartmentId=c", "")
	assert.Equal(t, http.StatusUnauthorized, status)

	body := `{"tenancy":"` + testTenancy + `","user":"` + testUser + `","publicKey":` + quote(e.kp.pubPEM) + `}`

	status, out, _ := rawDo(t, http.MethodPost, e.base+"/_gopherstack/oci/keys", body)
	require.Equal(t, http.StatusCreated, status, out)

	_, err := e.client.ListTables(t.Context(), nosql.ListTablesRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)

	status, _, _ = rawDo(t, http.MethodPost, e.base+"/_gopherstack/oci/keys", `{"tenancy":"x"}`)
	assert.Equal(t, http.StatusBadRequest, status)
}

func quote(s string) string {
	out := []byte{'"'}

	for _, r := range s {
		if r == '\n' {
			out = append(out, '\\', 'n')

			continue
		}

		out = append(out, string(r)...)
	}

	return string(append(out, '"'))
}

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{register: true})
	ctx := t.Context()

	seedUsers(t, e)
	e.createIndex(t, "users", "idx_age", nosql.IndexKey{ColumnName: new("age")})
	e.mkTable(
		t,
		"ids",
		"CREATE TABLE ids (id INTEGER GENERATED ALWAYS AS IDENTITY, v STRING, PRIMARY KEY(id)) USING TTL 2 DAYS",
	)
	e.put(t, "ids", map[string]any{"v": "a"})
	e.settle()

	snap := e.backend.Snapshot(ctx)
	require.NotEmpty(t, snap)

	restored := newEnv(t, envOpts{})
	require.NoError(t, restored.backend.Restore(ctx, snap))
	restored.clock.ns.Store(e.clock.ns.Load())

	tbl := restored.getTable(t, "users")
	assert.Equal(t, nosql.TableLifecycleStateActive, tbl.LifecycleState)
	assert.Len(t, tbl.Schema.Columns, 5)

	got := restored.query(t, "SELECT id FROM users ORDER BY age DESC LIMIT 2", 0, nil)
	assert.Equal(t, []int{3, 6}, ids(got.Items), "rows and the ACTIVE index survive")

	next := restored.put(t, "ids", map[string]any{"v": "b"})
	assert.Equal(t, "2", *next.GeneratedValue, "identity counter survives")
	assert.Equal(t, 2, *restored.getTable(t, "ids").Schema.Ttl)

	list, err := restored.client.ListWorkRequests(
		ctx,
		nosql.ListWorkRequestsRequest{CompartmentId: new(testCompartment)},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, list.Items)

	require.Error(t, restored.backend.Restore(ctx, []byte("{")))
	require.Error(t, restored.backend.Restore(ctx, persistence.MarshalSnapshot(ctx, "ocinosql",
		map[string]any{"version": 1, "tables": map[string]any{"x": nil}})))

	restored.backend.Reset()

	empty, err := restored.client.ListTables(ctx, nosql.ListTablesRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
	assert.Empty(t, empty.Items)
}

func TestIdentityKeyLinking(t *testing.T) {
	t.Parallel()

	a := newEnv(t, envOpts{enforce: true})
	b := newEnv(t, envOpts{enforce: true, register: true})

	a.backend.Auth.Link(b.backend.Auth)

	c, err := nosql.NewNosqlClientWithConfigurationProvider(b.prov)
	require.NoError(t, err)

	c.Host = a.base

	_, err = c.ListTables(t.Context(), nosql.ListTablesRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err, "keys registered with the linked registry authenticate here")
}

func TestCompartmentChecker(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.backend.SetCompartmentChecker(func(id string) bool { return id == testCompartment })

	_, err := e.client.CreateTable(t.Context(), nosql.CreateTableRequest{
		Name: new(
			"t",
		),
		CompartmentId: new(otherCompart),
		DdlStatement:  new(usersDDL("t")),
		TableLimits:   provisioned(5, 5, 5),
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	e.createTable(t, "t", usersDDL("t"), nil)
}
