package ocinosql_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/nosql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTableLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{register: true, enforce: true})
	ctx := t.Context()

	ddl := "CREATE TABLE orders (id LONG, region STRING, total NUMBER NOT NULL DEFAULT 0, tags ARRAY(STRING), " +
		"attrs MAP(INTEGER), addr RECORD(city STRING, zip INTEGER), at TIMESTAMP(3), blob BINARY, " +
		"PRIMARY KEY(SHARD(region), id)) USING TTL 5 DAYS"

	created := e.createTable(t, "orders", ddl, provisioned(100, 200, 25))

	wr := e.workRequest(t, created.OpcWorkRequestId)
	assert.Equal(t, nosql.WorkRequestStatusInProgress, wr.Status)
	assert.Equal(t, nosql.WorkRequestOperationTypeCreateTable, wr.OperationType)
	assert.Equal(t, testCompartment, *wr.CompartmentId)

	tbl := e.getTable(t, "ORDERS")
	assert.Equal(t, nosql.TableLifecycleStateCreating, tbl.LifecycleState)

	_, err := e.client.GetRow(ctx, nosql.GetRowRequest{
		TableNameOrId: new("orders"), CompartmentId: new(testCompartment), Key: []string{"region:a", "id:1"},
	})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "rows unavailable while CREATING")

	e.settle()

	tbl = e.getTable(t, "orders")
	assert.Equal(t, nosql.TableLifecycleStateActive, tbl.LifecycleState)
	assert.Equal(t, nosql.WorkRequestStatusSucceeded, e.workRequest(t, created.OpcWorkRequestId).Status)
	assert.Equal(t, 100, *tbl.TableLimits.MaxReadUnits)
	assert.Equal(t, 200, *tbl.TableLimits.MaxWriteUnits)
	assert.Equal(t, 25, *tbl.TableLimits.MaxStorageInGBs)
	assert.Equal(t, nosql.TableLimitsCapacityModeProvisioned, tbl.TableLimits.CapacityMode)
	assert.Equal(t, 5, *tbl.Schema.Ttl)
	assert.Equal(t, []string{"region", "id"}, tbl.Schema.PrimaryKey)
	assert.Equal(t, []string{"region"}, tbl.Schema.ShardKey)

	types := map[string]string{}
	nullable := map[string]bool{}

	for _, c := range tbl.Schema.Columns {
		types[*c.Name], nullable[*c.Name] = *c.Type, *c.IsNullable
	}

	assert.Equal(t, map[string]string{
		"id": "LONG", "region": "STRING", "total": "NUMBER", "tags": "ARRAY(STRING)", "attrs": "MAP(INTEGER)",
		"addr": "RECORD(city STRING, zip INTEGER)", "at": "TIMESTAMP(3)", "blob": "BINARY",
	}, types)
	assert.False(t, nullable["id"], "key columns are NOT NULL")
	assert.False(t, nullable["total"])
	assert.True(t, nullable["tags"])

	byID, err := e.client.GetTable(ctx, nosql.GetTableRequest{TableNameOrId: tbl.Id})
	require.NoError(t, err)
	assert.Equal(t, "orders", *byID.Name)
	assert.NotEmpty(t, *byID.Etag)

	list, err := e.client.ListTables(ctx, nosql.ListTablesRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, "orders", *list.Items[0].Name)

	del, err := e.client.DeleteTable(ctx, nosql.DeleteTableRequest{
		TableNameOrId: tbl.Id, IfMatch: byID.Etag,
	})
	require.NoError(t, err)
	assert.Equal(t, nosql.TableLifecycleStateDeleting, e.getTable(t, "orders").LifecycleState)
	e.settle()
	assert.Equal(t, nosql.WorkRequestStatusSucceeded, e.workRequest(t, del.OpcWorkRequestId).Status)

	list, err = e.client.ListTables(ctx, nosql.ListTablesRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
	assert.Empty(t, list.Items)

	all, err := e.client.ListTables(ctx, nosql.ListTablesRequest{
		CompartmentId: new(testCompartment), LifecycleState: nosql.ListTablesLifecycleStateAll,
	})
	require.NoError(t, err)
	require.Len(t, all.Items, 1)
	assert.Equal(t, nosql.TableLifecycleStateDeleted, all.Items[0].LifecycleState)

	e.createTable(t, "orders", ddl, provisioned(10, 10, 1))
}

func TestCreateTableErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.mkTable(t, "taken", usersDDL("taken"))

	tests := []struct {
		limits *nosql.TableLimits
		name   string
		tbl    string
		ddl    string
		code   string
		status int
	}{
		{name: "duplicate", tbl: "taken", ddl: usersDDL("taken"), status: 409, code: "Conflict"},
		{name: "name mismatch", tbl: "other", ddl: usersDDL("x0"), status: 400, code: "InvalidParameter"},
		{name: "not create", tbl: "x1", ddl: "SELECT * FROM x1", status: 400, code: "InvalidParameter"},
		{name: "no primary key", tbl: "x2", ddl: "CREATE TABLE x2 (a INTEGER)", status: 400, code: "InvalidParameter"},
		{
			name:   "pk column missing",
			tbl:    "x3",
			ddl:    "CREATE TABLE x3 (a INTEGER, PRIMARY KEY(b))",
			status: 400,
			code:   "InvalidParameter",
		},
		{
			name:   "pk bad type",
			tbl:    "x4",
			ddl:    "CREATE TABLE x4 (a JSON, PRIMARY KEY(a))",
			status: 400,
			code:   "InvalidParameter",
		},
		{
			name:   "duplicate column",
			tbl:    "x5",
			ddl:    "CREATE TABLE x5 (a INTEGER, a STRING, PRIMARY KEY(a))",
			status: 400,
			code:   "InvalidParameter",
		},
		{
			name:   "unknown type",
			tbl:    "x6",
			ddl:    "CREATE TABLE x6 (a INTEGER, b BLOB, PRIMARY KEY(a))",
			status: 400,
			code:   "InvalidParameter",
		},
		{
			name:   "shard not prefix",
			tbl:    "x7",
			ddl:    "CREATE TABLE x7 (a INTEGER, b INTEGER, PRIMARY KEY(a, SHARD(b)))",
			status: 400,
			code:   "InvalidParameter",
		},
		{
			name:   "child table",
			tbl:    "x8",
			ddl:    "CREATE TABLE x8.child (a INTEGER, PRIMARY KEY(a))",
			status: 400,
			code:   "InvalidParameter",
		},
		{
			name:   "bad default",
			tbl:    "x9",
			ddl:    "CREATE TABLE x9 (a INTEGER, b INTEGER DEFAULT 'z', PRIMARY KEY(a))",
			status: 400,
			code:   "InvalidParameter",
		},
		{
			name:   "trailing junk",
			tbl:    "y1",
			ddl:    "CREATE TABLE y1 (a INTEGER, PRIMARY KEY(a)) FOO",
			status: 400,
			code:   "InvalidParameter",
		},
		{
			name:   "bad ttl unit",
			tbl:    "y2",
			ddl:    "CREATE TABLE y2 (a INTEGER, PRIMARY KEY(a)) USING TTL 1 WEEKS",
			status: 400,
			code:   "InvalidParameter",
		},
		{
			name:   "read units zero",
			tbl:    "y3",
			ddl:    "CREATE TABLE y3 (a INTEGER, PRIMARY KEY(a))",
			limits: provisioned(0, 10, 1),
			status: 400,
			code:   "InvalidParameter",
		},
		{
			name:   "write units too big",
			tbl:    "y4",
			ddl:    "CREATE TABLE y4 (a INTEGER, PRIMARY KEY(a))",
			limits: provisioned(10, 40001, 1),
			status: 400,
			code:   "InvalidParameter",
		},
		{
			name:   "storage too big",
			tbl:    "y5",
			ddl:    "CREATE TABLE y5 (a INTEGER, PRIMARY KEY(a))",
			limits: provisioned(10, 10, 5001),
			status: 400,
			code:   "InvalidParameter",
		},
		{
			name:   "storage zero",
			tbl:    "y6",
			ddl:    "CREATE TABLE y6 (a INTEGER, PRIMARY KEY(a))",
			limits: provisioned(10, 10, 0),
			status: 400,
			code:   "InvalidParameter",
		},
		{
			name:   "bad capacity mode",
			tbl:    "y7",
			ddl:    "CREATE TABLE y7 (a INTEGER, PRIMARY KEY(a))",
			status: 400,
			code:   "InvalidParameter",
			limits: &nosql.TableLimits{MaxStorageInGBs: new(1), CapacityMode: "BURST"},
		},
		{
			name:   "bad table name",
			tbl:    "1bad",
			ddl:    "CREATE TABLE 1bad (a INTEGER, PRIMARY KEY(a))",
			status: 400,
			code:   "InvalidParameter",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			limits := tt.limits
			if limits == nil {
				limits = provisioned(10, 10, 1)
			}

			_, err := e.client.CreateTable(t.Context(), nosql.CreateTableRequest{
				Name: new(tt.tbl), CompartmentId: new(testCompartment), DdlStatement: new(tt.ddl), TableLimits: limits,
			})
			se := svcErr(t, err)
			assert.Equal(t, tt.status, se.GetHTTPStatusCode())
			assert.Equal(t, tt.code, se.GetCode())
		})
	}
}

func TestTableLimits(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()

	_, err := e.client.CreateTable(ctx, nosql.CreateTableRequest{
		Name: new("nolimits"), CompartmentId: new(testCompartment),
		DdlStatement: new("CREATE TABLE nolimits (a INTEGER, PRIMARY KEY(a))")})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "limits required")

	_, err = e.client.CreateTable(ctx, nosql.CreateTableRequest{
		Name: new("reclaim"), CompartmentId: new(testCompartment), IsAutoReclaimable: new(true),
		DdlStatement: new("CREATE TABLE reclaim (a INTEGER, PRIMARY KEY(a))")})
	require.NoError(t, err)

	e.createTable(t, "ondemand", "CREATE TABLE ondemand (a INTEGER, PRIMARY KEY(a))",
		&nosql.TableLimits{MaxStorageInGBs: new(40), CapacityMode: nosql.TableLimitsCapacityModeOnDemand})
	e.settle()

	od := e.getTable(t, "ondemand")
	assert.Equal(t, nosql.TableLimitsCapacityModeOnDemand, od.TableLimits.CapacityMode)
	assert.Equal(t, 0, *od.TableLimits.MaxReadUnits)
	assert.Equal(t, 0, *od.TableLimits.MaxWriteUnits)
	assert.Equal(t, 40, *od.TableLimits.MaxStorageInGBs)

	rc := e.getTable(t, "reclaim")
	assert.True(t, *rc.IsAutoReclaimable)
	assert.Equal(t, 25, *rc.TableLimits.MaxStorageInGBs)

	tests := []struct {
		limits *nosql.TableLimits
		name   string
		status int
	}{
		{name: "to provisioned", limits: provisioned(300, 400, 80), status: 202},
		{name: "invalid", limits: provisioned(0, 400, 80), status: 400},
	}

	for _, tt := range tests {
		resp, uerr := e.client.UpdateTable(ctx, nosql.UpdateTableRequest{
			TableNameOrId: new("ondemand"),
			CompartmentId: new(testCompartment), TableLimits: tt.limits,
		})
		if tt.status == http.StatusAccepted {
			require.NoError(t, uerr, tt.name)
			assert.Equal(t, nosql.TableLifecycleStateUpdating, e.getTable(t, "ondemand").LifecycleState)

			_, err = e.client.UpdateTable(ctx, nosql.UpdateTableRequest{
				TableNameOrId: new("ondemand"),
				CompartmentId: new(testCompartment), TableLimits: tt.limits,
			})
			assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "UPDATING blocks further updates")

			e.settle()
			assert.Equal(t, nosql.WorkRequestStatusSucceeded, e.workRequest(t, resp.OpcWorkRequestId).Status)

			got := e.getTable(t, "ondemand")
			assert.Equal(t, nosql.TableLimitsCapacityModeProvisioned, got.TableLimits.CapacityMode)
			assert.Equal(t, 300, *got.TableLimits.MaxReadUnits)

			continue
		}

		assert.Equal(t, tt.status, svcErr(t, uerr).GetHTTPStatusCode(), tt.name)
	}
}

func TestAlterTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check  func(t *testing.T, tbl nosql.Table)
		name   string
		ddl    string
		status int
	}{
		{
			name:   "add column",
			ddl:    "ALTER TABLE t (ADD nick STRING)",
			status: 202,
			check: func(t *testing.T, tbl nosql.Table) {
				t.Helper()
				assert.Len(t, tbl.Schema.Columns, 6)
			},
		},
		{name: "add with default", ddl: "ALTER TABLE t (ADD score INTEGER NOT NULL DEFAULT 7)", status: 202,
			check: func(t *testing.T, tbl nosql.Table) {
				t.Helper()
				assert.Len(t, tbl.Schema.Columns, 6)
			}},
		{name: "drop column", ddl: "ALTER TABLE t (DROP age)", status: 202, check: func(t *testing.T, tbl nosql.Table) {
			t.Helper()
			assert.Len(t, tbl.Schema.Columns, 4)
		}},
		{name: "ttl", ddl: "ALTER TABLE t USING TTL 3 DAYS", status: 202, check: func(t *testing.T, tbl nosql.Table) {
			t.Helper()
			assert.Equal(t, 3, *tbl.Schema.Ttl)
		}},
		{
			name:   "ttl hours",
			ddl:    "ALTER TABLE t USING TTL 48 HOURS",
			status: 202,
			check: func(t *testing.T, tbl nosql.Table) {
				t.Helper()
				assert.Equal(t, 2, *tbl.Schema.Ttl)
			},
		},
		{name: "add existing", ddl: "ALTER TABLE t (ADD age INTEGER)", status: 400},
		{name: "drop missing", ddl: "ALTER TABLE t (DROP nothing)", status: 400},
		{name: "drop key", ddl: "ALTER TABLE t (DROP id)", status: 400},
		{name: "not null no default", ddl: "ALTER TABLE t (ADD n INTEGER NOT NULL)", status: 400},
		{name: "wrong table", ddl: "ALTER TABLE other (ADD n INTEGER)", status: 400},
		{name: "not alter", ddl: "DROP TABLE t", status: 400},
		{name: "empty alter", ddl: "ALTER TABLE t", status: 400},
		{name: "syntax", ddl: "ALTER TABLE t (ADD)", status: 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			e.mkTable(t, "t", usersDDL("t"))
			e.put(t, "t", map[string]any{"id": 1, "name": "ann", "age": 30})

			resp, err := e.client.UpdateTable(t.Context(), nosql.UpdateTableRequest{
				TableNameOrId: new("t"),
				CompartmentId: new(testCompartment), DdlStatement: new(tt.ddl),
			})
			if tt.status != http.StatusAccepted {
				assert.Equal(t, tt.status, svcErr(t, err).GetHTTPStatusCode())

				return
			}

			require.NoError(t, err)
			e.settle()
			assert.Equal(
				t,
				nosql.WorkRequestOperationTypeUpdateTable,
				e.workRequest(t, resp.OpcWorkRequestId).OperationType,
			)
			tt.check(t, e.getTable(t, "t"))
		})
	}
}

func TestAlterBackfillAndDrop(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.mkTable(t, "t", usersDDL("t"))
	e.put(t, "t", map[string]any{"id": 1, "name": "ann", "age": 30})

	alter := func(ddl string) {
		_, err := e.client.UpdateTable(t.Context(), nosql.UpdateTableRequest{
			TableNameOrId: new("t"),
			CompartmentId: new(testCompartment), DdlStatement: new(ddl),
		})
		require.NoError(t, err)
		e.settle()
	}

	alter("ALTER TABLE t (ADD score INTEGER NOT NULL DEFAULT 7)")

	rows := e.query(t, "SELECT id, score FROM t", 0, nil)
	require.Len(t, rows.Items, 1)
	assert.InDelta(t, 7, rows.Items[0]["score"], 0)

	alter("ALTER TABLE t (DROP score)")
	alter("ALTER TABLE t (ADD score STRING)")

	rows = e.query(t, "SELECT id, score FROM t", 0, nil)
	require.Len(t, rows.Items, 1)
	assert.Nil(t, rows.Items[0]["score"], "a dropped column's values do not resurface")
}

func TestIfMatchAndIfExists(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.mkTable(t, "t", usersDDL("t"))

	_, err := e.client.UpdateTable(t.Context(), nosql.UpdateTableRequest{
		TableNameOrId: new("t"), IfMatch: new("stale"),
		CompartmentId: new(testCompartment), FreeformTags: map[string]string{"a": "b"},
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.DeleteTable(t.Context(), nosql.DeleteTableRequest{
		TableNameOrId: new("t"), CompartmentId: new(testCompartment), IfMatch: new("stale"),
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.DeleteTable(t.Context(), nosql.DeleteTableRequest{
		TableNameOrId: new("missing"), CompartmentId: new(testCompartment),
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	resp, err := e.client.DeleteTable(t.Context(), nosql.DeleteTableRequest{
		TableNameOrId: new("missing"), CompartmentId: new(testCompartment), IsIfExists: new(true),
	})
	require.NoError(t, err)
	assert.Nil(t, resp.OpcWorkRequestId)

	_, err = e.client.GetTable(t.Context(), nosql.GetTableRequest{TableNameOrId: new("t")})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "a table name needs a compartment")

	tbl := e.getTable(t, "t")
	_, err = e.client.GetTable(
		t.Context(),
		nosql.GetTableRequest{TableNameOrId: tbl.Id, CompartmentId: new(otherCompart)},
	)
	assert.Equal(
		t,
		http.StatusBadRequest,
		svcErr(t, err).GetHTTPStatusCode(),
		"compartment must match the OCID's table",
	)

	_, err = e.client.UpdateTable(t.Context(), nosql.UpdateTableRequest{
		TableNameOrId: new("t"),
		CompartmentId: new(testCompartment), DdlStatement: new("ALTER TABLE t USING TTL 1 DAYS"),
		TableLimits: provisioned(5, 5, 5),
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "ddl and limits are exclusive")
}

func TestChangeTableCompartment(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	id := e.mkTable(t, "t", usersDDL("t"))

	tests := []struct {
		name   string
		to     string
		status int
	}{
		{name: "moves", to: otherCompart, status: 202},
	}

	for _, tt := range tests {
		resp, err := e.client.ChangeTableCompartment(t.Context(), nosql.ChangeTableCompartmentRequest{
			TableNameOrId:   new(id),
			ToCompartmentId: new(tt.to), FromCompartmentId: new(testCompartment),
		})
		require.NoError(t, err, tt.name)
		assert.NotNil(t, resp.OpcWorkRequestId)
	}

	got, err := e.client.GetTable(t.Context(), nosql.GetTableRequest{TableNameOrId: new(id)})
	require.NoError(t, err)
	assert.Equal(t, otherCompart, *got.CompartmentId)

	list, err := e.client.ListTables(t.Context(), nosql.ListTablesRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
	assert.Empty(t, list.Items)

	e.mkTable(t, "t", usersDDL("t"))

	_, err = e.client.ChangeTableCompartment(t.Context(), nosql.ChangeTableCompartmentRequest{
		TableNameOrId:   new(id),
		ToCompartmentId: new(testCompartment),
	})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "name already used there")
}

func TestListTablesPaging(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	for _, n := range []string{"c_t", "a_t", "b_t"} {
		e.createTable(t, n, "CREATE TABLE "+n+" (a INTEGER, PRIMARY KEY(a))", nil)
	}

	e.settle()

	var names []string

	var page *string

	for {
		resp, err := e.client.ListTables(t.Context(), nosql.ListTablesRequest{
			CompartmentId: new(testCompartment), Limit: new(2), Page: page,
			SortBy: nosql.ListTablesSortByName, SortOrder: nosql.ListTablesSortOrderAsc,
		})
		require.NoError(t, err)

		for _, it := range resp.Items {
			names = append(names, *it.Name)
		}

		if resp.OpcNextPage == nil {
			break
		}

		page = resp.OpcNextPage
	}

	assert.Equal(t, []string{"a_t", "b_t", "c_t"}, names)

	one, err := e.client.ListTables(
		t.Context(),
		nosql.ListTablesRequest{CompartmentId: new(testCompartment), Name: new("B_T")},
	)
	require.NoError(t, err)
	assert.Len(t, one.Items, 1)
}

func TestIfNotExistsDDL(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.mkTable(t, "t", usersDDL("t"))

	resp, err := e.client.CreateTable(t.Context(), nosql.CreateTableRequest{
		Name: new("t"), CompartmentId: new(testCompartment), TableLimits: provisioned(5, 5, 5),
		DdlStatement: new("CREATE TABLE IF NOT EXISTS t (id INTEGER, PRIMARY KEY(id))")})
	require.NoError(t, err)
	assert.Equal(t, nosql.WorkRequestStatusSucceeded, e.workRequest(t, resp.OpcWorkRequestId).Status)
	assert.Len(t, e.getTable(t, "t").Schema.Columns, 5, "existing table is untouched")
}
