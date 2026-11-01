package ocinosql_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/nosql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func wideDDL(table string, cols int) string {
	parts := []string{"id INTEGER"}
	for i := 1; i < cols; i++ {
		parts = append(parts, fmt.Sprintf("c%d STRING", i))
	}

	return "CREATE TABLE " + table + " (" + strings.Join(parts, ", ") + ", PRIMARY KEY(id))"
}

func TestServiceLimitsOnCreateTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ddl      string
		limits   *nosql.TableLimits
		wantCode string
		wantOK   bool
	}{
		{name: "write 20000", ddl: usersDDL("t1"), limits: provisioned(40000, 20000, 10), wantOK: true},
		{name: "write 20001", ddl: usersDDL("t2"), limits: provisioned(100, 20001, 10), wantCode: "InvalidParameter"},
		{name: "read 40001", ddl: usersDDL("t3"), limits: provisioned(40001, 100, 10), wantCode: "InvalidParameter"},
		{name: "50 columns", ddl: wideDDL("t4", 50), limits: provisioned(10, 10, 1), wantOK: true},
		{name: "51 columns", ddl: wideDDL("t5", 51), limits: provisioned(10, 10, 1), wantCode: "LimitExceeded"},
		{
			name:   "column name 64",
			ddl:    "CREATE TABLE t6 (id INTEGER, " + strings.Repeat("a", 64) + " STRING, PRIMARY KEY(id))",
			limits: provisioned(10, 10, 1), wantOK: true,
		},
		{
			name:     "column name 65",
			ddl:      "CREATE TABLE t7 (id INTEGER, " + strings.Repeat("a", 65) + " STRING, PRIMARY KEY(id))",
			limits:   provisioned(10, 10, 1),
			wantCode: "InvalidParameter",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			name := strings.Fields(tt.ddl)[2]

			_, err := e.client.CreateTable(t.Context(), nosql.CreateTableRequest{
				Name: &name, CompartmentId: new(testCompartment), DdlStatement: new(tt.ddl), TableLimits: tt.limits,
			})
			if tt.wantOK {
				require.NoError(t, err)

				return
			}

			se := svcErr(t, err)
			assert.Equal(t, tt.wantCode, se.GetCode())
			assert.NotEqual(t, http.StatusOK, se.GetHTTPStatusCode())
		})
	}
}

func TestIndexLimits(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.mkTable(t, "idxlim", wideDDL("idxlim", 8))

	_, err := e.client.CreateIndex(t.Context(), nosql.CreateIndexRequest{
		TableNameOrId: new("idxlim"), CompartmentId: new(testCompartment),
		Name: new(strings.Repeat("i", 65)), Keys: []nosql.IndexKey{{ColumnName: new("c1")}},
	})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode(), "index name over 64 characters")

	for i := 1; i <= 5; i++ {
		_, err = e.client.CreateIndex(t.Context(), nosql.CreateIndexRequest{
			TableNameOrId: new("idxlim"), CompartmentId: new(testCompartment),
			Name: new(fmt.Sprintf("ix%d", i)), Keys: []nosql.IndexKey{{ColumnName: new(fmt.Sprintf("c%d", i))}},
		})
		require.NoError(t, err)
	}

	_, err = e.client.CreateIndex(t.Context(), nosql.CreateIndexRequest{
		TableNameOrId: new("idxlim"), CompartmentId: new(testCompartment),
		Name: new("ix6"), Keys: []nosql.IndexKey{{ColumnName: new("c6")}},
	})
	assert.Equal(t, "LimitExceeded", svcErr(t, err).GetCode(), "sixth index")
}

func TestRowQueryAndKeySizeLimits(t *testing.T) {
	t.Parallel()

	const rowLimit = 512 * 1024

	tests := []struct {
		name   string
		sk     int
		pk     int
		data   int
		wantOK bool
	}{
		{name: "key 64", sk: 64, pk: 0, data: 100, wantOK: true},
		{name: "key 62 plus 2", sk: 62, pk: 2, data: 100, wantOK: true},
		{name: "key 66", sk: 64, pk: 2, data: 0},
		{name: "row at limit", sk: 1, pk: 63, data: rowLimit - 64, wantOK: true},
		{name: "row over limit", sk: 2, pk: 2, data: rowLimit},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			e.mkTable(t, "sized", "CREATE TABLE sized (sk STRING, pk STRING, data STRING, PRIMARY KEY(SHARD(sk), pk))")

			_, err := e.client.UpdateRow(t.Context(), nosql.UpdateRowRequest{
				TableNameOrId: new("sized"), CompartmentId: new(testCompartment),
				Value: map[string]any{
					"sk": strings.Repeat("s", tt.sk), "pk": strings.Repeat("p", tt.pk),
					"data": strings.Repeat("d", tt.data),
				},
			})
			if tt.wantOK {
				require.NoError(t, err)

				return
			}

			se := svcErr(t, err)
			assert.Equal(t, "LimitExceeded", se.GetCode())
			assert.Equal(t, http.StatusBadRequest, se.GetHTTPStatusCode())
		})
	}
}

func TestQueryStringLimit(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.mkTable(t, "qlim", "CREATE TABLE qlim (id INTEGER, PRIMARY KEY(id))")

	long := "SELECT id FROM qlim WHERE id IN (" + strings.Repeat("1, ", 4000) + "1)"
	require.Greater(t, len(long), 10*1024)

	_, err := e.client.Query(t.Context(), nosql.QueryRequest{
		CompartmentId: new(testCompartment), Statement: new(long),
	})
	assert.Equal(t, "LimitExceeded", svcErr(t, err).GetCode())

	short := "SELECT id FROM qlim WHERE id IN (" + strings.Repeat("1, ", 100) + "1)"
	_, err = e.client.Query(t.Context(), nosql.QueryRequest{
		CompartmentId: new(testCompartment), Statement: new(short),
	})
	require.NoError(t, err)
}
