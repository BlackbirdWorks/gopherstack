package ocinosql_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/nosql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestThroughputThrottling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		op        func(t *testing.T, e *env) error
		name      string
		enforce   bool
		onDemand  bool
		wantThrot bool
	}{
		{name: "read", enforce: true, wantThrot: true, op: opGet},
		{name: "write", enforce: true, wantThrot: true, op: opPut},
		{name: "delete", enforce: true, wantThrot: true, op: opDelete},
		{name: "query", enforce: true, wantThrot: true, op: opQuery},
		{name: "off", enforce: false, wantThrot: false, op: opGet},
		{name: "ondemand", enforce: true, onDemand: true, wantThrot: false, op: opPut},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			e.backend.SetEnforceThroughput(tc.enforce)

			limits := provisioned(1, 1, 1)
			if tc.onDemand {
				limits = &nosql.TableLimits{
					MaxStorageInGBs: new(1), CapacityMode: nosql.TableLimitsCapacityModeOnDemand,
				}
			}

			e.createTable(t, "t", usersDDL("t"), limits)
			e.settle()
			e.put(t, "t", map[string]any{"id": 1, "name": "a"})
			e.clock.Advance(time.Second)

			require.NoError(t, tc.op(t, e))

			err := tc.op(t, e)
			if !tc.wantThrot {
				require.NoError(t, err)

				return
			}

			se := svcErr(t, err)
			assert.Equal(t, http.StatusTooManyRequests, se.GetHTTPStatusCode())
			assert.Equal(t, "TooManyRequests", se.GetCode())

			e.clock.Advance(time.Second)
			require.NoError(t, tc.op(t, e))

			usage, uerr := e.client.ListTableUsage(t.Context(), nosql.ListTableUsageRequest{
				TableNameOrId: new("t"), CompartmentId: new(testCompartment),
			})
			require.NoError(t, uerr)

			total := 0
			for _, it := range usage.Items {
				total += *it.ReadThrottleCount + *it.WriteThrottleCount
			}

			assert.Equal(t, 1, total)
		})
	}
}

func opGet(t *testing.T, e *env) error {
	t.Helper()

	_, err := e.client.GetRow(t.Context(), nosql.GetRowRequest{
		TableNameOrId: new("t"), CompartmentId: new(testCompartment), Key: []string{"id:1"},
	})

	return err
}

func opPut(t *testing.T, e *env) error {
	t.Helper()

	_, err := e.client.UpdateRow(t.Context(), nosql.UpdateRowRequest{
		TableNameOrId: new("t"), Value: map[string]any{"id": 2, "name": "b"}, CompartmentId: new(testCompartment),
	})

	return err
}

func opDelete(t *testing.T, e *env) error {
	t.Helper()

	_, err := e.client.DeleteRow(t.Context(), nosql.DeleteRowRequest{
		TableNameOrId: new("t"), CompartmentId: new(testCompartment), Key: []string{"id:1"},
	})

	return err
}

func opQuery(t *testing.T, e *env) error {
	t.Helper()

	_, err := e.client.Query(t.Context(), nosql.QueryRequest{
		CompartmentId: new(testCompartment), Statement: new("SELECT * FROM t"),
	})

	return err
}

func TestThrottleCountsSurviveRestore(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.backend.SetEnforceThroughput(true)
	e.createTable(t, "t", usersDDL("t"), provisioned(1, 1, 1))
	e.settle()
	e.clock.Advance(time.Second)

	require.NoError(t, opPut(t, e))
	require.Error(t, opPut(t, e))

	fresh := newEnv(t, envOpts{})
	require.NoError(t, fresh.backend.Restore(t.Context(), e.backend.Snapshot(t.Context())))

	usage, err := fresh.client.ListTableUsage(t.Context(), nosql.ListTableUsageRequest{
		TableNameOrId: new("t"), CompartmentId: new(testCompartment),
		TimeStart: &common.SDKTime{Time: e.clock.Now().Add(-time.Hour)},
		TimeEnd:   &common.SDKTime{Time: e.clock.Now().Add(time.Hour)},
	})
	require.NoError(t, err)
	require.NotEmpty(t, usage.Items)

	total := 0
	for _, it := range usage.Items {
		total += *it.WriteThrottleCount
	}

	assert.Equal(t, 1, total)
}
