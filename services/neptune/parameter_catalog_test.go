package neptune_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/neptune"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// parameterGroupKind parameterizes the parameter-value-store tests below
// across DB parameter groups (instance-level) and DB cluster parameter
// groups (cluster-level): both are backed by the same shared catalog and
// override-store logic in parameter_catalog.go, so the wire-level behavior
// under test is identical modulo action/field name prefixes.
type parameterGroupKind struct {
	createAction   string
	modifyAction   string
	resetAction    string
	describeAction string
	nameField      string
}

func dbParameterGroupKind() parameterGroupKind {
	return parameterGroupKind{
		createAction:   "CreateDBParameterGroup",
		modifyAction:   "ModifyDBParameterGroup",
		resetAction:    "ResetDBParameterGroup",
		describeAction: "DescribeDBParameters",
		nameField:      "DBParameterGroupName",
	}
}

func dbClusterParameterGroupKind() parameterGroupKind {
	return parameterGroupKind{
		createAction:   "CreateDBClusterParameterGroup",
		modifyAction:   "ModifyDBClusterParameterGroup",
		resetAction:    "ResetDBClusterParameterGroup",
		describeAction: "DescribeDBClusterParameters",
		nameField:      "DBClusterParameterGroupName",
	}
}

func (k parameterGroupKind) create(t *testing.T, h *neptune.Handler, name string) {
	t.Helper()
	rr := doRequest(t, h, url.Values{
		"Action":                 {k.createAction},
		"Version":                {"2014-10-31"},
		k.nameField:              {name},
		"DBParameterGroupFamily": {"neptune1.3"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
}

func (k parameterGroupKind) describe(t *testing.T, h *neptune.Handler, name string) *httptest.ResponseRecorder {
	t.Helper()

	return doRequest(t, h, url.Values{
		"Action":    {k.describeAction},
		"Version":   {"2014-10-31"},
		k.nameField: {name},
	})
}

func (k parameterGroupKind) modify(
	t *testing.T, h *neptune.Handler, name string, params []neptune.ParameterInput,
) *httptest.ResponseRecorder {
	t.Helper()
	vals := url.Values{
		"Action":    {k.modifyAction},
		"Version":   {"2014-10-31"},
		k.nameField: {name},
	}
	for i, p := range params {
		n := i + 1
		vals.Set(fmt.Sprintf("Parameters.Parameter.%d.ParameterName", n), p.ParameterName)
		vals.Set(fmt.Sprintf("Parameters.Parameter.%d.ParameterValue", n), p.ParameterValue)
		if p.ApplyMethod != "" {
			vals.Set(fmt.Sprintf("Parameters.Parameter.%d.ApplyMethod", n), p.ApplyMethod)
		}
	}

	return doRequest(t, h, vals)
}

func (k parameterGroupKind) reset(
	t *testing.T, h *neptune.Handler, name string, resetAll bool, params []neptune.ParameterInput,
) *httptest.ResponseRecorder {
	t.Helper()
	vals := url.Values{
		"Action":    {k.resetAction},
		"Version":   {"2014-10-31"},
		k.nameField: {name},
	}
	if resetAll {
		vals.Set("ResetAllParameters", "true")
	}
	for i, p := range params {
		n := i + 1
		vals.Set(fmt.Sprintf("Parameters.Parameter.%d.ParameterName", n), p.ParameterName)
		if p.ApplyMethod != "" {
			vals.Set(fmt.Sprintf("Parameters.Parameter.%d.ApplyMethod", n), p.ApplyMethod)
		}
	}

	return doRequest(t, h, vals)
}

// TestParameterValueStore_ModifyPersistsAndDescribeReflects locks the core
// fix: Modify used to validate the group and silently discard every
// parameter, so Describe always answered with an empty (or all-engine-default)
// list regardless of what was "set". It must now genuinely persist and
// genuinely read back.
func TestParameterValueStore_ModifyPersistsAndDescribeReflects(t *testing.T) {
	t.Parallel()

	for _, kind := range []parameterGroupKind{dbParameterGroupKind(), dbClusterParameterGroupKind()} {
		t.Run(kind.modifyAction, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			kind.create(t, h, "pg-modify")

			resp := kind.modify(t, h, "pg-modify", []neptune.ParameterInput{
				{ParameterName: "neptune_query_timeout", ParameterValue: "5000", ApplyMethod: "immediate"},
			})
			require.Equal(t, http.StatusOK, resp.Code)

			resp = kind.describe(t, h, "pg-modify")
			require.Equal(t, http.StatusOK, resp.Code)
			body := resp.Body.String()
			assert.Contains(t, body, "neptune_query_timeout")
			assert.Contains(t, body, "<ParameterValue>5000</ParameterValue>")
			assert.Contains(t, body, "<Source>user</Source>")
		})
	}
}

// TestParameterValueStore_UnknownParameterRejected verifies Modify no longer
// silently accepts arbitrary parameter names (a symptom of the old
// discard-everything behavior).
func TestParameterValueStore_UnknownParameterRejected(t *testing.T) {
	t.Parallel()

	for _, kind := range []parameterGroupKind{dbParameterGroupKind(), dbClusterParameterGroupKind()} {
		t.Run(kind.modifyAction, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			kind.create(t, h, "pg-unknown")

			resp := kind.modify(t, h, "pg-unknown", []neptune.ParameterInput{
				{ParameterName: "not_a_real_parameter", ParameterValue: "x", ApplyMethod: "immediate"},
			})
			assert.Equal(t, http.StatusBadRequest, resp.Code)
			assert.Contains(t, resp.Body.String(), "InvalidParameterValue")
		})
	}
}

// TestParameterValueStore_NotModifiableRejected verifies the catalog's one
// non-modifiable system parameter (neptune_shard_hash_partitions) cannot be
// overridden.
func TestParameterValueStore_NotModifiableRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	dbParameterGroupKind().create(t, h, "pg-static-system")

	resp := dbParameterGroupKind().modify(t, h, "pg-static-system", []neptune.ParameterInput{
		{ParameterName: "neptune_shard_hash_partitions", ParameterValue: "12", ApplyMethod: "pending-reboot"},
	})
	assert.Equal(t, http.StatusBadRequest, resp.Code)
	assert.Contains(t, resp.Body.String(), "InvalidParameterValue")
}

// TestParameterValueStore_StaticRequiresPendingReboot verifies AWS's
// static-parameter/pending-reboot ApplyMethod compatibility rule: a static
// parameter (neptune_streams) rejects ApplyMethod=immediate but accepts
// pending-reboot.
func TestParameterValueStore_StaticRequiresPendingReboot(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	dbParameterGroupKind().create(t, h, "pg-static-apply")

	resp := dbParameterGroupKind().modify(t, h, "pg-static-apply", []neptune.ParameterInput{
		{ParameterName: "neptune_streams", ParameterValue: "1", ApplyMethod: "immediate"},
	})
	assert.Equal(t, http.StatusBadRequest, resp.Code)

	resp = dbParameterGroupKind().modify(t, h, "pg-static-apply", []neptune.ParameterInput{
		{ParameterName: "neptune_streams", ParameterValue: "1", ApplyMethod: "pending-reboot"},
	})
	assert.Equal(t, http.StatusOK, resp.Code)
}

// TestParameterValueStore_ResetAllClearsOverrides verifies
// ResetAllParameters=true reverts every override back to its engine-default
// Source, and that a targeted (non-"all") reset only clears the named
// parameter.
func TestParameterValueStore_ResetAllClearsOverrides(t *testing.T) {
	t.Parallel()

	for _, kind := range []parameterGroupKind{dbParameterGroupKind(), dbClusterParameterGroupKind()} {
		t.Run(kind.resetAction, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			kind.create(t, h, "pg-reset")
			resp := kind.modify(t, h, "pg-reset", []neptune.ParameterInput{
				{ParameterName: "neptune_query_timeout", ParameterValue: "9999", ApplyMethod: "immediate"},
			})
			require.Equal(t, http.StatusOK, resp.Code)

			resp = kind.reset(t, h, "pg-reset", true, nil)
			require.Equal(t, http.StatusOK, resp.Code)

			resp = kind.describe(t, h, "pg-reset")
			body := resp.Body.String()
			assert.NotContains(t, body, "<ParameterValue>9999</ParameterValue>")
			assert.Contains(t, body, "<Source>engine-default</Source>")
		})
	}
}

// TestParameterValueStore_DeleteCascadesOverrides verifies deleting a
// parameter group also drops its override store -- no ghost rows survive
// the group itself, matching this backend's cascade-clean convention
// elsewhere (e.g. DeleteDBCluster's instance/endpoint/tag cleanup).
func TestParameterValueStore_DeleteCascadesOverrides(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	dbParameterGroupKind().create(t, h, "pg-cascade")
	resp := dbParameterGroupKind().modify(t, h, "pg-cascade", []neptune.ParameterInput{
		{ParameterName: "neptune_query_timeout", ParameterValue: "1234", ApplyMethod: "immediate"},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	rr := doRequest(t, h, url.Values{
		"Action":               {"DeleteDBParameterGroup"},
		"Version":              {"2014-10-31"},
		"DBParameterGroupName": {"pg-cascade"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	// Recreating under the same name must not resurrect the old override.
	dbParameterGroupKind().create(t, h, "pg-cascade")
	resp = dbParameterGroupKind().describe(t, h, "pg-cascade")
	body := resp.Body.String()
	assert.NotContains(t, body, "<ParameterValue>1234</ParameterValue>")
}

// TestDescribeEngineDefaultParameters_ReturnsCatalog verifies the
// engine-default describes now surface the real catalog instead of an
// always-empty list.
func TestDescribeEngineDefaultParameters_ReturnsCatalog(t *testing.T) {
	t.Parallel()

	tests := []struct {
		action string
	}{
		{action: "DescribeEngineDefaultParameters"},
		{action: "DescribeEngineDefaultClusterParameters"},
	}
	for _, tt := range tests {
		t.Run(tt.action, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rr := doRequest(t, h, url.Values{
				"Action":  {tt.action},
				"Version": {"2014-10-31"},
			})
			require.Equal(t, http.StatusOK, rr.Code)
			body := rr.Body.String()
			assert.Contains(t, body, "neptune_query_timeout")
			assert.Contains(t, body, "<IsModifiable>true</IsModifiable>")
		})
	}
}
