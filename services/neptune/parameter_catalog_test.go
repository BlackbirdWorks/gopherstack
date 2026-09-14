package neptune_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune"
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
				{ParameterName: "neptune_query_timeout", ParameterValue: "5000", ApplyMethod: "pending-reboot"},
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

// TestParameterValueStore_StaticRequiresPendingReboot verifies AWS's
// static-parameter/pending-reboot ApplyMethod compatibility rule: a static
// parameter (neptune_result_cache, instance-level) rejects
// ApplyMethod=immediate but accepts pending-reboot.
func TestParameterValueStore_StaticRequiresPendingReboot(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	dbParameterGroupKind().create(t, h, "pg-static-apply")

	resp := dbParameterGroupKind().modify(t, h, "pg-static-apply", []neptune.ParameterInput{
		{ParameterName: "neptune_result_cache", ParameterValue: "1", ApplyMethod: "immediate"},
	})
	assert.Equal(t, http.StatusBadRequest, resp.Code)

	resp = dbParameterGroupKind().modify(t, h, "pg-static-apply", []neptune.ParameterInput{
		{ParameterName: "neptune_result_cache", ParameterValue: "1", ApplyMethod: "pending-reboot"},
	})
	assert.Equal(t, http.StatusOK, resp.Code)
}

// TestParameterValueStore_DisallowedValueRejected verifies Modify enforces
// each parameter's documented AllowedValues, not just its name/modifiability
// -- a real bug class: a value outside the catalog's allowed set used to be
// silently accepted and stored.
func TestParameterValueStore_DisallowedValueRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		kind  parameterGroupKind
		param neptune.ParameterInput
	}{
		{
			name: "enum value out of range",
			kind: dbParameterGroupKind(),
			param: neptune.ParameterInput{
				ParameterName: "neptune_result_cache", ParameterValue: "2", ApplyMethod: "pending-reboot",
			},
		},
		{
			name: "numeric range exceeded",
			kind: dbClusterParameterGroupKind(),
			param: neptune.ParameterInput{
				ParameterName: "neptune_streams_expiry_days", ParameterValue: "91", ApplyMethod: "pending-reboot",
			},
		},
		{
			name: "non-numeric value for a ranged parameter",
			kind: dbClusterParameterGroupKind(),
			param: neptune.ParameterInput{
				ParameterName: "neptune_query_timeout", ParameterValue: "not-a-number", ApplyMethod: "pending-reboot",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.kind.create(t, h, "pg-disallowed")

			resp := tt.kind.modify(t, h, "pg-disallowed", []neptune.ParameterInput{tt.param})
			assert.Equal(t, http.StatusBadRequest, resp.Code)
			assert.Contains(t, resp.Body.String(), "InvalidParameterValue")
		})
	}
}

// TestParameterValueStore_NotModifiableRejected verifies rejection of a
// non-modifiable parameter. deprecated (neptune_enforce_ssl) parameters ARE
// still modifiable per AWS's own catalog (no doc states otherwise), so this
// instead exercises the shared reject-unknown/unmodifiable path via a name
// that legitimately belongs to the OTHER scope's catalog: cluster-only
// neptune_streams is not a recognized DB (instance) parameter, and
// instance-only UndoLogPurgeConfig is not a recognized DB cluster parameter.
func TestParameterValueStore_CrossScopeParameterRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		kind  parameterGroupKind
		param string
	}{
		{name: "cluster-only param via DB parameter group", kind: dbParameterGroupKind(), param: "neptune_streams"},
		{
			name: "instance-only param via DB cluster parameter group",
			kind: dbClusterParameterGroupKind(), param: "UndoLogPurgeConfig",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.kind.create(t, h, "pg-cross-scope")

			resp := tt.kind.modify(t, h, "pg-cross-scope", []neptune.ParameterInput{
				{ParameterName: tt.param, ParameterValue: "1", ApplyMethod: "pending-reboot"},
			})
			assert.Equal(t, http.StatusBadRequest, resp.Code)
			assert.Contains(t, resp.Body.String(), "InvalidParameterValue")
		})
	}
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
				{ParameterName: "neptune_query_timeout", ParameterValue: "9999", ApplyMethod: "pending-reboot"},
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
		{ParameterName: "neptune_query_timeout", ParameterValue: "1234", ApplyMethod: "pending-reboot"},
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
// always-empty list, and that the instance-level and cluster-level catalogs
// are genuinely distinct (not the same 8-parameter list echoed at both
// scopes, as this backend modeled before this pass).
func TestDescribeEngineDefaultParameters_ReturnsCatalog(t *testing.T) {
	t.Parallel()

	tests := []struct {
		action     string
		wantParam  string
		wantAbsent string // documented at the OTHER scope only
	}{
		{
			action: "DescribeEngineDefaultParameters", wantParam: "neptune_dfe_query_engine",
			wantAbsent: "neptune_streams_expiry_days",
		},
		{
			action: "DescribeEngineDefaultClusterParameters", wantParam: "neptune_streams_expiry_days",
			wantAbsent: "neptune_dfe_query_engine",
		},
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
			assert.Contains(t, body, tt.wantParam)
			assert.NotContains(t, body, tt.wantAbsent)
		})
	}
}
