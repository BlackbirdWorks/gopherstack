package docdb_test

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/docdb"
)

func TestHandler_ClusterParameterGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "create_param_group",
			vals: url.Values{
				"Action":                      {"CreateDBClusterParameterGroup"},
				"Version":                     {"2014-10-31"},
				"DBClusterParameterGroupName": {"my-pg"},
				"DBParameterGroupFamily":      {"docdb4.0"},
				"Description":                 {"test param group"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "my-pg",
		},
		{
			name: "describe_param_groups_all",
			vals: url.Values{
				"Action":  {"DescribeDBClusterParameterGroups"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DescribeDBClusterParameterGroupsResponse",
		},
		{
			name: "describe_param_group_by_name",
			vals: url.Values{
				"Action":                      {"DescribeDBClusterParameterGroups"},
				"Version":                     {"2014-10-31"},
				"DBClusterParameterGroupName": {"my-pg"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "my-pg",
		},
		{
			name: "modify_param_group",
			vals: url.Values{
				"Action":                      {"ModifyDBClusterParameterGroup"},
				"Version":                     {"2014-10-31"},
				"DBClusterParameterGroupName": {"my-pg"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "my-pg",
		},
		{
			name: "delete_param_group",
			vals: url.Values{
				"Action":                      {"DeleteDBClusterParameterGroup"},
				"Version":                     {"2014-10-31"},
				"DBClusterParameterGroupName": {"my-pg"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DeleteDBClusterParameterGroupResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.name != "create_param_group" {
				doRequest(t, h, url.Values{
					"Action":                      {"CreateDBClusterParameterGroup"},
					"Version":                     {"2014-10-31"},
					"DBClusterParameterGroupName": {"my-pg"},
					"DBParameterGroupFamily":      {"docdb4.0"},
					"Description":                 {"test"},
				})
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestCopyDBClusterParameterGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *docdb.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "copy_parameter_group",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":                      {"CreateDBClusterParameterGroup"},
					"Version":                     {"2014-10-31"},
					"DBClusterParameterGroupName": {"source-pg"},
					"DBParameterGroupFamily":      {"docdb4.0"},
					"Description":                 {"source"},
				})
			},
			vals: url.Values{
				"Action":  {"CopyDBClusterParameterGroup"},
				"Version": {"2014-10-31"},
				"SourceDBClusterParameterGroupIdentifier":  {"source-pg"},
				"TargetDBClusterParameterGroupIdentifier":  {"target-pg"},
				"TargetDBClusterParameterGroupDescription": {"target"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "target-pg",
		},
		{
			name: "copy_parameter_group_source_not_found",
			vals: url.Values{
				"Action":  {"CopyDBClusterParameterGroup"},
				"Version": {"2014-10-31"},
				"SourceDBClusterParameterGroupIdentifier": {"nonexistent"},
				"TargetDBClusterParameterGroupIdentifier": {"target-pg"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBParameterGroupNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_DescribeDBClusterParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *docdb.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "describe_parameters",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":                      {"CreateDBClusterParameterGroup"},
					"Version":                     {"2014-10-31"},
					"DBClusterParameterGroupName": {"my-pg"},
					"DBParameterGroupFamily":      {"docdb4.0"},
					"Description":                 {"test"},
				})
			},
			vals: url.Values{
				"Action":                      {"DescribeDBClusterParameters"},
				"Version":                     {"2014-10-31"},
				"DBClusterParameterGroupName": {"my-pg"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "tls",
		},
		{
			name: "describe_parameters_group_not_found",
			vals: url.Values{
				"Action":                      {"DescribeDBClusterParameters"},
				"Version":                     {"2014-10-31"},
				"DBClusterParameterGroupName": {"nonexistent"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBParameterGroupNotFound",
		},
		{
			name: "describe_parameters_missing_group_name",
			vals: url.Values{
				"Action":  {"DescribeDBClusterParameters"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestSortedDescribeParameterGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		names []string
		want  []string
	}{
		{
			name:  "sorted_order",
			names: []string{"pg-z", "pg-a", "pg-m"},
			want:  []string{"pg-a", "pg-m", "pg-z"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
			for _, name := range tt.names {
				b.AddDBClusterParameterGroupInternal(&docdb.DBClusterParameterGroup{DBClusterParameterGroupName: name})
			}

			got, err := b.DescribeDBClusterParameterGroups(context.Background(), "")
			require.NoError(t, err)

			gotNames := make([]string, len(got))
			for i, pg := range got {
				gotNames[i] = pg.DBClusterParameterGroupName
			}

			assert.Equal(t, tt.want, gotNames)
		})
	}
}

func TestHandler_EngineDefaultParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "describe_engine_default_params",
			vals: url.Values{
				"Action":                 {"DescribeEngineDefaultClusterParameters"},
				"Version":                {"2014-10-31"},
				"DBParameterGroupFamily": {"docdb4.0"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DescribeEngineDefaultClusterParametersResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_ResetDBClusterParameterGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *docdb.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "reset_parameter_group",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":                      {"CreateDBClusterParameterGroup"},
					"Version":                     {"2014-10-31"},
					"DBClusterParameterGroupName": {"my-pg"},
					"DBParameterGroupFamily":      {"docdb4.0"},
					"Description":                 {"test"},
				})
			},
			vals: url.Values{
				"Action":                      {"ResetDBClusterParameterGroup"},
				"Version":                     {"2014-10-31"},
				"DBClusterParameterGroupName": {"my-pg"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "my-pg",
		},
		{
			name: "reset_parameter_group_not_found",
			vals: url.Values{
				"Action":                      {"ResetDBClusterParameterGroup"},
				"Version":                     {"2014-10-31"},
				"DBClusterParameterGroupName": {"nonexistent"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBParameterGroupNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

// TestResetDBClusterParameterGroup_RealReset locks in the fix for a
// disguised no-op: ResetDBClusterParameterGroup previously validated the
// group and returned an unchanged clone without ever touching
// pg.Parameters, so a real caller's ResetAllParameters=true or
// per-parameter reset request silently did nothing. It must now genuinely
// clear the requested overrides.
func TestResetDBClusterParameterGroup_RealReset(t *testing.T) {
	t.Parallel()

	t.Run("reset_all_parameters_clears_every_override", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, url.Values{
			"Action":                      {"CreateDBClusterParameterGroup"},
			"Version":                     {"2014-10-31"},
			"DBClusterParameterGroupName": {"reset-all-pg"},
			"DBParameterGroupFamily":      {"docdb4.0"},
		})
		doRequest(t, h, url.Values{
			"Action":                                {"ModifyDBClusterParameterGroup"},
			"Version":                               {"2014-10-31"},
			"DBClusterParameterGroupName":           {"reset-all-pg"},
			"Parameters.Parameter.1.ParameterName":  {"tls"},
			"Parameters.Parameter.1.ParameterValue": {"disabled"},
		})

		// Confirm the override actually took before reset.
		beforeRR := doRequest(t, h, url.Values{
			"Action":                      {"DescribeDBClusterParameters"},
			"Version":                     {"2014-10-31"},
			"DBClusterParameterGroupName": {"reset-all-pg"},
		})
		require.Equal(t, http.StatusOK, beforeRR.Code)
		assert.Contains(t, beforeRR.Body.String(), "<ParameterValue>disabled</ParameterValue>")

		resetRR := doRequest(t, h, url.Values{
			"Action":                      {"ResetDBClusterParameterGroup"},
			"Version":                     {"2014-10-31"},
			"DBClusterParameterGroupName": {"reset-all-pg"},
			"ResetAllParameters":          {"true"},
		})
		require.Equal(t, http.StatusOK, resetRR.Code)

		afterRR := doRequest(t, h, url.Values{
			"Action":                      {"DescribeDBClusterParameters"},
			"Version":                     {"2014-10-31"},
			"DBClusterParameterGroupName": {"reset-all-pg"},
		})
		require.Equal(t, http.StatusOK, afterRR.Code)
		assert.NotContains(t, afterRR.Body.String(), "<ParameterValue>disabled</ParameterValue>",
			"ResetAllParameters=true must clear the user override back to the engine default")
		assert.Contains(t, afterRR.Body.String(), "<ParameterValue>enabled</ParameterValue>",
			"tls's engine default (enabled) must be visible again after reset")
	})

	t.Run("reset_named_parameter_only_clears_that_one", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, url.Values{
			"Action":                      {"CreateDBClusterParameterGroup"},
			"Version":                     {"2014-10-31"},
			"DBClusterParameterGroupName": {"reset-one-pg"},
			"DBParameterGroupFamily":      {"docdb4.0"},
		})
		doRequest(t, h, url.Values{
			"Action":                                {"ModifyDBClusterParameterGroup"},
			"Version":                               {"2014-10-31"},
			"DBClusterParameterGroupName":           {"reset-one-pg"},
			"Parameters.Parameter.1.ParameterName":  {"tls"},
			"Parameters.Parameter.1.ParameterValue": {"disabled"},
			"Parameters.Parameter.2.ParameterName":  {"ttl_monitor"},
			"Parameters.Parameter.2.ParameterValue": {"disabled"},
		})

		resetRR := doRequest(t, h, url.Values{
			"Action":                               {"ResetDBClusterParameterGroup"},
			"Version":                              {"2014-10-31"},
			"DBClusterParameterGroupName":          {"reset-one-pg"},
			"Parameters.Parameter.1.ParameterName": {"tls"},
		})
		require.Equal(t, http.StatusOK, resetRR.Code)

		afterRR := doRequest(t, h, url.Values{
			"Action":                      {"DescribeDBClusterParameters"},
			"Version":                     {"2014-10-31"},
			"DBClusterParameterGroupName": {"reset-one-pg"},
		})
		require.Equal(t, http.StatusOK, afterRR.Code)
		body := afterRR.Body.String()
		// tls reverted to its engine default (enabled); ttl_monitor keeps
		// the user override (disabled) since it wasn't named in the reset.
		assert.Contains(t, body, "<ParameterName>tls</ParameterName><ParameterValue>enabled</ParameterValue>")
		assert.Contains(t, body, "<ParameterName>ttl_monitor</ParameterName><ParameterValue>disabled</ParameterValue>")
	})
}

func TestDeleteParameterGroupInUse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "parameter_group_in_use_rejected",
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidDBParameterGroupState",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":                      {"CreateDBClusterParameterGroup"},
				"Version":                     {"2014-10-31"},
				"DBClusterParameterGroupName": {"my-pg"},
				"DBParameterGroupFamily":      {"docdb4.0"},
				"Description":                 {"test pg"},
			})
			doRequest(t, h, url.Values{
				"Action":                      {"CreateDBCluster"},
				"Version":                     {"2014-10-31"},
				"DBClusterIdentifier":         {"pg-cluster"},
				"DBClusterParameterGroupName": {"my-pg"},
			})
			rr := doRequest(t, h, url.Values{
				"Action":                      {"DeleteDBClusterParameterGroup"},
				"Version":                     {"2014-10-31"},
				"DBClusterParameterGroupName": {"my-pg"},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestDescribeDBClusterParameters_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		groupName    string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "missing_group_name_returns_error",
			groupName:    "",
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "unknown_group_returns_not_found",
			groupName:    "no-such-group",
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBParameterGroupNotFound",
		},
		{
			name:         "known_group_returns_params",
			groupName:    "test-group",
			wantStatus:   http.StatusOK,
			wantContains: "tls",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.groupName == "test-group" {
				b2CreateParamGroup(t, h, "test-group")
			}
			vals := url.Values{
				"Action":  {"DescribeDBClusterParameters"},
				"Version": {"2014-10-31"},
			}
			if tt.groupName != "" {
				vals.Set("DBClusterParameterGroupName", tt.groupName)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestCopyDBClusterParameterGroup_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		source       string
		target       string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "not_found_source_returns_error",
			source:       "no-such-group",
			target:       "new-group",
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBParameterGroupNotFound",
		},
		{
			name:         "duplicate_target_returns_error",
			source:       "src-group",
			target:       "dst-group",
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBParameterGroupAlreadyExists",
		},
		{
			name:         "valid_copy_succeeds",
			source:       "src-group2",
			target:       "dst-group2",
			wantStatus:   http.StatusOK,
			wantContains: "CopyDBClusterParameterGroupResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			// Pre-create source groups for tests that need them.
			if tt.source == "src-group" || tt.source == "src-group2" {
				b2CreateParamGroup(t, h, tt.source)
			}
			// Pre-create target to trigger duplicate error.
			if tt.name == "duplicate_target_returns_error" {
				b2CreateParamGroup(t, h, tt.target)
			}

			rr := doRequest(t, h, url.Values{
				"Action":  {"CopyDBClusterParameterGroup"},
				"Version": {"2014-10-31"},
				"SourceDBClusterParameterGroupIdentifier":  {tt.source},
				"TargetDBClusterParameterGroupIdentifier":  {tt.target},
				"TargetDBClusterParameterGroupDescription": {"copy"},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

// TestParity_ParameterGroupStorage verifies parameters are stored and returned.
func TestParameterGroupStorage(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	// Create param group.
	rr := doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterParameterGroup"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"my-pg"},
		"DBParameterGroupFamily":      {"docdb4.0"},
		"Description":                 {"test pg"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	// Modify: set a parameter.
	rr2 := doRequest(t, h, url.Values{
		"Action":                                {"ModifyDBClusterParameterGroup"},
		"Version":                               {"2014-10-31"},
		"DBClusterParameterGroupName":           {"my-pg"},
		"Parameters.Parameter.1.ParameterName":  {"tls"},
		"Parameters.Parameter.1.ParameterValue": {"disabled"},
		"Parameters.Parameter.1.ApplyMethod":    {"immediate"},
	})
	require.Equal(t, http.StatusOK, rr2.Code)

	// Describe params: should reflect user value.
	rr3 := doRequest(t, h, url.Values{
		"Action":                      {"DescribeDBClusterParameters"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"my-pg"},
	})
	require.Equal(t, http.StatusOK, rr3.Code)
	body := rr3.Body.String()
	assert.Contains(t, body, "<ParameterName>tls</ParameterName>")
	assert.Contains(t, body, "<ParameterValue>disabled</ParameterValue>")
	assert.Contains(t, body, "<Source>user</Source>")
}

// TestParity_ParameterGroupPagination verifies Marker pagination on DescribeDBClusterParameterGroups.
func TestParameterGroupPagination(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	// Create 3 param groups.
	for i := range 3 {
		rr := doRequest(t, h, url.Values{
			"Action":                      {"CreateDBClusterParameterGroup"},
			"Version":                     {"2014-10-31"},
			"DBClusterParameterGroupName": {strings.NewReplacer("{{i}}", string(rune('a'+i))).Replace("pg-{{i}}")},
			"DBParameterGroupFamily":      {"docdb4.0"},
			"Description":                 {"pg test"},
		})
		require.Equal(t, http.StatusOK, rr.Code)
	}

	// Fetch first page of 2.
	rr1 := doRequest(t, h, url.Values{
		"Action":     {"DescribeDBClusterParameterGroups"},
		"Version":    {"2014-10-31"},
		"MaxRecords": {"2"},
	})
	require.Equal(t, http.StatusOK, rr1.Code)
	body1 := rr1.Body.String()

	var page1 struct {
		XMLName xml.Name `xml:"DescribeDBClusterParameterGroupsResponse"`
		Result  struct {
			Marker string `xml:"Marker"`
		} `xml:"DescribeDBClusterParameterGroupsResult"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body1), &page1))
	assert.NotEmpty(t, page1.Result.Marker, "Marker must be set when more pages exist")

	// Fetch next page with marker.
	rr2 := doRequest(t, h, url.Values{
		"Action":     {"DescribeDBClusterParameterGroups"},
		"Version":    {"2014-10-31"},
		"MaxRecords": {"2"},
		"Marker":     {page1.Result.Marker},
	})
	require.Equal(t, http.StatusOK, rr2.Code)
	body2 := rr2.Body.String()
	assert.Contains(t, body2, "<DBClusterParameterGroupName>")

	var page2 struct {
		XMLName xml.Name `xml:"DescribeDBClusterParameterGroupsResponse"`
		Result  struct {
			Marker string `xml:"Marker"`
		} `xml:"DescribeDBClusterParameterGroupsResult"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body2), &page2))
	assert.Empty(t, page2.Result.Marker, "Marker must be empty on last page")
}
