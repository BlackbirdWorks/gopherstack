package rds_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClusterPG_CRUD(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()

	pg, err := b.CreateDBClusterParameterGroup("cpg1", "aurora-postgresql14", "test")
	require.NoError(t, err)
	assert.Equal(t, "cpg1", pg.DBParameterGroupName)

	groups, err := b.DescribeDBClusterParameterGroups("cpg1")
	require.NoError(t, err)
	require.Len(t, groups, 1)

	err = b.DeleteDBClusterParameterGroup("cpg1")
	require.NoError(t, err)

	_, err = b.DescribeDBClusterParameterGroups("cpg1")
	require.Error(t, err)
	assert.ErrorIs(t, err, rds.ErrParameterGroupNotFound)
}

func TestClusterPG_ModifyAndDescribe(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBClusterParameterGroup("cpg2", "aurora-mysql8.0", "test")
	require.NoError(t, err)

	params := []rds.DBParameter{
		{ParameterName: "binlog_format", ParameterValue: "ROW"},
	}
	name, err := b.ModifyDBClusterParameterGroup("cpg2", params)
	require.NoError(t, err)
	assert.Equal(t, "cpg2", name)

	got, err := b.DescribeDBClusterParameters("cpg2")
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "binlog_format", got[0].ParameterName)
}

func TestClusterPG_Reset(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBClusterParameterGroup("cpg3", "aurora-postgresql14", "test")
	require.NoError(t, err)

	params := []rds.DBParameter{
		{ParameterName: "max_connections", ParameterValue: "500"},
		{ParameterName: "shared_preload_libraries", ParameterValue: "pg_stat_statements"},
	}
	_, err = b.ModifyDBClusterParameterGroup("cpg3", params)
	require.NoError(t, err)

	_, err = b.ResetDBClusterParameterGroup("cpg3", true, nil)
	require.NoError(t, err)

	got, err := b.DescribeDBClusterParameters("cpg3")
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestClusterPG_Copy(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBClusterParameterGroup("src-cpg", "aurora-postgresql14", "source")
	require.NoError(t, err)

	_, err = b.ModifyDBClusterParameterGroup("src-cpg", []rds.DBParameter{
		{ParameterName: "log_min_duration_statement", ParameterValue: "1000"},
	})
	require.NoError(t, err)

	_, err = b.CopyDBClusterParameterGroup("src-cpg", "dst-cpg", "destination")
	require.NoError(t, err)

	got, err := b.DescribeDBClusterParameters("dst-cpg")
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "log_min_duration_statement", got[0].ParameterName)
}

func TestClusterPG_HTTP(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()

	rec := postRDSForm(t, h, url.Values{
		"Action":                      {"CreateDBClusterParameterGroup"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"http-cpg"},
		"DBParameterGroupFamily":      {"aurora-postgresql14"},
		"Description":                 {"http test"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":                      {"DescribeDBClusterParameterGroups"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"http-cpg"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "http-cpg")

	rec = postRDSForm(t, h, url.Values{
		"Action":                                {"ModifyDBClusterParameterGroup"},
		"Version":                               {"2014-10-31"},
		"DBClusterParameterGroupName":           {"http-cpg"},
		"Parameters.Parameter.1.ParameterName":  {"max_connections"},
		"Parameters.Parameter.1.ParameterValue": {"100"},
		"Parameters.Parameter.1.ApplyMethod":    {"pending-reboot"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":                      {"DescribeDBClusterParameters"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"http-cpg"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "max_connections")

	rec = postRDSForm(t, h, url.Values{
		"Action":                      {"ResetDBClusterParameterGroup"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"http-cpg"},
		"ResetAllParameters":          {"true"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":                      {"DeleteDBClusterParameterGroup"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"http-cpg"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestCreateDBClusterParameterGroup_UsesClusterFieldName(t *testing.T) {
	t.Parallel()

	h := rds.NewHandler(rds.NewInMemoryBackend("000000000000", "us-east-1"))

	rec := postRDSForm(t, h, url.Values{
		"Action":                      {"CreateDBClusterParameterGroup"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"cpg-create"},
		"DBParameterGroupFamily":      {"aurora-postgresql14"},
		"Description":                 {"test cluster pg"},
	}.Encode())

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "DBClusterParameterGroupName",
		"response must use DBClusterParameterGroupName not DBParameterGroupName")
	assert.Contains(t, body, "cpg-create",
		"group name value must appear in response")
	assert.NotContains(t, body, "<DBParameterGroupName>",
		"response must not use DBParameterGroupName element for cluster PG")
}

func TestDescribeDBClusterParameterGroups_UsesClusterFieldName(t *testing.T) {
	t.Parallel()

	h := rds.NewHandler(rds.NewInMemoryBackend("000000000000", "us-east-1"))

	postRDSForm(t, h, url.Values{
		"Action":                      {"CreateDBClusterParameterGroup"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"cpg-describe"},
		"DBParameterGroupFamily":      {"aurora-mysql8.0"},
		"Description":                 {"describe test"},
	}.Encode())

	rec := postRDSForm(t, h, url.Values{
		"Action":                      {"DescribeDBClusterParameterGroups"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"cpg-describe"},
	}.Encode())

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "DBClusterParameterGroupName",
		"describe response must use DBClusterParameterGroupName element")
	assert.Contains(t, body, "cpg-describe",
		"group name value must appear in describe response")
	assert.Contains(t, body, "DBClusterParameterGroup>",
		"list must wrap members in DBClusterParameterGroup elements")
	assert.NotContains(t, body, "<DBParameterGroupName>",
		"describe response must not use DBParameterGroupName for cluster PG")
	assert.NotContains(t, body, "<DBParameterGroup>",
		"describe response list must not use DBParameterGroup element for cluster PG")
}

func TestCopyDBClusterParameterGroup_UsesClusterFieldName(t *testing.T) {
	t.Parallel()

	h := rds.NewHandler(rds.NewInMemoryBackend("000000000000", "us-east-1"))

	postRDSForm(t, h, url.Values{
		"Action":                      {"CreateDBClusterParameterGroup"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"cpg-src"},
		"DBParameterGroupFamily":      {"aurora-postgresql14"},
		"Description":                 {"source"},
	}.Encode())

	rec := postRDSForm(t, h, url.Values{
		"Action":  {"CopyDBClusterParameterGroup"},
		"Version": {"2014-10-31"},
		"SourceDBClusterParameterGroupIdentifier":  {"cpg-src"},
		"TargetDBClusterParameterGroupIdentifier":  {"cpg-dst"},
		"TargetDBClusterParameterGroupDescription": {"destination"},
	}.Encode())

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "DBClusterParameterGroupName",
		"copy response must use DBClusterParameterGroupName element")
	assert.Contains(t, body, "cpg-dst",
		"destination group name must appear in copy response")
	assert.NotContains(t, body, "<DBParameterGroupName>",
		"copy response must not use DBParameterGroupName for cluster PG")
}

func TestDBClusterPG_NonClusterPG_FieldNamesDistinct(t *testing.T) {
	t.Parallel()

	h := rds.NewHandler(rds.NewInMemoryBackend("000000000000", "us-east-1"))

	postRDSForm(t, h, url.Values{
		"Action":                 {"CreateDBParameterGroup"},
		"Version":                {"2014-10-31"},
		"DBParameterGroupName":   {"pg-instance"},
		"DBParameterGroupFamily": {"postgres14"},
		"Description":            {"instance pg"},
	}.Encode())

	recInstance := postRDSForm(t, h, url.Values{
		"Action":               {"DescribeDBParameterGroups"},
		"Version":              {"2014-10-31"},
		"DBParameterGroupName": {"pg-instance"},
	}.Encode())
	require.Equal(t, http.StatusOK, recInstance.Code)
	instanceBody := recInstance.Body.String()
	assert.Contains(t, instanceBody, "<DBParameterGroupName>",
		"instance PG describe must use DBParameterGroupName")
	assert.NotContains(t, instanceBody, "DBClusterParameterGroupName",
		"instance PG describe must not use cluster field name")

	postRDSForm(t, h, url.Values{
		"Action":                      {"CreateDBClusterParameterGroup"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"cpg-cluster"},
		"DBParameterGroupFamily":      {"aurora-postgresql14"},
		"Description":                 {"cluster pg"},
	}.Encode())

	recCluster := postRDSForm(t, h, url.Values{
		"Action":                      {"DescribeDBClusterParameterGroups"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"cpg-cluster"},
	}.Encode())
	require.Equal(t, http.StatusOK, recCluster.Code)
	clusterBody := recCluster.Body.String()
	assert.Contains(t, clusterBody, "DBClusterParameterGroupName",
		"cluster PG describe must use DBClusterParameterGroupName")
	assert.NotContains(t, clusterBody, "<DBParameterGroupName>",
		"cluster PG describe must not use instance field name")
}

// TestRDSBackend_CopyDBClusterParameterGroup tests CopyDBClusterParameterGroup.
func TestRDSBackend_CopyDBClusterParameterGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs   error
		setup       func(b *rds.InMemoryBackend)
		name        string
		source      string
		target      string
		description string
		wantErr     bool
	}{
		{
			name: "success",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBClusterParameterGroup("src-pg", "aurora-postgresql14", "Source group")
			},
			source:      "src-pg",
			target:      "dst-pg",
			description: "Copied group",
		},
		{
			name: "inherits_description",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBClusterParameterGroup("src-pg", "aurora-postgresql14", "Original desc")
			},
			source: "src-pg",
			target: "dst-pg",
		},
		{
			name:      "source_not_found",
			setup:     func(_ *rds.InMemoryBackend) {},
			source:    "no-such-pg",
			target:    "dst-pg",
			wantErr:   true,
			wantErrIs: rds.ErrParameterGroupNotFound,
		},
		{
			name: "target_already_exists",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBClusterParameterGroup("src-pg", "aurora-postgresql14", "Source")
				_, _ = b.CreateDBClusterParameterGroup("dst-pg", "aurora-postgresql14", "Dest")
			},
			source:    "src-pg",
			target:    "dst-pg",
			wantErr:   true,
			wantErrIs: rds.ErrParameterGroupAlreadyExists,
		},
		{
			name:      "empty_source",
			setup:     func(_ *rds.InMemoryBackend) {},
			source:    "",
			target:    "dst-pg",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
		{
			name:      "empty_target",
			setup:     func(_ *rds.InMemoryBackend) {},
			source:    "src-pg",
			target:    "",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			pg, err := b.CopyDBClusterParameterGroup(tt.source, tt.target, tt.description)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErrIs)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.target, pg.DBParameterGroupName)
		})
	}
}

func TestDeleteDBClusterParameterGroup(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		pgName    string
		wantErr   bool
	}{
		{name: "success", pgName: "my-cluster-pg"},
		{name: "not found", pgName: "missing", wantErr: true, wantErrIs: rds.ErrParameterGroupNotFound},
		{name: "empty name", pgName: "", wantErr: true, wantErrIs: rds.ErrInvalidParameter},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if tt.name == "success" {
				_, err := b.CreateDBClusterParameterGroup(tt.pgName, "aurora-mysql8.0", "desc")
				require.NoError(t, err)
			}
			err := b.DeleteDBClusterParameterGroup(tt.pgName)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestModifyDBClusterParameterGroup(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		pgName    string
		params    []rds.DBParameter
		wantErr   bool
	}{
		{
			name:   "success",
			pgName: "my-cluster-pg",
			params: []rds.DBParameter{{ParameterName: "max_connections", ParameterValue: "100"}},
		},
		{
			name:      "not found",
			pgName:    "missing",
			wantErr:   true,
			wantErrIs: rds.ErrParameterGroupNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateDBClusterParameterGroup(tt.pgName, "aurora-mysql8.0", "desc")
				require.NoError(t, err)
			}
			got, err := b.ModifyDBClusterParameterGroup(tt.pgName, tt.params)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.pgName, got)
		})
	}
}

func TestDescribeDBClusterParameters(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		pgName    string
		wantLen   int
		wantErr   bool
	}{
		{name: "success", pgName: "my-cluster-pg", wantLen: 1},
		{name: "not found", pgName: "missing", wantErr: true, wantErrIs: rds.ErrParameterGroupNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateDBClusterParameterGroup(tt.pgName, "aurora-mysql8.0", "desc")
				require.NoError(t, err)
				_, err = b.ModifyDBClusterParameterGroup(
					tt.pgName,
					[]rds.DBParameter{{ParameterName: "max_connections", ParameterValue: "100"}},
				)
				require.NoError(t, err)
			}
			got, err := b.DescribeDBClusterParameters(tt.pgName)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Len(t, got, tt.wantLen)
		})
	}
}

func TestResetDBClusterParameterGroup(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		pgName    string
		params    []string
		resetAll  bool
		wantErr   bool
	}{
		{name: "reset all", pgName: "my-cluster-pg", resetAll: true},
		{name: "reset named", pgName: "my-cluster-pg", params: []string{"max_connections"}},
		{name: "not found", pgName: "missing", wantErr: true, wantErrIs: rds.ErrParameterGroupNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateDBClusterParameterGroup(tt.pgName, "aurora-mysql8.0", "desc")
				require.NoError(t, err)
				_, err = b.ModifyDBClusterParameterGroup(
					tt.pgName,
					[]rds.DBParameter{{ParameterName: "max_connections", ParameterValue: "100"}},
				)
				require.NoError(t, err)
			}
			got, err := b.ResetDBClusterParameterGroup(tt.pgName, tt.resetAll, tt.params)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.pgName, got)
		})
	}
}

func TestDescribeEngineDefaultClusterParameters(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		family string
	}{
		{name: "aurora-mysql family", family: "aurora-mysql8.0"},
		{name: "empty family", family: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			got := b.DescribeEngineDefaultClusterParameters(tt.family)
			assert.NotNil(t, got)
		})
	}
}
