package rds_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

// ---- helpers ----

func newBatch2Backend() *rds.InMemoryBackend {
	return rds.NewInMemoryBackend("000000000000", "us-east-1")
}

func newBatch2Handler() *rds.Handler {
	return rds.NewHandler(newBatch2Backend())
}

// ---- DBParameterGroup edge cases ----

func TestBatch2_DBParameterGroup_ResetAll(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBParameterGroup("pg1", "postgres14", "test group")
	require.NoError(t, err)

	params := []rds.DBParameter{
		{ParameterName: "max_connections", ParameterValue: "200"},
		{ParameterName: "shared_buffers", ParameterValue: "256MB"},
	}
	_, err = b.ModifyDBParameterGroup("pg1", params)
	require.NoError(t, err)

	got, err := b.DescribeDBParameters("pg1")
	require.NoError(t, err)
	require.Len(t, got, 2)

	_, err = b.ResetDBParameterGroup("pg1", true, nil)
	require.NoError(t, err)

	gotAfter, err := b.DescribeDBParameters("pg1")
	require.NoError(t, err)
	require.Len(t, gotAfter, 2)
	for _, p := range gotAfter {
		assert.Empty(
			t,
			p.ParameterValue,
			"reset should clear parameter value for %s",
			p.ParameterName,
		)
	}
}

func TestBatch2_DBParameterGroup_ResetSpecific(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBParameterGroup("pg2", "postgres14", "test group")
	require.NoError(t, err)

	params := []rds.DBParameter{
		{ParameterName: "max_connections", ParameterValue: "200"},
		{ParameterName: "shared_buffers", ParameterValue: "256MB"},
	}
	_, err = b.ModifyDBParameterGroup("pg2", params)
	require.NoError(t, err)

	_, err = b.ResetDBParameterGroup("pg2", false, []string{"max_connections"})
	require.NoError(t, err)

	got, err := b.DescribeDBParameters("pg2")
	require.NoError(t, err)
	require.Len(t, got, 2)
	paramMap := make(map[string]string)
	for _, p := range got {
		paramMap[p.ParameterName] = p.ParameterValue
	}
	assert.Empty(t, paramMap["max_connections"], "reset should clear max_connections value")
	assert.Equal(t, "256MB", paramMap["shared_buffers"], "shared_buffers should be unchanged")
}

func TestBatch2_DBParameterGroup_NotFoundErrors(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()

	_, err := b.DescribeDBParameterGroups("noexist")
	require.ErrorIs(t, err, rds.ErrParameterGroupNotFound)

	err = b.DeleteDBParameterGroup("noexist")
	require.ErrorIs(t, err, rds.ErrParameterGroupNotFound)

	_, err = b.ModifyDBParameterGroup("noexist", nil)
	require.ErrorIs(t, err, rds.ErrParameterGroupNotFound)

	_, err = b.DescribeDBParameters("noexist")
	require.Error(t, err)
	assert.ErrorIs(t, err, rds.ErrParameterGroupNotFound)
}

func TestBatch2_DBParameterGroup_Duplicate(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBParameterGroup("dup-pg", "mysql8.0", "first")
	require.NoError(t, err)

	_, err = b.CreateDBParameterGroup("dup-pg", "mysql8.0", "second")
	require.Error(t, err)
	assert.ErrorIs(t, err, rds.ErrParameterGroupAlreadyExists)
}

func TestBatch2_DBParameterGroup_CopyPreservesParams(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBParameterGroup("src-pg", "postgres14", "src")
	require.NoError(t, err)

	params := []rds.DBParameter{
		{ParameterName: "work_mem", ParameterValue: "64MB"},
	}
	_, err = b.ModifyDBParameterGroup("src-pg", params)
	require.NoError(t, err)

	_, err = b.CopyDBParameterGroup("src-pg", "dst-pg", "copy of src")
	require.NoError(t, err)

	dstParams, err := b.DescribeDBParameters("dst-pg")
	require.NoError(t, err)
	require.Len(t, dstParams, 1)
	assert.Equal(t, "work_mem", dstParams[0].ParameterName)
	assert.Equal(t, "64MB", dstParams[0].ParameterValue)
}

func TestBatch2_DBParameterGroup_HTTP_CRUD(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()

	rec := postRDSForm(t, h, url.Values{
		"Action":                 {"CreateDBParameterGroup"},
		"Version":                {"2014-10-31"},
		"DBParameterGroupName":   {"http-pg"},
		"DBParameterGroupFamily": {"postgres14"},
		"Description":            {"http test"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "http-pg")

	rec = postRDSForm(t, h, url.Values{
		"Action":               {"DescribeDBParameterGroups"},
		"Version":              {"2014-10-31"},
		"DBParameterGroupName": {"http-pg"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "http-pg")

	rec = postRDSForm(t, h, url.Values{
		"Action":                                {"ModifyDBParameterGroup"},
		"Version":                               {"2014-10-31"},
		"DBParameterGroupName":                  {"http-pg"},
		"Parameters.Parameter.1.ParameterName":  {"max_connections"},
		"Parameters.Parameter.1.ParameterValue": {"300"},
		"Parameters.Parameter.1.ApplyMethod":    {"pending-reboot"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":               {"DescribeDBParameters"},
		"Version":              {"2014-10-31"},
		"DBParameterGroupName": {"http-pg"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "max_connections")

	rec = postRDSForm(t, h, url.Values{
		"Action":               {"ResetDBParameterGroup"},
		"Version":              {"2014-10-31"},
		"DBParameterGroupName": {"http-pg"},
		"ResetAllParameters":   {"true"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":               {"DeleteDBParameterGroup"},
		"Version":              {"2014-10-31"},
		"DBParameterGroupName": {"http-pg"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- OptionGroup edge cases ----

func TestBatch2_OptionGroup_ModifyAddRemove(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateOptionGroup("og1", "mysql", "8.0", "test og")
	require.NoError(t, err)

	toAdd := []rds.OptionGroupOption{
		{OptionName: "MEMCACHED"},
		{OptionName: "MARIADB_AUDIT_PLUGIN"},
	}
	og, err := b.ModifyOptionGroup("og1", toAdd, nil)
	require.NoError(t, err)
	assert.Len(t, og.Options, 2)

	og, err = b.ModifyOptionGroup("og1", nil, []string{"MEMCACHED"})
	require.NoError(t, err)
	assert.Len(t, og.Options, 1)
	assert.Equal(t, "MARIADB_AUDIT_PLUGIN", og.Options[0].OptionName)
}

func TestBatch2_OptionGroup_Duplicate(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateOptionGroup("dup-og", "mysql", "8.0", "first")
	require.NoError(t, err)

	_, err = b.CreateOptionGroup("dup-og", "mysql", "8.0", "second")
	require.Error(t, err)
	assert.ErrorIs(t, err, rds.ErrOptionGroupAlreadyExists)
}

func TestBatch2_OptionGroup_DeleteNotFound(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()

	err := b.DeleteOptionGroup("noexist")
	require.Error(t, err)
	assert.ErrorIs(t, err, rds.ErrOptionGroupNotFound)
}

func TestBatch2_OptionGroup_CopyPreservesOptions(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateOptionGroup("src-og", "mysql", "8.0", "source")
	require.NoError(t, err)

	_, err = b.ModifyOptionGroup("src-og", []rds.OptionGroupOption{{OptionName: "SSL"}}, nil)
	require.NoError(t, err)

	_, err = b.CopyOptionGroup("src-og", "dst-og", "destination")
	require.NoError(t, err)

	groups, err := b.DescribeOptionGroups("dst-og")
	require.NoError(t, err)
	require.Len(t, groups, 1)
	assert.Len(t, groups[0].Options, 1)
	assert.Equal(t, "SSL", groups[0].Options[0].OptionName)
}

func TestBatch2_OptionGroup_Concurrent(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateOptionGroup("conc-og", "mysql", "8.0", "concurrent test")
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_, _ = b.ModifyOptionGroup("conc-og", []rds.OptionGroupOption{
				{OptionName: fmt.Sprintf("OPT%d", n)},
			}, nil)
		}(i)
	}
	wg.Wait()

	groups, err := b.DescribeOptionGroups("conc-og")
	require.NoError(t, err)
	assert.NotEmpty(t, groups[0].Options)
}

func TestBatch2_OptionGroup_HTTP_CRUD(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()

	rec := postRDSForm(t, h, url.Values{
		"Action":                 {"CreateOptionGroup"},
		"Version":                {"2014-10-31"},
		"OptionGroupName":        {"http-og"},
		"EngineName":             {"mysql"},
		"MajorEngineVersion":     {"8.0"},
		"OptionGroupDescription": {"http test og"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":          {"ModifyOptionGroup"},
		"Version":         {"2014-10-31"},
		"OptionGroupName": {"http-og"},
		"OptionsToInclude.OptionConfiguration.1.OptionName": {"SSL"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":          {"DescribeOptionGroups"},
		"Version":         {"2014-10-31"},
		"OptionGroupName": {"http-og"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "http-og")

	rec = postRDSForm(t, h, url.Values{
		"Action":          {"DeleteOptionGroup"},
		"Version":         {"2014-10-31"},
		"OptionGroupName": {"http-og"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- DBClusterParameterGroup edge cases ----

func TestBatch2_ClusterPG_CRUD(t *testing.T) {
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

func TestBatch2_ClusterPG_ModifyAndDescribe(t *testing.T) {
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

func TestBatch2_ClusterPG_Reset(t *testing.T) {
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

func TestBatch2_ClusterPG_Copy(t *testing.T) {
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

func TestBatch2_ClusterPG_HTTP(t *testing.T) {
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

// ---- DBSubnetGroup edge cases ----

func TestBatch2_SubnetGroup_Modify(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBSubnetGroup("sg1", "original", "vpc-1", []string{"subnet-a", "subnet-b"})
	require.NoError(t, err)

	updated, err := b.ModifyDBSubnetGroup(
		"sg1",
		"updated description",
		[]string{"subnet-c", "subnet-d"},
	)
	require.NoError(t, err)
	assert.Equal(t, "updated description", updated.DBSubnetGroupDescription)
	assert.Len(t, updated.SubnetIDs, 2)
	assert.Equal(t, "subnet-c", updated.SubnetIDs[0])
}

func TestBatch2_SubnetGroup_NotFound(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()

	err := b.DeleteDBSubnetGroup("noexist")
	require.ErrorIs(t, err, rds.ErrSubnetGroupNotFound)

	_, err = b.DescribeDBSubnetGroups("noexist")
	require.ErrorIs(t, err, rds.ErrSubnetGroupNotFound)
}

func TestBatch2_SubnetGroup_Duplicate(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBSubnetGroup("dup-sg", "first", "vpc-1", []string{"subnet-a"})
	require.NoError(t, err)

	_, err = b.CreateDBSubnetGroup("dup-sg", "second", "vpc-1", []string{"subnet-b"})
	require.Error(t, err)
	assert.ErrorIs(t, err, rds.ErrSubnetGroupAlreadyExists)
}

func TestBatch2_SubnetGroup_HTTP_Modify(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()

	rec := postRDSForm(t, h, url.Values{
		"Action":                       {"CreateDBSubnetGroup"},
		"Version":                      {"2014-10-31"},
		"DBSubnetGroupName":            {"http-sg"},
		"DBSubnetGroupDescription":     {"original"},
		"SubnetIds.SubnetIdentifier.1": {"subnet-a"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":                       {"ModifyDBSubnetGroup"},
		"Version":                      {"2014-10-31"},
		"DBSubnetGroupName":            {"http-sg"},
		"DBSubnetGroupDescription":     {"modified"},
		"SubnetIds.SubnetIdentifier.1": {"subnet-b"},
		"SubnetIds.SubnetIdentifier.2": {"subnet-c"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "http-sg")
}

// ---- DBSecurityGroup (legacy) edge cases ----

func TestBatch2_DBSecurityGroup_CRUD(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()

	sg, err := b.CreateDBSecurityGroup("my-sg", "legacy security group")
	require.NoError(t, err)
	assert.Equal(t, "my-sg", sg.DBSecurityGroupName)

	groups, err := b.DescribeDBSecurityGroups("my-sg")
	require.NoError(t, err)
	require.Len(t, groups, 1)

	err = b.DeleteDBSecurityGroup("my-sg")
	require.NoError(t, err)

	_, err = b.DescribeDBSecurityGroups("my-sg")
	require.Error(t, err)
	assert.ErrorIs(t, err, rds.ErrDBSecurityGroupNotFound)
}

func TestBatch2_DBSecurityGroup_AuthorizeRevoke(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBSecurityGroup("auth-sg", "test")
	require.NoError(t, err)

	sg, err := b.AuthorizeDBSecurityGroupIngress("auth-sg", "10.0.0.0/8")
	require.NoError(t, err)
	require.Len(t, sg.IPRanges, 1)
	assert.Equal(t, "10.0.0.0/8", sg.IPRanges[0].CIDRIP)

	sg, err = b.RevokeDBSecurityGroupIngress("auth-sg", "10.0.0.0/8")
	require.NoError(t, err)
	assert.Empty(t, sg.IPRanges)
}

func TestBatch2_DBSecurityGroup_Duplicate(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBSecurityGroup("dup-sg", "first")
	require.NoError(t, err)

	_, err = b.CreateDBSecurityGroup("dup-sg", "second")
	require.Error(t, err)
	assert.ErrorIs(t, err, rds.ErrDBSecurityGroupAlreadyExists)
}

func TestBatch2_DBSecurityGroup_NotFound(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()

	// AuthorizeDBSecurityGroupIngress auto-creates the group (EC2-Classic behaviour)
	sg, err := b.AuthorizeDBSecurityGroupIngress("auto-created-sg", "0.0.0.0/0")
	require.NoError(t, err)
	assert.Equal(t, "auto-created-sg", sg.DBSecurityGroupName)

	err = b.DeleteDBSecurityGroup("noexist")
	require.Error(t, err)
	assert.ErrorIs(t, err, rds.ErrDBSecurityGroupNotFound)
}

func TestBatch2_DBSecurityGroup_HTTP(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()

	rec := postRDSForm(t, h, url.Values{
		"Action":                     {"CreateDBSecurityGroup"},
		"Version":                    {"2014-10-31"},
		"DBSecurityGroupName":        {"http-legacy-sg"},
		"DBSecurityGroupDescription": {"legacy sg for test"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":              {"DescribeDBSecurityGroups"},
		"Version":             {"2014-10-31"},
		"DBSecurityGroupName": {"http-legacy-sg"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "http-legacy-sg")

	rec = postRDSForm(t, h, url.Values{
		"Action":              {"AuthorizeDBSecurityGroupIngress"},
		"Version":             {"2014-10-31"},
		"DBSecurityGroupName": {"http-legacy-sg"},
		"CIDRIP":              {"192.168.0.0/16"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":              {"RevokeDBSecurityGroupIngress"},
		"Version":             {"2014-10-31"},
		"DBSecurityGroupName": {"http-legacy-sg"},
		"CIDRIP":              {"192.168.0.0/16"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":              {"DeleteDBSecurityGroup"},
		"Version":             {"2014-10-31"},
		"DBSecurityGroupName": {"http-legacy-sg"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- DBClusterSnapshot edge cases ----

func TestBatch2_ClusterSnapshot_CRUD(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()

	_, err := b.CreateDBCluster(
		"cluster-a",
		"aurora-postgresql",
		"admin",
		"",
		"",
		5432,
		nil,
		rds.DBClusterOptions{},
	)
	require.NoError(t, err)

	snap, err := b.CreateDBClusterSnapshot("snap-a", "cluster-a")
	require.NoError(t, err)
	assert.Equal(t, "snap-a", snap.DBClusterSnapshotIdentifier)
	assert.Equal(t, "aurora-postgresql", snap.Engine)

	snaps, err := b.DescribeDBClusterSnapshots("snap-a")
	require.NoError(t, err)
	require.Len(t, snaps, 1)

	_, err = b.DeleteDBClusterSnapshot("snap-a")
	require.NoError(t, err)

	_, err = b.DescribeDBClusterSnapshots("snap-a")
	require.Error(t, err)
	assert.ErrorIs(t, err, rds.ErrClusterSnapshotNotFound)
}

func TestBatch2_ClusterSnapshot_Copy(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBCluster(
		"cluster-b",
		"aurora-mysql",
		"admin",
		"",
		"",
		3306,
		nil,
		rds.DBClusterOptions{},
	)
	require.NoError(t, err)

	_, err = b.CreateDBClusterSnapshot("src-snap", "cluster-b")
	require.NoError(t, err)

	dst, err := b.CopyDBClusterSnapshot("src-snap", "dst-snap")
	require.NoError(t, err)
	assert.Equal(t, "dst-snap", dst.DBClusterSnapshotIdentifier)

	snaps, err := b.DescribeDBClusterSnapshots("")
	require.NoError(t, err)
	assert.Len(t, snaps, 2)
}

func TestBatch2_ClusterSnapshot_Duplicate(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBCluster(
		"cluster-c",
		"aurora-postgresql",
		"admin",
		"",
		"",
		5432,
		nil,
		rds.DBClusterOptions{},
	)
	require.NoError(t, err)

	_, err = b.CreateDBClusterSnapshot("dup-snap", "cluster-c")
	require.NoError(t, err)

	_, err = b.CreateDBClusterSnapshot("dup-snap", "cluster-c")
	require.Error(t, err)
	assert.ErrorIs(t, err, rds.ErrClusterSnapshotAlreadyExists)
}

func TestBatch2_ClusterSnapshot_DeleteNotFound(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.DeleteDBClusterSnapshot("noexist")
	require.Error(t, err)
	assert.ErrorIs(t, err, rds.ErrClusterSnapshotNotFound)
}

func TestBatch2_ClusterSnapshot_HTTP(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()

	rec := postRDSForm(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"cluster-snap-test"},
		"Engine":              {"aurora-postgresql"},
		"MasterUsername":      {"admin"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":                      {"CreateDBClusterSnapshot"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"http-cluster-snap"},
		"DBClusterIdentifier":         {"cluster-snap-test"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "http-cluster-snap")

	rec = postRDSForm(t, h, url.Values{
		"Action":                      {"DescribeDBClusterSnapshots"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"http-cluster-snap"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":                            {"CopyDBClusterSnapshot"},
		"Version":                           {"2014-10-31"},
		"SourceDBClusterSnapshotIdentifier": {"http-cluster-snap"},
		"TargetDBClusterSnapshotIdentifier": {"http-cluster-snap-copy"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":                      {"DeleteDBClusterSnapshot"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"http-cluster-snap"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- EventSubscription edge cases ----

func TestBatch2_EventSubscription_CRUD(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()

	sub, err := b.CreateEventSubscription(
		"sub1",
		"arn:aws:sns:us-east-1:123:topic",
		"db-instance",
		nil,
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, "sub1", sub.SubscriptionName)
	assert.True(t, sub.Enabled)

	subs, err := b.DescribeEventSubscriptions("sub1")
	require.NoError(t, err)
	require.Len(t, subs, 1)

	_, err = b.DeleteEventSubscription("sub1")
	require.NoError(t, err)

	_, err = b.DescribeEventSubscriptions("sub1")
	require.Error(t, err)
	assert.ErrorIs(t, err, rds.ErrEventSubscriptionNotFound)
}

func TestBatch2_EventSubscription_ModifyToggle(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateEventSubscription(
		"sub2",
		"arn:aws:sns:us-east-1:123:topic",
		"db-instance",
		nil,
		nil,
	)
	require.NoError(t, err)

	enabled := false
	_, err = b.ModifyEventSubscription("sub2", "", "db-cluster", nil, nil, &enabled)
	require.NoError(t, err)

	subs, err := b.DescribeEventSubscriptions("sub2")
	require.NoError(t, err)
	assert.False(t, subs[0].Enabled)
}

func TestBatch2_EventSubscription_AddRemoveSource(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateEventSubscription(
		"sub3",
		"arn:aws:sns:us-east-1:123:topic",
		"db-instance",
		nil,
		nil,
	)
	require.NoError(t, err)

	_, err = b.AddSourceIdentifierToSubscription("sub3", "db-instance-1")
	require.NoError(t, err)

	subs, err := b.DescribeEventSubscriptions("sub3")
	require.NoError(t, err)
	assert.Contains(t, subs[0].SourceIDs, "db-instance-1")

	_, err = b.RemoveSourceIdentifierFromSubscription("sub3", "db-instance-1")
	require.NoError(t, err)

	subs, err = b.DescribeEventSubscriptions("sub3")
	require.NoError(t, err)
	assert.NotContains(t, subs[0].SourceIDs, "db-instance-1")
}

func TestBatch2_EventSubscription_Duplicate(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateEventSubscription("dup-sub", "arn:aws:sns:us-east-1:123:topic", "", nil, nil)
	require.NoError(t, err)

	_, err = b.CreateEventSubscription("dup-sub", "arn:aws:sns:us-east-1:123:topic", "", nil, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, rds.ErrEventSubscriptionAlreadyExists)
}

func TestBatch2_EventSubscription_HTTP(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()

	rec := postRDSForm(t, h, url.Values{
		"Action":           {"CreateEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"http-sub"},
		"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:rds-events"},
		"Enabled":          {"true"},
		"SourceType":       {"db-instance"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":           {"ModifyEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"http-sub"},
		"Enabled":          {"false"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":           {"DescribeEventSubscriptions"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"http-sub"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":           {"DeleteEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"http-sub"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- BlueGreen Deployment edge cases ----

func TestBatch2_BlueGreen_CreateAndDescribe(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()

	bg, err := b.CreateBlueGreenDeployment("bg1", "arn:aws:rds:us-east-1:123:db:source-db")
	require.NoError(t, err)
	assert.Equal(t, "bg1", bg.BlueGreenDeploymentName)
	assert.NotEmpty(t, bg.BlueGreenDeploymentIdentifier)

	bgs, err := b.DescribeBlueGreenDeployments("")
	require.NoError(t, err)
	require.Len(t, bgs, 1)
}

func TestBatch2_BlueGreen_Switchover(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	bg, err := b.CreateBlueGreenDeployment("bg2", "arn:aws:rds:us-east-1:123:db:source-db")
	require.NoError(t, err)

	switched, err := b.SwitchoverBlueGreenDeployment(bg.BlueGreenDeploymentIdentifier)
	require.NoError(t, err)
	assert.NotEmpty(t, switched.BlueGreenDeploymentIdentifier)
}

func TestBatch2_BlueGreen_DeleteNotFound(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.DeleteBlueGreenDeployment("nonexistent-id")
	require.Error(t, err)
	assert.ErrorIs(t, err, rds.ErrBlueGreenDeploymentNotFound)
}

func TestBatch2_BlueGreen_HTTP(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()

	rec := postRDSForm(t, h, url.Values{
		"Action":                  {"CreateBlueGreenDeployment"},
		"Version":                 {"2014-10-31"},
		"BlueGreenDeploymentName": {"http-bg"},
		"Source":                  {"arn:aws:rds:us-east-1:000:db:src"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":  {"DescribeBlueGreenDeployments"},
		"Version": {"2014-10-31"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "http-bg")
}

// ---- ProxyTarget + TargetGroup edge cases ----

func TestBatch2_ProxyTargetGroup_DefaultCreated(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBProxy("proxy1", "POSTGRESQL", "arn:aws:iam::123:role/proxy-role", nil)
	require.NoError(t, err)

	groups, err := b.DescribeDBProxyTargetGroups("proxy1", "")
	require.NoError(t, err)
	require.NotEmpty(t, groups)
	assert.Equal(t, "default", groups[0].TargetGroupName)
}

func TestBatch2_ProxyTargetGroup_Modify(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBProxy("proxy2", "MYSQL", "arn:aws:iam::123:role/proxy-role", nil)
	require.NoError(t, err)

	tg, err := b.ModifyDBProxyTargetGroup("proxy2", "default", rds.ConnectionPoolConfig{
		MaxConnectionsPercent: 90,
	})
	require.NoError(t, err)
	assert.Equal(t, 90, tg.ConnectionPoolConfig.MaxConnectionsPercent)
}

func TestBatch2_ProxyTargets_RegisterByInstance(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBProxy("proxy3", "POSTGRESQL", "arn:aws:iam::123:role/proxy-role", nil)
	require.NoError(t, err)

	_, err = b.CreateDBInstance(
		"target-db",
		"postgres",
		"db.t3.micro",
		"",
		"admin",
		"",
		20,
		rds.DBInstanceOptions{},
	)
	require.NoError(t, err)

	targets, err := b.RegisterDBProxyTargets("proxy3", "default", []string{"target-db"}, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, targets)

	got, err := b.DescribeDBProxyTargets("proxy3", "")
	require.NoError(t, err)
	assert.NotEmpty(t, got)
}

func TestBatch2_ProxyTargets_Deregister(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBProxy("proxy4", "MYSQL", "arn:aws:iam::123:role/proxy-role", nil)
	require.NoError(t, err)

	_, err = b.RegisterDBProxyTargets("proxy4", "default", []string{"inst-1"}, nil)
	require.NoError(t, err)

	err = b.DeregisterDBProxyTargets("proxy4", "default", []string{"inst-1"}, nil)
	require.NoError(t, err)

	targets, err := b.DescribeDBProxyTargets("proxy4", "")
	require.NoError(t, err)
	assert.Empty(t, targets)
}

func TestBatch2_ProxyTargets_HTTP(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()

	rec := postRDSForm(t, h, url.Values{
		"Action":                   {"CreateDBProxy"},
		"Version":                  {"2014-10-31"},
		"DBProxyName":              {"http-proxy"},
		"EngineFamily":             {"POSTGRESQL"},
		"RoleArn":                  {"arn:aws:iam::000:role/rds-proxy"},
		"Auth.member.1.AuthScheme": {"SECRETS"},
		"Auth.member.1.IAMAuth":    {"DISABLED"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":      {"DescribeDBProxyTargetGroups"},
		"Version":     {"2014-10-31"},
		"DBProxyName": {"http-proxy"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":                         {"RegisterDBProxyTargets"},
		"Version":                        {"2014-10-31"},
		"DBProxyName":                    {"http-proxy"},
		"TargetGroupName":                {"default"},
		"DBInstanceIdentifiers.member.1": {"some-db"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":          {"DescribeDBProxyTargets"},
		"Version":         {"2014-10-31"},
		"DBProxyName":     {"http-proxy"},
		"TargetGroupName": {"default"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":                         {"DeregisterDBProxyTargets"},
		"Version":                        {"2014-10-31"},
		"DBProxyName":                    {"http-proxy"},
		"TargetGroupName":                {"default"},
		"DBInstanceIdentifiers.member.1": {"some-db"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- GlobalCluster promote/failover edge cases ----

func TestBatch2_GlobalCluster_Failover(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateGlobalCluster("gc1", "aurora-postgresql", "14.9", false, false)
	require.NoError(t, err)

	_, err = b.CreateDBCluster(
		"member-cluster",
		"aurora-postgresql",
		"admin",
		"",
		"",
		5432,
		nil,
		rds.DBClusterOptions{},
	)
	require.NoError(t, err)

	result, err := b.FailoverGlobalCluster("gc1", "member-cluster")
	require.NoError(t, err)
	assert.Equal(t, "gc1", result.GlobalClusterIdentifier)
}

func TestBatch2_GlobalCluster_Switchover(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateGlobalCluster("gc2", "aurora-mysql", "3.04.0", false, false)
	require.NoError(t, err)

	_, err = b.CreateDBCluster(
		"sw-cluster",
		"aurora-mysql",
		"admin",
		"",
		"",
		3306,
		nil,
		rds.DBClusterOptions{},
	)
	require.NoError(t, err)

	result, err := b.SwitchoverGlobalCluster("gc2", "sw-cluster")
	require.NoError(t, err)
	assert.Equal(t, "gc2", result.GlobalClusterIdentifier)
}

func TestBatch2_GlobalCluster_RemoveMember(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateGlobalCluster("gc3", "aurora-postgresql", "14.9", false, false)
	require.NoError(t, err)

	clusterARN := "arn:aws:rds:us-east-1:000000000000:cluster:member-to-remove"
	result, err := b.RemoveFromGlobalCluster("gc3", clusterARN)
	require.NoError(t, err)
	assert.Equal(t, "gc3", result.GlobalClusterIdentifier)
}

func TestBatch2_GlobalCluster_NotFound(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()

	_, err := b.FailoverGlobalCluster("noexist", "target")
	require.ErrorIs(t, err, rds.ErrGlobalClusterNotFound)

	_, err = b.DeleteGlobalCluster("noexist")
	require.ErrorIs(t, err, rds.ErrGlobalClusterNotFound)
}

func TestBatch2_GlobalCluster_HTTP(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()

	rec := postRDSForm(t, h, url.Values{
		"Action":                  {"CreateGlobalCluster"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"http-gc"},
		"Engine":                  {"aurora-postgresql"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":                  {"DescribeGlobalClusters"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"http-gc"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "http-gc")

	rec = postRDSForm(t, h, url.Values{
		"Action":                    {"FailoverGlobalCluster"},
		"Version":                   {"2014-10-31"},
		"GlobalClusterIdentifier":   {"http-gc"},
		"TargetDbClusterIdentifier": {"target-cluster"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":                  {"DeleteGlobalCluster"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"http-gc"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- CustomDBEngineVersion edge cases ----

func TestBatch2_CustomDBEV_CRUD(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()

	cev, err := b.CreateCustomDBEngineVersion("custom-oracle-ee", "19.0.0.0", "Oracle 19c")
	require.NoError(t, err)
	assert.Equal(t, "custom-oracle-ee", cev.Engine)
	assert.Equal(t, "available", cev.Status)

	_, err = b.ModifyCustomDBEngineVersion("custom-oracle-ee", "19.0.0.0", "Oracle 19c updated", "")
	require.NoError(t, err)

	_, err = b.DeleteCustomDBEngineVersion("custom-oracle-ee", "19.0.0.0")
	require.NoError(t, err)

	_, err = b.DeleteCustomDBEngineVersion("custom-oracle-ee", "19.0.0.0")
	require.Error(t, err)
}

func TestBatch2_CustomDBEV_ModifyStatus(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateCustomDBEngineVersion("custom-oracle-ee-cdb", "19.0.1.0", "Oracle 19c CDB")
	require.NoError(t, err)

	updated, err := b.ModifyCustomDBEngineVersion(
		"custom-oracle-ee-cdb",
		"19.0.1.0",
		"",
		"inactive-except-restore",
	)
	require.NoError(t, err)
	assert.Equal(t, "inactive-except-restore", updated.Status)
}

func TestBatch2_CustomDBEV_Duplicate(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateCustomDBEngineVersion("custom-oracle-ee-se2", "19.0.2.0", "test")
	require.NoError(t, err)

	_, err = b.CreateCustomDBEngineVersion("custom-oracle-ee-se2", "19.0.2.0", "duplicate")
	require.Error(t, err)
}

func TestBatch2_CustomDBEV_Concurrent(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	n := 20
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = b.CreateCustomDBEngineVersion(
				"custom-oracle-ee",
				fmt.Sprintf("19.0.%d.0", idx),
				fmt.Sprintf("Oracle 19.0.%d", idx),
			)
		}(i)
	}
	wg.Wait()

	successCount := 0
	for _, e := range errs {
		if e == nil {
			successCount++
		}
	}
	assert.Equal(t, n, successCount)
}

func TestBatch2_CustomDBEV_HTTP(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()

	rec := postRDSForm(t, h, url.Values{
		"Action":        {"CreateCustomDBEngineVersion"},
		"Version":       {"2014-10-31"},
		"Engine":        {"custom-oracle-ee"},
		"EngineVersion": {"19.0.0.0"},
		"Description":   {"Oracle EE 19c custom"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":        {"ModifyCustomDBEngineVersion"},
		"Version":       {"2014-10-31"},
		"Engine":        {"custom-oracle-ee"},
		"EngineVersion": {"19.0.0.0"},
		"Status":        {"inactive"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":        {"DeleteCustomDBEngineVersion"},
		"Version":       {"2014-10-31"},
		"Engine":        {"custom-oracle-ee"},
		"EngineVersion": {"19.0.0.0"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- DBCluster modify+failover edge cases ----

func TestBatch2_DBCluster_ModifyFields(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBCluster(
		"mod-cluster",
		"aurora-postgresql",
		"admin",
		"",
		"",
		5432,
		nil,
		rds.DBClusterOptions{},
	)
	require.NoError(t, err)

	updated, err := b.ModifyDBCluster("mod-cluster", "", rds.DBClusterOptions{
		DeletionProtection:    true,
		DeletionProtectionSet: true,
	})
	require.NoError(t, err)
	assert.True(t, updated.DeletionProtection)

	updated, err = b.ModifyDBCluster("mod-cluster", "", rds.DBClusterOptions{
		DeletionProtection:    false,
		DeletionProtectionSet: true,
	})
	require.NoError(t, err)
	assert.False(t, updated.DeletionProtection)
}

func TestBatch2_DBCluster_FailoverUpdatesState(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBCluster(
		"failover-cluster",
		"aurora-mysql",
		"admin",
		"",
		"",
		3306,
		nil,
		rds.DBClusterOptions{},
	)
	require.NoError(t, err)

	result, err := b.FailoverDBCluster("failover-cluster", "")
	require.NoError(t, err)
	assert.Equal(t, "failover-cluster", result.DBClusterIdentifier)
}

func TestBatch2_DBCluster_RebootTransitions(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBCluster(
		"reboot-cluster",
		"aurora-postgresql",
		"admin",
		"",
		"",
		5432,
		nil,
		rds.DBClusterOptions{},
	)
	require.NoError(t, err)

	result, err := b.RebootDBCluster("reboot-cluster")
	require.NoError(t, err)
	assert.Equal(t, "reboot-cluster", result.DBClusterIdentifier)
}

func TestBatch2_DBCluster_HTTP_Modify(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()

	rec := postRDSForm(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"http-mod-cluster"},
		"Engine":              {"aurora-postgresql"},
		"MasterUsername":      {"admin"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":              {"ModifyDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"http-mod-cluster"},
		"DeletionProtection":  {"true"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":              {"FailoverDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"http-mod-cluster"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- DBProxyEndpoint edge cases ----

func TestBatch2_ProxyEndpoint_CRUD(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBProxy("ep-proxy", "POSTGRESQL", "arn:aws:iam::123:role/r", nil)
	require.NoError(t, err)

	ep, err := b.CreateDBProxyEndpoint(
		"ep-proxy",
		"my-endpoint",
		"READ_ONLY",
		[]string{"subnet-a"},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, "my-endpoint", ep.DBProxyEndpointName)

	eps, err := b.DescribeDBProxyEndpoints("ep-proxy", "")
	require.NoError(t, err)
	require.NotEmpty(t, eps)

	_, err = b.DeleteDBProxyEndpoint("my-endpoint")
	require.NoError(t, err)

	eps, err = b.DescribeDBProxyEndpoints("ep-proxy", "")
	require.NoError(t, err)
	assert.Empty(t, eps)
}

func TestBatch2_ProxyEndpoint_Modify(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBProxy("ep-proxy2", "MYSQL", "arn:aws:iam::123:role/r", nil)
	require.NoError(t, err)

	_, err = b.CreateDBProxyEndpoint(
		"ep-proxy2",
		"mod-endpoint",
		"READ_ONLY",
		[]string{"subnet-a"},
		[]string{"sg-1"},
	)
	require.NoError(t, err)

	ep, err := b.ModifyDBProxyEndpoint("mod-endpoint", []string{"sg-2", "sg-3"})
	require.NoError(t, err)
	assert.Equal(t, "mod-endpoint", ep.DBProxyEndpointName)
}

func TestBatch2_ProxyEndpoint_ListFiltered(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBProxy("ep-proxy3", "POSTGRESQL", "arn:aws:iam::123:role/r", nil)
	require.NoError(t, err)

	_, err = b.CreateDBProxyEndpoint("ep-proxy3", "ep-a", "READ_ONLY", nil, nil)
	require.NoError(t, err)
	_, err = b.CreateDBProxyEndpoint("ep-proxy3", "ep-b", "READ_WRITE", nil, nil)
	require.NoError(t, err)

	all, err := b.DescribeDBProxyEndpoints("ep-proxy3", "")
	require.NoError(t, err)
	assert.Len(t, all, 2)

	specific, err := b.DescribeDBProxyEndpoints("ep-proxy3", "ep-a")
	require.NoError(t, err)
	assert.Len(t, specific, 1)
	assert.Equal(t, "ep-a", specific[0].DBProxyEndpointName)
}

func TestBatch2_ProxyEndpoint_HTTP(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()

	rec := postRDSForm(t, h, url.Values{
		"Action":                   {"CreateDBProxy"},
		"Version":                  {"2014-10-31"},
		"DBProxyName":              {"endpoint-proxy"},
		"EngineFamily":             {"POSTGRESQL"},
		"RoleArn":                  {"arn:aws:iam::000:role/rds-proxy"},
		"Auth.member.1.AuthScheme": {"SECRETS"},
		"Auth.member.1.IAMAuth":    {"DISABLED"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":                {"CreateDBProxyEndpoint"},
		"Version":               {"2014-10-31"},
		"DBProxyName":           {"endpoint-proxy"},
		"DBProxyEndpointName":   {"http-endpoint"},
		"TargetRole":            {"READ_ONLY"},
		"VpcSubnetIds.member.1": {"subnet-x"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":      {"DescribeDBProxyEndpoints"},
		"Version":     {"2014-10-31"},
		"DBProxyName": {"endpoint-proxy"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":                       {"ModifyDBProxyEndpoint"},
		"Version":                      {"2014-10-31"},
		"DBProxyEndpointName":          {"http-endpoint"},
		"VpcSecurityGroupIds.member.1": {"sg-new"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":              {"DeleteDBProxyEndpoint"},
		"Version":             {"2014-10-31"},
		"DBProxyEndpointName": {"http-endpoint"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- DBInstanceAutomatedBackup edge cases ----

func TestBatch2_InstanceBackup_CreatedWithRetention(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBInstance(
		"backup-db",
		"postgres",
		"db.t3.micro",
		"",
		"admin",
		"",
		20,
		rds.DBInstanceOptions{
			BackupRetentionPeriod: 7,
		},
	)
	require.NoError(t, err)

	backups := b.DescribeDBInstanceAutomatedBackups("backup-db")
	require.NotEmpty(t, backups)
	assert.Equal(t, "backup-db", backups[0].DBInstanceIdentifier)
}

func TestBatch2_InstanceBackup_NoRetentionNoBackup(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBInstance(
		"nobackup-db",
		"postgres",
		"db.t3.micro",
		"",
		"admin",
		"",
		20,
		rds.DBInstanceOptions{
			BackupRetentionPeriod: 0,
		},
	)
	require.NoError(t, err)

	backups := b.DescribeDBInstanceAutomatedBackups("nobackup-db")
	assert.Empty(t, backups)
}

func TestBatch2_InstanceBackup_Delete(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBInstance(
		"del-backup-db",
		"postgres",
		"db.t3.micro",
		"",
		"admin",
		"",
		20,
		rds.DBInstanceOptions{
			BackupRetentionPeriod: 3,
		},
	)
	require.NoError(t, err)

	backups := b.DescribeDBInstanceAutomatedBackups("del-backup-db")
	require.NotEmpty(t, backups)

	_, err = b.DeleteDBInstanceAutomatedBackup(backups[0].DbiResourceID)
	require.NoError(t, err)

	backupsAfter := b.DescribeDBInstanceAutomatedBackups("del-backup-db")
	assert.Empty(t, backupsAfter)
}

func TestBatch2_InstanceBackup_Replication(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	sourceARN := "arn:aws:rds:us-west-2:123456789012:db:source-db"

	backup, err := b.StartDBInstanceAutomatedBackupsReplication(sourceARN, 7)
	require.NoError(t, err)
	assert.Equal(t, sourceARN, backup.DBInstanceArn)
	assert.Equal(t, 7, backup.BackupRetentionPeriod)

	stopped, err := b.StopDBInstanceAutomatedBackupsReplication(sourceARN)
	require.NoError(t, err)
	assert.Equal(t, sourceARN, stopped.DBInstanceArn)
}

func TestBatch2_InstanceBackup_HTTP(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()

	rec := postRDSForm(t, h, url.Values{
		"Action":  {"DescribeDBInstanceAutomatedBackups"},
		"Version": {"2014-10-31"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":                {"StartDBInstanceAutomatedBackupsReplication"},
		"Version":               {"2014-10-31"},
		"SourceDBInstanceArn":   {"arn:aws:rds:us-west-2:123456789012:db:source-db"},
		"BackupRetentionPeriod": {"14"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":              {"StopDBInstanceAutomatedBackupsReplication"},
		"Version":             {"2014-10-31"},
		"SourceDBInstanceArn": {"arn:aws:rds:us-west-2:123456789012:db:source-db"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- OrderableDBInstanceOptions edge cases ----

func TestBatch2_OrderableOptions_AllEngines(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	opts := b.DescribeOrderableDBInstanceOptions("", "")
	assert.NotEmpty(t, opts)
}

func TestBatch2_OrderableOptions_FilterByEngine(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	opts := b.DescribeOrderableDBInstanceOptions("postgres", "")
	require.NotEmpty(t, opts)
	for _, o := range opts {
		assert.Equal(t, "postgres", o.Engine)
	}
}

func TestBatch2_OrderableOptions_ContainsExpectedClasses(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	opts := b.DescribeOrderableDBInstanceOptions("mysql", "")

	classSet := make(map[string]bool)
	for _, o := range opts {
		classSet[o.DBInstanceClass] = true
	}
	assert.True(t, classSet["db.t3.micro"], "expected db.t3.micro in orderable classes")
	assert.True(t, classSet["db.r5.large"], "expected db.r5.large in orderable classes")
}

func TestBatch2_OrderableOptions_HTTP(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()

	rec := postRDSForm(t, h, url.Values{
		"Action":  {"DescribeOrderableDBInstanceOptions"},
		"Version": {"2014-10-31"},
		"Engine":  {"postgres"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "postgres")
}

// ---- Persistence round-trip for batch-1 types ----

func TestBatch2_Persistence_CustomEngineVersions(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()

	_, err := b.CreateCustomDBEngineVersion("custom-oracle-ee", "19.0.0.0", "Oracle 19c")
	require.NoError(t, err)
	_, err = b.CreateCustomDBEngineVersion("custom-oracle-ee", "21.0.0.0", "Oracle 21c")
	require.NoError(t, err)

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := rds.NewInMemoryBackend("000000000000", "us-east-1")
	err = b2.Restore(snap)
	require.NoError(t, err)

	// Verify: creating the same engine again should fail (already exists)
	_, err = b2.CreateCustomDBEngineVersion("custom-oracle-ee", "19.0.0.0", "Oracle 19c again")
	require.Error(t, err, "duplicate should fail after restore")

	// New version should succeed
	_, err = b2.CreateCustomDBEngineVersion("custom-oracle-ee", "23.0.0.0", "Oracle 23c")
	require.NoError(t, err)
}

func TestBatch2_Persistence_ShardGroupsAndIntegrations(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()

	_, err := b.CreateDBShardGroup("shard-1", "cluster-1", 64.0, 2.0, 1, false)
	require.NoError(t, err)
	_, err = b.CreateDBShardGroup("shard-2", "cluster-1", 128.0, 4.0, 0, true)
	require.NoError(t, err)

	_, err = b.CreateIntegration(
		"intg-1",
		"arn:aws:rds:us-east-1:000:db:src",
		"arn:aws:redshift:us-east-1:000:ns:dst",
		"",
		"",
		"",
	)
	require.NoError(t, err)

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := rds.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(snap))

	shards, err := b2.DescribeDBShardGroups("")
	require.NoError(t, err)
	assert.Len(t, shards, 2)

	integrations, err := b2.DescribeIntegrations("")
	require.NoError(t, err)
	assert.Len(t, integrations, 1)
	assert.Equal(t, "intg-1", integrations[0].IntegrationName)
}

func TestBatch2_Persistence_TenantAndAutomatedBackups(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()

	_, err := b.CreateTenantDatabase("inst-1", "tenantdb", "admin")
	require.NoError(t, err)

	_, err = b.StartDBInstanceAutomatedBackupsReplication(
		"arn:aws:rds:us-west-2:123:db:src", 14,
	)
	require.NoError(t, err)

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := rds.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(snap))

	tenants, err := b2.DescribeTenantDatabases("inst-1", "")
	require.NoError(t, err)
	assert.Len(t, tenants, 1)
	assert.Equal(t, "tenantdb", tenants[0].TenantDBName)

	// Attempting to stop replication of the restored backup should work
	_, err = b2.StopDBInstanceAutomatedBackupsReplication(
		"arn:aws:rds:us-west-2:123:db:src",
	)
	require.NoError(t, err)
}

func TestBatch2_Persistence_ClusterAutomatedBackups(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()

	// Create a cluster first, which triggers an automated backup entry
	_, err := b.CreateDBCluster(
		"backup-cluster",
		"aurora-postgresql",
		"admin",
		"",
		"",
		5432,
		nil,
		rds.DBClusterOptions{},
	)
	require.NoError(t, err)
	b.CreateDBClusterAutomatedBackup("backup-cluster")

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := rds.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(snap))

	backups := b2.DescribeDBClusterAutomatedBackups("backup-cluster")
	assert.NotEmpty(t, backups)
	assert.Equal(t, "backup-cluster", backups[0].DBClusterIdentifier)
}

func TestBatch2_Persistence_SnapshotTenantDatabases(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()

	b.AddDBSnapshotTenantDatabase("snap-1", "inst-1", "tenantA", "postgres")
	b.AddDBSnapshotTenantDatabase("snap-1", "inst-1", "tenantB", "postgres")

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := rds.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(snap))

	tenants := b2.DescribeDBSnapshotTenantDatabases("snap-1", "")
	assert.Len(t, tenants, 2)
}

// ---- Full persistence round-trip JSON validation ----

func TestBatch2_Persistence_JSONContainsBatch1Keys(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, _ = b.CreateDBShardGroup("sg-x", "cl-x", 16, 2, 0, false)
	_, _ = b.CreateIntegration("intg-x", "arn:src", "arn:dst", "", "", "")

	snap := b.Snapshot()
	require.NotNil(t, snap)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(snap, &raw))

	for _, key := range []string{
		"customEngineVersions",
		"automatedBackups",
		"shardGroups",
		"integrations",
		"tenantDatabases",
		"clusterAutomatedBackups",
		"snapshotTenantDatabases",
	} {
		assert.Contains(t, raw, key, "snapshot JSON missing key: %s", key)
	}
}

// ---- Concurrent access safety ----

func TestBatch2_Concurrent_DBParameterGroup(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	for i := range 5 {
		_, err := b.CreateDBParameterGroup(fmt.Sprintf("pg%d", i), "postgres14", "test")
		require.NoError(t, err)
	}

	var wg sync.WaitGroup
	for i := range 5 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			name := fmt.Sprintf("pg%d", n)
			_, _ = b.ModifyDBParameterGroup(name, []rds.DBParameter{
				{ParameterName: "max_connections", ParameterValue: strconv.Itoa(100 + n)},
			})
			_, _ = b.DescribeDBParameters(name)
		}(i)
	}
	wg.Wait()
}

func TestBatch2_Concurrent_ClusterSnapshot(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBCluster(
		"conc-cluster",
		"aurora-postgresql",
		"admin",
		"",
		"",
		5432,
		nil,
		rds.DBClusterOptions{},
	)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			snapID := fmt.Sprintf("conc-snap-%d", n)
			_, _ = b.CreateDBClusterSnapshot(snapID, "conc-cluster")
			_, _ = b.DescribeDBClusterSnapshots(snapID)
		}(i)
	}
	wg.Wait()

	snaps, err := b.DescribeDBClusterSnapshots("")
	require.NoError(t, err)
	assert.Len(t, snaps, 10)
}

func TestBatch2_Concurrent_EventSubscription(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()

	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			name := fmt.Sprintf("sub-%d", n)
			_, _ = b.CreateEventSubscription(
				name,
				"arn:aws:sns:us-east-1:123:t",
				"db-instance",
				nil,
				nil,
			)
			_, _ = b.DescribeEventSubscriptions(name)
		}(i)
	}
	wg.Wait()

	subs, err := b.DescribeEventSubscriptions("")
	require.NoError(t, err)
	assert.Len(t, subs, 10)
}
