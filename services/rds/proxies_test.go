package rds_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

// TestModifyDBProxyCopied verifies ModifyDBProxy returns an independent copy.
func TestModifyDBProxyCopied(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	_, err := b.CreateDBProxy("my-proxy", "POSTGRESQL", "arn:aws:iam::123456789012:role/proxy-role",
		[]rds.UserAuthConfig{{SecretARN: "arn:aws:secretsmanager:us-east-1:123456789012:secret:s1"}})
	require.NoError(t, err)
	requireTLS := true
	proxy1, err := b.ModifyDBProxy("my-proxy", &requireTLS, nil, nil)
	require.NoError(t, err)
	// Verify the returned value is a copy (not a pointer to the stored proxy).
	// Modify a subsequent call and verify proxy1 is unaffected.
	requireTLS2 := false
	timeout := 900
	_, err = b.ModifyDBProxy("my-proxy", &requireTLS2, &timeout, nil)
	require.NoError(t, err)
	assert.True(t, proxy1.RequireTLS, "first ModifyDBProxy result should be independent copy")
	assert.Equal(t, 1800, proxy1.IdleClientTimeout, "first result should retain original timeout")
}
