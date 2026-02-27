package integration_test

import (
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func rdsPost(t *testing.T, form url.Values) *http.Response {
	t.Helper()

	form.Set("Version", "2014-10-31")

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint,
		strings.NewReader(form.Encode()))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func rdsReadBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return string(data)
}

func TestIntegration_RDS_CreateDBInstance(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := rdsPost(t, url.Values{
		"Action":               {"CreateDBInstance"},
		"DBInstanceIdentifier": {"integ-db"},
		"Engine":               {"postgres"},
		"DBInstanceClass":      {"db.t3.micro"},
		"MasterUsername":       {"admin"},
		"DBName":               {"testdb"},
		"AllocatedStorage":     {"20"},
	})
	body := rdsReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "CreateDBInstanceResponse")
	assert.Contains(t, body, "integ-db")
	assert.Contains(t, body, "postgres")
}

func TestIntegration_RDS_CreateDBInstance_MySQL(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := rdsPost(t, url.Values{
		"Action":               {"CreateDBInstance"},
		"DBInstanceIdentifier": {"integ-mysql-db"},
		"Engine":               {"mysql"},
		"DBInstanceClass":      {"db.t3.micro"},
	})
	body := rdsReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "CreateDBInstanceResponse")
	assert.Contains(t, body, "integ-mysql-db")
	assert.Contains(t, body, "mysql")
	assert.Contains(t, body, "<Port>3306</Port>")
}

func TestIntegration_RDS_DescribeDBInstances(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	rdsPost(t, url.Values{
		"Action":               {"CreateDBInstance"},
		"DBInstanceIdentifier": {"describe-db"},
	})

	resp := rdsPost(t, url.Values{
		"Action": {"DescribeDBInstances"},
	})
	body := rdsReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "DescribeDBInstancesResponse")
}

func TestIntegration_RDS_ModifyDBInstance(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	rdsPost(t, url.Values{
		"Action":               {"CreateDBInstance"},
		"DBInstanceIdentifier": {"modify-db"},
		"DBInstanceClass":      {"db.t3.micro"},
		"AllocatedStorage":     {"20"},
	})

	resp := rdsPost(t, url.Values{
		"Action":               {"ModifyDBInstance"},
		"DBInstanceIdentifier": {"modify-db"},
		"DBInstanceClass":      {"db.r5.large"},
		"AllocatedStorage":     {"100"},
	})
	body := rdsReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "ModifyDBInstanceResponse")
	assert.Contains(t, body, "db.r5.large")
}

func TestIntegration_RDS_CreateDBSnapshot(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	rdsPost(t, url.Values{
		"Action":               {"CreateDBInstance"},
		"DBInstanceIdentifier": {"snap-db"},
	})

	resp := rdsPost(t, url.Values{
		"Action":               {"CreateDBSnapshot"},
		"DBSnapshotIdentifier": {"integ-snap-1"},
		"DBInstanceIdentifier": {"snap-db"},
	})
	body := rdsReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "CreateDBSnapshotResponse")
	assert.Contains(t, body, "integ-snap-1")
}

func TestIntegration_RDS_DescribeDBSnapshots(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	rdsPost(t, url.Values{
		"Action":               {"CreateDBInstance"},
		"DBInstanceIdentifier": {"snap-db2"},
	})
	rdsPost(t, url.Values{
		"Action":               {"CreateDBSnapshot"},
		"DBSnapshotIdentifier": {"integ-snap-2"},
		"DBInstanceIdentifier": {"snap-db2"},
	})

	resp := rdsPost(t, url.Values{
		"Action": {"DescribeDBSnapshots"},
	})
	body := rdsReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "DescribeDBSnapshotsResponse")
}

func TestIntegration_RDS_DeleteDBInstance(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	rdsPost(t, url.Values{
		"Action":               {"CreateDBInstance"},
		"DBInstanceIdentifier": {"delete-integ-db"},
	})

	resp := rdsPost(t, url.Values{
		"Action":               {"DeleteDBInstance"},
		"DBInstanceIdentifier": {"delete-integ-db"},
	})
	body := rdsReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "DeleteDBInstanceResponse")
	assert.Contains(t, body, "delete-integ-db")
}

func TestIntegration_RDS_CreateDBSubnetGroup(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := rdsPost(t, url.Values{
		"Action":                   {"CreateDBSubnetGroup"},
		"DBSubnetGroupName":        {"integ-subnet-group"},
		"DBSubnetGroupDescription": {"Integration test subnet group"},
		"VpcId":                    {"vpc-12345"},
		"SubnetIds.member.1":       {"subnet-1"},
		"SubnetIds.member.2":       {"subnet-2"},
	})
	body := rdsReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "CreateDBSubnetGroupResponse")
	assert.Contains(t, body, "integ-subnet-group")
}

func TestIntegration_RDS_DescribeDBSubnetGroups(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	rdsPost(t, url.Values{
		"Action":            {"CreateDBSubnetGroup"},
		"DBSubnetGroupName": {"list-subnet-group"},
	})

	resp := rdsPost(t, url.Values{
		"Action": {"DescribeDBSubnetGroups"},
	})
	body := rdsReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "DescribeDBSubnetGroupsResponse")
}
