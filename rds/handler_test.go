package rds_test

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/rds"
)

func newRDSHandler() *rds.Handler {
	return rds.NewHandler(rds.NewInMemoryBackend("000000000000", "us-east-1"), slog.Default())
}

func postRDSForm(t *testing.T, h *rds.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestRDSHandler_Name(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	assert.Equal(t, "RDS", h.Name())
}

func TestRDSHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateDBInstance")
	assert.Contains(t, ops, "DeleteDBInstance")
	assert.Contains(t, ops, "DescribeDBInstances")
	assert.Contains(t, ops, "ModifyDBInstance")
	assert.Contains(t, ops, "CreateDBSnapshot")
	assert.Contains(t, ops, "DescribeDBSnapshots")
	assert.Contains(t, ops, "DeleteDBSnapshot")
	assert.Contains(t, ops, "CreateDBSubnetGroup")
	assert.Contains(t, ops, "DescribeDBSubnetGroups")
	assert.Contains(t, ops, "DeleteDBSubnetGroup")
}

func TestRDSHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	assert.Equal(t, 84, h.MatchPriority())
}

func TestRDSHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()

	e := echo.New()

	// Match: valid RDS request
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Version=2014-10-31&Action=DescribeDBInstances"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	c := e.NewContext(req, httptest.NewRecorder())
	matcher := h.RouteMatcher()
	assert.True(t, matcher(c))

	// No match: wrong version
	req2 := httptest.NewRequest(
		http.MethodPost,
		"/",
		strings.NewReader("Version=2012-12-01&Action=DescribeDBInstances"),
	)
	req2.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	c2 := e.NewContext(req2, httptest.NewRecorder())
	assert.False(t, matcher(c2))

	// No match: GET request
	req3 := httptest.NewRequest(http.MethodGet, "/", nil)
	req3.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	c3 := e.NewContext(req3, httptest.NewRecorder())
	assert.False(t, matcher(c3))

	// No match: dashboard path
	req4 := httptest.NewRequest(http.MethodPost, "/dashboard/rds", strings.NewReader("Version=2014-10-31"))
	req4.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	c4 := e.NewContext(req4, httptest.NewRecorder())
	assert.False(t, matcher(c4))
}

func TestRDSHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=CreateDBInstance&Version=2014-10-31"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	c := e.NewContext(req, httptest.NewRecorder())

	assert.Equal(t, "CreateDBInstance", h.ExtractOperation(c))
}

func TestRDSHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/",
		strings.NewReader("DBInstanceIdentifier=my-db&Version=2014-10-31"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	c := e.NewContext(req, httptest.NewRecorder())

	assert.Equal(t, "my-db", h.ExtractResource(c))
}

func TestRDSHandler_CreateDBInstance(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	rec := postRDSForm(t, h,
		"Action=CreateDBInstance&Version=2014-10-31"+
			"&DBInstanceIdentifier=test-db&Engine=postgres&DBInstanceClass=db.t3.micro"+
			"&MasterUsername=admin&DBName=mydb&AllocatedStorage=20")

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CreateDBInstanceResponse")
	assert.Contains(t, rec.Body.String(), "test-db")
	assert.Contains(t, rec.Body.String(), "postgres")
}

func TestRDSHandler_CreateDBInstance_MySQL(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	rec := postRDSForm(t, h,
		"Action=CreateDBInstance&Version=2014-10-31"+
			"&DBInstanceIdentifier=mysql-db&Engine=mysql&DBInstanceClass=db.t3.micro")

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CreateDBInstanceResponse")
	assert.Contains(t, rec.Body.String(), "mysql-db")
	assert.Contains(t, rec.Body.String(), "mysql")
	// MySQL uses port 3306
	assert.Contains(t, rec.Body.String(), "<Port>3306</Port>")
}

func TestRDSHandler_CreateDBInstance_DefaultEngine(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	rec := postRDSForm(t, h,
		"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=default-db")

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "postgres")
	assert.Contains(t, rec.Body.String(), "<Port>5432</Port>")
}

func TestRDSHandler_CreateDBInstance_EmptyID(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	rec := postRDSForm(t, h, "Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
}

func TestRDSHandler_CreateDBInstance_Duplicate(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	postRDSForm(t, h, "Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=dup-db")

	rec := postRDSForm(t, h, "Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=dup-db")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "DBInstanceAlreadyExists")
}

func TestRDSHandler_DeleteDBInstance(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	postRDSForm(t, h, "Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=del-db")

	rec := postRDSForm(t, h, "Action=DeleteDBInstance&Version=2014-10-31&DBInstanceIdentifier=del-db")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteDBInstanceResponse")
	assert.Contains(t, rec.Body.String(), "del-db")
}

func TestRDSHandler_DeleteDBInstance_NotFound(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	rec := postRDSForm(t, h, "Action=DeleteDBInstance&Version=2014-10-31&DBInstanceIdentifier=nonexistent")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "DBInstanceNotFound")
}

func TestRDSHandler_DescribeDBInstances(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	postRDSForm(t, h, "Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=desc-db")

	rec := postRDSForm(t, h, "Action=DescribeDBInstances&Version=2014-10-31")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeDBInstancesResponse")
	assert.Contains(t, rec.Body.String(), "desc-db")
}

func TestRDSHandler_DescribeDBInstances_ByID(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	postRDSForm(t, h, "Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=db-one")
	postRDSForm(t, h, "Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=db-two")

	rec := postRDSForm(t, h, "Action=DescribeDBInstances&Version=2014-10-31&DBInstanceIdentifier=db-one")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "db-one")
	assert.NotContains(t, rec.Body.String(), "db-two")
}

func TestRDSHandler_DescribeDBInstances_NotFound(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	rec := postRDSForm(t, h, "Action=DescribeDBInstances&Version=2014-10-31&DBInstanceIdentifier=nonexistent")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "DBInstanceNotFound")
}

func TestRDSHandler_ModifyDBInstance(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	postRDSForm(t, h,
		"Action=CreateDBInstance&Version=2014-10-31"+
			"&DBInstanceIdentifier=mod-db&DBInstanceClass=db.t3.micro&AllocatedStorage=20",
	)

	rec := postRDSForm(t, h,
		"Action=ModifyDBInstance&Version=2014-10-31"+
			"&DBInstanceIdentifier=mod-db&DBInstanceClass=db.r5.large&AllocatedStorage=100",
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ModifyDBInstanceResponse")
	assert.Contains(t, rec.Body.String(), "db.r5.large")
}

func TestRDSHandler_ModifyDBInstance_NotFound(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	rec := postRDSForm(t, h, "Action=ModifyDBInstance&Version=2014-10-31&DBInstanceIdentifier=nonexistent")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "DBInstanceNotFound")
}

func TestRDSHandler_CreateDBSnapshot(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	postRDSForm(t, h, "Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=snap-db")

	rec := postRDSForm(t, h,
		"Action=CreateDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=snap-1&DBInstanceIdentifier=snap-db")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CreateDBSnapshotResponse")
	assert.Contains(t, rec.Body.String(), "snap-1")
}

func TestRDSHandler_CreateDBSnapshot_InstanceNotFound(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	rec := postRDSForm(t, h,
		"Action=CreateDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=snap-1&DBInstanceIdentifier=nonexistent")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "DBInstanceNotFound")
}

func TestRDSHandler_CreateDBSnapshot_Duplicate(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	postRDSForm(t, h, "Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=snap-db2")
	postRDSForm(
		t,
		h,
		"Action=CreateDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=dup-snap&DBInstanceIdentifier=snap-db2",
	)

	rec := postRDSForm(
		t,
		h,
		"Action=CreateDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=dup-snap&DBInstanceIdentifier=snap-db2",
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "DBSnapshotAlreadyExists")
}

func TestRDSHandler_DescribeDBSnapshots(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	postRDSForm(t, h, "Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=snap-db3")
	postRDSForm(
		t,
		h,
		"Action=CreateDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=list-snap&DBInstanceIdentifier=snap-db3",
	)

	rec := postRDSForm(t, h, "Action=DescribeDBSnapshots&Version=2014-10-31")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeDBSnapshotsResponse")
	assert.Contains(t, rec.Body.String(), "list-snap")
}

func TestRDSHandler_DescribeDBSnapshots_NotFound(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	rec := postRDSForm(t, h, "Action=DescribeDBSnapshots&Version=2014-10-31&DBSnapshotIdentifier=nonexistent")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "DBSnapshotNotFound")
}

func TestRDSHandler_DeleteDBSnapshot(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	postRDSForm(t, h, "Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=snap-db4")
	postRDSForm(
		t,
		h,
		"Action=CreateDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=del-snap&DBInstanceIdentifier=snap-db4",
	)

	rec := postRDSForm(t, h, "Action=DeleteDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=del-snap")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteDBSnapshotResponse")
	assert.Contains(t, rec.Body.String(), "del-snap")
}

func TestRDSHandler_DeleteDBSnapshot_NotFound(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	rec := postRDSForm(t, h, "Action=DeleteDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=nonexistent")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "DBSnapshotNotFound")
}

func TestRDSHandler_CreateDBSubnetGroup(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	rec := postRDSForm(t, h,
		"Action=CreateDBSubnetGroup&Version=2014-10-31"+
			"&DBSubnetGroupName=my-subnet-group&DBSubnetGroupDescription=My+group"+
			"&VpcId=vpc-12345"+
			"&SubnetIds.member.1=subnet-1&SubnetIds.member.2=subnet-2")

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CreateDBSubnetGroupResponse")
	assert.Contains(t, rec.Body.String(), "my-subnet-group")
	assert.Contains(t, rec.Body.String(), "subnet-1")
}

func TestRDSHandler_CreateDBSubnetGroup_Duplicate(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	postRDSForm(t, h, "Action=CreateDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=dup-sg")

	rec := postRDSForm(t, h, "Action=CreateDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=dup-sg")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "DBSubnetGroupAlreadyExists")
}

func TestRDSHandler_DescribeDBSubnetGroups(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	postRDSForm(t, h, "Action=CreateDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=list-sg")

	rec := postRDSForm(t, h, "Action=DescribeDBSubnetGroups&Version=2014-10-31")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeDBSubnetGroupsResponse")
	assert.Contains(t, rec.Body.String(), "list-sg")
}

func TestRDSHandler_DescribeDBSubnetGroups_ByName(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	postRDSForm(t, h, "Action=CreateDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=find-sg")
	postRDSForm(t, h, "Action=CreateDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=other-sg")

	rec := postRDSForm(t, h, "Action=DescribeDBSubnetGroups&Version=2014-10-31&DBSubnetGroupName=find-sg")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "find-sg")
	assert.NotContains(t, rec.Body.String(), "other-sg")
}

func TestRDSHandler_DescribeDBSubnetGroups_NotFound(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	rec := postRDSForm(t, h, "Action=DescribeDBSubnetGroups&Version=2014-10-31&DBSubnetGroupName=nonexistent")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "DBSubnetGroupNotFound")
}

func TestRDSHandler_DeleteDBSubnetGroup(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	postRDSForm(t, h, "Action=CreateDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=del-sg")

	rec := postRDSForm(t, h, "Action=DeleteDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=del-sg")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteDBSubnetGroupResponse")
}

func TestRDSHandler_DeleteDBSubnetGroup_NotFound(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	rec := postRDSForm(t, h, "Action=DeleteDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=nonexistent")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "DBSubnetGroupNotFound")
}

func TestRDSHandler_InvalidAction(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	rec := postRDSForm(t, h, "Action=InvalidAction&Version=2014-10-31")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidAction")
}

func TestRDSHandler_MissingAction(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	rec := postRDSForm(t, h, "Version=2014-10-31")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "MissingAction")
}
