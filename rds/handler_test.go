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

func TestRDSHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "Name",
			run: func(t *testing.T) {
				h := newRDSHandler()
				assert.Equal(t, "RDS", h.Name())
			},
		},
		{
			name: "GetSupportedOperations",
			run: func(t *testing.T) {
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
			},
		},
		{
			name: "MatchPriority",
			run: func(t *testing.T) {
				h := newRDSHandler()
				assert.Equal(t, 84, h.MatchPriority())
			},
		},
		{
			name: "RouteMatcher",
			run: func(t *testing.T) {
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
			},
		},
		{
			name: "ExtractOperation",
			run: func(t *testing.T) {
				h := newRDSHandler()
				e := echo.New()

				req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=CreateDBInstance&Version=2014-10-31"))
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				c := e.NewContext(req, httptest.NewRecorder())

				assert.Equal(t, "CreateDBInstance", h.ExtractOperation(c))
			},
		},
		{
			name: "ExtractResource",
			run: func(t *testing.T) {
				h := newRDSHandler()
				e := echo.New()

				req := httptest.NewRequest(http.MethodPost, "/",
					strings.NewReader("DBInstanceIdentifier=my-db&Version=2014-10-31"))
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				c := e.NewContext(req, httptest.NewRecorder())

				assert.Equal(t, "my-db", h.ExtractResource(c))
			},
		},
		{
			name: "CreateDBInstance",
			run: func(t *testing.T) {
				h := newRDSHandler()
				rec := postRDSForm(t, h,
					"Action=CreateDBInstance&Version=2014-10-31"+
						"&DBInstanceIdentifier=test-db&Engine=postgres&DBInstanceClass=db.t3.micro"+
						"&MasterUsername=admin&DBName=mydb&AllocatedStorage=20")

				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "CreateDBInstanceResponse")
				assert.Contains(t, rec.Body.String(), "test-db")
				assert.Contains(t, rec.Body.String(), "postgres")
			},
		},
		{
			name: "CreateDBInstance_MySQL",
			run: func(t *testing.T) {
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
			},
		},
		{
			name: "CreateDBInstance_DefaultEngine",
			run: func(t *testing.T) {
				h := newRDSHandler()
				rec := postRDSForm(t, h,
					"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=default-db")

				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "postgres")
				assert.Contains(t, rec.Body.String(), "<Port>5432</Port>")
			},
		},
		{
			name: "CreateDBInstance_InvalidAllocatedStorage",
			run: func(t *testing.T) {
				h := newRDSHandler()
				rec := postRDSForm(t, h,
					"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=bad-db&AllocatedStorage=abc")

				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
			},
		},
		{
			name: "ModifyDBInstance_InvalidAllocatedStorage",
			run: func(t *testing.T) {
				h := newRDSHandler()
				postRDSForm(t, h, "Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=mod-bad-db")

				rec := postRDSForm(t, h,
					"Action=ModifyDBInstance&Version=2014-10-31&DBInstanceIdentifier=mod-bad-db&AllocatedStorage=notanumber")
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
			},
		},
		{
			name: "CreateDBSnapshot_EmptySnapshotID",
			run: func(t *testing.T) {
				h := newRDSHandler()
				postRDSForm(t, h, "Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=snap-empty-db")

				rec := postRDSForm(t, h,
					"Action=CreateDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=&DBInstanceIdentifier=snap-empty-db")
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
			},
		},
		{
			name: "CreateDBSnapshot_EmptyInstanceID",
			run: func(t *testing.T) {
				h := newRDSHandler()
				rec := postRDSForm(t, h,
					"Action=CreateDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=snap-noinst&DBInstanceIdentifier=")
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
			},
		},
		{
			name: "CreateDBSubnetGroup_EmptyName",
			run: func(t *testing.T) {
				h := newRDSHandler()
				rec := postRDSForm(t, h, "Action=CreateDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=")
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
			},
		},
		{
			name: "CreateDBInstance_EmptyID",
			run: func(t *testing.T) {
				h := newRDSHandler()
				rec := postRDSForm(t, h, "Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=")

				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
			},
		},
		{
			name: "CreateDBInstance_Duplicate",
			run: func(t *testing.T) {
				h := newRDSHandler()
				postRDSForm(t, h, "Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=dup-db")

				rec := postRDSForm(t, h, "Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=dup-db")
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "DBInstanceAlreadyExists")
			},
		},
		{
			name: "DeleteDBInstance",
			run: func(t *testing.T) {
				h := newRDSHandler()
				postRDSForm(t, h, "Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=del-db")

				rec := postRDSForm(t, h, "Action=DeleteDBInstance&Version=2014-10-31&DBInstanceIdentifier=del-db")
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "DeleteDBInstanceResponse")
				assert.Contains(t, rec.Body.String(), "del-db")
			},
		},
		{
			name: "DeleteDBInstance_NotFound",
			run: func(t *testing.T) {
				h := newRDSHandler()
				rec := postRDSForm(t, h, "Action=DeleteDBInstance&Version=2014-10-31&DBInstanceIdentifier=nonexistent")
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "DBInstanceNotFound")
			},
		},
		{
			name: "DescribeDBInstances",
			run: func(t *testing.T) {
				h := newRDSHandler()
				postRDSForm(t, h, "Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=desc-db")

				rec := postRDSForm(t, h, "Action=DescribeDBInstances&Version=2014-10-31")
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "DescribeDBInstancesResponse")
				assert.Contains(t, rec.Body.String(), "desc-db")
			},
		},
		{
			name: "DescribeDBInstances_ByID",
			run: func(t *testing.T) {
				h := newRDSHandler()
				postRDSForm(t, h, "Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=db-one")
				postRDSForm(t, h, "Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=db-two")

				rec := postRDSForm(t, h, "Action=DescribeDBInstances&Version=2014-10-31&DBInstanceIdentifier=db-one")
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "db-one")
				assert.NotContains(t, rec.Body.String(), "db-two")
			},
		},
		{
			name: "DescribeDBInstances_NotFound",
			run: func(t *testing.T) {
				h := newRDSHandler()
				rec := postRDSForm(t, h, "Action=DescribeDBInstances&Version=2014-10-31&DBInstanceIdentifier=nonexistent")
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "DBInstanceNotFound")
			},
		},
		{
			name: "ModifyDBInstance",
			run: func(t *testing.T) {
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
			},
		},
		{
			name: "ModifyDBInstance_NotFound",
			run: func(t *testing.T) {
				h := newRDSHandler()
				rec := postRDSForm(t, h, "Action=ModifyDBInstance&Version=2014-10-31&DBInstanceIdentifier=nonexistent")
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "DBInstanceNotFound")
			},
		},
		{
			name: "CreateDBSnapshot",
			run: func(t *testing.T) {
				h := newRDSHandler()
				postRDSForm(t, h, "Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=snap-db")

				rec := postRDSForm(t, h,
					"Action=CreateDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=snap-1&DBInstanceIdentifier=snap-db")
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "CreateDBSnapshotResponse")
				assert.Contains(t, rec.Body.String(), "snap-1")
			},
		},
		{
			name: "CreateDBSnapshot_InstanceNotFound",
			run: func(t *testing.T) {
				h := newRDSHandler()
				rec := postRDSForm(t, h,
					"Action=CreateDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=snap-1&DBInstanceIdentifier=nonexistent")
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "DBInstanceNotFound")
			},
		},
		{
			name: "CreateDBSnapshot_Duplicate",
			run: func(t *testing.T) {
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
			},
		},
		{
			name: "DescribeDBSnapshots",
			run: func(t *testing.T) {
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
			},
		},
		{
			name: "DescribeDBSnapshots_NotFound",
			run: func(t *testing.T) {
				h := newRDSHandler()
				rec := postRDSForm(t, h, "Action=DescribeDBSnapshots&Version=2014-10-31&DBSnapshotIdentifier=nonexistent")
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "DBSnapshotNotFound")
			},
		},
		{
			name: "DeleteDBSnapshot",
			run: func(t *testing.T) {
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
			},
		},
		{
			name: "DeleteDBSnapshot_NotFound",
			run: func(t *testing.T) {
				h := newRDSHandler()
				rec := postRDSForm(t, h, "Action=DeleteDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=nonexistent")
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "DBSnapshotNotFound")
			},
		},
		{
			name: "CreateDBSubnetGroup",
			run: func(t *testing.T) {
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
			},
		},
		{
			name: "CreateDBSubnetGroup_Duplicate",
			run: func(t *testing.T) {
				h := newRDSHandler()
				postRDSForm(t, h, "Action=CreateDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=dup-sg")

				rec := postRDSForm(t, h, "Action=CreateDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=dup-sg")
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "DBSubnetGroupAlreadyExists")
			},
		},
		{
			name: "DescribeDBSubnetGroups",
			run: func(t *testing.T) {
				h := newRDSHandler()
				postRDSForm(t, h, "Action=CreateDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=list-sg")

				rec := postRDSForm(t, h, "Action=DescribeDBSubnetGroups&Version=2014-10-31")
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "DescribeDBSubnetGroupsResponse")
				assert.Contains(t, rec.Body.String(), "list-sg")
			},
		},
		{
			name: "DescribeDBSubnetGroups_ByName",
			run: func(t *testing.T) {
				h := newRDSHandler()
				postRDSForm(t, h, "Action=CreateDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=find-sg")
				postRDSForm(t, h, "Action=CreateDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=other-sg")

				rec := postRDSForm(t, h, "Action=DescribeDBSubnetGroups&Version=2014-10-31&DBSubnetGroupName=find-sg")
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "find-sg")
				assert.NotContains(t, rec.Body.String(), "other-sg")
			},
		},
		{
			name: "DescribeDBSubnetGroups_NotFound",
			run: func(t *testing.T) {
				h := newRDSHandler()
				rec := postRDSForm(t, h, "Action=DescribeDBSubnetGroups&Version=2014-10-31&DBSubnetGroupName=nonexistent")
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "DBSubnetGroupNotFound")
			},
		},
		{
			name: "DeleteDBSubnetGroup",
			run: func(t *testing.T) {
				h := newRDSHandler()
				postRDSForm(t, h, "Action=CreateDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=del-sg")

				rec := postRDSForm(t, h, "Action=DeleteDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=del-sg")
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "DeleteDBSubnetGroupResponse")
			},
		},
		{
			name: "DeleteDBSubnetGroup_NotFound",
			run: func(t *testing.T) {
				h := newRDSHandler()
				rec := postRDSForm(t, h, "Action=DeleteDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=nonexistent")
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "DBSubnetGroupNotFound")
			},
		},
		{
			name: "ListTagsForResource",
			run: func(t *testing.T) {
				h := newRDSHandler()
				arn := "arn:aws:rds:us-east-1:000000000000:db:test-db"

				// No tags yet – list should be empty.
				rec := postRDSForm(t, h,
					"Action=ListTagsForResource&Version=2014-10-31&ResourceName="+arn)
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "ListTagsForResourceResponse")
				assert.NotContains(t, rec.Body.String(), "<Tag>")
			},
		},
		{
			name: "AddTagsToResource",
			run: func(t *testing.T) {
				h := newRDSHandler()
				arn := "arn:aws:rds:us-east-1:000000000000:db:test-db"

				// Add two tags (SDK encodes as Tags.Tag.N.Key).
				rec := postRDSForm(t, h,
					"Action=AddTagsToResource&Version=2014-10-31"+
						"&ResourceName="+arn+
						"&Tags.Tag.1.Key=Env&Tags.Tag.1.Value=prod"+
						"&Tags.Tag.2.Key=Team&Tags.Tag.2.Value=platform")
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "AddTagsToResourceResponse")

				// List should now return both tags.
				rec2 := postRDSForm(t, h,
					"Action=ListTagsForResource&Version=2014-10-31&ResourceName="+arn)
				assert.Equal(t, http.StatusOK, rec2.Code)
				body := rec2.Body.String()
				assert.Contains(t, body, "<Key>Env</Key>")
				assert.Contains(t, body, "<Value>prod</Value>")
				assert.Contains(t, body, "<Key>Team</Key>")
				assert.Contains(t, body, "<Value>platform</Value>")
			},
		},
		{
			name: "AddTagsToResource_Overwrite",
			run: func(t *testing.T) {
				h := newRDSHandler()
				arn := "arn:aws:rds:us-east-1:000000000000:db:tag-db"

				// Add initial tag.
				postRDSForm(t, h,
					"Action=AddTagsToResource&Version=2014-10-31"+
						"&ResourceName="+arn+
						"&Tags.Tag.1.Key=Env&Tags.Tag.1.Value=staging")

				// Overwrite same key.
				postRDSForm(t, h,
					"Action=AddTagsToResource&Version=2014-10-31"+
						"&ResourceName="+arn+
						"&Tags.Tag.1.Key=Env&Tags.Tag.1.Value=prod")

				// Should return only one Env tag with the new value.
				rec := postRDSForm(t, h,
					"Action=ListTagsForResource&Version=2014-10-31&ResourceName="+arn)
				body := rec.Body.String()
				assert.Contains(t, body, "<Value>prod</Value>")
				assert.NotContains(t, body, "<Value>staging</Value>")
			},
		},
		{
			name: "RemoveTagsFromResource",
			run: func(t *testing.T) {
				h := newRDSHandler()
				arn := "arn:aws:rds:us-east-1:000000000000:db:rm-db"

				// Add two tags.
				postRDSForm(t, h,
					"Action=AddTagsToResource&Version=2014-10-31"+
						"&ResourceName="+arn+
						"&Tags.Tag.1.Key=Env&Tags.Tag.1.Value=test"+
						"&Tags.Tag.2.Key=Team&Tags.Tag.2.Value=infra")

				// Remove one (SDK encodes as TagKeys.member.N).
				rec := postRDSForm(t, h,
					"Action=RemoveTagsFromResource&Version=2014-10-31"+
						"&ResourceName="+arn+
						"&TagKeys.member.1=Env")
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "RemoveTagsFromResourceResponse")

				// List should only have Team left.
				rec2 := postRDSForm(t, h,
					"Action=ListTagsForResource&Version=2014-10-31&ResourceName="+arn)
				body := rec2.Body.String()
				assert.NotContains(t, body, "<Key>Env</Key>")
				assert.Contains(t, body, "<Key>Team</Key>")
			},
		},
		{
			name: "InvalidAction",
			run: func(t *testing.T) {
				h := newRDSHandler()
				rec := postRDSForm(t, h, "Action=InvalidAction&Version=2014-10-31")
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "InvalidAction")
			},
		},
		{
			name: "MissingAction",
			run: func(t *testing.T) {
				h := newRDSHandler()
				rec := postRDSForm(t, h, "Version=2014-10-31")
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "MissingAction")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}
