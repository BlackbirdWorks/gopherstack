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
	matcher := h.RouteMatcher()

	tests := []struct {
		name   string
		method string
		path   string
		body   string
		want   bool
	}{
		{
			name:   "valid RDS request",
			method: http.MethodPost,
			path:   "/",
			body:   "Version=2014-10-31&Action=DescribeDBInstances",
			want:   true,
		},
		{
			name:   "wrong version",
			method: http.MethodPost,
			path:   "/",
			body:   "Version=2012-12-01&Action=DescribeDBInstances",
			want:   false,
		},
		{
			name:   "GET request",
			method: http.MethodGet,
			path:   "/",
			want:   false,
		},
		{
			name:   "dashboard path",
			method: http.MethodPost,
			path:   "/dashboard/rds",
			body:   "Version=2014-10-31",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.want, matcher(c))
		})
	}
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

func TestRDSHandler_FormActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		body            string
		setupBodies     []string
		wantContains    []string
		wantNotContains []string
		wantCode        int
	}{
		{
			name: "CreateDBInstance",
			body: "Action=CreateDBInstance&Version=2014-10-31" +
				"&DBInstanceIdentifier=test-db&Engine=postgres&DBInstanceClass=db.t3.micro" +
				"&MasterUsername=admin&DBName=mydb&AllocatedStorage=20",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateDBInstanceResponse", "test-db", "postgres"},
		},
		{
			name: "CreateDBInstance_MySQL",
			body: "Action=CreateDBInstance&Version=2014-10-31" +
				"&DBInstanceIdentifier=mysql-db&Engine=mysql&DBInstanceClass=db.t3.micro",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateDBInstanceResponse", "mysql-db", "mysql", "<Port>3306</Port>"},
		},
		{
			name:         "CreateDBInstance_DefaultEngine",
			body:         "Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=default-db",
			wantCode:     http.StatusOK,
			wantContains: []string{"postgres", "<Port>5432</Port>"},
		},
		{
			name:         "CreateDBInstance_InvalidAllocatedStorage",
			body:         "Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=bad-db&AllocatedStorage=abc",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:        "ModifyDBInstance_InvalidAllocatedStorage",
			setupBodies: []string{"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=mod-bad-db"},
			body: "Action=ModifyDBInstance&Version=2014-10-31&DBInstanceIdentifier=mod-bad-db&" +
				"AllocatedStorage=notanumber",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "CreateDBSnapshot_EmptySnapshotID",
			setupBodies:  []string{"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=snap-empty-db"},
			body:         "Action=CreateDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=&DBInstanceIdentifier=snap-empty-db",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "CreateDBSnapshot_EmptyInstanceID",
			body:         "Action=CreateDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=snap-noinst&DBInstanceIdentifier=",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "CreateDBSubnetGroup_EmptyName",
			body:         "Action=CreateDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "CreateDBInstance_EmptyID",
			body:         "Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "CreateDBInstance_Duplicate",
			setupBodies:  []string{"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=dup-db"},
			body:         "Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=dup-db",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBInstanceAlreadyExists"},
		},
		{
			name:         "DeleteDBInstance",
			setupBodies:  []string{"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=del-db"},
			body:         "Action=DeleteDBInstance&Version=2014-10-31&DBInstanceIdentifier=del-db",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteDBInstanceResponse", "del-db"},
		},
		{
			name:         "DeleteDBInstance_NotFound",
			body:         "Action=DeleteDBInstance&Version=2014-10-31&DBInstanceIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBInstanceNotFound"},
		},
		{
			name:         "DescribeDBInstances",
			setupBodies:  []string{"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=desc-db"},
			body:         "Action=DescribeDBInstances&Version=2014-10-31",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeDBInstancesResponse", "desc-db"},
		},
		{
			name: "DescribeDBInstances_ByID",
			setupBodies: []string{
				"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=db-one",
				"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=db-two",
			},
			body:            "Action=DescribeDBInstances&Version=2014-10-31&DBInstanceIdentifier=db-one",
			wantCode:        http.StatusOK,
			wantContains:    []string{"db-one"},
			wantNotContains: []string{"db-two"},
		},
		{
			name:         "DescribeDBInstances_NotFound",
			body:         "Action=DescribeDBInstances&Version=2014-10-31&DBInstanceIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBInstanceNotFound"},
		},
		{
			name: "ModifyDBInstance",
			setupBodies: []string{
				"Action=CreateDBInstance&Version=2014-10-31" +
					"&DBInstanceIdentifier=mod-db&DBInstanceClass=db.t3.micro&AllocatedStorage=20",
			},
			body: "Action=ModifyDBInstance&Version=2014-10-31" +
				"&DBInstanceIdentifier=mod-db&DBInstanceClass=db.r5.large&AllocatedStorage=100",
			wantCode:     http.StatusOK,
			wantContains: []string{"ModifyDBInstanceResponse", "db.r5.large"},
		},
		{
			name:         "ModifyDBInstance_NotFound",
			body:         "Action=ModifyDBInstance&Version=2014-10-31&DBInstanceIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBInstanceNotFound"},
		},
		{
			name:         "CreateDBSnapshot",
			setupBodies:  []string{"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=snap-db"},
			body:         "Action=CreateDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=snap-1&DBInstanceIdentifier=snap-db",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateDBSnapshotResponse", "snap-1"},
		},
		{
			name: "CreateDBSnapshot_InstanceNotFound",
			body: "Action=CreateDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=snap-1&" +
				"DBInstanceIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBInstanceNotFound"},
		},
		{
			name: "CreateDBSnapshot_Duplicate",
			setupBodies: []string{
				"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=snap-db2",
				"Action=CreateDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=dup-snap&DBInstanceIdentifier=snap-db2",
			},
			body: "Action=CreateDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=dup-snap&" +
				"DBInstanceIdentifier=snap-db2",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBSnapshotAlreadyExists"},
		},
		{
			name: "DescribeDBSnapshots",
			setupBodies: []string{
				"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=snap-db3",
				"Action=CreateDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=list-snap&DBInstanceIdentifier=snap-db3",
			},
			body:         "Action=DescribeDBSnapshots&Version=2014-10-31",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeDBSnapshotsResponse", "list-snap"},
		},
		{
			name:         "DescribeDBSnapshots_NotFound",
			body:         "Action=DescribeDBSnapshots&Version=2014-10-31&DBSnapshotIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBSnapshotNotFound"},
		},
		{
			name: "DeleteDBSnapshot",
			setupBodies: []string{
				"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=snap-db4",
				"Action=CreateDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=del-snap&DBInstanceIdentifier=snap-db4",
			},
			body:         "Action=DeleteDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=del-snap",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteDBSnapshotResponse", "del-snap"},
		},
		{
			name:         "DeleteDBSnapshot_NotFound",
			body:         "Action=DeleteDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBSnapshotNotFound"},
		},
		{
			name: "CreateDBSubnetGroup",
			body: "Action=CreateDBSubnetGroup&Version=2014-10-31" +
				"&DBSubnetGroupName=my-subnet-group&DBSubnetGroupDescription=My+group" +
				"&VpcId=vpc-12345" +
				"&SubnetIds.member.1=subnet-1&SubnetIds.member.2=subnet-2",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateDBSubnetGroupResponse", "my-subnet-group", "subnet-1"},
		},
		{
			name:         "CreateDBSubnetGroup_Duplicate",
			setupBodies:  []string{"Action=CreateDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=dup-sg"},
			body:         "Action=CreateDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=dup-sg",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBSubnetGroupAlreadyExists"},
		},
		{
			name:         "DescribeDBSubnetGroups",
			setupBodies:  []string{"Action=CreateDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=list-sg"},
			body:         "Action=DescribeDBSubnetGroups&Version=2014-10-31",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeDBSubnetGroupsResponse", "list-sg"},
		},
		{
			name: "DescribeDBSubnetGroups_ByName",
			setupBodies: []string{
				"Action=CreateDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=find-sg",
				"Action=CreateDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=other-sg",
			},
			body:            "Action=DescribeDBSubnetGroups&Version=2014-10-31&DBSubnetGroupName=find-sg",
			wantCode:        http.StatusOK,
			wantContains:    []string{"find-sg"},
			wantNotContains: []string{"other-sg"},
		},
		{
			name:         "DescribeDBSubnetGroups_NotFound",
			body:         "Action=DescribeDBSubnetGroups&Version=2014-10-31&DBSubnetGroupName=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBSubnetGroupNotFound"},
		},
		{
			name:         "DeleteDBSubnetGroup",
			setupBodies:  []string{"Action=CreateDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=del-sg"},
			body:         "Action=DeleteDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=del-sg",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteDBSubnetGroupResponse"},
		},
		{
			name:         "DeleteDBSubnetGroup_NotFound",
			body:         "Action=DeleteDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBSubnetGroupNotFound"},
		},
		{
			name: "ListTagsForResource",
			body: "Action=ListTagsForResource&Version=2014-10-31&" +
				"ResourceName=arn:aws:rds:us-east-1:000000000000:db:test-db",
			wantCode:        http.StatusOK,
			wantContains:    []string{"ListTagsForResourceResponse"},
			wantNotContains: []string{"<Tag>"},
		},
		{
			name: "AddTagsToResource_Overwrite",
			setupBodies: []string{
				"Action=AddTagsToResource&Version=2014-10-31" +
					"&ResourceName=arn:aws:rds:us-east-1:000000000000:db:tag-db" +
					"&Tags.Tag.1.Key=Env&Tags.Tag.1.Value=staging",
				"Action=AddTagsToResource&Version=2014-10-31" +
					"&ResourceName=arn:aws:rds:us-east-1:000000000000:db:tag-db" +
					"&Tags.Tag.1.Key=Env&Tags.Tag.1.Value=prod",
			},
			body: "Action=ListTagsForResource&Version=2014-10-31&" +
				"ResourceName=arn:aws:rds:us-east-1:000000000000:db:tag-db",
			wantCode:        http.StatusOK,
			wantContains:    []string{"<Value>prod</Value>"},
			wantNotContains: []string{"<Value>staging</Value>"},
		},
		{
			name:         "InvalidAction",
			body:         "Action=InvalidAction&Version=2014-10-31",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidAction"},
		},
		{
			name:         "MissingAction",
			body:         "Version=2014-10-31",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"MissingAction"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRDSHandler()

			for _, setup := range tt.setupBodies {
				postRDSForm(t, h, setup)
			}

			rec := postRDSForm(t, h, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			body := rec.Body.String()
			for _, s := range tt.wantContains {
				assert.Contains(t, body, s)
			}
			for _, s := range tt.wantNotContains {
				assert.NotContains(t, body, s)
			}
		})
	}
}

func TestRDSHandler_AddTagsToResource(t *testing.T) {
	t.Parallel()

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
}

func TestRDSHandler_RemoveTagsFromResource(t *testing.T) {
	t.Parallel()

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
}
