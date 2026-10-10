package lakeformation_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lakeformation"
)

// Rows follow lf-permissions-reference.html "Lake Formation permissions per resource type".
func TestGrantPermissions_PermissionsMatchResourceKind(t *testing.T) {
	t.Parallel()

	db := map[string]any{"Database": map[string]any{"Name": "db"}}
	table := map[string]any{"Table": map[string]any{"DatabaseName": "db", "Name": "t"}}
	columns := map[string]any{"TableWithColumns": map[string]any{
		"DatabaseName": "db", "Name": "t", "ColumnNames": []any{"a"},
	}}
	location := map[string]any{"DataLocation": map[string]any{"ResourceArn": "arn:aws:s3:::b"}}
	catalog := map[string]any{"Catalog": map[string]any{}}

	tests := []struct {
		resource map[string]any
		name     string
		perms    []any
		want     int
	}{
		{name: "select on table", resource: table, perms: []any{"SELECT"}, want: http.StatusOK},
		{name: "select on database", resource: db, perms: []any{"SELECT"}, want: http.StatusBadRequest},
		{name: "create table on database", resource: db, perms: []any{"CREATE_TABLE"}, want: http.StatusOK},
		{name: "create table on table", resource: table, perms: []any{"CREATE_TABLE"}, want: http.StatusBadRequest},
		{name: "insert on columns", resource: columns, perms: []any{"INSERT"}, want: http.StatusBadRequest},
		{name: "select on columns", resource: columns, perms: []any{"SELECT"}, want: http.StatusOK},
		{name: "location access", resource: location, perms: []any{"DATA_LOCATION_ACCESS"}, want: http.StatusOK},
		{name: "select on location", resource: location, perms: []any{"SELECT"}, want: http.StatusBadRequest},
		{name: "create database on catalog", resource: catalog, perms: []any{"CREATE_DATABASE"}, want: http.StatusOK},
		{name: "drop on catalog", resource: catalog, perms: []any{"DROP"}, want: http.StatusOK},
		{name: "alter on catalog", resource: catalog, perms: []any{"ALTER"}, want: http.StatusOK},
		{name: "super user on catalog", resource: catalog, perms: []any{"SUPER_USER"}, want: http.StatusOK},
		{name: "super user on database", resource: db, perms: []any{"SUPER_USER"}, want: http.StatusBadRequest},
		{name: "delete on table", resource: table, perms: []any{"DELETE"}, want: http.StatusOK},
		{name: "drop on database", resource: db, perms: []any{"DROP"}, want: http.StatusOK},
		{
			name:     "create database on database",
			resource: db,
			perms:    []any{"CREATE_DATABASE"},
			want:     http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := lakeformation.NewHandler(lakeformation.NewInMemoryBackend())
			rec := postJSON(t, h, "/GrantPermissions", map[string]any{
				"Principal":   map[string]any{"DataLakePrincipalIdentifier": "arn:aws:iam::123456789012:user/u"},
				"Resource":    tt.resource,
				"Permissions": tt.perms,
			})
			require.Equal(t, tt.want, rec.Code)

			if tt.want == http.StatusBadRequest {
				assert.NotContains(t, rec.Body.String(), "validation error")
			}
		})
	}
}

func TestListOps_PaginationInputValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
		path string
		want int
	}{
		{
			name: "bad permissions token", path: "/ListPermissions",
			body: map[string]any{"NextToken": "!!"}, want: http.StatusBadRequest,
		},
		{
			name: "max results zero", path: "/ListPermissions",
			body: map[string]any{"MaxResults": 0}, want: http.StatusBadRequest,
		},
		{
			name: "max results too large", path: "/ListResources",
			body: map[string]any{"MaxResults": 1001}, want: http.StatusBadRequest,
		},
		{
			name: "good bounds", path: "/ListResources",
			body: map[string]any{"MaxResults": 1000}, want: http.StatusOK,
		},
		{
			name: "bad lf tags token", path: "/ListLFTags",
			body: map[string]any{"NextToken": "xx"}, want: http.StatusBadRequest,
		},
		{
			name: "bad search token", path: "/SearchTablesByLFTags", want: http.StatusBadRequest,
			body: map[string]any{
				"NextToken": "xx", "Expression": []any{map[string]any{"TagKey": "k", "TagValues": []any{"v"}}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := lakeformation.NewHandler(lakeformation.NewInMemoryBackend())
			rec := postJSON(t, h, tt.path, tt.body)
			assert.Equal(t, tt.want, rec.Code)
		})
	}
}

func TestRegisterResource_RejectsNonARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		arn  string
		want int
	}{
		{name: "s3 arn", arn: "arn:aws:s3:::bucket/prefix", want: http.StatusOK},
		{name: "plain string", arn: "bucket", want: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := lakeformation.NewHandler(lakeformation.NewInMemoryBackend())
			rec := postJSON(t, h, "/RegisterResource", map[string]any{
				"ResourceArn": tt.arn, "UseServiceLinkedRole": true,
			})
			assert.Equal(t, tt.want, rec.Code)
		})
	}
}
