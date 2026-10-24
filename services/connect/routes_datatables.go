package connect

import (
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func dataTableRoutes() []restroute.Route {
	const (
		list  = "/data-tables/{InstanceId}"
		table = list + "/{DataTableId}"
		attrs = table + "/attributes"
		attr  = attrs + "/{AttributeName}"
		vals  = table + "/values"
	)

	return []restroute.Route{
		{Method: http.MethodPut, Path: list, Op: "CreateDataTable"},
		{Method: http.MethodGet, Path: list, Op: "ListDataTables"},
		{Method: http.MethodGet, Path: table, Op: "DescribeDataTable"},
		{Method: http.MethodPost, Path: table, Op: "UpdateDataTableMetadata"},
		{Method: http.MethodDelete, Path: table, Op: "DeleteDataTable"},
		{Method: http.MethodPut, Path: attrs, Op: "CreateDataTableAttribute"},
		{Method: http.MethodPost, Path: attrs, Op: "ListDataTableAttributes"},
		{Method: http.MethodGet, Path: attr, Op: "DescribeDataTableAttribute"},
		{Method: http.MethodPost, Path: attr, Op: "UpdateDataTableAttribute"},
		{Method: http.MethodDelete, Path: attr, Op: "DeleteDataTableAttribute"},
		{Method: http.MethodPost, Path: vals + "/create", Op: "BatchCreateDataTableValue"},
		{Method: http.MethodPost, Path: vals + "/update", Op: "BatchUpdateDataTableValue"},
		{Method: http.MethodPost, Path: vals + "/delete", Op: "BatchDeleteDataTableValue"},
		{Method: http.MethodPost, Path: vals + "/describe", Op: "BatchDescribeDataTableValue"},
		{Method: http.MethodPost, Path: vals + "/list", Op: "ListDataTableValues"},
		{Method: http.MethodPost, Path: vals + "/list-primary", Op: "ListDataTablePrimaryValues"},
		{Method: http.MethodPost, Path: vals + "/evaluate", Op: "EvaluateDataTableValues"},
		{Method: http.MethodPost, Path: vals + "/update-primary", Op: "UpdateDataTablePrimaryValues"},
	}
}
