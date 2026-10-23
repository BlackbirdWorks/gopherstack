package gcpbigquery

import (
	"net/http"
	"net/url"
	"strings"
)

// Operation names, used for routing, metrics and GetSupportedOperations.
const (
	opListProjects    = "ListProjects"
	opInsertDataset   = "InsertDataset"
	opGetDataset      = "GetDataset"
	opListDatasets    = "ListDatasets"
	opPatchDataset    = "PatchDataset"
	opUpdateDataset   = "UpdateDataset"
	opDeleteDataset   = "DeleteDataset"
	opInsertTable     = "InsertTable"
	opGetTable        = "GetTable"
	opListTables      = "ListTables"
	opPatchTable      = "PatchTable"
	opUpdateTable     = "UpdateTable"
	opDeleteTable     = "DeleteTable"
	opListTableData   = "ListTableData"
	opInsertAll       = "InsertAll"
	opInsertJob       = "InsertJob"
	opUploadJob       = "UploadJob"
	opGetJob          = "GetJob"
	opListJobs        = "ListJobs"
	opCancelJob       = "CancelJob"
	opDeleteJob       = "DeleteJob"
	opQuery           = "Query"
	opGetQueryResults = "GetQueryResults"
	unknownOperation  = "Unknown"
	segProjects       = "projects"
	segDatasets       = "datasets"
	segTables         = "tables"
	segJobs           = "jobs"
	segQueries        = "queries"
	apiPrefix         = "bigquery/v2"
	uploadPrefix      = "upload/bigquery/v2"
	projectsOnly      = 1
	projectCollection = 3
	datasetItem       = 4
	datasetCollection = 5
	tableItem         = 6
	tableSubresource  = 7
	jobItem           = 4
	jobSubresource    = 5
)

type route struct {
	op      string
	project string
	dataset string
	table   string
	job     string
}

func splitSegments(escaped string) []string {
	escaped = strings.Trim(escaped, "/")
	if escaped == "" {
		return nil
	}

	segs := strings.Split(escaped, "/")
	for i, s := range segs {
		if u, err := url.PathUnescape(s); err == nil {
			segs[i] = u
		}
	}

	return segs
}

func stripPrefix(path, prefix string) (string, bool) {
	trimmed := strings.TrimPrefix(path, "/")

	if trimmed == prefix {
		return "", true
	}

	rest, ok := strings.CutPrefix(trimmed, prefix+"/")

	return rest, ok
}

func methodOp(method string, ops map[string]string) string {
	if op, ok := ops[method]; ok {
		return op
	}

	return unknownOperation
}

func parseRoute(r *http.Request) route {
	if rest, ok := stripPrefix(r.URL.EscapedPath(), uploadPrefix); ok {
		segs := splitSegments(rest)
		if len(segs) == projectCollection && segs[0] == segProjects && segs[2] == segJobs {
			return route{
				op: methodOp(
					r.Method,
					map[string]string{http.MethodPost: opUploadJob, http.MethodPut: opUploadJob},
				),
				project: segs[1],
			}
		}

		return route{op: unknownOperation}
	}

	path := r.URL.EscapedPath()
	if rest, ok := stripPrefix(path, apiPrefix); ok {
		path = rest
	}

	return apiRoute(r.Method, splitSegments(path))
}

func apiRoute(method string, s []string) route {
	if len(s) == 0 || s[0] != segProjects {
		return route{op: unknownOperation}
	}

	if len(s) == projectsOnly {
		return route{op: methodOp(method, map[string]string{http.MethodGet: opListProjects})}
	}

	rt := route{project: s[1], op: unknownOperation}
	if len(s) == projectsOnly+1 {
		return rt
	}

	switch s[2] {
	case segDatasets:
		return datasetRoute(method, s, rt)
	case segJobs:
		return jobRoute(method, s, rt)
	case segQueries:
		return queryRoute(method, s, rt)
	}

	return rt
}

func queryRoute(method string, s []string, rt route) route {
	switch len(s) {
	case projectCollection:
		rt.op = methodOp(method, map[string]string{http.MethodPost: opQuery})
	case datasetItem:
		rt.job = s[3]
		rt.op = methodOp(method, map[string]string{http.MethodGet: opGetQueryResults})
	}

	return rt
}

func jobRoute(method string, s []string, rt route) route {
	switch len(s) {
	case projectCollection:
		rt.op = methodOp(method, map[string]string{http.MethodGet: opListJobs, http.MethodPost: opInsertJob})
	case jobItem:
		rt.job = s[3]
		rt.op = methodOp(method, map[string]string{http.MethodGet: opGetJob})
	case jobSubresource:
		rt.job = s[3]

		switch {
		case s[4] == "cancel" && method == http.MethodPost:
			rt.op = opCancelJob
		case s[4] == "delete" && method == http.MethodDelete:
			rt.op = opDeleteJob
		}
	}

	return rt
}

func datasetRoute(method string, s []string, rt route) route {
	switch len(s) {
	case projectCollection:
		rt.op = methodOp(method, map[string]string{http.MethodGet: opListDatasets, http.MethodPost: opInsertDataset})

		return rt
	case datasetItem:
		rt.dataset = s[3]
		rt.op = methodOp(method, map[string]string{
			http.MethodGet: opGetDataset, http.MethodPatch: opPatchDataset, http.MethodPut: opUpdateDataset,
			http.MethodDelete: opDeleteDataset,
		})

		return rt
	}

	rt.dataset = s[3]
	if s[4] != segTables {
		return rt
	}

	return tableRoute(method, s, rt)
}

func tableRoute(method string, s []string, rt route) route {
	switch len(s) {
	case datasetCollection:
		rt.op = methodOp(method, map[string]string{http.MethodGet: opListTables, http.MethodPost: opInsertTable})
	case tableItem:
		rt.table = s[5]
		rt.op = methodOp(method, map[string]string{
			http.MethodGet: opGetTable, http.MethodPatch: opPatchTable, http.MethodPut: opUpdateTable,
			http.MethodDelete: opDeleteTable,
		})
	case tableSubresource:
		rt.table = s[5]

		switch {
		case s[6] == "data" && method == http.MethodGet:
			rt.op = opListTableData
		case s[6] == "insertAll" && method == http.MethodPost:
			rt.op = opInsertAll
		}
	}

	return rt
}
