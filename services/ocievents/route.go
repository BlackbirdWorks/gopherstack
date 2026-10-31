package ocievents

import (
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// Operation names, matching the OCI SDK operation names.
const (
	opCreateRule        = "CreateRule"
	opGetRule           = "GetRule"
	opListRules         = "ListRules"
	opUpdateRule        = "UpdateRule"
	opDeleteRule        = "DeleteRule"
	opChangeRuleCompart = "ChangeRuleCompartment"
	opAdmin             = "Admin"
	opUnknown           = "Unknown"
	adminPrefix         = "/_gopherstack/"
	base                = "/20181201"
	rulePath            = base + "/rules/{ruleId}"
)

//nolint:gochecknoglobals // immutable route table
var router = restroute.New([]restroute.Route{
	{Method: http.MethodPost, Path: base + "/rules", Op: opCreateRule},
	{Method: http.MethodGet, Path: base + "/rules", Op: opListRules},
	{Method: http.MethodGet, Path: rulePath, Op: opGetRule},
	{Method: http.MethodPut, Path: rulePath, Op: opUpdateRule},
	{Method: http.MethodDelete, Path: rulePath, Op: opDeleteRule},
	{Method: http.MethodPost, Path: rulePath + "/actions/changeCompartment", Op: opChangeRuleCompart},
})

type route struct {
	params map[string]string
	op     string
}

// parseRoute maps method and path to an operation.
func parseRoute(r *http.Request) route {
	path := r.URL.EscapedPath()
	if strings.HasPrefix(path, adminPrefix) {
		return route{op: opAdmin}
	}

	op, params := router.Match(r.Method, path)
	if op == "" {
		op = opUnknown
	}

	return route{op: op, params: params}
}
