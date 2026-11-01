package lambdaweb

import (
	"net/http"
	"net/url"
	"strings"
)

const (
	apiPrefix = "/2025-03-07/"

	segFunctions     = "web-functions"
	segEndpoints     = "endpoints"
	segRevisions     = "revisions"
	segListEndpoints = "list-endpoints"
	segListRevisions = "list-revisions"
	segPolicy        = "resource-policy"
	segAccount       = "web-account-settings"
	segTags          = "tags"

	labelRouteLen = 2
)

// Operation names, matching the SDK.
const (
	opCreateWebFunction         = "CreateWebFunction"
	opGetWebFunction            = "GetWebFunction"
	opListWebFunctions          = "ListWebFunctions"
	opDeleteWebFunction         = "DeleteWebFunction"
	opCreateWebFunctionRevision = "CreateWebFunctionRevision"
	opGetWebFunctionRevision    = "GetWebFunctionRevision"
	opListWebFunctionRevisions  = "ListWebFunctionRevisions"
	opDeleteWebFunctionRevision = "DeleteWebFunctionRevision"
	opCreateWebFunctionEndpoint = "CreateWebFunctionEndpoint"
	opGetWebFunctionEndpoint    = "GetWebFunctionEndpoint"
	opListWebFunctionEndpoints  = "ListWebFunctionEndpoints"
	opUpdateWebFunctionEndpoint = "UpdateWebFunctionEndpoint"
	opDeleteWebFunctionEndpoint = "DeleteWebFunctionEndpoint"
	opPutResourcePolicy         = "PutResourcePolicy"
	opGetResourcePolicy         = "GetResourcePolicy"
	opDeleteResourcePolicy      = "DeleteResourcePolicy"
	opGetWebAccountSettings     = "GetWebAccountSettings"
	opTagResource               = "TagResource"
	opUntagResource             = "UntagResource"
	opListTags                  = "ListTags"

	opInvokeEndpoint = "InvokeWebFunctionEndpoint"
)

// operations lists every control-plane operation the SDK client exposes.
func operations() []string {
	return []string{
		opCreateWebFunction, opGetWebFunction, opListWebFunctions, opDeleteWebFunction,
		opCreateWebFunctionRevision, opGetWebFunctionRevision, opListWebFunctionRevisions, opDeleteWebFunctionRevision,
		opCreateWebFunctionEndpoint, opGetWebFunctionEndpoint, opListWebFunctionEndpoints, opUpdateWebFunctionEndpoint,
		opDeleteWebFunctionEndpoint, opPutResourcePolicy, opGetResourcePolicy, opDeleteResourcePolicy,
		opGetWebAccountSettings, opTagResource, opUntagResource, opListTags,
	}
}

// target is a parsed request: the operation and its path labels.
type target struct {
	op       string
	function string
	child    string
	resource string
}

// effectivePath returns the raw (percent-encoded) path so ARN labels keep their escaped slashes.
func effectivePath(r *http.Request) string {
	if r.URL.RawPath != "" {
		return r.URL.RawPath
	}

	return r.URL.Path
}

// parseRequest maps a method and API path onto an operation.
func parseRequest(method, path string) target {
	rest, ok := strings.CutPrefix(path, apiPrefix)
	if !ok {
		return target{}
	}

	segs := strings.Split(strings.TrimSuffix(rest, "/"), "/")
	for i, s := range segs {
		if u, err := url.PathUnescape(s); err == nil {
			segs[i] = u
		}
	}

	switch segs[0] {
	case segFunctions:
		return parseFunctionRoute(method, segs[1:])
	case segPolicy:
		return parseLabelRoute(method, segs, map[string]string{
			http.MethodPut: opPutResourcePolicy, http.MethodGet: opGetResourcePolicy,
			http.MethodDelete: opDeleteResourcePolicy,
		})
	case segTags:
		return parseLabelRoute(method, segs, map[string]string{
			http.MethodPost: opTagResource, http.MethodGet: opListTags, http.MethodDelete: opUntagResource,
		})
	case segAccount:
		if len(segs) == 1 && method == http.MethodGet {
			return target{op: opGetWebAccountSettings}
		}
	}

	return target{}
}

func parseLabelRoute(method string, segs []string, ops map[string]string) target {
	if len(segs) != labelRouteLen || segs[1] == "" {
		return target{}
	}

	if op, ok := ops[method]; ok {
		return target{op: op, resource: segs[1]}
	}

	return target{}
}

func parseFunctionRoute(method string, segs []string) target {
	switch len(segs) {
	case 0:
		switch method {
		case http.MethodPut:
			return target{op: opCreateWebFunction}
		case http.MethodPost:
			return target{op: opListWebFunctions}
		}
	case 1:
		switch method {
		case http.MethodGet:
			return target{op: opGetWebFunction, function: segs[0]}
		case http.MethodDelete:
			return target{op: opDeleteWebFunction, function: segs[0]}
		}
	case 2: //nolint:mnd // function label plus a collection segment
		return parseCollectionRoute(method, segs[0], segs[1])
	case 3: //nolint:mnd // function label, collection segment and item label
		return parseItemRoute(method, segs[0], segs[1], segs[2])
	}

	return target{}
}

func parseCollectionRoute(method, fn, seg string) target {
	ops := map[[2]string]string{
		{http.MethodPut, segEndpoints}:      opCreateWebFunctionEndpoint,
		{http.MethodPost, segListEndpoints}: opListWebFunctionEndpoints,
		{http.MethodPost, segRevisions}:     opCreateWebFunctionRevision,
		{http.MethodPost, segListRevisions}: opListWebFunctionRevisions,
	}

	if op, ok := ops[[2]string{method, seg}]; ok {
		return target{op: op, function: fn}
	}

	return target{}
}

func parseItemRoute(method, fn, seg, child string) target {
	ops := map[[2]string]string{
		{http.MethodGet, segEndpoints}:    opGetWebFunctionEndpoint,
		{http.MethodDelete, segEndpoints}: opDeleteWebFunctionEndpoint,
		{http.MethodPatch, segEndpoints}:  opUpdateWebFunctionEndpoint,
		{http.MethodGet, segRevisions}:    opGetWebFunctionRevision,
		{http.MethodDelete, segRevisions}: opDeleteWebFunctionRevision,
	}

	if op, ok := ops[[2]string{method, seg}]; ok && child != "" {
		return target{op: op, function: fn, child: child}
	}

	return target{}
}
