package opensearch

import (
	"net/http"
	"strings"
)

// OpenSearch transport action names the data plane maps its routes onto.
const (
	actionIndexDoc    = "indices:data/write/index"
	actionUpdateDoc   = "indices:data/write/update"
	actionDeleteDoc   = "indices:data/write/delete"
	actionBulk        = "indices:data/write/bulk"
	actionGetDoc      = "indices:data/read/get"
	actionSearch      = "indices:data/read/search"
	actionCreateIndex = "indices:admin/create"
	actionDeleteIndex = "indices:admin/delete"
	actionGetIndex    = "indices:admin/get"
	actionClusterMain = "cluster:monitor/main"
	actionClusterHlth = "cluster:monitor/health"
)

// Kinds of data-plane operation the enforcement layer treats specially.
const (
	opKindOther  = "other"
	opKindSearch = "search"
	opKindCount  = "count"
	opKindGet    = "get"
	opKindBulk   = "bulk"
)

// indexCheck is one index-level permission an operation needs.
type indexCheck struct {
	Index  string
	Action string
}

// dataOp is the permission footprint of a data-plane request.
type dataOp struct {
	Kind    string
	Index   string
	Cluster []string
	Indices []indexCheck
}

// normalizeDataPath strips the canonical "/index/" prefix and surrounding slashes.
func normalizeDataPath(p string) []string {
	p = strings.Trim(p, "/")
	if p == "" {
		return nil
	}

	segs := strings.Split(p, "/")
	if segs[0] == segIndex && len(segs) > 1 {
		segs = segs[1:]
	}

	return segs
}

func indexOpAction(method, op string) (string, string) {
	switch op {
	case indexOpSearch:
		return actionSearch, opKindSearch
	case indexOpCount:
		return actionSearch, opKindCount
	case indexOpDoc:
		switch method {
		case http.MethodGet, http.MethodHead:
			return actionGetDoc, opKindGet
		case http.MethodDelete:
			return actionDeleteDoc, opKindOther
		default:
			return actionIndexDoc, opKindOther
		}
	case "_create":
		return actionIndexDoc, opKindOther
	case "_update":
		return actionUpdateDoc, opKindOther
	default:
		return "indices:admin/" + strings.TrimPrefix(op, "_"), opKindOther
	}
}

func indexLevelAction(method string, segs []string) (string, string) {
	if len(segs) == 1 {
		switch method {
		case http.MethodGet, http.MethodHead:
			return actionGetIndex, opKindOther
		case http.MethodDelete:
			return actionDeleteIndex, opKindOther
		default:
			return actionCreateIndex, opKindOther
		}
	}

	return indexOpAction(method, segs[1])
}

// analyzeDataRequest maps a data-plane request onto the OpenSearch actions it needs. Bulk item
// permissions are checked per item by the bulk handler.
func analyzeDataRequest(method, p string) dataOp {
	segs := normalizeDataPath(p)

	switch {
	case len(segs) == 0:
		return dataOp{Kind: opKindOther, Cluster: []string{actionClusterMain}}
	case segs[0] == segBulk || (len(segs) > 1 && segs[1] == segBulk):
		return dataOp{Kind: opKindBulk, Cluster: []string{actionBulk}}
	case segs[0] == "_cluster" && len(segs) > 1 && segs[1] == "health":
		return dataOp{Kind: opKindOther, Cluster: []string{actionClusterHlth}}
	case strings.HasPrefix(segs[0], "_"):
		return dataOp{Kind: opKindOther, Cluster: []string{"cluster:admin/" + strings.TrimPrefix(segs[0], "_")}}
	}

	action, kind := indexLevelAction(method, segs)
	op := dataOp{Kind: kind, Index: segs[0]}

	for idx := range strings.SplitSeq(segs[0], ",") {
		op.Indices = append(op.Indices, indexCheck{Index: idx, Action: action})
	}

	return op
}

// denied returns the first action the permissions do not grant, or "".
func (o dataOp) denied(perms effectivePerms) string {
	for _, a := range o.Cluster {
		if !perms.AllowCluster(a) {
			return a
		}
	}

	for _, c := range o.Indices {
		if !perms.AllowIndex(c.Index, c.Action) {
			return c.Action
		}
	}

	return ""
}
