package azureappconfig

import (
	"net/http"
	"net/url"
	"strings"
)

const (
	opGetKeyValue    = "GetKeyValue"
	opPutKeyValue    = "PutKeyValue"
	opDeleteKeyValue = "DeleteKeyValue"
	opListKeyValues  = "GetKeyValues"
	opListKeys       = "GetKeys"
	opListLabels     = "GetLabels"
	opListRevisions  = "GetRevisions"
	opPutLock        = "PutLock"
	opDeleteLock     = "DeleteLock"
	opCreateSnapshot = "CreateSnapshot"
	opGetSnapshot    = "GetSnapshot"
	opUpdateSnapshot = "UpdateSnapshot"
	opListSnapshots  = "GetSnapshots"
	opGetOperation   = "GetOperationDetails"
	unknownOperation = "Unknown"

	storesPrefix = "stores"
)

// splitStorePrefix strips an optional /stores/{name} path prefix.
func splitStorePrefix(escaped string) (string, string) {
	rest, ok := strings.CutPrefix(strings.TrimPrefix(escaped, "/"), storesPrefix+"/")
	if !ok {
		return "", escaped
	}

	name, tail, _ := strings.Cut(rest, "/")
	if name == "" {
		return "", escaped
	}

	n, err := url.PathUnescape(name)
	if err != nil {
		return "", escaped
	}

	return n, "/" + tail
}

// routeSegments splits an escaped path into unescaped segments; the key of /kv/{key}
// and /locks/{key} may itself contain slashes, so the tail is rejoined by callers.
func routeSegments(escaped string) []string {
	var out []string

	for s := range strings.SplitSeq(escaped, "/") {
		if s == "" {
			continue
		}

		if u, err := url.PathUnescape(s); err == nil {
			s = u
		}

		out = append(out, s)
	}

	return out
}

func isRead(method string) bool { return method == http.MethodGet || method == http.MethodHead }

func operationOf(method string, segs []string) string {
	if len(segs) == 0 {
		return unknownOperation
	}

	switch strings.ToLower(segs[0]) {
	case "kv":
		return kvOperation(method, len(segs) > 1)
	case "keys":
		return opListKeys
	case "labels":
		return opListLabels
	case "revisions":
		return opListRevisions
	case "locks":
		if method == http.MethodDelete {
			return opDeleteLock
		}

		return opPutLock
	case "snapshots":
		return snapshotOperation(method, len(segs) > 1)
	case "operations":
		return opGetOperation
	}

	return unknownOperation
}

func kvOperation(method string, single bool) string {
	switch {
	case !single:
		return opListKeyValues
	case method == http.MethodPut:
		return opPutKeyValue
	case method == http.MethodDelete:
		return opDeleteKeyValue
	}

	return opGetKeyValue
}

func snapshotOperation(method string, single bool) string {
	switch {
	case !single:
		return opListSnapshots
	case method == http.MethodPut:
		return opCreateSnapshot
	case method == http.MethodPatch:
		return opUpdateSnapshot
	}

	return opGetSnapshot
}
