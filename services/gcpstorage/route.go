package gcpstorage

import (
	"net/http"
	"net/url"
	"strings"
)

// Operation names, used for routing, metrics and GetSupportedOperations.
const (
	opListBuckets         = "ListBuckets"
	opInsertBucket        = "InsertBucket"
	opGetBucket           = "GetBucket"
	opPatchBucket         = "PatchBucket"
	opUpdateBucket        = "UpdateBucket"
	opDeleteBucket        = "DeleteBucket"
	opGetIamPolicy        = "GetBucketIamPolicy"
	opSetIamPolicy        = "SetBucketIamPolicy"
	opTestIamPermissions  = "TestBucketIamPermissions"
	opListObjects         = "ListObjects"
	opGetObject           = "GetObject"
	opPatchObject         = "PatchObject"
	opUpdateObject        = "UpdateObject"
	opDeleteObject        = "DeleteObject"
	opCopyObject          = "CopyObject"
	opRewriteObject       = "RewriteObject"
	opComposeObject       = "ComposeObject"
	opInsertObject        = "InsertObject"
	opResumableUpload     = "ResumableUpload"
	opCancelUpload        = "CancelResumableUpload"
	opDownloadObject      = "DownloadObject"
	opXMLGetObject        = "XMLGetObject"
	opXMLHeadObject       = "XMLHeadObject"
	opXMLPutObject        = "XMLPutObject"
	opXMLDeleteObject     = "XMLDeleteObject"
	unknownOperation      = "Unknown"
	segB                  = "b"
	segO                  = "o"
	segIam                = "iam"
	copyRouteSegments     = 9
	composeRouteSegments  = 5
	objectRouteSegments   = 4
	bucketSubRouteSegment = 3
)

type route struct {
	op        string
	bucket    string
	object    string
	dstBucket string
	dstObject string
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

func hasPrefix(segs []string, prefix ...string) bool {
	if len(segs) < len(prefix) {
		return false
	}

	for i, p := range prefix {
		if segs[i] != p {
			return false
		}
	}

	return true
}

func parseRoute(r *http.Request) route {
	segs := splitSegments(r.URL.EscapedPath())

	switch {
	case hasPrefix(segs, "storage", "v1"):
		return jsonRoute(r.Method, segs[2:])
	case hasPrefix(segs, "upload", "storage", "v1"):
		return uploadRoute(r.Method, segs[3:], r.URL.Query().Has("upload_id"))
	case hasPrefix(segs, "download", "storage", "v1"):
		s := segs[3:]
		if r.Method == http.MethodGet && len(s) == objectRouteSegments && s[0] == segB && s[2] == segO {
			return route{op: opDownloadObject, bucket: s[1], object: s[3]}
		}

		return route{op: unknownOperation}
	default:
		return xmlRoute(r.Method, segs)
	}
}

func uploadRoute(method string, s []string, session bool) route {
	if len(s) != bucketSubRouteSegment || s[0] != segB || s[2] != segO {
		return route{op: unknownOperation}
	}

	rt := route{bucket: s[1], op: unknownOperation}

	switch {
	case method == http.MethodPost && !session:
		rt.op = opInsertObject
	case method == http.MethodPut || method == http.MethodPost:
		rt.op = opResumableUpload
	case method == http.MethodDelete:
		rt.op = opCancelUpload
	}

	return rt
}

func xmlRoute(method string, segs []string) route {
	const minXMLSegments = 2

	if len(segs) < minXMLSegments {
		return route{op: unknownOperation}
	}

	rt := route{bucket: segs[0], object: strings.Join(segs[1:], "/"), op: unknownOperation}

	switch method {
	case http.MethodGet:
		rt.op = opXMLGetObject
	case http.MethodHead:
		rt.op = opXMLHeadObject
	case http.MethodPut:
		rt.op = opXMLPutObject
	case http.MethodDelete:
		rt.op = opXMLDeleteObject
	}

	return rt
}

func methodOp(method string, ops map[string]string) string {
	if op, ok := ops[method]; ok {
		return op
	}

	return unknownOperation
}

func jsonRoute(method string, s []string) route {
	if len(s) == 0 || s[0] != segB {
		return route{op: unknownOperation}
	}

	if len(s) == 1 {
		return route{op: methodOp(method, map[string]string{
			http.MethodGet: opListBuckets, http.MethodPost: opInsertBucket,
		})}
	}

	rt := route{bucket: s[1], op: unknownOperation}

	switch {
	case len(s) == 2: //nolint:mnd // /b/{bucket}
		rt.op = methodOp(method, map[string]string{
			http.MethodGet: opGetBucket, http.MethodPatch: opPatchBucket,
			http.MethodPut: opUpdateBucket, http.MethodDelete: opDeleteBucket,
		})
	case len(s) == bucketSubRouteSegment && s[2] == segO:
		rt.op = methodOp(method, map[string]string{http.MethodGet: opListObjects})
	case len(s) == bucketSubRouteSegment && s[2] == segIam:
		rt.op = methodOp(method, map[string]string{http.MethodGet: opGetIamPolicy, http.MethodPut: opSetIamPolicy})
	case len(s) == objectRouteSegments && s[2] == segIam && s[3] == "testPermissions":
		rt.op = methodOp(method, map[string]string{http.MethodGet: opTestIamPermissions})
	default:
		rt = objectRoute(method, s)
	}

	return rt
}

func objectRoute(method string, s []string) route {
	rt := route{bucket: s[1], op: unknownOperation}
	if len(s) < objectRouteSegments || s[2] != segO {
		return rt
	}

	rt.object = s[3]

	switch {
	case len(s) == objectRouteSegments:
		rt.op = methodOp(method, map[string]string{
			http.MethodGet: opGetObject, http.MethodPatch: opPatchObject,
			http.MethodPut: opUpdateObject, http.MethodDelete: opDeleteObject,
		})
	case len(s) == composeRouteSegments && s[4] == "compose" && method == http.MethodPost:
		rt.op = opComposeObject
	case len(s) == copyRouteSegments && s[5] == segB && s[7] == segO && method == http.MethodPost:
		rt.dstBucket, rt.dstObject = s[6], s[8]

		switch s[4] {
		case "copyTo":
			rt.op = opCopyObject
		case "rewriteTo":
			rt.op = opRewriteObject
		}
	}

	return rt
}
