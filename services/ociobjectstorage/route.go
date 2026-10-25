package ociobjectstorage

import (
	"net/http"
	"net/url"
	"strings"
)

// Operation names, matching the OCI SDK operation names.
const (
	opGetNamespace         = "GetNamespace"
	opGetNamespaceMetadata = "GetNamespaceMetadata"
	opUpdateNamespaceMeta  = "UpdateNamespaceMetadata"
	opListBuckets          = "ListBuckets"
	opCreateBucket         = "CreateBucket"
	opGetBucket            = "GetBucket"
	opHeadBucket           = "HeadBucket"
	opUpdateBucket         = "UpdateBucket"
	opDeleteBucket         = "DeleteBucket"
	opListObjects          = "ListObjects"
	opListObjectVersions   = "ListObjectVersions"
	opPutObject            = "PutObject"
	opGetObject            = "GetObject"
	opHeadObject           = "HeadObject"
	opDeleteObject         = "DeleteObject"
	opCreateMultipart      = "CreateMultipartUpload"
	opUploadPart           = "UploadPart"
	opCommitMultipart      = "CommitMultipartUpload"
	opAbortMultipart       = "AbortMultipartUpload"
	opListMultipartUploads = "ListMultipartUploads"
	opListMultipartParts   = "ListMultipartUploadParts"
	opCopyObject           = "CopyObject"
	opRenameObject         = "RenameObject"
	opRestoreObjects       = "RestoreObjects"
	opUpdateStorageTier    = "UpdateObjectStorageTier"
	opBatchDeleteObjects   = "BatchDeleteObjects"
	opMakeBucketWritable   = "MakeBucketWritable"
	opCreatePAR            = "CreatePreauthenticatedRequest"
	opListPARs             = "ListPreauthenticatedRequests"
	opGetPAR               = "GetPreauthenticatedRequest"
	opDeletePAR            = "DeletePreauthenticatedRequest"
	opPutLifecycle         = "PutObjectLifecyclePolicy"
	opGetLifecycle         = "GetObjectLifecyclePolicy"
	opDeleteLifecycle      = "DeleteObjectLifecyclePolicy"
	opCreateRetention      = "CreateRetentionRule"
	opGetRetention         = "GetRetentionRule"
	opListRetention        = "ListRetentionRules"
	opUpdateRetention      = "UpdateRetentionRule"
	opDeleteRetention      = "DeleteRetentionRule"
	opGetWorkRequest       = "GetWorkRequest"
	opListWorkRequests     = "ListWorkRequests"
	opDeleteWorkRequest    = "CancelWorkRequest"
	opListWRErrors         = "ListWorkRequestErrors"
	opListWRLogs           = "ListWorkRequestLogs"
	opAdminKeys            = "ManageAPIKeys"
	opUnknown              = "Unknown"
)

type route struct {
	op       string
	ns       string
	bucket   string
	object   string
	id       string
	parToken string
}

const adminPrefix = "_gopherstack"

func unesc(s string) string {
	if v, err := url.PathUnescape(s); err == nil {
		return v
	}

	return s
}

// parseRoute maps method + path to an operation.
func parseRoute(r *http.Request) route {
	segs := strings.Split(strings.Trim(r.URL.EscapedPath(), "/"), "/")
	m := r.Method

	switch segs[0] {
	case adminPrefix:
		return route{op: opAdminKeys}
	case "p":
		return parsePAR(r, segs)
	case "workRequests":
		return parseWorkRequest(m, segs)
	case "n":
		return parseNamespace(m, segs)
	}

	return route{op: opUnknown}
}

func parsePAR(r *http.Request, segs []string) route {
	const minSegs = 6 // p/<token>/n/<ns>/b/<bucket>

	if len(segs) < minSegs || segs[2] != "n" || segs[4] != "b" {
		return route{op: opUnknown}
	}

	rt := route{parToken: unesc(segs[1]), ns: unesc(segs[3]), bucket: unesc(segs[5])}
	rest := segs[minSegs:]

	if len(rest) == 0 || rest[0] != "o" {
		return route{op: opUnknown}
	}

	obj := unesc(strings.Join(rest[1:], "/"))
	if obj == "" {
		if r.Method == http.MethodGet {
			rt.op = opListObjects
		} else {
			rt.op = opUnknown
		}

		return rt
	}

	rt.object = obj

	switch r.Method {
	case http.MethodGet:
		rt.op = opGetObject
	case http.MethodHead:
		rt.op = opHeadObject
	case http.MethodPut:
		rt.op = opPutObject
	default:
		rt.op = opUnknown
	}

	return rt
}

func parseWorkRequest(m string, segs []string) route {
	switch {
	case len(segs) == 1 && m == http.MethodGet:
		return route{op: opListWorkRequests}
	case len(segs) == 2 && m == http.MethodGet:
		return route{op: opGetWorkRequest, id: unesc(segs[1])}
	case len(segs) == 2 && m == http.MethodDelete:
		return route{op: opDeleteWorkRequest, id: unesc(segs[1])}
	case len(segs) == 3 && m == http.MethodGet && segs[2] == "errors":
		return route{op: opListWRErrors, id: unesc(segs[1])}
	case len(segs) == 3 && m == http.MethodGet && segs[2] == "logs":
		return route{op: opListWRLogs, id: unesc(segs[1])}
	}

	return route{op: opUnknown}
}

func parseNamespace(m string, segs []string) route {
	const (
		nsIdx     = 1
		bucketsAt = 2
		bucketIdx = 3
		subIdx    = 4
	)

	switch {
	case len(segs) == 1 && m == http.MethodGet:
		return route{op: opGetNamespace}
	case len(segs) == bucketsAt:
		rt := route{ns: unesc(segs[nsIdx])}

		switch m {
		case http.MethodGet:
			rt.op = opGetNamespaceMetadata
		case http.MethodPut:
			rt.op = opUpdateNamespaceMeta
		default:
			rt.op = opUnknown
		}

		return rt
	case segs[bucketsAt] != "b":
		return route{op: opUnknown}
	}

	rt := route{ns: unesc(segs[nsIdx])}

	if len(segs) == bucketIdx {
		rt.op = map[string]string{http.MethodGet: opListBuckets, http.MethodPost: opCreateBucket}[m]

		return orUnknown(rt)
	}

	rt.bucket = unesc(segs[bucketIdx])

	if len(segs) == subIdx {
		rt.op = map[string]string{
			http.MethodGet: opGetBucket, http.MethodHead: opHeadBucket,
			http.MethodPost: opUpdateBucket, http.MethodDelete: opDeleteBucket,
		}[m]

		return orUnknown(rt)
	}

	return parseBucketSub(rt, m, segs[subIdx:])
}

func orUnknown(rt route) route {
	if rt.op == "" {
		rt.op = opUnknown
	}

	return rt
}

func parseBucketSub(rt route, m string, sub []string) route {
	rest := sub[1:]

	switch sub[0] {
	case "o":
		if len(rest) == 0 || (len(rest) == 1 && rest[0] == "") {
			rt.op = map[string]string{http.MethodGet: opListObjects}[m]

			return orUnknown(rt)
		}

		rt.object = unesc(strings.Join(rest, "/"))
		rt.op = map[string]string{
			http.MethodGet: opGetObject, http.MethodHead: opHeadObject,
			http.MethodPut: opPutObject, http.MethodDelete: opDeleteObject,
		}[m]
	case "u":
		if len(rest) == 0 {
			rt.op = map[string]string{http.MethodGet: opListMultipartUploads, http.MethodPost: opCreateMultipart}[m]

			break
		}

		rt.object = unesc(strings.Join(rest, "/"))
		rt.op = map[string]string{
			http.MethodPut: opUploadPart, http.MethodPost: opCommitMultipart,
			http.MethodDelete: opAbortMultipart, http.MethodGet: opListMultipartParts,
		}[m]
	case "p":
		if len(rest) == 0 {
			rt.op = map[string]string{http.MethodPost: opCreatePAR, http.MethodGet: opListPARs}[m]

			break
		}

		rt.id = unesc(rest[0])
		rt.op = map[string]string{http.MethodGet: opGetPAR, http.MethodDelete: opDeletePAR}[m]
	default:
		return parseBucketConfig(rt, m, sub)
	}

	return orUnknown(rt)
}

func parseBucketConfig(rt route, m string, sub []string) route {
	rest := sub[1:]

	switch sub[0] {
	case "l":
		rt.op = map[string]string{
			http.MethodPut: opPutLifecycle, http.MethodGet: opGetLifecycle, http.MethodDelete: opDeleteLifecycle,
		}[m]
	case "retentionRules":
		if len(rest) == 0 {
			rt.op = map[string]string{http.MethodPost: opCreateRetention, http.MethodGet: opListRetention}[m]

			break
		}

		rt.id = unesc(rest[0])
		rt.op = map[string]string{
			http.MethodGet: opGetRetention, http.MethodPut: opUpdateRetention, http.MethodDelete: opDeleteRetention,
		}[m]
	case "objectversions":
		rt.op = map[string]string{http.MethodGet: opListObjectVersions}[m]
	case "actions":
		if len(rest) == 1 && m == http.MethodPost {
			rt.op = map[string]string{
				"copyObject": opCopyObject, "renameObject": opRenameObject, "restoreObjects": opRestoreObjects,
				"updateObjectStorageTier": opUpdateStorageTier, "batchDeleteObjects": opBatchDeleteObjects,
				"makeBucketWritable": opMakeBucketWritable,
			}[rest[0]]
		}
	}

	return orUnknown(rt)
}
