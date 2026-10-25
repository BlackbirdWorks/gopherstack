package ociobjectstorage

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var opTable = map[string]opFunc{
	opGetNamespace: (*Handler).getNamespace, opGetNamespaceMetadata: (*Handler).getNamespaceMeta,
	opUpdateNamespaceMeta: (*Handler).updateNamespaceMeta,
	opListBuckets:         (*Handler).listBuckets, opCreateBucket: (*Handler).createBucket,
	opGetBucket: (*Handler).getBucket, opHeadBucket: (*Handler).getBucket,
	opUpdateBucket: (*Handler).updateBucket, opDeleteBucket: (*Handler).deleteBucket,
	opMakeBucketWritable: (*Handler).makeBucketWritable,
	opListObjects:        (*Handler).listObjects, opListObjectVersions: (*Handler).listObjectVersions,
	opPutObject: (*Handler).putObject, opGetObject: (*Handler).getObject, opHeadObject: (*Handler).getObject,
	opDeleteObject: (*Handler).deleteObject, opCopyObject: (*Handler).copyObject,
	opRenameObject: (*Handler).renameObject, opRestoreObjects: (*Handler).restoreObjects,
	opUpdateStorageTier: (*Handler).updateStorageTier, opBatchDeleteObjects: (*Handler).batchDelete,
	opCreateMultipart: (*Handler).createUpload, opUploadPart: (*Handler).uploadPart,
	opCommitMultipart: (*Handler).commitUpload, opAbortMultipart: (*Handler).abortUpload,
	opListMultipartUploads: (*Handler).listUploads, opListMultipartParts: (*Handler).listParts,
	opCreatePAR: (*Handler).createPAR, opListPARs: (*Handler).listPARs,
	opGetPAR: (*Handler).getPAR, opDeletePAR: (*Handler).deletePAR,
	opPutLifecycle: (*Handler).putLifecycle, opGetLifecycle: (*Handler).getLifecycle,
	opDeleteLifecycle: (*Handler).deleteLifecycle,
	opCreateRetention: (*Handler).createRetention, opGetRetention: (*Handler).getRetention,
	opListRetention: (*Handler).listRetention, opUpdateRetention: (*Handler).updateRetention,
	opDeleteRetention: (*Handler).deleteRetention,
	opGetWorkRequest: (*Handler).getWorkRequest, opListWorkRequests: (*Handler).listWorkRequests,
	opDeleteWorkRequest: (*Handler).cancelWorkRequest,
	opListWRErrors:      (*Handler).listWRErrors, opListWRLogs: (*Handler).listWRLogs,
}

func (q *req) query(k string) string { return q.r.URL.Query().Get(k) }

func (q *req) limit() int {
	n, _ := strconv.Atoi(q.query("limit"))

	return n
}

func (q *req) pre() Preconds {
	return Preconds{IfMatch: q.r.Header.Get("If-Match"), IfNoneMatch: q.r.Header.Get("If-None-Match")}
}

func (q *req) decode(v any) error {
	if len(q.body) == 0 {
		return errMissing("A request body is required")
	}

	if err := json.Unmarshal(q.body, v); err != nil {
		return newErr(http.StatusBadRequest, "CannotParseRequest", "The request body is not valid JSON for this operation")
	}

	return nil
}

func (q *req) nextPage(next string) {
	if next != "" {
		q.header("opc-next-page", next)
	}
}

func httpDate(t time.Time) string { return t.UTC().Format(http.TimeFormat) }

func (h *Handler) getNamespace(q *req) error { return q.json(http.StatusOK, h.Backend.Namespace()) }

type nsMetaWire struct {
	Namespace                 string `json:"namespace"`
	DefaultS3CompartmentID    string `json:"defaultS3CompartmentId"`
	DefaultSwiftCompartmentID string `json:"defaultSwiftCompartmentId"`
}

func (h *Handler) getNamespaceMeta(q *req) error {
	m, err := h.Backend.NamespaceMetadata(q.rt.ns)
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, m)
}

func (h *Handler) updateNamespaceMeta(q *req) error {
	var in nsMetaWire
	if err := q.decode(&in); err != nil {
		return err
	}

	m, err := h.Backend.UpdateNamespaceMetadata(q.rt.ns, in)
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, m)
}

func (h *Handler) listBuckets(q *req) error {
	p, err := h.Backend.ListBuckets(q.rt.ns, q.query("compartmentId"), q.query("page"), q.limit())
	if err != nil {
		return err
	}

	q.nextPage(p.Next)

	return q.json(http.StatusOK, p.Data)
}

func fieldList(s string) []string {
	if s == "" {
		return nil
	}

	return strings.Split(s, ",")
}

func (h *Handler) createBucket(q *req) error {
	var in CreateBucketInput
	if err := q.decode(&in); err != nil {
		return err
	}

	w, err := h.Backend.CreateBucket(q.rt.ns, in, q.user())
	if err != nil {
		return err
	}

	q.header("ETag", w.Etag)
	q.header("Location", "/n/"+q.rt.ns+"/b/"+w.Name)

	return q.json(http.StatusOK, w)
}

func (h *Handler) getBucket(q *req) error {
	w, err := h.Backend.GetBucket(q.rt.ns, q.rt.bucket, fieldList(q.query("fields")), q.pre())
	if err != nil {
		return err
	}

	q.header("ETag", w.Etag)

	if q.rt.op == opHeadBucket {
		return q.status(http.StatusOK)
	}

	return q.json(http.StatusOK, w)
}

func (h *Handler) updateBucket(q *req) error {
	var in UpdateBucketInput
	if err := q.decode(&in); err != nil {
		return err
	}

	w, err := h.Backend.UpdateBucket(q.rt.ns, q.rt.bucket, in, q.pre())
	if err != nil {
		return err
	}

	q.header("ETag", w.Etag)

	return q.json(http.StatusOK, w)
}

func (h *Handler) deleteBucket(q *req) error {
	if err := h.Backend.DeleteBucket(q.rt.ns, q.rt.bucket, q.pre()); err != nil {
		return err
	}

	return q.status(http.StatusNoContent)
}

func (h *Handler) makeBucketWritable(q *req) error {
	if _, err := h.Backend.GetBucket(q.rt.ns, q.rt.bucket, nil, Preconds{}); err != nil {
		return err
	}

	return q.status(http.StatusOK)
}

func (h *Handler) listObjects(q *req) error {
	opt := ListOptions{
		Prefix: q.query("prefix"), Start: q.query("start"), End: q.query("end"),
		StartAfter: q.query("startAfter"), Delimiter: q.query("delimiter"), Limit: q.limit(),
	}

	objs, prefixes, next, err := h.Backend.ListObjects(q.rt.ns, q.rt.bucket, opt)
	if err != nil {
		return err
	}

	fields := map[string]bool{"name": true}
	for _, f := range fieldList(q.query("fields")) {
		fields[strings.TrimSpace(f)] = true
	}

	out := make([]map[string]any, len(objs))

	for i := range objs {
		out[i] = summary(&objs[i], fields)
	}

	resp := map[string]any{"objects": out}
	if len(prefixes) > 0 {
		resp["prefixes"] = prefixes
	}

	if next != "" {
		resp["nextStartWith"] = next
	}

	return q.json(http.StatusOK, resp)
}

func summary(o *ObjectInfo, fields map[string]bool) map[string]any {
	m := map[string]any{"name": o.Name}

	if fields["size"] {
		m["size"] = len(o.Data)
	}

	if fields["etag"] {
		m["etag"] = o.Etag
	}

	if fields["md5"] && o.MultipartMD5 == "" {
		m["md5"] = o.MD5
	}

	if fields["timeCreated"] {
		m["timeCreated"] = o.TimeCreated
	}

	if fields["timeModified"] {
		m["timeModified"] = o.TimeModified
	}

	if fields["storageTier"] {
		m["storageTier"] = o.Meta.StorageTier
	}

	if fields["archivalState"] && o.Archival != "" {
		m["archivalState"] = o.Archival
	}

	return m
}

func (h *Handler) listObjectVersions(q *req) error {
	opt := ListOptions{
		Prefix: q.query("prefix"), Start: q.query("start"), End: q.query("end"), StartAfter: q.query("startAfter"),
		Delimiter: q.query("delimiter"), Page: q.query("page"), Limit: q.limit(),
	}

	pg, prefixes, err := h.Backend.ListObjectVersions(q.rt.ns, q.rt.bucket, opt)
	if err != nil {
		return err
	}

	fields := map[string]bool{"name": true}
	for _, f := range fieldList(q.query("fields")) {
		fields[strings.TrimSpace(f)] = true
	}

	items := make([]map[string]any, len(pg.Data))

	for i := range pg.Data {
		o := &pg.Data[i]
		m := summary(o, fields)
		m["versionId"] = o.VersionID
		m["isDeleteMarker"] = o.IsDeleteMarker
		m["timeModified"] = o.TimeModified
		items[i] = m
	}

	q.nextPage(pg.Next)

	resp := map[string]any{"items": items}
	if len(prefixes) > 0 {
		resp["prefixes"] = prefixes
	}

	return q.json(http.StatusOK, resp)
}

const metaPrefix = "opc-meta-"

func (q *req) objectMeta() ObjectMeta {
	h := q.r.Header
	m := ObjectMeta{
		ContentType: h.Get("Content-Type"), ContentLanguage: h.Get("Content-Language"),
		ContentEncoding: h.Get("Content-Encoding"), ContentDisposition: h.Get("Content-Disposition"),
		CacheControl: h.Get("Cache-Control"), StorageTier: h.Get("storage-tier"),
	}

	for k, v := range h {
		if lk := strings.ToLower(k); strings.HasPrefix(lk, metaPrefix) && len(v) > 0 {
			if m.Metadata == nil {
				m.Metadata = map[string]string{}
			}

			m.Metadata[strings.TrimPrefix(lk, metaPrefix)] = v[0]
		}
	}

	return m
}

func (q *req) objectHeaders(o *ObjectInfo) {
	q.header("ETag", o.Etag)
	q.header("Last-Modified", httpDate(o.TimeModified))
	q.header("version-id", o.VersionID)
	q.header("Content-Type", o.Meta.ContentType)
	q.header("storage-tier", o.Meta.StorageTier)

	if o.MultipartMD5 != "" {
		q.header("opc-multipart-md5", o.MultipartMD5)
	} else {
		q.header("Content-MD5", o.MD5)
	}

	for k, v := range map[string]string{
		"Content-Language": o.Meta.ContentLanguage, "Content-Encoding": o.Meta.ContentEncoding,
		"Content-Disposition": o.Meta.ContentDisposition, "Cache-Control": o.Meta.CacheControl,
		"archival-state": o.Archival,
	} {
		if v != "" {
			q.header(k, v)
		}
	}

	for k, v := range o.Meta.Metadata {
		q.header(metaPrefix+k, v)
	}
}

func (h *Handler) putObject(q *req) error {
	o, err := h.Backend.PutObject(
		q.rt.ns, q.rt.bucket, q.rt.object, q.body, q.objectMeta(), q.pre(), q.r.Header.Get("Content-MD5"))
	if err != nil {
		return err
	}

	q.header("ETag", o.Etag)
	q.header("opc-content-md5", o.MD5)
	q.header("Last-Modified", httpDate(o.TimeModified))
	q.header("version-id", o.VersionID)

	return q.status(http.StatusOK)
}

func (h *Handler) getObject(q *req) error {
	head := q.rt.op == opHeadObject

	o, err := h.Backend.GetObject(q.rt.ns, q.rt.bucket, q.rt.object, q.query("versionId"), q.pre(), !head)
	if err != nil {
		return err
	}

	q.objectHeaders(&o)
	q.header("Accept-Ranges", "bytes")

	for qk, hk := range map[string]string{
		"httpResponseContentType": "Content-Type", "httpResponseContentDisposition": "Content-Disposition",
		"httpResponseCacheControl": "Cache-Control", "httpResponseContentLanguage": "Content-Language",
		"httpResponseContentEncoding": "Content-Encoding", "httpResponseExpires": "Expires",
	} {
		if v := q.query(qk); v != "" {
			q.header(hk, v)
		}
	}

	data, status := o.Data, http.StatusOK

	if rng := q.r.Header.Get("Range"); rng != "" {
		start, end, ok := parseRange(rng, int64(len(o.Data)))
		if !ok {
			return newErr(http.StatusRequestedRangeNotSatisfiable, "InvalidRange", "The requested range is not satisfiable")
		}

		data, status = o.Data[start:end+1], http.StatusPartialContent
		q.header("Content-Range", "bytes "+strconv.FormatInt(start, 10)+"-"+strconv.FormatInt(end, 10)+"/"+strconv.Itoa(len(o.Data)))
	}

	q.header("Content-Length", strconv.Itoa(len(data)))
	q.w.WriteHeader(status)

	if !head {
		_, _ = q.w.Write(data)
	}

	return nil
}

// parseRange handles bytes=a-b, bytes=a- and bytes=-n; end is inclusive.
func parseRange(h string, size int64) (start, end int64, ok bool) {
	spec, found := strings.CutPrefix(h, "bytes=")
	if !found || strings.Contains(spec, ",") {
		return 0, 0, false
	}

	a, b, found := strings.Cut(spec, "-")
	if !found {
		return 0, 0, false
	}

	switch {
	case a == "":
		n, err := strconv.ParseInt(b, 10, 64)
		if err != nil || n <= 0 || size == 0 {
			return 0, 0, false
		}

		return max(size-n, 0), size - 1, true
	default:
		s, err := strconv.ParseInt(a, 10, 64)
		if err != nil || s < 0 || s >= size {
			return 0, 0, false
		}

		e := size - 1

		if b != "" {
			v, perr := strconv.ParseInt(b, 10, 64)
			if perr != nil || v < s {
				return 0, 0, false
			}

			e = min(v, size-1)
		}

		return s, e, true
	}
}

func (h *Handler) deleteObject(q *req) error {
	res, err := h.Backend.DeleteObject(q.rt.ns, q.rt.bucket, q.rt.object, q.query("versionId"), q.r.Header.Get("If-Match"))
	if err != nil {
		return err
	}

	q.header("Last-Modified", httpDate(res.LastModified))
	q.header("version-id", res.VersionID)
	q.header("is-delete-marker", strconv.FormatBool(res.IsDeleteMarker))

	return q.status(http.StatusNoContent)
}

func (h *Handler) copyObject(q *req) error {
	var in CopyInput
	if err := q.decode(&in); err != nil {
		return err
	}

	id, err := h.Backend.CopyObject(q.rt.ns, q.rt.bucket, in)
	if err != nil {
		return err
	}

	q.header("opc-work-request-id", id)

	return q.status(http.StatusAccepted)
}

func (h *Handler) renameObject(q *req) error {
	var in RenameInput
	if err := q.decode(&in); err != nil {
		return err
	}

	o, err := h.Backend.RenameObject(q.rt.ns, q.rt.bucket, in)
	if err != nil {
		return err
	}

	q.header("ETag", o.Etag)
	q.header("Last-Modified", httpDate(o.TimeModified))
	q.header("version-id", o.VersionID)

	return q.status(http.StatusOK)
}

func (h *Handler) restoreObjects(q *req) error {
	var in struct {
		ObjectName string `json:"objectName"`
		VersionID  string `json:"versionId"`
		Hours      int    `json:"hours"`
	}
	if err := q.decode(&in); err != nil {
		return err
	}

	if err := h.Backend.RestoreObjects(q.rt.ns, q.rt.bucket, in.ObjectName, in.VersionID, in.Hours); err != nil {
		return err
	}

	return q.status(http.StatusAccepted)
}

func (h *Handler) updateStorageTier(q *req) error {
	var in struct {
		ObjectName  string `json:"objectName"`
		StorageTier string `json:"storageTier"`
		VersionID   string `json:"versionId"`
	}
	if err := q.decode(&in); err != nil {
		return err
	}

	if in.ObjectName == "" {
		return errMissing("objectName is required")
	}

	if err := h.Backend.UpdateStorageTier(q.rt.ns, q.rt.bucket, in.ObjectName, in.VersionID, in.StorageTier); err != nil {
		return err
	}

	return q.status(http.StatusOK)
}

func (h *Handler) batchDelete(q *req) error {
	var in struct {
		IsSkipDeletedResult *bool       `json:"isSkipDeletedResult"`
		Objects             []BatchItem `json:"objects"`
	}
	if err := q.decode(&in); err != nil {
		return err
	}

	deleted, failed, err := h.Backend.BatchDelete(q.rt.ns, q.rt.bucket, in.Objects)
	if err != nil {
		return err
	}

	if in.IsSkipDeletedResult != nil && *in.IsSkipDeletedResult {
		deleted = []BatchDeleted{}
	}

	return q.json(http.StatusOK, map[string]any{"deleted": deleted, "failed": failed})
}

func (h *Handler) createUpload(q *req) error {
	var in CreateUploadInput
	if err := q.decode(&in); err != nil {
		return err
	}

	u, err := h.Backend.CreateUpload(q.rt.ns, q.rt.bucket, in, q.pre())
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, UploadInfo(q.rt.ns, q.rt.bucket, u))
}

func (h *Handler) uploadPart(q *req) error {
	num, err := strconv.Atoi(q.query("uploadPartNum"))
	if err != nil {
		return errParam("uploadPartNum must be an integer")
	}

	etag, sum, err := h.Backend.UploadPart(
		q.rt.ns, q.rt.bucket, q.rt.object, q.query("uploadId"), num, q.body, q.r.Header.Get("Content-MD5"))
	if err != nil {
		return err
	}

	q.header("ETag", etag)
	q.header("opc-content-md5", sum)

	return q.status(http.StatusOK)
}

func (h *Handler) commitUpload(q *req) error {
	var in CommitInput
	if err := q.decode(&in); err != nil {
		return err
	}

	o, err := h.Backend.CommitUpload(q.rt.ns, q.rt.bucket, q.rt.object, q.query("uploadId"), in, q.pre())
	if err != nil {
		return err
	}

	q.header("ETag", o.Etag)
	q.header("opc-multipart-md5", o.MultipartMD5)
	q.header("Last-Modified", httpDate(o.TimeModified))
	q.header("version-id", o.VersionID)

	return q.status(http.StatusOK)
}

func (h *Handler) abortUpload(q *req) error {
	if err := h.Backend.AbortUpload(q.rt.ns, q.rt.bucket, q.rt.object, q.query("uploadId")); err != nil {
		return err
	}

	return q.status(http.StatusNoContent)
}

func (h *Handler) listUploads(q *req) error {
	p, err := h.Backend.ListUploads(q.rt.ns, q.rt.bucket, q.query("page"), q.limit())
	if err != nil {
		return err
	}

	q.nextPage(p.Next)

	return q.json(http.StatusOK, p.Data)
}

func (h *Handler) listParts(q *req) error {
	p, err := h.Backend.ListParts(q.rt.ns, q.rt.bucket, q.rt.object, q.query("uploadId"), q.query("page"), q.limit())
	if err != nil {
		return err
	}

	q.nextPage(p.Next)

	return q.json(http.StatusOK, p.Data)
}

func (h *Handler) createPAR(q *req) error {
	var in CreatePARInput
	if err := q.decode(&in); err != nil {
		return err
	}

	w, err := h.Backend.CreatePAR(q.rt.ns, q.rt.bucket, in)
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, struct {
		PARWire
		FullPath string `json:"fullPath"`
	}{w, "http://" + q.r.Host + w.AccessURI})
}

func (h *Handler) listPARs(q *req) error {
	p, err := h.Backend.ListPARs(q.rt.ns, q.rt.bucket, q.query("objectNamePrefix"), q.query("page"), q.limit())
	if err != nil {
		return err
	}

	q.nextPage(p.Next)

	return q.json(http.StatusOK, p.Data)
}

func (h *Handler) getPAR(q *req) error {
	w, err := h.Backend.GetPAR(q.rt.ns, q.rt.bucket, q.rt.id)
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, w)
}

func (h *Handler) deletePAR(q *req) error {
	if err := h.Backend.DeletePAR(q.rt.ns, q.rt.bucket, q.rt.id); err != nil {
		return err
	}

	return q.status(http.StatusNoContent)
}

func (h *Handler) putLifecycle(q *req) error {
	var in struct {
		Items []LifecycleRule `json:"items"`
	}
	if err := q.decode(&in); err != nil {
		return err
	}

	w, etag, err := h.Backend.PutLifecycle(q.rt.ns, q.rt.bucket, in.Items, q.pre())
	if err != nil {
		return err
	}

	q.header("ETag", etag)

	return q.json(http.StatusOK, w)
}

func (h *Handler) getLifecycle(q *req) error {
	w, etag, err := h.Backend.GetLifecycle(q.rt.ns, q.rt.bucket)
	if err != nil {
		return err
	}

	q.header("ETag", etag)

	return q.json(http.StatusOK, w)
}

func (h *Handler) deleteLifecycle(q *req) error {
	if err := h.Backend.DeleteLifecycle(q.rt.ns, q.rt.bucket, q.pre()); err != nil {
		return err
	}

	return q.status(http.StatusNoContent)
}

func (h *Handler) createRetention(q *req) error {
	var in RetentionInput
	if err := q.decode(&in); err != nil {
		return err
	}

	w, err := h.Backend.CreateRetention(q.rt.ns, q.rt.bucket, in)
	if err != nil {
		return err
	}

	q.header("ETag", w.Etag)

	return q.json(http.StatusOK, w)
}

func (h *Handler) getRetention(q *req) error {
	w, err := h.Backend.GetRetention(q.rt.ns, q.rt.bucket, q.rt.id)
	if err != nil {
		return err
	}

	q.header("ETag", w.Etag)

	return q.json(http.StatusOK, w)
}

func (h *Handler) listRetention(q *req) error {
	p, err := h.Backend.ListRetention(q.rt.ns, q.rt.bucket, q.query("page"))
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, map[string]any{"items": p.Data})
}

func (h *Handler) updateRetention(q *req) error {
	var in RetentionInput
	if err := q.decode(&in); err != nil {
		return err
	}

	w, err := h.Backend.UpdateRetention(q.rt.ns, q.rt.bucket, q.rt.id, in, q.pre())
	if err != nil {
		return err
	}

	q.header("ETag", w.Etag)

	return q.json(http.StatusOK, w)
}

func (h *Handler) deleteRetention(q *req) error {
	if err := h.Backend.DeleteRetention(q.rt.ns, q.rt.bucket, q.rt.id, q.pre()); err != nil {
		return err
	}

	return q.status(http.StatusNoContent)
}

func (h *Handler) getWorkRequest(q *req) error {
	w, err := h.Backend.GetWorkRequest(q.rt.id)
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, w)
}

func (h *Handler) listWorkRequests(q *req) error {
	p, err := h.Backend.ListWorkRequests(q.query("compartmentId"), q.query("page"), q.limit())
	if err != nil {
		return err
	}

	q.nextPage(p.Next)

	return q.json(http.StatusOK, p.Data)
}

func (h *Handler) cancelWorkRequest(q *req) error {
	if err := h.Backend.DeleteWorkRequest(q.rt.id); err != nil {
		return err
	}

	return q.status(http.StatusNoContent)
}

func (h *Handler) listWRErrors(q *req) error {
	if _, err := h.Backend.GetWorkRequest(q.rt.id); err != nil {
		return err
	}

	return q.json(http.StatusOK, []any{})
}

func (h *Handler) listWRLogs(q *req) error {
	w, err := h.Backend.GetWorkRequest(q.rt.id)
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, []map[string]any{{"message": "Copy completed", "timestamp": w.TimeFinished}})
}
