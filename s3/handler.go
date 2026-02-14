package s3

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	pathSplitParts   = 2
	tagKeyValueParts = 2
	defaultMaxKeys   = 1000

	locationConstraintXML = `<LocationConstraint xmlns=` +
		`"http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`
)

// Handler implements [http.Handler] for S3-compatible API requests.
type Handler struct {
	Backend StorageBackend
}

// NewHandler creates a new S3 Handler with the given backend.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{
		Backend: backend,
	}
}

// ServeHTTP dispatches incoming requests to the appropriate bucket or object handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(path, "/", pathSplitParts)

	bucketName := ""
	key := ""

	if path != "" && path != "/" {
		bucketName = parts[0]
		if len(parts) > 1 {
			key = parts[1]
		}
	}

	if bucketName == "" {
		h.listBuckets(w, r)

		return
	}

	if key == "" {
		h.handleBucketOperation(w, r, bucketName)

		return
	}

	h.handleObjectOperation(w, r, bucketName, key)
}

func (h *Handler) handleBucketOperation(w http.ResponseWriter, r *http.Request, bucket string) {
	switch r.Method {
	case http.MethodPut:
		h.routeBucketPut(w, r, bucket)
	case http.MethodDelete:
		h.deleteBucket(w, r, bucket)
	case http.MethodGet:
		h.routeBucketGet(w, r, bucket)
	case http.MethodHead:
		h.headBucket(w, bucket)
	default:
		h.writeError(w, r, ErrMethodNotAllowed, http.StatusMethodNotAllowed)
	}
}

func (h *Handler) routeBucketPut(w http.ResponseWriter, r *http.Request, bucket string) {
	switch {
	case r.URL.Query().Has("versioning"):
		h.putBucketVersioning(w, r, bucket)
	case r.URL.Query().Has("tagging"):
		h.writeError(w, r, ErrNotImplemented, http.StatusNotImplemented)
	default:
		h.createBucket(w, r, bucket)
	}
}

func (h *Handler) routeBucketGet(w http.ResponseWriter, r *http.Request, bucket string) {
	switch {
	case r.URL.Query().Has("versioning"):
		h.getBucketVersioning(w, r, bucket)
	case r.URL.Query().Has("location"):
		_, _ = w.Write([]byte(xml.Header + locationConstraintXML))
	case r.URL.Query().Has("tagging"):
		h.writeError(w, r, ErrNotImplemented, http.StatusNotImplemented)
	default:
		h.listObjects(w, r, bucket)
	}
}

func (h *Handler) handleObjectOperation(w http.ResponseWriter, r *http.Request, bucket, key string) {
	switch r.Method {
	case http.MethodPut:
		h.routeObjectPut(w, r, bucket, key)
	case http.MethodGet:
		h.routeObjectGet(w, r, bucket, key)
	case http.MethodDelete:
		h.routeObjectDelete(w, r, bucket, key)
	case http.MethodHead:
		h.headObject(w, r, bucket, key)
	default:
		h.writeError(w, r, ErrMethodNotAllowed, http.StatusMethodNotAllowed)
	}
}

func (h *Handler) routeObjectPut(w http.ResponseWriter, r *http.Request, bucket, key string) {
	switch {
	case r.URL.Query().Has("tagging"):
		h.putObjectTagging(w, r, bucket, key)
	case r.URL.Query().Has("acl"):
		w.WriteHeader(http.StatusOK) // ACLs ignored
	default:
		h.putObject(w, r, bucket, key)
	}
}

func (h *Handler) routeObjectGet(w http.ResponseWriter, r *http.Request, bucket, key string) {
	switch {
	case r.URL.Query().Has("tagging"):
		h.getObjectTagging(w, r, bucket, key)
	case r.URL.Query().Has("acl"):
		w.WriteHeader(http.StatusNotImplemented) // ACLs ignored
	default:
		h.getObject(w, r, bucket, key)
	}
}

func (h *Handler) routeObjectDelete(w http.ResponseWriter, r *http.Request, bucket, key string) {
	switch {
	case r.URL.Query().Has("tagging"):
		h.deleteObjectTagging(w, r, bucket, key)
	default:
		h.deleteObject(w, r, bucket, key)
	}
}

func (h *Handler) listBuckets(w http.ResponseWriter, r *http.Request) {
	buckets, err := h.Backend.ListBuckets()
	if err != nil {
		h.writeError(w, r, err, http.StatusInternalServerError)

		return
	}

	resp := ListAllMyBucketsResult{
		Owner: &Owner{
			ID:          "gopherstack",
			DisplayName: "gopherstack",
		},
	}

	for _, b := range buckets {
		resp.Buckets = append(resp.Buckets, BucketXML{
			Name:         b.Name,
			CreationDate: b.CreationDate.Format(time.RFC3339),
		})
	}

	h.writeXML(w, resp)
}

func (h *Handler) createBucket(w http.ResponseWriter, r *http.Request, bucketName string) {
	err := h.Backend.CreateBucket(bucketName)
	if errors.Is(err, ErrBucketAlreadyExists) {
		h.writeError(w, r, err, http.StatusConflict)

		return
	}

	if err != nil {
		h.writeError(w, r, err, http.StatusInternalServerError)

		return
	}

	w.Header().Set("Location", "/"+bucketName)
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) deleteBucket(w http.ResponseWriter, r *http.Request, bucketName string) {
	err := h.Backend.DeleteBucket(bucketName)
	if errors.Is(err, ErrNoSuchBucket) {
		h.writeError(w, r, err, http.StatusNotFound)

		return
	}

	if errors.Is(err, ErrBucketNotEmpty) {
		h.writeError(w, r, err, http.StatusConflict)

		return
	}

	if err != nil {
		h.writeError(w, r, err, http.StatusInternalServerError)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) headBucket(w http.ResponseWriter, bucketName string) {
	_, err := h.Backend.HeadBucket(bucketName)
	if errors.Is(err, ErrNoSuchBucket) {
		w.WriteHeader(http.StatusNotFound)

		return
	}

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)

		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *Handler) listObjects(w http.ResponseWriter, r *http.Request, bucketName string) {
	prefix := r.URL.Query().Get("prefix")

	objects, err := h.Backend.ListObjects(bucketName, prefix)
	if errors.Is(err, ErrNoSuchBucket) {
		h.writeError(w, r, err, http.StatusNotFound)

		return
	}

	if err != nil {
		h.writeError(w, r, err, http.StatusInternalServerError)

		return
	}

	resp := ListBucketResult{
		Name:        bucketName,
		Prefix:      prefix,
		KeyCount:    len(objects),
		MaxKeys:     defaultMaxKeys,
		IsTruncated: false,
	}

	for _, obj := range objects {
		ver, getErr := h.Backend.GetObject(bucketName, obj.Key, "")
		if getErr != nil {
			continue
		}

		resp.Contents = append(resp.Contents, ObjectXML{
			Key:               obj.Key,
			LastModified:      obj.LastModified.Format(time.RFC3339),
			Size:              obj.Size,
			ETag:              ver.ETag,
			StorageClass:      "STANDARD",
			ChecksumAlgorithm: ver.ChecksumAlgorithm,
		})
	}

	h.writeXML(w, resp)
}

func (h *Handler) putObject(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		h.writeError(w, r, err, http.StatusInternalServerError)

		return
	}

	tags := parseTaggingHeader(r.Header.Get("X-Amz-Tagging"))

	algo := strings.ToUpper(r.Header.Get("X-Amz-Checksum-Algorithm"))
	if algo == "" {
		algo = strings.ToUpper(r.Header.Get("X-Amz-Sdk-Checksum-Algorithm"))
	}

	checksum := ""
	if algo != "" {
		headerName := "X-Amz-Checksum-" + strings.ToLower(algo)
		checksum = r.Header.Get(headerName)
	}

	meta := ObjectMetadata{
		Tags:              tags,
		ContentType:       r.Header.Get("Content-Type"),
		ChecksumAlgorithm: algo,
		ChecksumValue:     checksum,
	}

	ver, err := h.Backend.PutObject(bucketName, key, data, meta)
	if errors.Is(err, ErrNoSuchBucket) {
		h.writeError(w, r, err, http.StatusNotFound)

		return
	}

	if err != nil {
		h.writeError(w, r, err, http.StatusInternalServerError)

		return
	}

	w.Header().Set("ETag", ver.ETag)

	if ver.ChecksumAlgorithm != "" {
		w.Header().Set("X-Amz-Checksum-Algorithm", ver.ChecksumAlgorithm)
		headerName := "X-Amz-Checksum-" + strings.ToUpper(ver.ChecksumAlgorithm)
		w.Header().Set(headerName, ver.ChecksumValue)
	}

	if ver.VersionID != NullVersion {
		w.Header().Set("X-Amz-Version-Id", ver.VersionID)
	}

	w.WriteHeader(http.StatusOK)
}

func (h *Handler) getObject(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	versionID := r.URL.Query().Get("versionId")

	ver, err := h.Backend.GetObject(bucketName, key, versionID)
	if errors.Is(err, ErrNoSuchBucket) || errors.Is(err, ErrNoSuchKey) {
		h.writeError(w, r, err, http.StatusNotFound)

		return
	}

	if err != nil {
		h.writeError(w, r, err, http.StatusInternalServerError)

		return
	}

	w.Header().Set("ETag", ver.ETag)
	w.Header().Set("Content-Length", strconv.Itoa(len(ver.Data)))
	w.Header().Set("Last-Modified", ver.LastModified.Format(http.TimeFormat))
	w.Header().Set("Content-Type", "application/octet-stream")

	if ver.ChecksumAlgorithm != "" {
		w.Header().Set("X-Amz-Checksum-Algorithm", ver.ChecksumAlgorithm)
		headerName := "X-Amz-Checksum-" + strings.ToUpper(ver.ChecksumAlgorithm)
		w.Header().Set(headerName, ver.ChecksumValue)
	}

	if ver.VersionID != NullVersion {
		w.Header().Set("X-Amz-Version-Id", ver.VersionID)
	}

	_, _ = w.Write(ver.Data)
}

func (h *Handler) deleteObject(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	versionID := r.URL.Query().Get("versionId")

	delMarker, err := h.Backend.DeleteObject(bucketName, key, versionID)
	if errors.Is(err, ErrNoSuchBucket) {
		h.writeError(w, r, err, http.StatusNotFound)

		return
	}

	if errors.Is(err, ErrNoSuchKey) {
		w.WriteHeader(http.StatusNoContent)

		return
	}

	if err != nil {
		h.writeError(w, r, err, http.StatusInternalServerError)

		return
	}

	if delMarker != "" && delMarker != NullVersion {
		w.Header().Set("X-Amz-Version-Id", delMarker)
		w.Header().Set("X-Amz-Delete-Marker", "true")
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) headObject(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	versionID := r.URL.Query().Get("versionId")

	ver, err := h.Backend.HeadObject(bucketName, key, versionID)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)

		return
	}

	w.Header().Set("ETag", ver.ETag)
	w.Header().Set("Content-Length", strconv.FormatInt(ver.Size, 10))
	w.Header().Set("Last-Modified", ver.LastModified.Format(http.TimeFormat))

	if ver.ChecksumAlgorithm != "" {
		w.Header().Set("X-Amz-Checksum-Algorithm", ver.ChecksumAlgorithm)
		headerName := "X-Amz-Checksum-" + strings.ToUpper(ver.ChecksumAlgorithm)
		w.Header().Set(headerName, ver.ChecksumValue)
	}

	if ver.VersionID != NullVersion {
		w.Header().Set("X-Amz-Version-Id", ver.VersionID)
	}

	w.WriteHeader(http.StatusOK)
}

func (h *Handler) putBucketVersioning(w http.ResponseWriter, r *http.Request, bucketName string) {
	var conf VersioningConfiguration
	if err := xml.NewDecoder(r.Body).Decode(&conf); err != nil {
		h.writeError(w, r, err, http.StatusBadRequest)

		return
	}

	if backend, ok := h.Backend.(*InMemoryBackend); ok {
		backend.mu.Lock()

		b, exists := backend.buckets[bucketName]
		if !exists {
			backend.mu.Unlock()
			h.writeError(w, r, ErrNoSuchBucket, http.StatusNotFound)

			return
		}

		b.Versioning = VersioningStatus(conf.Status)
		backend.mu.Unlock()
	}

	w.WriteHeader(http.StatusOK)
}

func (h *Handler) getBucketVersioning(w http.ResponseWriter, r *http.Request, bucketName string) {
	b, err := h.Backend.HeadBucket(bucketName)
	if err != nil {
		h.writeError(w, r, err, http.StatusNotFound)

		return
	}

	resp := VersioningConfiguration{
		Status: string(b.Versioning),
	}

	h.writeXML(w, resp)
}

func (h *Handler) putObjectTagging(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	var tagging Tagging
	if err := xml.NewDecoder(r.Body).Decode(&tagging); err != nil {
		h.writeError(w, r, err, http.StatusBadRequest)

		return
	}

	tags := make(map[string]string, len(tagging.TagSet.Tags))
	for _, t := range tagging.TagSet.Tags {
		tags[t.Key] = t.Value
	}

	versionID := r.URL.Query().Get("versionId")
	if err := h.Backend.PutObjectTagging(bucketName, key, versionID, tags); err != nil {
		h.writeError(w, r, err, http.StatusInternalServerError)

		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *Handler) getObjectTagging(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	versionID := r.URL.Query().Get("versionId")

	tags, err := h.Backend.GetObjectTagging(bucketName, key, versionID)
	if err != nil {
		h.writeError(w, r, err, http.StatusNotFound)

		return
	}

	resp := Tagging{
		TagSet: TagSet{},
	}

	for k, v := range tags {
		resp.TagSet.Tags = append(resp.TagSet.Tags, Tag{Key: k, Value: v})
	}

	h.writeXML(w, resp)
}

func (h *Handler) deleteObjectTagging(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	versionID := r.URL.Query().Get("versionId")
	if err := h.Backend.DeleteObjectTagging(bucketName, key, versionID); err != nil {
		h.writeError(w, r, err, http.StatusInternalServerError)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) writeXML(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/xml")
	_, _ = w.Write([]byte(xml.Header))
	_ = xml.NewEncoder(w).Encode(v)
}

func (h *Handler) writeError(w http.ResponseWriter, r *http.Request, err error, code int) {
	w.WriteHeader(code)

	errResp := ErrorResponse{
		Code:      mapErrorCode(err),
		Message:   err.Error(),
		Resource:  r.URL.Path,
		RequestID: "req-id",
	}

	h.writeXML(w, errResp)
}

func mapErrorCode(err error) string {
	switch {
	case errors.Is(err, ErrNoSuchBucket):
		return "NoSuchBucket"
	case errors.Is(err, ErrNoSuchKey):
		return "NoSuchKey"
	case errors.Is(err, ErrBucketAlreadyExists):
		return "BucketAlreadyExists"
	case errors.Is(err, ErrBucketNotEmpty):
		return "BucketNotEmpty"
	default:
		return "InternalError"
	}
}

func parseTaggingHeader(header string) map[string]string {
	tags := make(map[string]string)

	if header == "" {
		return tags
	}

	for p := range strings.SplitSeq(header, "&") {
		kv := strings.SplitN(p, "=", tagKeyValueParts)
		if len(kv) == tagKeyValueParts {
			tags[kv[0]] = kv[1]
		}
	}

	return tags
}
