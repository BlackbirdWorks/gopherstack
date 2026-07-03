package s3

// handler_stubs.go adds routing and stub handlers for S3 SDK operations not yet
// fully implemented.  Each stub returns a minimal valid XML/JSON response.

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// s3AccelerateConfiguration is the XML response for Get/PutBucketAccelerateConfiguration.
type s3AccelerateConfiguration struct {
	XMLName xml.Name `xml:"AccelerateConfiguration"`
	Xmlns   string   `xml:"xmlns,attr"`
	Status  string   `xml:"Status,omitempty"`
}

// s3PolicyStatus is the XML response for GetBucketPolicyStatus.
type s3PolicyStatus struct {
	XMLName  xml.Name `xml:"PolicyStatus"`
	Xmlns    string   `xml:"xmlns,attr"`
	IsPublic string   `xml:"IsPublic,omitempty"`
}

// s3AbacConfiguration is the XML response for Get/PutBucketAbac.
type s3AbacConfiguration struct {
	XMLName xml.Name `xml:"AbacConfiguration"`
	Xmlns   string   `xml:"xmlns,attr"`
	Status  string   `xml:"Status,omitempty"`
}

// s3DirectoryBucketEntry is one bucket in a ListDirectoryBuckets response.
type s3DirectoryBucketEntry struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate,omitempty"`
}

// s3DirectoryBucketsResult is the XML response for ListDirectoryBuckets.
type s3DirectoryBucketsResult struct {
	XMLName xml.Name                 `xml:"ListDirectoryBucketsResult"`
	Xmlns   string                   `xml:"xmlns,attr"`
	Buckets []s3DirectoryBucketEntry `xml:"Buckets>Bucket,omitempty"`
}

// routeBucketGetStubsExtra handles additional bucket GET sub-resource stubs.
// Returns true if handled.
func (h *S3Handler) routeBucketGetStubsExtra(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) bool {
	q := r.URL.Query()

	switch {
	case q.Has("accelerate"):
		h.setOperation(ctx, "GetBucketAccelerateConfiguration")

		status, err := h.Backend.GetBucketAccelerateConfiguration(ctx, bucket)
		if err != nil {
			WriteError(ctx, w, r, err)

			return true
		}

		httputils.WriteXML(ctx, w, http.StatusOK,
			s3AccelerateConfiguration{Xmlns: xmlNamespaceS3, Status: status})

		return true

	case q.Has("policyStatus"):
		h.handleGetBucketPolicyStatus(ctx, w, r, bucket)

		return true

	case q.Has("abac"):
		h.setOperation(ctx, "GetBucketAbac")

		configXML, err := h.Backend.GetBucketAbac(ctx, bucket)
		if err != nil {
			WriteError(ctx, w, r, err)

			return true
		}

		var cfg s3AbacConfiguration
		if configXML != "" {
			_ = xml.Unmarshal([]byte(configXML), &cfg)
		}

		cfg.Xmlns = xmlNamespaceS3
		httputils.WriteXML(ctx, w, http.StatusOK, cfg)

		return true
	}

	return false
}

// policyStatement is a single statement in a bucket policy JSON document.
type policyStatement struct {
	Principal any    `json:"Principal"`
	Action    any    `json:"Action"`
	Effect    string `json:"Effect"`
}

// policyDoc is the minimal structure needed to evaluate public access.
type policyDoc struct {
	Statement []policyStatement `json:"Statement"`
}

// handleGetBucketPolicyStatus implements GetBucketPolicyStatus.
// It returns IsPublic=true when the bucket policy grants s3:GetObject to "*".
func (h *S3Handler) handleGetBucketPolicyStatus(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "GetBucketPolicyStatus")

	isPublic := sqlValFalse

	policy, err := h.Backend.GetBucketPolicy(ctx, bucket)
	if err != nil && !errors.Is(err, ErrNoBucketPolicy) {
		WriteError(ctx, w, r, err)

		return
	}

	// A public policy only makes the bucket public when the Public Access Block
	// isn't restricting public bucket policies. RestrictPublicBuckets causes S3
	// to report IsPublic=false regardless of the policy content.
	pab, _ := loadPublicAccessBlock(ctx, h.Backend, bucket)
	if policy != "" && policyGrantsPublicGetObject(policy) && !pab.RestrictPublicBuckets {
		isPublic = sqlValTrue
	}

	httputils.WriteXML(ctx, w, http.StatusOK,
		s3PolicyStatus{Xmlns: xmlNamespaceS3, IsPublic: isPublic})
}

// policyGrantsPublicGetObject returns true if the JSON policy document
// contains an Allow statement that grants s3:GetObject to the wildcard principal "*".
func policyGrantsPublicGetObject(policyJSON string) bool {
	var doc policyDoc
	if err := json.Unmarshal([]byte(policyJSON), &doc); err != nil {
		return false
	}

	for _, stmt := range doc.Statement {
		if stmt.Effect != "Allow" {
			continue
		}

		if !principalIsWildcard(stmt.Principal) {
			continue
		}

		if actionIncludesGetObject(stmt.Action) {
			return true
		}
	}

	return false
}

// principalIsWildcard returns true when the policy principal is "*" (any principal).
func principalIsWildcard(principal any) bool {
	switch v := principal.(type) {
	case string:
		return v == "*"
	case map[string]any:
		if aws, ok := v["AWS"]; ok {
			return principalIsWildcard(aws)
		}
	case []any:
		return slices.ContainsFunc(v, principalIsWildcard)
	}

	return false
}

// actionIncludesGetObject returns true when the action list includes s3:GetObject or s3:*.
func actionIncludesGetObject(action any) bool {
	check := func(s string) bool {
		s = strings.ToLower(s)

		return s == actionGetObjectLower || s == "s3:*" || s == "*"
	}

	switch v := action.(type) {
	case string:
		return check(v)
	case []any:
		return slices.ContainsFunc(v, func(item any) bool {
			s, ok := item.(string)

			return ok && check(s)
		})
	}

	return false
}

// objectAttributesResult is the populated XML body for GetObjectAttributes.
type objectAttributesResult struct {
	XMLName      xml.Name           `xml:"GetObjectAttributesResult"`
	Xmlns        string             `xml:"xmlns,attr"`
	ETag         string             `xml:"ETag,omitempty"`
	Checksum     *attrsChecksumElem `xml:"Checksum,omitempty"`
	StorageClass string             `xml:"StorageClass,omitempty"`
	ObjectSize   int64              `xml:"ObjectSize,omitempty"`
}

type attrsChecksumElem struct {
	ChecksumCRC32     string `xml:"ChecksumCRC32,omitempty"`
	ChecksumCRC32C    string `xml:"ChecksumCRC32C,omitempty"`
	ChecksumCRC64NVME string `xml:"ChecksumCRC64NVME,omitempty"`
	ChecksumSHA1      string `xml:"ChecksumSHA1,omitempty"`
	ChecksumSHA256    string `xml:"ChecksumSHA256,omitempty"`
}

// handleGetObjectAttributes handles GET /{bucket}/{key}?attributes.
// The X-Amz-Object-Attributes header lists which attributes the caller wants;
// we always return the full set we support (ETag, ObjectSize, StorageClass, Checksum).
func (h *S3Handler) handleGetObjectAttributes(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
) {
	h.setOperation(ctx, "GetObjectAttributes")

	bucket, key, ok := h.resolveBucketAndKey(ctx, w, r)
	if !ok {
		return
	}
	if bucket == "" || key == "" {
		WriteError(ctx, w, r, ErrNoSuchKey)

		return
	}

	versionID := r.URL.Query().Get("versionId")

	attrs, err := h.Backend.GetObjectAttributes(ctx, bucket, key, versionID)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	out := objectAttributesResult{
		Xmlns:        xmlNamespaceS3,
		ETag:         attrs.ETag,
		ObjectSize:   attrs.ObjectSize,
		StorageClass: attrs.StorageClass,
	}

	if len(attrs.Checksum) > 0 {
		c := &attrsChecksumElem{
			ChecksumCRC32:     attrs.Checksum["ChecksumCRC32"],
			ChecksumCRC32C:    attrs.Checksum["ChecksumCRC32C"],
			ChecksumCRC64NVME: attrs.Checksum["ChecksumCRC64NVME"],
			ChecksumSHA1:      attrs.Checksum["ChecksumSHA1"],
			ChecksumSHA256:    attrs.Checksum["ChecksumSHA256"],
		}
		out.Checksum = c
	}

	if versionID != "" {
		w.Header().Set("X-Amz-Version-Id", versionID)
	}

	httputils.WriteXML(ctx, w, http.StatusOK, out)
}

// handleGetObjectTorrent handles GET /{bucket}/{key}?torrent.
// As of 2022 AWS S3 itself returns NotImplemented for new buckets and the
// operation is being phased out. We mirror that behaviour rather than emit
// an empty/invalid torrent payload that would be parsed as malformed.
func (h *S3Handler) handleGetObjectTorrent(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
) {
	h.setOperation(ctx, "GetObjectTorrent")
	WriteError(ctx, w, r, ErrNotImplemented)
}

// restoreRequest is the XML body for POST ?restore.
type restoreRequest struct {
	XMLName xml.Name `xml:"RestoreRequest"`
	Days    int      `xml:"Days"`
}

// handleRestoreObject handles POST /{bucket}/{key}?restore.
// Reads the XML RestoreRequest body to extract Days, then marks the latest
// version as restored for that period.
func (h *S3Handler) handleRestoreObject(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
) {
	h.setOperation(ctx, "RestoreObject")

	bucket, key, ok := h.resolveBucketAndKey(ctx, w, r)
	if !ok {
		return
	}
	if bucket == "" || key == "" {
		WriteError(ctx, w, r, ErrNoSuchKey)

		return
	}

	var req restoreRequest

	body, _ := httputils.ReadBody(r)
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &req)
	}

	if err := h.Backend.RestoreObject(ctx, bucket, key, req.Days); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	// AWS fires s3:ObjectRestore:Post when a restore is initiated; downstream
	// SQS/SNS/Lambda consumers depend on it for archival workflows.
	if h.notifier != nil {
		if notifXML, ncErr := h.Backend.GetBucketNotificationConfiguration(
			ctx, bucket,
		); ncErr == nil && notifXML != "" {
			go h.notifier.DispatchObjectRestorePost(
				h.notificationDispatchContext(), bucket, key, notifXML,
			)
		}
	}

	w.WriteHeader(http.StatusAccepted)
}

// handleRenameObject handles PUT /{bucket}/{key}?rename.
// AWS S3 sends the rename target via the x-amz-rename-source header (the
// existing source key) and uses the request URL path as the destination key.
// To match common usage we accept both forms: x-amz-rename-source as source,
// or X-Amz-Copy-Source for compatibility.
func (h *S3Handler) handleRenameObject(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
) {
	h.setOperation(ctx, "RenameObject")

	bucket, targetKey, ok := h.resolveBucketAndKey(ctx, w, r)
	if !ok {
		return
	}
	if bucket == "" || targetKey == "" {
		WriteError(ctx, w, r, ErrNoSuchKey)

		return
	}

	source := r.Header.Get("X-Amz-Rename-Source")
	if source == "" {
		source = r.Header.Get("X-Amz-Copy-Source")
	}

	source = strings.TrimPrefix(source, "/")

	// "bucket/key" or just "key" forms supported.
	srcKey := source
	if rest, hasPrefix := strings.CutPrefix(source, bucket+"/"); hasPrefix {
		srcKey = rest
	}

	if srcKey == "" {
		WriteError(ctx, w, r, ErrNoSuchKey)

		return
	}

	if err := h.Backend.RenameObject(ctx, bucket, srcKey, targetKey); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleUpdateObjectEncryption handles PUT /{bucket}/{key}?encryption (object-level).
// Updates the object's SSE algorithm and KMS key based on the
// x-amz-server-side-encryption headers.
func (h *S3Handler) handleUpdateObjectEncryption(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
) {
	h.setOperation(ctx, "UpdateObjectEncryption")

	bucket, key, ok := h.resolveBucketAndKey(ctx, w, r)
	if !ok {
		return
	}
	if bucket == "" || key == "" {
		WriteError(ctx, w, r, ErrNoSuchKey)

		return
	}

	algo := r.Header.Get("X-Amz-Server-Side-Encryption")
	kmsKey := r.Header.Get("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id")

	if err := h.Backend.UpdateObjectEncryption(ctx, bucket, key, algo, kmsKey); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	if algo != "" {
		w.Header().Set("X-Amz-Server-Side-Encryption", algo)
	}
	if kmsKey != "" {
		w.Header().Set("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id", kmsKey)
	}

	w.WriteHeader(http.StatusOK)
}

// handleListDirectoryBuckets handles GET / with ?list-type=directory.
// Returns only S3 Express directory buckets (name suffix --x-s3), matching the
// AWS partition between ListBuckets (general-purpose) and ListDirectoryBuckets.
func (h *S3Handler) handleListDirectoryBuckets(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
) {
	h.setOperation(ctx, "ListDirectoryBuckets")

	buckets, err := h.Backend.ListDirectoryBuckets(ctx)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	entries := make([]s3DirectoryBucketEntry, 0, len(buckets))
	for _, b := range buckets {
		entry := s3DirectoryBucketEntry{Name: aws.ToString(b.Name)}
		if b.CreationDate != nil {
			entry.CreationDate = b.CreationDate.UTC().Format(time.RFC3339)
		}

		entries = append(entries, entry)
	}

	httputils.WriteXML(ctx, w, http.StatusOK,
		s3DirectoryBucketsResult{Xmlns: xmlNamespaceS3, Buckets: entries})
}

// handlePutBucketAccelerate handles PUT /{bucket}?accelerate.
// Reads the AccelerateConfiguration XML body to extract Status (Enabled/Suspended).
func (h *S3Handler) handlePutBucketAccelerate(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
) {
	h.setOperation(ctx, "PutBucketAccelerateConfiguration")

	bucket, _, ok := h.resolveBucketAndKey(ctx, w, r)
	if !ok {
		return
	}
	if bucket == "" {
		WriteError(ctx, w, r, ErrNoSuchBucket)

		return
	}

	var cfg s3AccelerateConfiguration

	body, _ := httputils.ReadBody(r)
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &cfg)
	}

	if cfg.Status != statusEnabled && cfg.Status != "Suspended" {
		cfg.Status = "Suspended"
	}

	if err := h.Backend.PutBucketAccelerateConfiguration(ctx, bucket, cfg.Status); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}

// handlePutBucketAbac handles PUT /{bucket}?abac.
// Parses and stores the AbacConfiguration XML so that GetBucketAbac returns
// the persisted config, matching real S3 Tables behaviour.
func (h *S3Handler) handlePutBucketAbac(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
) {
	h.setOperation(ctx, "PutBucketAbac")

	bucket, _, ok := h.resolveBucketAndKey(ctx, w, r)
	if !ok {
		return
	}
	if bucket == "" {
		WriteError(ctx, w, r, ErrNoSuchBucket)

		return
	}

	body, _ := httputils.ReadBody(r)

	if err := h.Backend.PutBucketAbac(ctx, bucket, string(body)); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}

// requestPaymentConfiguration is the XML body for PUT/GET ?requestPayment.
type requestPaymentConfiguration struct {
	XMLName xml.Name `xml:"RequestPaymentConfiguration"`
	Xmlns   string   `xml:"xmlns,attr,omitempty"`
	Payer   string   `xml:"Payer,omitempty"`
}

// handlePutBucketRequestPayment handles PUT /{bucket}?requestPayment.
func (h *S3Handler) handlePutBucketRequestPayment(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
) {
	h.setOperation(ctx, "PutBucketRequestPayment")

	bucket, _, ok := h.resolveBucketAndKey(ctx, w, r)
	if !ok {
		return
	}
	if bucket == "" {
		WriteError(ctx, w, r, ErrNoSuchBucket)

		return
	}

	var cfg requestPaymentConfiguration

	body, _ := httputils.ReadBody(r)
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &cfg)
	}

	if cfg.Payer != "Requester" && cfg.Payer != "BucketOwner" {
		cfg.Payer = "BucketOwner"
	}

	if err := h.Backend.PutBucketRequestPayment(ctx, bucket, cfg.Payer); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleGetBucketRequestPayment handles GET /{bucket}?requestPayment.
func (h *S3Handler) handleGetBucketRequestPayment(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
) {
	h.setOperation(ctx, "GetBucketRequestPayment")

	bucket, _, ok := h.resolveBucketAndKey(ctx, w, r)
	if !ok {
		return
	}
	if bucket == "" {
		WriteError(ctx, w, r, ErrNoSuchBucket)

		return
	}

	payer, err := h.Backend.GetBucketRequestPayment(ctx, bucket)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	httputils.WriteXML(ctx, w, http.StatusOK,
		requestPaymentConfiguration{Xmlns: xmlNamespaceS3, Payer: payer})
}

// handleUpdateBucketMetadataInventoryTableConfig handles PUT /{bucket}?metadataInventoryTableConfiguration.
// Persists the inventory table configuration so it survives round-trips, matching real S3 behaviour.
func (h *S3Handler) handleUpdateBucketMetadataInventoryTableConfig(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
) {
	h.setOperation(ctx, "UpdateBucketMetadataInventoryTableConfiguration")

	bucket, _, ok := h.resolveBucketAndKey(ctx, w, r)
	if !ok {
		return
	}
	if bucket == "" {
		WriteError(ctx, w, r, ErrNoSuchBucket)

		return
	}

	body, _ := httputils.ReadBody(r)

	if err := h.Backend.UpdateBucketMetadataInventoryTableConfig(ctx, bucket, string(body)); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleUpdateBucketMetadataJournalTableConfig handles PUT /{bucket}?metadataJournalTableConfiguration.
// Persists the journal table configuration so it survives round-trips, matching real S3 behaviour.
func (h *S3Handler) handleUpdateBucketMetadataJournalTableConfig(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
) {
	h.setOperation(ctx, "UpdateBucketMetadataJournalTableConfiguration")

	bucket, _, ok := h.resolveBucketAndKey(ctx, w, r)
	if !ok {
		return
	}
	if bucket == "" {
		WriteError(ctx, w, r, ErrNoSuchBucket)

		return
	}

	body, _ := httputils.ReadBody(r)

	if err := h.Backend.UpdateBucketMetadataJournalTableConfig(ctx, bucket, string(body)); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}

// isWriteGetObjectResponseRequest returns true when the request targets WriteGetObjectResponse.
func isWriteGetObjectResponseRequest(r *http.Request) bool {
	return r.URL.Query().Has("writeGetObjectResponse")
}

// isListDirectoryBucketsRequest returns true when the request targets ListDirectoryBuckets.
func isListDirectoryBucketsRequest(r *http.Request) bool {
	return r.URL.Query().Get("list-type") == "directory"
}
