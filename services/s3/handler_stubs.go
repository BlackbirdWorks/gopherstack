package s3

// handler_stubs.go adds routing and stub handlers for S3 SDK operations not yet
// fully implemented.  Each stub returns a minimal valid XML/JSON response.

import (
	"context"
	"encoding/xml"
	"net/http"

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
}

// s3DirectoryBucketsResult is the XML response for ListDirectoryBuckets.
type s3DirectoryBucketsResult struct {
	XMLName xml.Name `xml:"ListDirectoryBucketsResult"`
	Xmlns   string   `xml:"xmlns,attr"`
	Buckets []string `xml:"Buckets>Bucket>Name,omitempty"`
}

// s3ObjectAttributesResult is the XML response for GetObjectAttributes.
type s3ObjectAttributesResult struct {
	XMLName xml.Name `xml:"GetObjectAttributesResult"`
	Xmlns   string   `xml:"xmlns,attr"`
}

// routeBucketGetStubsExtra handles additional bucket GET sub-resource stubs.
// Returns true if handled.
func (h *S3Handler) routeBucketGetStubsExtra(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
) bool {
	q := r.URL.Query()

	switch {
	case q.Has("accelerate"):
		h.setOperation(ctx, "GetBucketAccelerateConfiguration")
		httputils.WriteXML(ctx, w, http.StatusOK,
			s3AccelerateConfiguration{Xmlns: xmlNamespaceS3, Status: "Suspended"})

		return true

	case q.Has("policyStatus"):
		h.setOperation(ctx, "GetBucketPolicyStatus")
		httputils.WriteXML(ctx, w, http.StatusOK,
			s3PolicyStatus{Xmlns: xmlNamespaceS3, IsPublic: sqlValFalse})

		return true

	case q.Has("abac"):
		h.setOperation(ctx, "GetBucketAbac")
		httputils.WriteXML(ctx, w, http.StatusOK,
			s3AbacConfiguration{Xmlns: xmlNamespaceS3})

		return true
	}

	return false
}

// handleGetObjectAttributes handles GET /{bucket}/{key}?attributes.
func (h *S3Handler) handleGetObjectAttributes(
	ctx context.Context,
	w http.ResponseWriter,
	_ *http.Request,
) {
	h.setOperation(ctx, "GetObjectAttributes")
	httputils.WriteXML(ctx, w, http.StatusOK, s3ObjectAttributesResult{Xmlns: xmlNamespaceS3})
}

// handleGetObjectTorrent handles GET /{bucket}/{key}?torrent.
func (h *S3Handler) handleGetObjectTorrent(
	ctx context.Context,
	w http.ResponseWriter,
	_ *http.Request,
) {
	h.setOperation(ctx, "GetObjectTorrent")
	w.Header().Set("Content-Type", "application/x-bittorrent")
	w.WriteHeader(http.StatusOK)
}

// handleRestoreObject handles POST /{bucket}/{key}?restore.
func (h *S3Handler) handleRestoreObject(
	ctx context.Context,
	w http.ResponseWriter,
	_ *http.Request,
) {
	h.setOperation(ctx, "RestoreObject")
	w.WriteHeader(http.StatusAccepted)
}

// handleRenameObject handles PUT /{bucket}/{key}?rename.
func (h *S3Handler) handleRenameObject(
	ctx context.Context,
	w http.ResponseWriter,
	_ *http.Request,
) {
	h.setOperation(ctx, "RenameObject")
	w.WriteHeader(http.StatusOK)
}

// handleUpdateObjectEncryption handles PUT /{bucket}/{key}?encryption (object-level).
func (h *S3Handler) handleUpdateObjectEncryption(
	ctx context.Context,
	w http.ResponseWriter,
	_ *http.Request,
) {
	h.setOperation(ctx, "UpdateObjectEncryption")
	w.WriteHeader(http.StatusOK)
}

// handleWriteGetObjectResponse handles POST /?writeGetObjectResponse.
func (h *S3Handler) handleWriteGetObjectResponse(
	ctx context.Context,
	w http.ResponseWriter,
	_ *http.Request,
) {
	h.setOperation(ctx, "WriteGetObjectResponse")
	w.WriteHeader(http.StatusOK)
}

// handleListDirectoryBuckets handles GET / with ?list-type=directory.
func (h *S3Handler) handleListDirectoryBuckets(
	ctx context.Context,
	w http.ResponseWriter,
	_ *http.Request,
) {
	h.setOperation(ctx, "ListDirectoryBuckets")
	httputils.WriteXML(ctx, w, http.StatusOK,
		s3DirectoryBucketsResult{Xmlns: xmlNamespaceS3, Buckets: []string{}})
}

// handlePutBucketAccelerate handles PUT /{bucket}?accelerate.
func (h *S3Handler) handlePutBucketAccelerate(
	ctx context.Context,
	w http.ResponseWriter,
	_ *http.Request,
) {
	h.setOperation(ctx, "PutBucketAccelerateConfiguration")
	w.WriteHeader(http.StatusOK)
}

// handlePutBucketAbac handles PUT /{bucket}?abac.
func (h *S3Handler) handlePutBucketAbac(
	ctx context.Context,
	w http.ResponseWriter,
	_ *http.Request,
) {
	h.setOperation(ctx, "PutBucketAbac")
	w.WriteHeader(http.StatusOK)
}

// handlePutBucketRequestPayment handles PUT /{bucket}?requestPayment.
func (h *S3Handler) handlePutBucketRequestPayment(
	ctx context.Context,
	w http.ResponseWriter,
	_ *http.Request,
) {
	h.setOperation(ctx, "PutBucketRequestPayment")
	w.WriteHeader(http.StatusOK)
}

// handleUpdateBucketMetadataInventoryTableConfig handles PUT /{bucket}?metadataInventoryTableConfiguration.
func (h *S3Handler) handleUpdateBucketMetadataInventoryTableConfig(
	ctx context.Context,
	w http.ResponseWriter,
	_ *http.Request,
) {
	h.setOperation(ctx, "UpdateBucketMetadataInventoryTableConfiguration")
	w.WriteHeader(http.StatusOK)
}

// handleUpdateBucketMetadataJournalTableConfig handles PUT /{bucket}?metadataJournalTableConfiguration.
func (h *S3Handler) handleUpdateBucketMetadataJournalTableConfig(
	ctx context.Context,
	w http.ResponseWriter,
	_ *http.Request,
) {
	h.setOperation(ctx, "UpdateBucketMetadataJournalTableConfiguration")
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
