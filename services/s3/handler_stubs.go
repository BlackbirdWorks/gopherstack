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
	bucket string,
) bool {
	q := r.URL.Query()

	switch {
	case q.Has("accelerate"):
		h.setOperation(ctx, "GetBucketAccelerateConfiguration")
		httputils.WriteXML(ctx, w, http.StatusOK,
			s3AccelerateConfiguration{Xmlns: xmlNamespaceS3, Status: "Suspended"})

		return true

	case q.Has("policyStatus"):
		h.handleGetBucketPolicyStatus(ctx, w, r, bucket)

		return true

	case q.Has("abac"):
		h.setOperation(ctx, "GetBucketAbac")
		httputils.WriteXML(ctx, w, http.StatusOK,
			s3AbacConfiguration{Xmlns: xmlNamespaceS3})

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

	if policy != "" && policyGrantsPublicGetObject(policy) {
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

		return s == "s3:getobject" || s == "s3:*" || s == "*"
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
