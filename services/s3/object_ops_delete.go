package s3

import (
	"context"
	"encoding/xml"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// bypassGovernanceRetentionHeader parses the X-Amz-Bypass-Governance-Retention
// request header (DeleteObjectInput.BypassGovernanceRetention /
// DeleteObjectsInput.BypassGovernanceRetention on the wire).
func bypassGovernanceRetentionHeader(r *http.Request) *bool {
	v := r.Header.Get("X-Amz-Bypass-Governance-Retention")
	if v == "" {
		return nil
	}

	return aws.Bool(strings.EqualFold(v, "true"))
}

// setDeleteObjectResponseHeaders copies the optional version-id and
// delete-marker response headers from a backend DeleteObject result. Pulled
// out of deleteObject so its cyclomatic complexity stays under the linter cap.
func setDeleteObjectResponseHeaders(w http.ResponseWriter, out *s3.DeleteObjectOutput) {
	if out.VersionId != nil && *out.VersionId != "" && *out.VersionId != NullVersion {
		w.Header().Set("X-Amz-Version-Id", *out.VersionId)
	}
	if out.DeleteMarker != nil && *out.DeleteMarker {
		w.Header().Set("X-Amz-Delete-Marker", "true")
	}
}

func (h *S3Handler) deleteObject(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "DeleteObject")

	if err := h.authorizeObjectAccess(ctx, r, bucketName, key, actionDeleteObject); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	// AWS S3 supports If-Match on DeleteObject for ETag-conditional deletes.
	// We only honour it when no version is targeted: per-version deletes are
	// idempotent in real S3 and don't carry preconditions.
	if r.URL.Query().Get("versionId") == "" {
		if err := h.enforceDeleteObjectPreconditions(ctx, r, bucketName, key); err != nil {
			WriteError(ctx, w, r, err)

			return
		}
	}

	versionID := r.URL.Query().Get("versionId")
	logger.Load(ctx).DebugContext(
		ctx,
		"S3 deleteObject input",
		"bucket",
		bucketName,
		"key",
		key,
		"versionId",
		versionID,
	)

	var vid *string
	if versionID != "" {
		vid = aws.String(versionID)
	}

	out, err := h.Backend.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket:                    aws.String(bucketName),
		Key:                       aws.String(key),
		VersionId:                 vid,
		BypassGovernanceRetention: bypassGovernanceRetentionHeader(r),
	})
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	setDeleteObjectResponseHeaders(w, out)

	logger.Load(ctx).DebugContext(
		ctx,
		"S3 deleteObject output",
		"bucket", bucketName, "key", key, "deleteMarker", aws.ToBool(out.DeleteMarker),
	)

	// Dispatch S3 notification if configured.
	if h.notifier != nil {
		if notifXML, ncErr := h.Backend.GetBucketNotificationConfiguration(
			ctx,
			bucketName,
		); ncErr == nil &&
			notifXML != "" {
			go h.notifier.DispatchObjectDeleted(
				h.notificationDispatchContext(),
				bucketName,
				key,
				notifXML,
			)
		}
	}

	h.dispatchAccessLog(ctx, r, bucketName, "REST.DELETE.OBJECT", key, http.StatusNoContent, 0)

	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) deleteObjects(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName string,
) {
	h.setOperation(ctx, "DeleteObjects")
	var req DeleteRequest
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteError(ctx, w, r, ErrInvalidArgument)

		return
	}

	// AWS caps DeleteObjects at 1000 keys per request and rejects a larger
	// request with HTTP 400 MalformedXML (the request fails XML schema
	// validation), not a generic InvalidArgument.
	if len(req.Objects) > maxDeleteObjects {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    errMalformedXML,
			Message: errMalformedXMLMsg,
		}, http.StatusBadRequest)

		return
	}

	input := &s3.DeleteObjectsInput{
		Bucket: aws.String(bucketName),
		Delete: &types.Delete{
			Objects: make([]types.ObjectIdentifier, 0, len(req.Objects)),
			Quiet:   aws.Bool(req.Quiet),
		},
		BypassGovernanceRetention: bypassGovernanceRetentionHeader(r),
	}

	for _, obj := range req.Objects {
		input.Delete.Objects = append(input.Delete.Objects, types.ObjectIdentifier{
			Key:       aws.String(obj.Key),
			VersionId: obj.VersionID,
		})
	}

	out, err := h.Backend.DeleteObjects(ctx, input)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	resp := DeleteResult{
		Deleted: make([]DeletedXML, 0, len(out.Deleted)),
		Errors:  make([]DeleteErrorXML, 0, len(out.Errors)),
	}

	for _, d := range out.Deleted {
		if !req.Quiet {
			resp.Deleted = append(resp.Deleted, DeletedXML{
				Key:                   aws.ToString(d.Key),
				VersionID:             d.VersionId,
				DeleteMarker:          aws.ToBool(d.DeleteMarker),
				DeleteMarkerVersionID: d.DeleteMarkerVersionId,
			})
		}
	}

	for _, e := range out.Errors {
		resp.Errors = append(resp.Errors, DeleteErrorXML{
			Key:       aws.ToString(e.Key),
			Code:      aws.ToString(e.Code),
			Message:   aws.ToString(e.Message),
			VersionID: e.VersionId,
		})
	}

	httputils.WriteXML(ctx, w, http.StatusOK, resp)

	// Dispatch S3 delete notifications for each successfully deleted object.
	if h.notifier != nil {
		if notifXML, ncErr := h.Backend.GetBucketNotificationConfiguration(
			ctx,
			bucketName,
		); ncErr == nil && notifXML != "" {
			for _, d := range out.Deleted {
				key := aws.ToString(d.Key)
				go h.notifier.DispatchObjectDeleted(
					h.notificationDispatchContext(),
					bucketName,
					key,
					notifXML,
				)
			}
		}
	}
}
