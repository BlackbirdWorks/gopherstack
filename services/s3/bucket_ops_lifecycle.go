package s3

import (
	"context"
	"encoding/xml"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *S3Handler) putBucketLifecycleConfiguration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "PutBucketLifecycleConfiguration")
	body, err := httputils.ReadBody(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	if code, msg := validateLifecycleConfiguration(body); code != "" {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{Code: code, Message: msg}, http.StatusBadRequest)

		return
	}

	transitionDefaultMinObjectSize := r.Header.Get("X-Amz-Transition-Default-Minimum-Object-Size")

	err = h.Backend.PutBucketLifecycleConfiguration(ctx, bucket, string(body), transitionDefaultMinObjectSize)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) getBucketLifecycleConfiguration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "GetBucketLifecycleConfiguration")
	lifecycleXML, err := h.Backend.GetBucketLifecycleConfiguration(ctx, bucket)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	if !strings.Contains(lifecycleXML, `xmlns="`) {
		lifecycleXML = strings.Replace(
			lifecycleXML,
			"<LifecycleConfiguration>",
			`<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`,
			1,
		)
	}

	minSize, minSizeErr := h.Backend.GetBucketLifecycleTransitionDefaultMinObjectSize(ctx, bucket)
	if minSizeErr == nil && minSize != "" {
		w.Header().Set("X-Amz-Transition-Default-Minimum-Object-Size", minSize)
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(lifecycleXML))
}

func (h *S3Handler) deleteBucketLifecycle(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "DeleteBucketLifecycle")
	if err := h.Backend.DeleteBucketLifecycle(ctx, bucket); err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// validateLifecycleConfiguration returns an S3 error code and message for an invalid
// lifecycle document, or empty strings when it is acceptable.
func validateLifecycleConfiguration(body []byte) (string, string) {
	var cfg lifecycleConfiguration
	if err := xml.Unmarshal(body, &cfg); err != nil || len(cfg.Rules) == 0 {
		return errMalformedXML, errMalformedXMLMsg
	}

	seen := make(map[string]struct{}, len(cfg.Rules))

	for i := range cfg.Rules {
		rule := &cfg.Rules[i]
		if rule.Status != "Enabled" && rule.Status != "Disabled" {
			return errMalformedXML, errMalformedXMLMsg
		}

		if rule.ID != "" {
			if _, dup := seen[rule.ID]; dup {
				return errInvalidArgument, "Rule ID must be unique. Found same ID for more than one rule"
			}

			seen[rule.ID] = struct{}{}
		}

		if rule.Expiration.Days != nil && *rule.Expiration.Days <= 0 {
			return errInvalidArgument, "'Days' for Expiration action must be a positive integer"
		}
	}

	return "", ""
}
