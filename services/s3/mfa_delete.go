package s3

import (
	"context"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// requireMFA rejects a request lacking x-amz-mfa when needed and the bucket has MFA Delete enabled
// (api_op_DeleteObject.go); the token is not validated, no MFA device exists.
func (h *S3Handler) requireMFA(ctx context.Context, r *http.Request, bucketName string, needed bool) error {
	if !needed || r.Header.Get("X-Amz-Mfa") != "" {
		return nil
	}

	out, err := h.Backend.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(bucketName)})
	if err == nil && out.MFADelete == types.MFADeleteStatusEnabled {
		return ErrAccessDenied
	}

	return nil
}
