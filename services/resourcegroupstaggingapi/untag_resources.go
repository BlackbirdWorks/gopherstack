package resourcegroupstaggingapi

import (
	"context"
	"fmt"
	"net/http"
	"slices"
)

// maxTagKeysPerUntag is the maximum number of tag keys per UntagResources request.
const maxTagKeysPerUntag = 50

// maxTagKeyLengthUntag is the maximum length of a tag key in UntagResources.
const maxTagKeyLengthUntag = 128

// UntagResourcesInput is the request payload for UntagResources.
type UntagResourcesInput struct {
	// ResourceARNList is the list of ARNs to untag.
	ResourceARNList []string `json:"ResourceARNList"`
	// TagKeys is the list of tag keys to remove.
	TagKeys []string `json:"TagKeys"`
}

// UntagResourcesOutput is the response payload for UntagResources.
type UntagResourcesOutput struct {
	// FailedResourcesMap maps ARN to failure reason.
	FailedResourcesMap map[string]FailureInfo `json:"FailedResourcesMap,omitempty"`
}

// validateTagKeysForUntag validates the TagKeys list for UntagResources.
func validateTagKeysForUntag(keys []string) error {
	if len(keys) > maxTagKeysPerUntag {
		return fmt.Errorf("%w: TagKeys exceeds maximum of %d", ErrValidation, maxTagKeysPerUntag)
	}

	for _, k := range keys {
		if k == "" {
			return fmt.Errorf("%w: tag key must not be empty", ErrValidation)
		}

		if len(k) > maxTagKeyLengthUntag {
			return fmt.Errorf("%w: tag key exceeds maximum length of %d", ErrValidation, maxTagKeyLengthUntag)
		}
	}

	return nil
}

// UntagResources removes the specified tag keys from the given resources.
func (b *InMemoryBackend) UntagResources(
	ctx context.Context,
	input *UntagResourcesInput,
) (*UntagResourcesOutput, error) {
	if len(input.ResourceARNList) == 0 {
		return nil, fmt.Errorf("%w: ResourceARNList must not be empty", ErrValidation)
	}

	if len(input.ResourceARNList) > maxARNsPerTagRequest {
		return nil, fmt.Errorf("%w: ResourceARNList exceeds maximum of %d", ErrValidation, maxARNsPerTagRequest)
	}

	if len(input.TagKeys) == 0 {
		return nil, fmt.Errorf("%w: TagKeys must not be empty", ErrValidation)
	}

	if err := validateTagKeysForUntag(input.TagKeys); err != nil {
		return nil, err
	}

	if err := validateARNList(input.ResourceARNList); err != nil {
		return nil, err
	}

	b.mu.Lock("UntagResources")
	untaggers := slices.Clone(b.untaggers)
	b.invalidateCache()
	b.mu.Unlock()

	failed := make(map[string]FailureInfo)

	for _, arn := range input.ResourceARNList {
		var handled bool

		for _, u := range untaggers {
			ok, err := u(ctx, arn, input.TagKeys)
			if ok {
				handled = true
				if err != nil {
					failed[arn] = FailureInfo{
						ErrorCode:    "InternalServiceException",
						ErrorMessage: err.Error(),
						StatusCode:   http.StatusInternalServerError,
					}
				}

				break
			}
		}

		if !handled {
			failed[arn] = FailureInfo{
				ErrorCode:    errCodeInvalidParameter,
				ErrorMessage: "no registered untagger handles ARN: " + arn,
				StatusCode:   http.StatusBadRequest,
			}
		}
	}

	out := &UntagResourcesOutput{}
	if len(failed) > 0 {
		out.FailedResourcesMap = failed
	}

	return out, nil
}
