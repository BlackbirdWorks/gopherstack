package resourcegroupstaggingapi

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"

	awsarn "github.com/aws/aws-sdk-go-v2/aws/arn"
)

// maxARNsPerTagRequest is the maximum number of ARNs in a single TagResources or
// UntagResources request, matching the AWS API limit. Shared by tag_resources.go
// and untag_resources.go.
const maxARNsPerTagRequest = 20

// maxTagsPerRequest is the maximum number of tag key-value pairs in a TagResources request.
const maxTagsPerRequest = 50

// maxTagKeyLength is the maximum length of a tag key.
const maxTagKeyLength = 128

// maxTagValueLength is the maximum length of a tag value.
const maxTagValueLength = 256

// maxResourceARNLength is the real API's ResourceARN shape max length (botocore
// resourcegroupstaggingapi/2017-01-26/service-2.json: max 1011, min 1; pattern
// `[\s\S]*`, i.e. unconstrained format). Shared by TagResources, UntagResources, and
// GetResources, which all reuse this shape for their ARN-list members.
const maxResourceARNLength = 1011

// TagResourcesInput is the request payload for TagResources.
type TagResourcesInput struct {
	Tags            map[string]string `json:"Tags"`
	ResourceARNList []string          `json:"ResourceARNList"`
}

// TagResourcesOutput is the response payload for TagResources.
type TagResourcesOutput struct {
	// FailedResourcesMap maps ARN to failure reason for resources that could not be tagged.
	FailedResourcesMap map[string]FailureInfo `json:"FailedResourcesMap,omitempty"`
}

// FailureInfo describes why a particular resource could not be tagged.
type FailureInfo struct {
	// ErrorCode is the error code.
	ErrorCode string `json:"ErrorCode"`
	// ErrorMessage is the human-readable error message.
	ErrorMessage string `json:"ErrorMessage"`
	// StatusCode is the HTTP status code.
	StatusCode int `json:"StatusCode"`
}

// validateTagResourcesInput validates the TagResources request parameters.
func validateTagResourcesInput(input *TagResourcesInput) error {
	if len(input.ResourceARNList) == 0 {
		return fmt.Errorf("%w: ResourceARNList must not be empty", ErrValidation)
	}

	if len(input.ResourceARNList) > maxARNsPerTagRequest {
		return fmt.Errorf("%w: ResourceARNList exceeds maximum of %d", ErrValidation, maxARNsPerTagRequest)
	}

	if len(input.Tags) == 0 {
		return fmt.Errorf("%w: Tags must not be empty", ErrValidation)
	}

	if len(input.Tags) > maxTagsPerRequest {
		return fmt.Errorf("%w: Tags exceeds maximum of %d", ErrValidation, maxTagsPerRequest)
	}

	return validateTagEntries(input.Tags)
}

// validateTagEntries validates all tag key-value pairs against AWS limits.
func validateTagEntries(tags map[string]string) error {
	for k, v := range tags {
		if k == "" {
			return fmt.Errorf("%w: tag key must not be empty", ErrValidation)
		}

		if strings.HasPrefix(k, "aws:") {
			return fmt.Errorf("%w: tag key %q starts with reserved prefix \"aws:\"", ErrValidation, k)
		}

		if len(k) > maxTagKeyLength {
			return fmt.Errorf("%w: tag key exceeds maximum length of %d", ErrValidation, maxTagKeyLength)
		}

		if len(v) > maxTagValueLength {
			return fmt.Errorf(
				"%w: tag value for key %q exceeds maximum length of %d",
				ErrValidation,
				k,
				maxTagValueLength,
			)
		}
	}

	return nil
}

// validateARNList validates each ARN in arns using awsarn.Parse. Shared by
// tag_resources.go and untag_resources.go. Returns ErrValidation if any ARN is empty or
// structurally malformed.
func validateARNList(arns []string) error {
	for _, arn := range arns {
		if arn == "" {
			return fmt.Errorf("%w: ARN must not be empty", ErrValidation)
		}

		if err := validateResourceARNLength(arn); err != nil {
			return err
		}

		if _, err := awsarn.Parse(arn); err != nil {
			return fmt.Errorf("%w: invalid ARN %q: %s", ErrValidation, arn, err.Error())
		}
	}

	return nil
}

// validateResourceARNLength enforces the real API's ResourceARN shape length ceiling.
func validateResourceARNLength(arn string) error {
	if len(arn) > maxResourceARNLength {
		return fmt.Errorf("%w: ARN exceeds maximum length of %d", ErrValidation, maxResourceARNLength)
	}

	return nil
}

// TagResources applies tags to the specified resources by routing to registered ARN taggers.
// Resources whose ARN does not match any registered tagger are reported in FailedResourcesMap
// with an InvalidParameterException, matching the AWS API behavior.
func (b *InMemoryBackend) TagResources(ctx context.Context, input *TagResourcesInput) (*TagResourcesOutput, error) {
	if err := validateTagResourcesInput(input); err != nil {
		return nil, err
	}

	if err := validateARNList(input.ResourceARNList); err != nil {
		return nil, err
	}

	b.mu.Lock("TagResources")
	taggers := slices.Clone(b.taggers)
	b.invalidateCache()
	b.mu.Unlock()

	// Deep-copy the tag map so that tagger callbacks cannot mutate the caller's map.
	tagsCopy := maps.Clone(input.Tags)

	failed := make(map[string]FailureInfo)

	for _, arn := range input.ResourceARNList {
		var handled bool

		for _, t := range taggers {
			ok, err := t(ctx, arn, tagsCopy)
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
				ErrorMessage: "no registered tagger handles ARN: " + arn,
				StatusCode:   http.StatusBadRequest,
			}
		}
	}

	out := &TagResourcesOutput{}
	if len(failed) > 0 {
		out.FailedResourcesMap = failed
	}

	return out, nil
}
