package acm

import (
	"context"
	"encoding/json"
	"fmt"

	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// resolveTaggableResourceArn validates resourceArn against the generic ACM
// resource-ARN shape and, if well-formed, checks it against every taggable
// resource type this service knows about (certificate, acme-endpoint,
// acme-external-account-binding, acme-domain-validation), in most-specific-
// first order (the EAB/domain-validation ARN shapes are supersets of the
// bare endpoint shape's prefix). Certificates deliberately share the exact
// same h.tags-backed store the dedicated AddTagsToCertificate/
// ListTagsForCertificate/RemoveTagsFromCertificate ops use (handler_tags.go)
// -- a resource's tags are one underlying set in real AWS regardless of
// which API reads/writes them, so TagResource on a CertificateArn is
// expected to see (and be seen by) AddTagsToCertificate, not a second,
// disjoint copy.
func (h *Handler) resolveTaggableResourceArn(ctx context.Context, resourceArn string) error {
	if resourceArn == "" {
		return fmt.Errorf("%w: ResourceArn is required", ErrInvalidParameter)
	}

	if !genericAcmResourceArnPattern.MatchString(resourceArn) {
		return fmt.Errorf("%w: %q is not a valid ACM resource ARN", ErrInvalidParameter, resourceArn)
	}

	switch {
	case acmeEABArnPattern.MatchString(resourceArn):
		if !h.Backend.EABExists(ctx, resourceArn) {
			return fmt.Errorf("%w: resource %s not found", ErrAcmeResourceNotFound, resourceArn)
		}
	case acmeDomainValidationArnPattern.MatchString(resourceArn):
		if !h.Backend.DomainValidationExists(ctx, resourceArn) {
			return fmt.Errorf("%w: resource %s not found", ErrAcmeResourceNotFound, resourceArn)
		}
	case acmeEndpointArnPattern.MatchString(resourceArn):
		if !h.Backend.EndpointExists(ctx, resourceArn) {
			return fmt.Errorf("%w: resource %s not found", ErrAcmeResourceNotFound, resourceArn)
		}
	case certArnPattern.MatchString(resourceArn):
		if !h.Backend.CertExists(ctx, resourceArn) {
			return fmt.Errorf("%w: resource %s not found", ErrAcmeResourceNotFound, resourceArn)
		}
	default:
		// Shape matches the generic ACM pattern but not any resource type
		// gopherstack knows how to tag -- treat as not found rather than
		// inventing a new error code.
		return fmt.Errorf("%w: resource %s not found", ErrAcmeResourceNotFound, resourceArn)
	}

	return nil
}

type listTagsForResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
}

type listTagsForResourceOutput struct {
	Tags []map[string]string `json:"Tags"`
}

func (h *Handler) jsonListTagsForResource(ctx context.Context, body []byte) (any, error) {
	var input listTagsForResourceInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	if err := h.resolveTaggableResourceArn(ctx, input.ResourceArn); err != nil {
		return nil, err
	}

	return &listTagsForResourceOutput{Tags: h.getTags(input.ResourceArn)}, nil
}

type tagResourceInput struct {
	ResourceArn string       `json:"ResourceArn"`
	Tags        []svcTags.KV `json:"Tags"`
}

type tagResourceOutput struct{}

func (h *Handler) jsonTagResource(ctx context.Context, body []byte) (any, error) {
	var input tagResourceInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	if err := h.resolveTaggableResourceArn(ctx, input.ResourceArn); err != nil {
		return nil, err
	}

	if len(input.Tags) == 0 {
		return nil, fmt.Errorf("%w: Tags is required", ErrInvalidParameter)
	}

	kv := make(map[string]string, len(input.Tags))
	for _, t := range input.Tags {
		kv[t.Key] = t.Value
	}

	if err := h.setTags(input.ResourceArn, kv); err != nil {
		return nil, err
	}

	return &tagResourceOutput{}, nil
}

type untagResourceInput struct {
	ResourceArn string   `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
}

type untagResourceOutput struct{}

func (h *Handler) jsonUntagResource(ctx context.Context, body []byte) (any, error) {
	var input untagResourceInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	if err := h.resolveTaggableResourceArn(ctx, input.ResourceArn); err != nil {
		return nil, err
	}

	h.removeTags(input.ResourceArn, input.TagKeys)

	return &untagResourceOutput{}, nil
}
