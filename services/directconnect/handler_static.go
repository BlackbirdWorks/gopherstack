package directconnect

import "context"

// staticAndTagOps returns the dispatch table for the static reference-data
// ops (locations/router config/customer metadata/customer agreement) and
// the three native tagging ops (7 ops).
func (h *Handler) staticAndTagOps() map[string]opFunc {
	return map[string]opFunc{
		"DescribeLocations":           h.handleDescribeLocations,
		"DescribeRouterConfiguration": h.handleDescribeRouterConfiguration,
		"DescribeCustomerMetadata":    h.handleDescribeCustomerMetadata,
		"ConfirmCustomerAgreement":    h.handleConfirmCustomerAgreement,
		"DescribeTags":                h.handleDescribeTags,
		"TagResource":                 h.handleTagResource,
		"UntagResource":               h.handleUntagResource,
	}
}

func (h *Handler) handleDescribeLocations(_ context.Context, _ []byte) ([]byte, error) {
	return marshalResponse(locationsListResponse{Locations: h.Backend.DescribeLocations()})
}

func (h *Handler) handleDescribeRouterConfiguration(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[describeRouterConfigurationRequest](body)
	if err != nil {
		return nil, err
	}

	resp, err := h.Backend.DescribeRouterConfiguration(req.VirtualInterfaceID, req.RouterTypeIdentifier)
	if err != nil {
		return nil, err
	}

	return marshalResponse(resp)
}

func (h *Handler) handleDescribeCustomerMetadata(_ context.Context, _ []byte) ([]byte, error) {
	return marshalResponse(h.Backend.DescribeCustomerMetadata())
}

func (h *Handler) handleConfirmCustomerAgreement(_ context.Context, body []byte) ([]byte, error) {
	// AgreementName is accepted but unused: this backend never generates
	// real signed customer agreements (see PARITY.md's honest-gap section),
	// so confirming one is always a no-op success report of "signed" --
	// there is nothing outstanding to actually confirm regardless of which
	// agreement name (or none) the caller names.
	if _, err := decodeBody[confirmCustomerAgreementRequest](body); err != nil {
		return nil, err
	}

	return marshalResponse(statusResponse{Status: "signed"})
}

func (h *Handler) handleDescribeTags(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[describeTagsRequest](body)
	if err != nil {
		return nil, err
	}

	if len(req.ResourceArns) == 0 {
		return nil, clientError("resourceArns is required")
	}

	return marshalResponse(describeTagsResponse{ResourceTags: h.Backend.DescribeTags(req.ResourceArns)})
}

func (h *Handler) handleTagResource(ctx context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[tagResourceRequest](body)
	if err != nil {
		return nil, err
	}

	if req.ResourceArn == "" {
		return nil, clientError("resourceArn is required")
	}

	if validateErr := validateNewTags(tagWireKeys(req.Tags)); validateErr != nil {
		return nil, validateErr
	}

	if tagErr := h.Backend.TagResource(ctx, req.ResourceArn, tagWireToMap(req.Tags)); tagErr != nil {
		return nil, tagErr
	}

	return marshalResponse(struct{}{})
}

func (h *Handler) handleUntagResource(ctx context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[untagResourceRequest](body)
	if err != nil {
		return nil, err
	}

	if req.ResourceArn == "" {
		return nil, clientError("resourceArn is required")
	}

	if untagErr := h.Backend.UntagResource(ctx, req.ResourceArn, req.TagKeys); untagErr != nil {
		return nil, untagErr
	}

	return marshalResponse(struct{}{})
}
