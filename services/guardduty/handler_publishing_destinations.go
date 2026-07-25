package guardduty

import (
	"encoding/json"
	"net/http"
)

func (h *Handler) dispatchPublishingDestOps(op, path string, body []byte) (any, int, bool, error) {
	switch op {
	case opCreatePublishingDestination:
		detectorID := extractID(path, pathDetector)
		result, code, err := h.handleCreatePublishingDestination(detectorID, body)

		return result, code, true, err

	case opDeletePublishingDestination:
		detectorID, destID := extractDetectorAndSubID(path)
		code, err := h.handleDeletePublishingDestination(detectorID, destID)

		return nil, code, true, err

	case opDescribePublishingDestination:
		detectorID, destID := extractDetectorAndSubID(path)
		result, code, err := h.handleDescribePublishingDestination(detectorID, destID)

		return result, code, true, err

	case opListPublishingDestinations:
		detectorID := extractID(path, pathDetector)
		result, code, err := h.handleListPublishingDestinations(detectorID)

		return result, code, true, err

	case opUpdatePublishingDestination:
		detectorID, destID := extractDetectorAndSubID(path)
		code, err := h.handleUpdatePublishingDestination(detectorID, destID, body)

		return nil, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) handleCreatePublishingDestination(detectorID string, body []byte) (any, int, error) {
	var req struct {
		DestinationProperties DestinationProperties `json:"destinationProperties"`
		Tags                  map[string]string     `json:"tags"`
		DestinationType       string                `json:"destinationType"`
		ClientToken           string                `json:"clientToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	dest, err := h.Backend.CreatePublishingDestination(
		detectorID, req.DestinationType, req.DestinationProperties, req.Tags,
	)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	return map[string]any{"destinationId": dest.DestinationID}, http.StatusOK, nil //nolint:goconst // existing issue.
}

func (h *Handler) handleDeletePublishingDestination(detectorID, destID string) (int, error) {
	if err := h.Backend.DeletePublishingDestination(detectorID, destID); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleDescribePublishingDestination(detectorID, destID string) (any, int, error) {
	dest, err := h.Backend.DescribePublishingDestination(detectorID, destID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{
		"destinationId":   dest.DestinationID,
		"destinationType": dest.DestinationType,
		keyStatus:         dest.Status,
		// Real GuardDuty wire key is "publishingFailureStartTimestamp" (epoch
		// milliseconds), not "publishingFailureStartedAt" -- see
		// aws-sdk-go-v2/service/guardduty deserializers.go's
		// awsRestjson1_deserializeOpDocumentDescribePublishingDestinationOutput.
		// The old key name meant a real SDK client's
		// PublishingFailureStartTimestamp field was silently left nil.
		"publishingFailureStartTimestamp": dest.PublishingFailureStartedAt,
		"destinationProperties":           dest.DestinationProperties,
		keyTags:                           tagsOrEmpty(dest.Tags),
	}, http.StatusOK, nil
}

func (h *Handler) handleListPublishingDestinations(detectorID string) (any, int, error) {
	dests, err := h.Backend.ListPublishingDestinations(detectorID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	out := make([]map[string]any, 0, len(dests))
	for _, d := range dests {
		out = append(out, map[string]any{
			"destinationId":   d.DestinationID,
			"destinationType": d.DestinationType,
			keyStatus:         d.Status,
		})
	}

	return map[string]any{"destinations": out}, http.StatusOK, nil
}

func (h *Handler) handleUpdatePublishingDestination(detectorID, destID string, body []byte) (int, error) {
	var req struct {
		DestinationProperties DestinationProperties `json:"destinationProperties"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.UpdatePublishingDestination(detectorID, destID, req.DestinationProperties); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}
