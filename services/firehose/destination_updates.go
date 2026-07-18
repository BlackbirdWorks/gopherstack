package firehose

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// UpdateDestinationInput holds the destination update fields for UpdateDestination.
// Exactly one destination field should be non-nil.
type UpdateDestinationInput struct {
	S3Destination           *S3DestinationDescription
	HTTPEndpointDestination *HTTPEndpointDestinationDescription
	RedshiftDestination     *RedshiftDestinationDescription
	OpenSearchDestination   *OpenSearchDestinationDescription
	SplunkDestination       *SplunkDestinationDescription
}

// applyDestinationUpdate sets the single destination supplied in input and clears every
// other destination type, so an UpdateDestination call can switch a stream from one
// destination type to another. AWS permits exactly one destination update per call and a
// stream has exactly one active destination; providing none or more than one is rejected.
func applyDestinationUpdate(s *DeliveryStream, input UpdateDestinationInput) error {
	provided := 0
	if input.S3Destination != nil {
		provided++
	}
	if input.HTTPEndpointDestination != nil {
		provided++
	}
	if input.RedshiftDestination != nil {
		provided++
	}
	if input.OpenSearchDestination != nil {
		provided++
	}
	if input.SplunkDestination != nil {
		provided++
	}

	if provided != 1 {
		return fmt.Errorf("%w: exactly one destination update must be specified, got %d", ErrValidation, provided)
	}

	// Preserve the existing DestinationId across the switch when present.
	destID := currentDestinationID(s)

	s.S3Destination = input.S3Destination
	s.HTTPEndpointDestination = input.HTTPEndpointDestination
	s.RedshiftDestination = input.RedshiftDestination
	s.OpenSearchDestination = input.OpenSearchDestination
	s.SplunkDestination = input.SplunkDestination

	setDestinationID(s, destID)

	return nil
}

// currentDestinationID returns the DestinationId currently set on the stream's active
// destination, or the default when none is set.
func currentDestinationID(s *DeliveryStream) string {
	switch {
	case s.S3Destination != nil && s.S3Destination.DestinationID != "":
		return s.S3Destination.DestinationID
	case s.HTTPEndpointDestination != nil && s.HTTPEndpointDestination.DestinationID != "":
		return s.HTTPEndpointDestination.DestinationID
	case s.OpenSearchDestination != nil && s.OpenSearchDestination.DestinationID != "":
		return s.OpenSearchDestination.DestinationID
	case s.SplunkDestination != nil && s.SplunkDestination.DestinationID != "":
		return s.SplunkDestination.DestinationID
	case s.RedshiftDestination != nil && s.RedshiftDestination.DestinationID != "":
		return s.RedshiftDestination.DestinationID
	default:
		return "destinationId-000000000001"
	}
}

// setDestinationID stamps destID onto whichever destination is now active on the stream.
func setDestinationID(s *DeliveryStream, destID string) {
	switch {
	case s.S3Destination != nil:
		s.S3Destination.DestinationID = destID
	case s.HTTPEndpointDestination != nil:
		s.HTTPEndpointDestination.DestinationID = destID
	case s.OpenSearchDestination != nil:
		s.OpenSearchDestination.DestinationID = destID
	case s.SplunkDestination != nil:
		s.SplunkDestination.DestinationID = destID
	case s.RedshiftDestination != nil:
		s.RedshiftDestination.DestinationID = destID
	}
}

// UpdateDestination updates the destination configuration of an existing stream.
// AWS allows updating exactly one destination type per call.
func (b *InMemoryBackend) UpdateDestination(
	ctx context.Context,
	streamName, currentVersionID string,
	input UpdateDestinationInput,
) error {
	b.mu.Lock("UpdateDestination")
	defer b.mu.Unlock()

	region := getRegionFromContext(ctx, b)

	s, ok := b.streams.Get(regionKey(region, streamName))
	if !ok {
		return fmt.Errorf("%w: stream %s not found", ErrNotFound, streamName)
	}

	// AWS requires CurrentDeliveryStreamVersionId on every UpdateDestination call and
	// rejects the request when it does not match the stream's current version.
	if currentVersionID == "" {
		return fmt.Errorf("%w: CurrentDeliveryStreamVersionId is required", ErrValidation)
	}

	if s.VersionID != currentVersionID {
		return fmt.Errorf("%w: version mismatch: expected %s got %s", ErrValidation, currentVersionID, s.VersionID)
	}

	if err := applyDestinationUpdate(s, input); err != nil {
		return err
	}

	s.LastUpdateTimestamp = time.Now()

	v, err := strconv.Atoi(s.VersionID)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx,
			"firehose: unexpected non-integer VersionID; resetting to 1",
			"stream", streamName, "versionID", s.VersionID, "error", err)

		v = 0
	}

	s.VersionID = strconv.Itoa(v + 1)

	return nil
}
