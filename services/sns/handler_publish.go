package sns

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

func (h *Handler) handlePublish(c *echo.Context) error {
	topicArn := c.Request().FormValue("TopicArn")
	targetArn := c.Request().FormValue("TargetArn")
	phoneNumber := c.Request().FormValue("PhoneNumber")
	message := c.Request().FormValue("Message")
	subject := c.Request().FormValue("Subject")
	messageStructure := c.Request().FormValue("MessageStructure")

	// Exactly one of TopicArn, TargetArn, or PhoneNumber must be specified.
	if message == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "Message is required")
	}

	if topicArn == "" && targetArn == "" && phoneNumber == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter",
			"TopicArn, TargetArn, or PhoneNumber is required")
	}

	attrs := extractMessageAttributes(c.Request().Form)

	var messageID string
	var err error

	switch {
	case topicArn != "":
		// FIFO topics require MessageGroupId. Apply dedup (explicit or content-based).
		if strings.HasSuffix(topicArn, ".fifo") {
			return h.publishFIFOTopic(c, topicArn, message, subject, messageStructure, attrs)
		}

		messageID, err = h.Backend.Publish(topicArn, message, subject, messageStructure, attrs)
		if err != nil {
			return h.handleBackendError(c, err)
		}
	case targetArn != "":
		// TargetArn addresses a platform endpoint. In the mock, generate a message ID.
		messageID, err = h.Backend.PublishToTargetArn(targetArn, message, subject, attrs)
		if err != nil {
			return h.handleBackendError(c, err)
		}
	default:
		// PhoneNumber direct SMS publish — generate a message ID in the mock.
		messageID, err = h.Backend.PublishSMS(phoneNumber, message)
		if err != nil {
			return h.handleBackendError(c, err)
		}
	}

	return h.writeXML(c, PublishResponse{
		PublishResult:    PublishResult{MessageID: messageID},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

// publishFIFOTopic handles a Publish call to a .fifo topic: it enforces
// MessageGroupId, resolves the effective deduplication ID via the
// ContentBasedDeduplication rules, drops the publish on a duplicate, and
// records the dedup ID after a successful underlying Publish.
func (h *Handler) publishFIFOTopic(
	c *echo.Context,
	topicArn, message, subject, messageStructure string,
	attrs map[string]MessageAttribute,
) error {
	if c.Request().FormValue("MessageGroupId") == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter",
			"MessageGroupId is required for FIFO topics")
	}

	explicitDedupID := c.Request().FormValue("MessageDeduplicationId")

	effectiveDedupID, dedupErr := h.resolveFIFODedupID(topicArn, explicitDedupID, message)
	if dedupErr != nil {
		return h.handleBackendError(c, dedupErr)
	}

	if effectiveDedupID != "" && h.dedup.isDuplicate(topicArn, effectiveDedupID) {
		// Duplicate within the 5-minute window: AWS still returns success with a
		// synthesized message ID and does not actually re-publish the message.
		return h.writeXML(c, PublishResponse{
			PublishResult: PublishResult{
				MessageID:      uuid.New().String(),
				SequenceNumber: h.nextFIFOSeqNum(topicArn),
			},
			ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
		})
	}

	messageID, err := h.Backend.Publish(topicArn, message, subject, messageStructure, attrs)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	if effectiveDedupID != "" {
		h.dedup.record(topicArn, effectiveDedupID)
	}

	return h.writeXML(c, PublishResponse{
		PublishResult: PublishResult{
			MessageID:      messageID,
			SequenceNumber: h.nextFIFOSeqNum(topicArn),
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

// resolveFIFODedupID enforces ContentBasedDeduplication semantics for a FIFO topic
// publish and returns the effective deduplication ID to use:
//
//   - When CBD is enabled: explicit MessageDeduplicationId is forbidden, and the
//     SHA-256 hex digest of the message body is used as the implicit dedup ID.
//   - When CBD is disabled (the default): explicit MessageDeduplicationId is required.
//
// Caller must already have validated that topicArn names a FIFO topic.
func (h *Handler) resolveFIFODedupID(topicArn, explicitDedupID, message string) (string, error) {
	attrs, err := h.Backend.GetTopicAttributes(topicArn)
	if err != nil {
		return "", err
	}

	cbdEnabled := strings.EqualFold(attrs["ContentBasedDeduplication"], "true")

	if cbdEnabled {
		if explicitDedupID != "" {
			return "", fmt.Errorf(
				"%w: MessageDeduplicationId must not be set when ContentBasedDeduplication is enabled",
				ErrInvalidParameter,
			)
		}

		sum := sha256.Sum256([]byte(message))

		return hex.EncodeToString(sum[:]), nil
	}

	if explicitDedupID == "" {
		return "", fmt.Errorf(
			"%w: MessageDeduplicationId is required for FIFO topics without ContentBasedDeduplication",
			ErrInvalidParameter,
		)
	}

	return explicitDedupID, nil
}

func (h *Handler) handlePublishBatch(c *echo.Context) error {
	topicArn := c.Request().FormValue("TopicArn")
	if topicArn == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "TopicArn is required")
	}

	entries := extractBatchEntries(c.Request().Form)

	if len(entries) == 0 {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameter",
			"PublishBatchRequestEntries is required",
		)
	}

	if len(entries) > maxPublishBatchEntries {
		msg := fmt.Sprintf(
			"The batch request contains more entries than permissible. Maximum is %d.",
			maxPublishBatchEntries,
		)

		return h.writeError(c, http.StatusBadRequest, "TooManyEntriesInBatchRequest", msg)
	}

	// Validate each entry ID format (non-empty, max 80 chars, alphanumeric/-/_).
	for _, entry := range entries {
		if !isValidBatchEntryID(entry.id) {
			return h.writeError(
				c,
				http.StatusBadRequest,
				"InvalidBatchEntryId",
				fmt.Sprintf(
					"Id '%s' is invalid: must be 1-%d chars, alphanumeric, hyphen, or underscore",
					entry.id,
					maxBatchEntryIDLen,
				),
			)
		}
	}

	// Validate batch entry IDs are unique within this request.
	seen := make(map[string]bool, len(entries))
	for _, entry := range entries {
		if seen[entry.id] {
			return h.writeError(c, http.StatusBadRequest, "BatchEntryIdsNotDistinct",
				fmt.Sprintf("Id '%s' appears more than once in the batch request", entry.id))
		}

		seen[entry.id] = true
	}

	// AWS SNS PublishBatch returns a top-level NotFoundException when the topic
	// does not exist, rather than per-entry failures. Pre-check topic existence.
	if _, err := h.Backend.GetTopicAttributes(topicArn); err != nil {
		return h.handleBackendError(c, err)
	}

	// AWS rejects PublishBatch when the combined message bodies exceed the
	// 256 KiB per-request limit (BatchRequestTooLong).
	totalBytes := 0
	for _, entry := range entries {
		totalBytes += len(entry.message)
	}

	if totalBytes > maxMessageSizeBytes {
		return h.writeError(c, http.StatusBadRequest, "BatchRequestTooLong",
			fmt.Sprintf("Batch requests cannot be longer than %d bytes; got %d.",
				maxMessageSizeBytes, totalBytes))
	}

	successful := make([]XMLPublishBatchSuccessEntry, 0, len(entries))
	failed := make([]XMLPublishBatchFailEntry, 0, len(entries))

	isFIFO := strings.HasSuffix(topicArn, ".fifo")

	for _, entry := range entries {
		ok, fail := h.processBatchEntry(topicArn, entry, isFIFO)
		if fail != nil {
			failed = append(failed, *fail)

			continue
		}

		successful = append(successful, *ok)
	}

	return h.writeXML(c, PublishBatchResponse{
		PublishBatchResult: PublishBatchResult{Successful: successful, Failed: failed},
		ResponseMetadata:   ResponseMetadata{RequestID: uuid.New().String()},
	})
}

// processBatchEntry executes a single PublishBatch entry: it validates FIFO
// requirements, applies deduplication, performs the underlying Publish, and
// returns either a success or failure entry. Exactly one of the returned
// pointers is non-nil.
func (h *Handler) processBatchEntry(
	topicArn string, entry batchEntry, isFIFO bool,
) (*XMLPublishBatchSuccessEntry, *XMLPublishBatchFailEntry) {
	if isFIFO && entry.messageGroupID == "" {
		return nil, &XMLPublishBatchFailEntry{
			ID:          entry.id,
			Code:        "InvalidParameter",
			Message:     "MessageGroupId is required for FIFO topics",
			SenderFault: true,
		}
	}

	var effectiveDedupID string

	if isFIFO {
		id, dup, fail := h.batchEntryFIFODedup(topicArn, entry)
		if fail != nil {
			return nil, fail
		}

		if dup != nil {
			return dup, nil
		}

		effectiveDedupID = id
	}

	msgID, err := h.Backend.Publish(
		topicArn,
		entry.message,
		entry.subject,
		entry.messageStructure,
		entry.attrs,
	)
	if err != nil {
		return nil, &XMLPublishBatchFailEntry{
			ID:          entry.id,
			Code:        errorCode(err),
			Message:     err.Error(),
			SenderFault: true,
		}
	}

	if effectiveDedupID != "" {
		h.dedup.record(topicArn, effectiveDedupID)
	}

	ok := &XMLPublishBatchSuccessEntry{MessageID: msgID, ID: entry.id}
	if isFIFO {
		ok.SequenceNumber = h.nextFIFOSeqNum(topicArn)
	}

	return ok, nil
}

// batchEntryFIFODedup resolves the effective FIFO deduplication ID for a single
// batch entry. It returns:
//   - (id, nil, nil): the entry should be published with `id` recorded after success.
//   - ("", dup, nil): the entry is a duplicate within the dedup window; AWS
//     returns a synthesized success entry without re-publishing.
//   - ("", nil, fail): the dedup configuration is invalid (e.g. CBD conflict).
func (h *Handler) batchEntryFIFODedup(
	topicArn string, entry batchEntry,
) (string, *XMLPublishBatchSuccessEntry, *XMLPublishBatchFailEntry) {
	id, dedupErr := h.resolveFIFODedupID(topicArn, entry.dedupID, entry.message)
	if dedupErr != nil {
		return "", nil, &XMLPublishBatchFailEntry{
			ID:          entry.id,
			Code:        errorCode(dedupErr),
			Message:     dedupErr.Error(),
			SenderFault: true,
		}
	}

	if id != "" && h.dedup.isDuplicate(topicArn, id) {
		return "", &XMLPublishBatchSuccessEntry{
			MessageID:      uuid.New().String(),
			SequenceNumber: h.nextFIFOSeqNum(topicArn),
			ID:             entry.id,
		}, nil
	}

	return id, nil, nil
}

// extractBatchEntries reads PublishBatchRequestEntries.member.N entries from the form.
func extractBatchEntries(form url.Values) []batchEntry {
	entries := make([]batchEntry, 0)

	for i := 1; ; i++ {
		id := form.Get(fmt.Sprintf("PublishBatchRequestEntries.member.%d.Id", i))
		if id == "" {
			return entries
		}

		// Extract per-entry MessageAttributes. The AWS query-protocol wire shape
		// nests each entry's attribute map under its own "MessageAttributes" key
		// (PublishBatchRequestEntries.member.N.MessageAttributes.entry.M.Name/Value...),
		// matching the same shape a top-level Publish uses under "MessageAttributes.".
		// A prior version omitted the "MessageAttributes." segment, so no batch
		// entry's message attributes were ever parsed from a real SDK request.
		prefix := fmt.Sprintf("PublishBatchRequestEntries.member.%d.MessageAttributes.", i)
		attrs := extractMessageAttributesWithPrefix(form, prefix)

		entries = append(entries, batchEntry{
			id: id,
			message: form.Get(
				fmt.Sprintf("PublishBatchRequestEntries.member.%d.Message", i),
			),
			subject: form.Get(
				fmt.Sprintf("PublishBatchRequestEntries.member.%d.Subject", i),
			),
			attrs: attrs,
			messageGroupID: form.Get(
				fmt.Sprintf("PublishBatchRequestEntries.member.%d.MessageGroupId", i),
			),
			messageStructure: form.Get(
				fmt.Sprintf("PublishBatchRequestEntries.member.%d.MessageStructure", i),
			),
			dedupID: form.Get(
				fmt.Sprintf("PublishBatchRequestEntries.member.%d.MessageDeduplicationId", i),
			),
		})
	}
}
