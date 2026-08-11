package sqs

import (
	"encoding/json"
	"maps"
	"slices"
	"strconv"
	"time"
)

// GetQueueAttributes returns queue attributes, computing dynamic ones on the fly.
func (b *InMemoryBackend) GetQueueAttributes(
	input *GetQueueAttributesInput,
) (*GetQueueAttributesOutput, error) {
	b.mu.RLock("GetQueueAttributes")
	defer b.mu.RUnlock()

	name := queueNameFromInput(input.QueueURL)

	q, ok := b.lookupQueueByName(input.Region, name)
	if !ok {
		return nil, ErrQueueNotFound
	}

	q.mu.Lock()
	computed := computeDynamicAttributes(q)
	q.mu.Unlock()
	wantAll := len(input.AttributeNames) == 0 || containsAll(input.AttributeNames)

	result := make(map[string]string)

	for k, v := range q.Attributes {
		if wantAll || containsStr(input.AttributeNames, k) {
			result[k] = v
		}
	}

	for k, v := range computed {
		if wantAll || containsStr(input.AttributeNames, k) {
			result[k] = v
		}
	}

	return &GetQueueAttributesOutput{Attributes: result}, nil
}

// computeDynamicAttributes returns the dynamically computed attributes for a queue.
// Uses q.delayedCount (maintained on mutations) to avoid an O(depth) walk (#59).
func computeDynamicAttributes(q *Queue) map[string]string {
	delayed := q.delayedCount

	return map[string]string{
		AttrApproxMessages:           strconv.Itoa(len(q.messages) - delayed),
		AttrApproxMessagesNotVisible: strconv.Itoa(len(q.inFlightMessages)),
		attrApproxMessagesDelayed:    strconv.Itoa(delayed),
	}
}

// containsAll reports whether names contains the "All" sentinel.
func containsAll(names []string) bool {
	return slices.Contains(names, attrAll)
}

// containsStr reports whether slice contains s.
func containsStr(slice []string, s string) bool {
	return slices.Contains(slice, s)
}

// SetQueueAttributes updates attributes on an existing queue.
func (b *InMemoryBackend) SetQueueAttributes(input *SetQueueAttributesInput) error {
	// FifoQueue is immutable after creation (AWS spec).
	if _, hasFIFO := input.Attributes[attrFifoQueue]; hasFIFO {
		return ErrInvalidAttributeName
	}

	if err := validateQueueAttributes(input.Attributes); err != nil {
		return err
	}

	b.mu.Lock("SetQueueAttributes")
	defer b.mu.Unlock()

	name := queueNameFromInput(input.QueueURL)

	q, ok := b.lookupQueueByName(input.Region, name)
	if !ok {
		return ErrQueueNotFound
	}

	if !fifoThroughputPairingValid(q.Attributes, input.Attributes) {
		return ErrInvalidAttribute
	}

	if _, hasRedrive := input.Attributes[attrRedrivePolicy]; hasRedrive {
		if err := applyRedrivePolicy(q, input.Attributes, b); err != nil {
			return err
		}
	}

	maps.Copy(q.Attributes, input.Attributes)

	q.Attributes[attrLastModifiedTimestamp] = strconv.FormatInt(time.Now().Unix(), 10)

	return nil
}

// validateQueueAttributes returns an error if any of the provided queue attributes
// fall outside their allowed ranges (matching AWS SQS validation), or if a key is
// not one of AWS's 21 settable QueueAttributeName values (aws-sdk-go-v2/service/sqs
// @v1.46.4 types/enums.go:62-83, excluding the read-only/computed ones already
// filtered out of isConfigurableQueueAttribute).
func validateQueueAttributes(attrs map[string]string) error {
	for k := range attrs {
		if !isConfigurableQueueAttribute(k) {
			return ErrInvalidAttributeName
		}
	}

	if err := validateIntAttrRange(attrs, attrVisibilityTimeout, 0, maxVisibilityTimeoutSeconds); err != nil {
		return err
	}

	if err := validateIntAttrRange(attrs, attrDelaySeconds, 0, maxDelaySeconds); err != nil {
		return err
	}

	if err := validateIntAttrRange(
		attrs,
		attrMessageRetentionPeriod,
		minMessageRetentionPeriod,
		maxMessageRetentionPeriod,
	); err != nil {
		return err
	}

	if err := validateIntAttrRange(
		attrs,
		attrReceiveMessageWaitTimeSeconds,
		0,
		maxReceiveMessageWaitTimeSeconds,
	); err != nil {
		return err
	}

	if err := validateIntAttrRange(
		attrs, attrMaximumMessageSize, minMaximumMessageSize, defaultMaxMessageSize,
	); err != nil {
		return err
	}

	// AWS allows KmsDataKeyReusePeriodSeconds in [minKMSDataKeyReuseSecs, maxKMSDataKeyReuseSecs].
	if err := validateIntAttrRange(
		attrs, attrKmsDataKeyReusePeriodSecs, minKMSDataKeyReuseSecs, maxKMSDataKeyReuseSecs,
	); err != nil {
		return err
	}

	return validateFIFOAttributes(attrs)
}

// validateFIFOAttributes covers the FIFO-specific enum and redrive-allow
// validation pulled out of validateQueueAttributes to keep cognitive
// complexity below the linter cap.
func validateFIFOAttributes(attrs map[string]string) error {
	if v, ok := attrs[attrDeduplicationScope]; ok {
		if v != fifoDedupScopeQueue && v != fifoDedupScopePerMessageGroup {
			return ErrInvalidAttribute
		}
	}

	if v, ok := attrs[attrFifoThroughputLimit]; ok {
		if v != fifoThroughputLimitPerQueue && v != fifoThroughputLimitPerMessageGroupID {
			return ErrInvalidAttribute
		}
	}

	if v, ok := attrs[attrRedriveAllowPolicy]; ok {
		if err := validateRedriveAllowPolicy(v); err != nil {
			return err
		}
	}

	return nil
}

// validateRedriveAllowPolicy verifies the JSON shape of a RedriveAllowPolicy
// value. AWS accepts three forms of redrivePermission: allowAll, denyAll, or
// byQueue with a sourceQueueArns array (max 10 entries). Empty / malformed
// JSON is rejected with InvalidAttributeValue.
func validateRedriveAllowPolicy(raw string) error {
	var policy struct {
		RedrivePermission string   `json:"redrivePermission"`
		SourceQueueArns   []string `json:"sourceQueueArns"`
	}

	if err := json.Unmarshal([]byte(raw), &policy); err != nil {
		return ErrInvalidAttribute
	}

	switch policy.RedrivePermission {
	case "allowAll", "denyAll":
		if len(policy.SourceQueueArns) > 0 {
			return ErrInvalidAttribute
		}
	case "byQueue":
		if len(policy.SourceQueueArns) == 0 || len(policy.SourceQueueArns) > 10 {
			return ErrInvalidAttribute
		}
	default:
		return ErrInvalidAttribute
	}

	return nil
}

const (
	minKMSDataKeyReuseSecs = 60
	maxKMSDataKeyReuseSecs = 86400
)

// maxReceiveMessageWaitTimeSeconds is the AWS maximum for ReceiveMessageWaitTimeSeconds.
const maxReceiveMessageWaitTimeSeconds = 20

// validateIntAttrRange returns ErrInvalidAttribute if the named attribute exists and
// its integer value falls outside [attrMin, attrMax].
func validateIntAttrRange(attrs map[string]string, name string, attrMin, attrMax int) error {
	v, ok := attrs[name]
	if !ok {
		return nil
	}

	n, err := strconv.Atoi(v)
	if err != nil || n < attrMin || n > attrMax {
		return ErrInvalidAttribute
	}

	return nil
}

// maxVisibilityTimeoutSeconds is the AWS maximum for VisibilityTimeout (12 hours).
const maxVisibilityTimeoutSeconds = 43200
