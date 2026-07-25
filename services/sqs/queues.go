package sqs

import (
	"maps"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// isConfigurableQueueAttribute reports whether name is a user-visible attribute key
// that is compared during the idempotency check in CreateQueue. System-managed
// attributes (ARN, timestamps, approximate counts, etc.) are excluded.
func isConfigurableQueueAttribute(name string) bool {
	switch name {
	case attrVisibilityTimeout,
		attrMaximumMessageSize,
		attrMessageRetentionPeriod,
		attrDelaySeconds,
		attrReceiveMessageWaitTimeSeconds,
		attrFifoQueue,
		attrContentBasedDeduplication,
		attrRedrivePolicy,
		attrPolicy,
		attrSqsManagedSseEnabled,
		attrKmsMasterKeyID,
		attrKmsDataKeyReusePeriodSecs,
		attrRedriveAllowPolicy,
		attrDeduplicationScope,
		attrFifoThroughputLimit:
		return true
	}

	return false
}

// queueNamePattern is a regex-free check used by validateQueueName.
// AWS allows [a-zA-Z0-9_-]{1,80}.  FIFO queues must end with ".fifo".
const maxQueueNameLength = 80

// isValidQueueNameChar reports whether c is a character allowed in an SQS queue name.
// AWS allows letters, digits, hyphens, and underscores only.  Periods are NOT part of
// the standard character set; they appear exclusively in the ".fifo" suffix which is
// checked separately in validateQueueName.
func isValidQueueNameChar(c rune) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9') || c == '_' || c == '-'
}

// validateQueueName returns an error if name violates AWS queue-name rules.
func validateQueueName(name string) error {
	if len(name) == 0 || len(name) > maxQueueNameLength {
		return ErrInvalidQueueName
	}

	// For FIFO queues, the ".fifo" suffix is allowed.  Strip it before character validation
	// so that the only allowed character that is not in [a-zA-Z0-9_-] is the trailing ".fifo".
	base := name
	if strings.HasSuffix(name, fifoSuffix) {
		base = name[:len(name)-len(fifoSuffix)]
		if base == "" {
			return ErrInvalidQueueName
		}
	}

	for _, c := range base {
		if !isValidQueueNameChar(c) {
			return ErrInvalidQueueName
		}
	}

	return nil
}

// buildDefaultAttributes initialises the attribute map for a new queue.
func buildDefaultAttributes(queueName, accountID, region string, isFIFO bool) map[string]string {
	now := strconv.FormatInt(time.Now().Unix(), 10)
	queueARN := arn.Build("sqs", region, accountID, queueName)

	attrs := map[string]string{
		attrVisibilityTimeout:             strconv.Itoa(defaultVisibilityTimeout),
		attrMaximumMessageSize:            strconv.Itoa(defaultMaxMessageSize),
		attrMessageRetentionPeriod:        strconv.Itoa(defaultMessageRetentionPeriod),
		attrDelaySeconds:                  strconv.Itoa(defaultDelaySeconds),
		attrReceiveMessageWaitTimeSeconds: strconv.Itoa(defaultWaitTimeSeconds),
		attrCreatedTimestamp:              now,
		attrLastModifiedTimestamp:         now,
		attrQueueArn:                      queueARN,
		attrApproxMessagesDelayed:         attrValZero,
		attrSqsManagedSseEnabled:          attrValTrue,
	}

	if isFIFO {
		attrs[attrFifoQueue] = attrValTrue
		attrs[attrContentBasedDeduplication] = attrValFalse
	}

	return attrs
}

// CreateQueue creates a new SQS queue.
func (b *InMemoryBackend) CreateQueue(input *CreateQueueInput) (*CreateQueueOutput, error) {
	if err := validateQueueName(input.QueueName); err != nil {
		return nil, err
	}

	if err := validateQueueAttributes(input.Attributes); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateQueue")
	defer b.mu.Unlock()

	isFIFO := strings.HasSuffix(input.QueueName, fifoSuffix)
	region := b.effectiveRegion(input.Region)

	// AWS idempotency: if a queue with the same name exists in the same region
	// and the caller-supplied configurable attributes are the same (or absent),
	// return the existing URL. Different configurable attributes yields
	// QueueNameExists. A name collision in a different region is allowed.
	if q, exists := b.queues.Get(queueKey(region, input.QueueName)); exists {
		for k, v := range input.Attributes {
			if !isConfigurableQueueAttribute(k) {
				continue
			}

			if existing := q.Attributes[k]; existing != v {
				return nil, ErrQueueAlreadyExists
			}
		}

		return &CreateQueueOutput{QueueURL: q.URL}, nil
	}

	if err := b.checkQueueDeletedRecently(region, input.QueueName, time.Now()); err != nil {
		return nil, err
	}

	attrs := buildDefaultAttributes(input.QueueName, b.accountID, region, isFIFO)

	maps.Copy(attrs, input.Attributes)

	scheme := input.Scheme
	if scheme == "" {
		scheme = "http"
	}

	queueURL := scheme + "://" + input.Endpoint + "/" + b.accountID + "/" + input.QueueName

	tagName := "sqs.queue." + input.QueueName + ".tags"

	q := &Queue{
		Name:                input.QueueName,
		URL:                 queueURL,
		Region:              region,
		IsFIFO:              isFIFO,
		Attributes:          attrs,
		Tags:                tags.FromMap(tagName, input.Tags),
		Permissions:         make(map[string]*QueuePermissionEntry),
		DeduplicationIDs:    make(map[string]time.Time),
		deduplicationMsgIDs: make(map[string]string),
		notify:              make(chan struct{}),
		inFlightByHandle:    make(map[string]*InFlightMessage),
	}

	if err := applyRedrivePolicy(q, attrs, b); err != nil {
		return nil, err
	}

	b.queues.Put(q)

	return &CreateQueueOutput{QueueURL: queueURL}, nil
}

// checkQueueDeletedRecently returns ErrQueueDeletedRecently if a queue named
// name (in region) was deleted less than queueDeletedRecentlyWindowSecs ago.
// A stale (expired) entry is pruned inline so the map does not require the
// caller to have run the janitor recently. Caller must hold b.mu (write).
func (b *InMemoryBackend) checkQueueDeletedRecently(region, name string, now time.Time) error {
	key := queueKey(region, name)

	deletedAt, ok := b.recentlyDeleted[key]
	if !ok {
		return nil
	}

	if now.Sub(deletedAt) >= queueDeletedRecentlyWindowSecs*time.Second {
		delete(b.recentlyDeleted, key)

		return nil
	}

	return ErrQueueDeletedRecently
}

// DeleteQueue removes a queue by its URL.
func (b *InMemoryBackend) DeleteQueue(input *DeleteQueueInput) error {
	b.mu.Lock("DeleteQueue")
	defer b.mu.Unlock()

	q, ok := b.lookupQueueByURL(input.Region, input.QueueURL)
	if !ok {
		return ErrQueueNotFound
	}

	queueARN := q.Attributes[attrQueueArn]

	// Close the notify channel so that any goroutines blocked on long-polling
	// wake up immediately and receive ErrQueueNotFound on their next receiveOnce call.
	close(q.notify)

	if q.Tags != nil {
		q.Tags.Close()
	}

	b.recentlyDeleted[queueKey(q.Region, q.Name)] = time.Now()

	b.queues.Delete(queueKey(q.Region, q.Name))

	// Cancel any active move tasks that involve this queue (either as source or
	// destination) to prevent goroutine leaks.
	for _, task := range b.moveTasks.All() {
		b.cancelMoveTaskIfInvolved(task, queueARN)
	}

	return nil
}

// ListQueues returns queue URLs in the requested region, optionally filtered by prefix.
func (b *InMemoryBackend) ListQueues(input *ListQueuesInput) (*ListQueuesOutput, error) {
	b.mu.RLock("ListQueues")
	defer b.mu.RUnlock()

	scope := b.effectiveRegion(input.Region)

	urls := make([]string, 0, b.queues.Len())

	for _, q := range b.queues.All() {
		if q.Region != scope {
			continue
		}
		if input.QueueNamePrefix == "" || strings.HasPrefix(q.Name, input.QueueNamePrefix) {
			urls = append(urls, q.URL)
		}
	}

	sort.Strings(urls)

	p := page.New(urls, input.NextToken, input.MaxResults, sqsDefaultMaxResults)

	return &ListQueuesOutput{QueueURLs: p.Data, NextToken: p.Next}, nil
}

// GetQueueURL returns the URL for a queue by name in the requested region.
func (b *InMemoryBackend) GetQueueURL(input *GetQueueURLInput) (*GetQueueURLOutput, error) {
	b.mu.RLock("GetQueueURL")
	defer b.mu.RUnlock()

	q, ok := b.lookupQueueByName(input.Region, input.QueueName)
	if !ok {
		return nil, ErrQueueNotFound
	}

	return &GetQueueURLOutput{QueueURL: q.URL}, nil
}

// PurgeQueue removes all messages from a queue without deleting it.
// AWS enforces a 60-second cooldown between PurgeQueue calls on the same queue.
func (b *InMemoryBackend) PurgeQueue(input *PurgeQueueInput) error {
	b.mu.Lock("PurgeQueue")
	defer b.mu.Unlock()

	name := queueNameFromInput(input.QueueURL)

	q, ok := b.lookupQueueByName(input.Region, name)
	if !ok {
		return ErrQueueNotFound
	}

	// AWS enforces a 60-second cooldown between PurgeQueue calls on the same queue.
	// b.mu is already held (write-locked above), so this read is safe.
	if !q.lastPurgedAt.IsZero() && time.Since(q.lastPurgedAt) < purgeCooldownSecs*time.Second {
		return ErrPurgeQueueInProgress
	}

	q.messages = nil
	q.inFlightMessages = nil
	q.inFlightByHandle = make(map[string]*InFlightMessage)
	q.delayedCount = 0
	q.lastPurgedAt = time.Now()

	// For FIFO queues, purging messages also resets the deduplication state so
	// that producers can re-send messages with the same deduplication IDs.
	if q.IsFIFO {
		q.DeduplicationIDs = make(map[string]time.Time)
		q.deduplicationMsgIDs = make(map[string]string)
	}

	return nil
}
