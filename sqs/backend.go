package sqs

import (
	"crypto/md5" //nolint:gosec // MD5 used for SQS wire protocol compatibility, not security
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// StorageBackend defines the interface for an SQS backend.
type StorageBackend interface {
	CreateQueue(input *CreateQueueInput) (*CreateQueueOutput, error)
	DeleteQueue(input *DeleteQueueInput) error
	ListQueues(input *ListQueuesInput) (*ListQueuesOutput, error)
	GetQueueURL(input *GetQueueURLInput) (*GetQueueURLOutput, error)
	GetQueueAttributes(input *GetQueueAttributesInput) (*GetQueueAttributesOutput, error)
	SetQueueAttributes(input *SetQueueAttributesInput) error
	SendMessage(input *SendMessageInput) (*SendMessageOutput, error)
	ReceiveMessage(input *ReceiveMessageInput) (*ReceiveMessageOutput, error)
	DeleteMessage(input *DeleteMessageInput) error
	ChangeMessageVisibility(input *ChangeMessageVisibilityInput) error
	SendMessageBatch(input *SendMessageBatchInput) (*SendMessageBatchOutput, error)
	DeleteMessageBatch(input *DeleteMessageBatchInput) (*DeleteMessageBatchOutput, error)
	PurgeQueue(input *PurgeQueueInput) error
	TagQueue(input *TagQueueInput) error
	UntagQueue(input *UntagQueueInput) error
	ListQueueTags(input *ListQueueTagsInput) (*ListQueueTagsOutput, error)
	ChangeMessageVisibilityBatch(input *ChangeMessageVisibilityBatchInput) (*ChangeMessageVisibilityBatchOutput, error)
	ListAll() []QueueInfo
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	queues    map[string]*Queue
	mu        *lockmetrics.RWMutex
	accountID string
	region    string
}

const sqsDefaultMaxResults = 1000

// NewInMemoryBackend creates a new empty InMemoryBackend with default account/region.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion)
}

// NewInMemoryBackendWithConfig creates a new InMemoryBackend with the given account ID and region.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		queues:    make(map[string]*Queue),
		accountID: accountID,
		region:    region,
		mu:        lockmetrics.New("sqs"),
	}
}

// queueNameFromInput extracts the queue name from a queue URL.
func queueNameFromInput(queueURL string) string {
	parts := strings.Split(queueURL, "/")
	if len(parts) == 0 {
		return ""
	}

	return parts[len(parts)-1]
}

// redrivePolicy represents the JSON structure of an SQS RedrivePolicy attribute.
type redrivePolicy struct {
	DeadLetterTargetArn string      `json:"deadLetterTargetArn"`
	MaxReceiveCount     json.Number `json:"maxReceiveCount"`
}

// applyRedrivePolicy parses the RedrivePolicy attribute and wires up DLQ fields on q.
func applyRedrivePolicy(q *Queue, attrs map[string]string, backend *InMemoryBackend) {
	raw, ok := attrs[attrRedrivePolicy]
	if !ok || raw == "" {
		return
	}

	var pol redrivePolicy

	if err := json.Unmarshal([]byte(raw), &pol); err != nil {
		return
	}

	count, err := pol.MaxReceiveCount.Int64()
	if err != nil || count <= 0 {
		return
	}

	dlqName := queueNameFromARN(pol.DeadLetterTargetArn)

	dlq, exists := backend.queues[dlqName]
	if !exists {
		return
	}

	q.MaxReceiveCount = int(count)
	q.dlq = dlq
}

// computeMD5 returns the hex-encoded MD5 hash of the given string.
func computeMD5(body string) string {
	//nolint:gosec // MD5 required by SQS wire protocol
	hash := md5.Sum([]byte(body))

	return hex.EncodeToString(hash[:])
}

// computeMD5OfMessageAttributes computes the MD5 of message attributes per the AWS SQS algorithm.
// Attributes are sorted alphabetically, then each is encoded as:
// 4-byte big-endian name length, name, 4-byte big-endian data-type length, data type,
// 1-byte transport type (1=String/Number, 2=Binary), 4-byte big-endian value length, value bytes.
func computeMD5OfMessageAttributes(attrs map[string]MessageAttributeValue) string {
	if len(attrs) == 0 {
		return ""
	}

	names := make([]string, 0, len(attrs))
	for name := range attrs {
		names = append(names, name)
	}
	sort.Strings(names)

	var buf []byte
	for _, name := range names {
		attr := attrs[name]
		buf = appendWithLength(buf, []byte(name))
		buf = appendWithLength(buf, []byte(attr.DataType))

		if strings.HasPrefix(attr.DataType, "Binary") {
			buf = append(buf, msgAttrTransportTypeBinary)
			buf = appendWithLength(buf, attr.BinaryValue)
		} else {
			buf = append(buf, msgAttrTransportTypeString)
			buf = appendWithLength(buf, []byte(attr.StringValue))
		}
	}

	//nolint:gosec // MD5 required by SQS wire protocol
	hash := md5.Sum(buf)

	return hex.EncodeToString(hash[:])
}

// appendWithLength appends a 4-byte big-endian length prefix followed by data to buf.
func appendWithLength(buf, data []byte) []byte {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(
		lenBuf[:],
		uint32(len(data)), //nolint:gosec // length is always non-negative and bounded by message size limits
	)

	buf = append(buf, lenBuf[:]...)
	buf = append(buf, data...)

	return buf
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
	}

	if isFIFO {
		attrs[attrFifoQueue] = attrValTrue
		attrs[attrContentBasedDeduplication] = attrValFalse
	}

	return attrs
}

// CreateQueue creates a new SQS queue.
func (b *InMemoryBackend) CreateQueue(input *CreateQueueInput) (*CreateQueueOutput, error) {
	b.mu.Lock("CreateQueue")
	defer b.mu.Unlock()

	if _, exists := b.queues[input.QueueName]; exists {
		return nil, ErrQueueAlreadyExists
	}

	isFIFO := strings.HasSuffix(input.QueueName, fifoSuffix)
	region := b.region
	if input.Region != "" {
		region = input.Region
	}
	attrs := buildDefaultAttributes(input.QueueName, b.accountID, region, isFIFO)

	maps.Copy(attrs, input.Attributes)

	queueURL := "http://" + input.Endpoint + "/" + b.accountID + "/" + input.QueueName

	q := &Queue{
		Name:                input.QueueName,
		URL:                 queueURL,
		IsFIFO:              isFIFO,
		Attributes:          attrs,
		DeduplicationIDs:    make(map[string]time.Time),
		deduplicationMsgIDs: make(map[string]string),
		notify:              make(chan struct{}),
	}

	b.queues[input.QueueName] = q

	applyRedrivePolicy(q, attrs, b)

	return &CreateQueueOutput{QueueURL: queueURL}, nil
}

// DeleteQueue removes a queue by its URL.
func (b *InMemoryBackend) DeleteQueue(input *DeleteQueueInput) error {
	b.mu.Lock("DeleteQueue")
	defer b.mu.Unlock()

	name := queueNameFromInput(input.QueueURL)

	if _, exists := b.queues[name]; !exists {
		return ErrQueueNotFound
	}

	delete(b.queues, name)

	return nil
}

// ListQueues returns all queue URLs, optionally filtered by prefix.
func (b *InMemoryBackend) ListQueues(input *ListQueuesInput) (*ListQueuesOutput, error) {
	b.mu.RLock("ListQueues")
	defer b.mu.RUnlock()

	var urls []string

	for name, q := range b.queues {
		if input.QueueNamePrefix == "" || strings.HasPrefix(name, input.QueueNamePrefix) {
			urls = append(urls, q.URL)
		}
	}

	sort.Strings(urls)

	p := page.New(urls, input.NextToken, input.MaxResults, sqsDefaultMaxResults)

	return &ListQueuesOutput{QueueURLs: p.Data, NextToken: p.Next}, nil
}

// GetQueueURL returns the URL for a queue by name.
func (b *InMemoryBackend) GetQueueURL(input *GetQueueURLInput) (*GetQueueURLOutput, error) {
	b.mu.RLock("GetQueueURL")
	defer b.mu.RUnlock()

	q, ok := b.queues[input.QueueName]
	if !ok {
		return nil, ErrQueueNotFound
	}

	return &GetQueueURLOutput{QueueURL: q.URL}, nil
}

// GetQueueAttributes returns queue attributes, computing dynamic ones on the fly.
func (b *InMemoryBackend) GetQueueAttributes(input *GetQueueAttributesInput) (*GetQueueAttributesOutput, error) {
	b.mu.Lock("GetQueueAttributes")
	defer b.mu.Unlock()

	name := queueNameFromInput(input.QueueURL)

	q, ok := b.queues[name]
	if !ok {
		return nil, ErrQueueNotFound
	}

	computed := computeDynamicAttributes(q)
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
func computeDynamicAttributes(q *Queue) map[string]string {
	return map[string]string{
		AttrApproxMessages:           strconv.Itoa(len(q.messages)),
		AttrApproxMessagesNotVisible: strconv.Itoa(len(q.inFlightMessages)),
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
	b.mu.Lock("SetQueueAttributes")
	defer b.mu.Unlock()

	name := queueNameFromInput(input.QueueURL)

	q, ok := b.queues[name]
	if !ok {
		return ErrQueueNotFound
	}

	maps.Copy(q.Attributes, input.Attributes)

	if _, hasRedrive := input.Attributes[attrRedrivePolicy]; hasRedrive {
		applyRedrivePolicy(q, input.Attributes, b)
	}

	q.Attributes[attrLastModifiedTimestamp] = strconv.FormatInt(time.Now().Unix(), 10)

	return nil
}

// SendMessage adds a message to the specified queue.
func (b *InMemoryBackend) SendMessage(input *SendMessageInput) (*SendMessageOutput, error) {
	b.mu.Lock("SendMessage")
	defer b.mu.Unlock()

	name := queueNameFromInput(input.QueueURL)

	q, ok := b.queues[name]
	if !ok {
		return nil, ErrQueueNotFound
	}

	md5Body := computeMD5(input.MessageBody)
	md5Attrs := computeMD5OfMessageAttributes(input.MessageAttributes)

	if q.IsFIFO {
		if out, dup := checkDedup(
			q,
			input.MessageDeduplicationID,
			md5Body,
			q.Attributes[attrContentBasedDeduplication],
		); dup {
			return out, nil
		}
	}

	now := time.Now()
	msgID := uuid.New().String()
	sentTS := strconv.FormatInt(now.UnixMilli(), 10)

	msg := &Message{
		MessageID:              msgID,
		Body:                   input.MessageBody,
		MD5OfBody:              md5Body,
		MD5OfMessageAttributes: md5Attrs,
		MessageGroupID:         input.MessageGroupID,
		MessageDeduplicationID: input.MessageDeduplicationID,
		SentTimestamp:          now.UnixMilli(),
		MessageAttributes:      input.MessageAttributes,
		Attributes: map[string]string{
			attrSentTimestamp:      sentTS,
			attrApproxReceiveCount: attrValZero,
		},
	}

	if q.IsFIFO {
		storeDedup(q, input.MessageDeduplicationID, md5Body, q.Attributes[attrContentBasedDeduplication], msgID, now)
	}

	q.messages = append(q.messages, msg)

	// Close the current notify channel to broadcast to all long-polling receivers,
	// then replace it with a fresh channel for the next wait cycle.
	// Only broadcast on empty→non-empty transition; if the queue already had
	// messages, receivers would not have been waiting.
	if len(q.messages) == 1 {
		close(q.notify)
		q.notify = make(chan struct{})
	}

	return &SendMessageOutput{MessageID: msgID, MD5OfBody: md5Body, MD5OfMessageAttributes: md5Attrs}, nil
}

// checkDedup checks for a duplicate FIFO message and returns the original output if found.
func checkDedup(q *Queue, dedupID, md5Body, contentBasedDedup string) (*SendMessageOutput, bool) {
	effectiveID := dedupID
	if effectiveID == "" && contentBasedDedup == attrValTrue {
		effectiveID = md5Body
	}

	if effectiveID == "" {
		return nil, false
	}

	expiry, found := q.DeduplicationIDs[effectiveID]
	if !found || !time.Now().Before(expiry) {
		return nil, false
	}

	origMsgID := q.deduplicationMsgIDs[effectiveID]

	return &SendMessageOutput{MessageID: origMsgID, MD5OfBody: md5Body}, true
}

// storeDedup records a deduplication entry for a FIFO message.
func storeDedup(q *Queue, dedupID, md5Body, contentBasedDedup, msgID string, now time.Time) {
	effectiveID := dedupID
	if effectiveID == "" && contentBasedDedup == attrValTrue {
		effectiveID = md5Body
	}

	if effectiveID == "" {
		return
	}

	q.DeduplicationIDs[effectiveID] = now.Add(deduplicationWindowSecs * time.Second)
	q.deduplicationMsgIDs[effectiveID] = msgID
}

// pruneDedup removes expired deduplication entries from a FIFO queue.
func pruneDedup(q *Queue, now time.Time) {
	for k, expiry := range q.DeduplicationIDs {
		if !now.Before(expiry) {
			delete(q.DeduplicationIDs, k)
			delete(q.deduplicationMsgIDs, k)
		}
	}
}

// ReceiveMessage retrieves messages from the queue, with optional long-poll wait.
//
// Long polling uses a close-and-recreate broadcast: receiveOnce captures the
// queue's notify channel under the write lock. When SendMessage transitions the
// queue from empty to non-empty it closes the channel, waking all blocked
// receivers simultaneously, then creates a fresh channel for the next cycle.
// A 1-second recheck interval is also applied so that messages which reappear
// due to visibility-timeout expiry (reQueueExpired) are picked up promptly even
// when no new SendMessage occurs.
func (b *InMemoryBackend) ReceiveMessage(input *ReceiveMessageInput) (*ReceiveMessageOutput, error) {
	name := queueNameFromInput(input.QueueURL)
	deadline := time.Now().Add(time.Duration(input.WaitTimeSeconds) * time.Second)

	const recheckInterval = time.Second

	for {
		msgs, notifyCh, err := b.receiveOnce(name, input)
		if err != nil {
			return nil, err
		}

		if len(msgs) > 0 {
			return &ReceiveMessageOutput{Messages: msgs}, nil
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return &ReceiveMessageOutput{}, nil
		}

		sleep := min(remaining, recheckInterval)

		select {
		case <-notifyCh:
		case <-time.After(sleep):
		}
	}
}

// drainToDLQ moves messages that have hit maxReceiveCount into the DLQ queue.
func drainToDLQ(q *Queue) {
	if q.MaxReceiveCount <= 0 || q.dlq == nil {
		return
	}

	remaining := q.messages[:0]

	for _, msg := range q.messages {
		if msg.ApproximateReceiveCount >= q.MaxReceiveCount {
			msg.ReceiptHandle = ""
			q.dlq.messages = append(q.dlq.messages, msg)
		} else {
			remaining = append(remaining, msg)
		}
	}

	q.messages = remaining
}

// receiveOnce performs a single receive attempt under the backend lock.
func (b *InMemoryBackend) receiveOnce(name string, input *ReceiveMessageInput) ([]*Message, chan struct{}, error) {
	b.mu.Lock("receiveOnce")
	defer b.mu.Unlock()

	q, ok := b.queues[name]
	if !ok {
		return nil, nil, ErrQueueNotFound
	}

	now := time.Now()
	reQueueExpired(q, now)
	drainToDLQ(q)

	if q.IsFIFO {
		pruneDedup(q, now)
	}

	maxMessages := input.MaxNumberOfMessages
	if maxMessages <= 0 {
		maxMessages = 1
	}
	if maxMessages > maxBatchSize {
		maxMessages = maxBatchSize
	}

	vt := resolveVisibilityTimeout(input.VisibilityTimeout, q)

	return pickMessages(q, maxMessages, vt, now), q.notify, nil
}

// resolveVisibilityTimeout returns the effective visibility timeout for a receive operation.
func resolveVisibilityTimeout(requested int, q *Queue) int {
	if requested >= 0 {
		return requested
	}

	if v, err := strconv.Atoi(q.Attributes[attrVisibilityTimeout]); err == nil {
		return v
	}

	return defaultVisibilityTimeout
}

// reQueueExpired moves expired in-flight messages back to the queue.
func reQueueExpired(q *Queue, now time.Time) {
	var stillInFlight []*InFlightMessage

	for _, inf := range q.inFlightMessages {
		if now.After(inf.VisibleAt) {
			q.messages = append(q.messages, inf.Msg)
		} else {
			stillInFlight = append(stillInFlight, inf)
		}
	}

	q.inFlightMessages = stillInFlight
}

// pickMessages moves up to maxMessages from the queue to in-flight and returns them.
func pickMessages(q *Queue, maxMessages, vt int, now time.Time) []*Message {
	count := min(maxMessages, len(q.messages))
	if count == 0 {
		return nil
	}

	picked := q.messages[:count]
	q.messages = q.messages[count:]

	result := make([]*Message, 0, len(picked))

	for _, msg := range picked {
		receipt := uuid.New().String()
		msg.ReceiptHandle = receipt
		msg.ApproximateReceiveCount++
		msg.Attributes[attrApproxReceiveCount] = strconv.Itoa(msg.ApproximateReceiveCount)

		// Set ApproximateFirstReceiveTimestamp on the first receive.
		if msg.ApproximateFirstReceiveTimestamp == 0 {
			msg.ApproximateFirstReceiveTimestamp = now.UnixMilli()
			msg.Attributes[attrApproxFirstReceiveTimestamp] = strconv.FormatInt(
				msg.ApproximateFirstReceiveTimestamp,
				10,
			)
		}

		inf := &InFlightMessage{
			VisibleAt:     now.Add(time.Duration(vt) * time.Second),
			ReceiptHandle: receipt,
			Msg:           msg,
		}
		q.inFlightMessages = append(q.inFlightMessages, inf)
		result = append(result, msg)
	}

	return result
}

// DeleteMessage removes an in-flight message by its receipt handle.
func (b *InMemoryBackend) DeleteMessage(input *DeleteMessageInput) error {
	b.mu.Lock("DeleteMessage")
	defer b.mu.Unlock()

	name := queueNameFromInput(input.QueueURL)

	q, ok := b.queues[name]
	if !ok {
		return ErrQueueNotFound
	}

	for i, inf := range q.inFlightMessages {
		if inf.ReceiptHandle == input.ReceiptHandle {
			q.inFlightMessages = append(q.inFlightMessages[:i], q.inFlightMessages[i+1:]...)

			return nil
		}
	}

	return ErrReceiptHandleInvalid
}

// ChangeMessageVisibility updates the visibility timeout for an in-flight message.
func (b *InMemoryBackend) ChangeMessageVisibility(input *ChangeMessageVisibilityInput) error {
	b.mu.Lock("ChangeMessageVisibility")
	defer b.mu.Unlock()

	name := queueNameFromInput(input.QueueURL)

	q, ok := b.queues[name]
	if !ok {
		return ErrQueueNotFound
	}

	return changeVisibility(q, input.ReceiptHandle, input.VisibilityTimeout)
}

// changeVisibility updates the VisibleAt time for an in-flight message by receipt handle.
func changeVisibility(q *Queue, receiptHandle string, visibilityTimeout int) error {
	for _, inf := range q.inFlightMessages {
		if inf.ReceiptHandle == receiptHandle {
			inf.VisibleAt = time.Now().Add(time.Duration(visibilityTimeout) * time.Second)

			return nil
		}
	}

	return ErrReceiptHandleInvalid
}

// ChangeMessageVisibilityBatch updates visibility for a batch of in-flight messages.
func (b *InMemoryBackend) ChangeMessageVisibilityBatch(
	input *ChangeMessageVisibilityBatchInput,
) (*ChangeMessageVisibilityBatchOutput, error) {
	b.mu.Lock("ChangeMessageVisibilityBatch")
	defer b.mu.Unlock()

	name := queueNameFromInput(input.QueueURL)

	q, ok := b.queues[name]
	if !ok {
		return nil, ErrQueueNotFound
	}

	out := &ChangeMessageVisibilityBatchOutput{}

	for _, entry := range input.Entries {
		if err := changeVisibility(q, entry.ReceiptHandle, entry.VisibilityTimeout); err != nil {
			out.Failed = append(out.Failed, BatchErrorEntry{
				ID:          entry.ID,
				Code:        "ReceiptHandleIsInvalid",
				Message:     err.Error(),
				SenderFault: true,
			})
		} else {
			out.Successful = append(out.Successful, BatchResultEntry{ID: entry.ID})
		}
	}

	return out, nil
}

// SendMessageBatch sends a batch of messages to the specified queue.
func (b *InMemoryBackend) SendMessageBatch(input *SendMessageBatchInput) (*SendMessageBatchOutput, error) {
	if len(input.Entries) == 0 {
		return nil, ErrInvalidBatchEntry
	}

	if len(input.Entries) > maxBatchSize {
		return nil, ErrTooManyEntriesInBatch
	}

	out := &SendMessageBatchOutput{}

	for _, entry := range input.Entries {
		sendOut, err := b.SendMessage(&SendMessageInput{
			QueueURL:               input.QueueURL,
			MessageBody:            entry.MessageBody,
			MessageGroupID:         entry.MessageGroupID,
			MessageDeduplicationID: entry.MessageDeduplicationID,
			DelaySeconds:           entry.DelaySeconds,
			MessageAttributes:      entry.MessageAttributes,
		})
		if err != nil {
			out.Failed = append(out.Failed, BatchResultErrorEntry{
				ID:          entry.ID,
				Code:        err.Error(),
				Message:     err.Error(),
				SenderFault: true,
			})

			continue
		}

		out.Successful = append(out.Successful, SendMessageBatchResultEntry{
			ID:                     entry.ID,
			MessageID:              sendOut.MessageID,
			MD5OfBody:              sendOut.MD5OfBody,
			MD5OfMessageAttributes: sendOut.MD5OfMessageAttributes,
		})
	}

	return out, nil
}

// DeleteMessageBatch deletes a batch of messages from the specified queue.
func (b *InMemoryBackend) DeleteMessageBatch(input *DeleteMessageBatchInput) (*DeleteMessageBatchOutput, error) {
	if len(input.Entries) == 0 {
		return nil, ErrInvalidBatchEntry
	}

	out := &DeleteMessageBatchOutput{}

	for _, entry := range input.Entries {
		err := b.DeleteMessage(&DeleteMessageInput{
			QueueURL:      input.QueueURL,
			ReceiptHandle: entry.ReceiptHandle,
		})
		if err != nil {
			out.Failed = append(out.Failed, BatchResultErrorEntry{
				ID:          entry.ID,
				Code:        err.Error(),
				Message:     err.Error(),
				SenderFault: true,
			})

			continue
		}

		out.Successful = append(out.Successful, DeleteMessageBatchResultEntry{ID: entry.ID})
	}

	return out, nil
}

// PurgeQueue removes all messages from a queue without deleting it.
func (b *InMemoryBackend) PurgeQueue(input *PurgeQueueInput) error {
	b.mu.Lock("PurgeQueue")
	defer b.mu.Unlock()

	name := queueNameFromInput(input.QueueURL)

	q, ok := b.queues[name]
	if !ok {
		return ErrQueueNotFound
	}

	q.messages = nil
	q.inFlightMessages = nil

	return nil
}

// ListAll returns a snapshot of all queues as QueueInfo values.
// The returned slice contains value copies of the immutable queue metadata, safe for
// concurrent use after the lock is released.
func (b *InMemoryBackend) ListAll() []QueueInfo {
	b.mu.RLock("ListAll")
	defer b.mu.RUnlock()

	result := make([]QueueInfo, 0, len(b.queues))

	for _, q := range b.queues {
		result = append(result, QueueInfo{Name: q.Name, URL: q.URL, IsFIFO: q.IsFIFO})
	}

	return result
}

// TagQueue adds or updates tags on a queue.
func (b *InMemoryBackend) TagQueue(input *TagQueueInput) error {
	b.mu.Lock("TagQueue")
	defer b.mu.Unlock()

	name := queueNameFromInput(input.QueueURL)

	q, ok := b.queues[name]
	if !ok {
		return ErrQueueNotFound
	}

	if q.Tags == nil {
		q.Tags = tags.New("sqs.queue." + q.Name + ".tags")
	}

	if input.Tags != nil {
		q.Tags.Merge(input.Tags.Clone())
	}

	return nil
}

// UntagQueue removes tags from a queue.
func (b *InMemoryBackend) UntagQueue(input *UntagQueueInput) error {
	b.mu.Lock("UntagQueue")
	defer b.mu.Unlock()

	name := queueNameFromInput(input.QueueURL)

	q, ok := b.queues[name]
	if !ok {
		return ErrQueueNotFound
	}

	if q.Tags != nil {
		q.Tags.DeleteKeys(input.TagKeys)
	}

	return nil
}

// ListQueueTags returns the tags for a queue.
func (b *InMemoryBackend) ListQueueTags(input *ListQueueTagsInput) (*ListQueueTagsOutput, error) {
	b.mu.RLock("ListQueueTags")
	defer b.mu.RUnlock()

	name := queueNameFromInput(input.QueueURL)

	q, ok := b.queues[name]
	if !ok {
		return nil, ErrQueueNotFound
	}

	if q.Tags == nil {
		return &ListQueueTagsOutput{Tags: tags.New("sqs.queue." + q.Name + ".tags")}, nil
	}

	return &ListQueueTagsOutput{Tags: q.Tags}, nil
}
