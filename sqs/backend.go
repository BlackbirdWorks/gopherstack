package sqs

import (
	"crypto/md5" //nolint:gosec // MD5 used for SQS wire protocol compatibility, not security
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
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
	ListAll() []*Queue
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	queues map[string]*Queue
	mu     sync.RWMutex
}

// NewInMemoryBackend creates a new empty InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		queues: make(map[string]*Queue),
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

// computeMD5 returns the hex-encoded MD5 hash of the given string.
func computeMD5(body string) string {
	//nolint:gosec // MD5 required by SQS wire protocol
	hash := md5.Sum([]byte(body))

	return hex.EncodeToString(hash[:])
}

// buildDefaultAttributes initialises the attribute map for a new queue.
func buildDefaultAttributes(queueName string, isFIFO bool) map[string]string {
	now := strconv.FormatInt(time.Now().Unix(), 10)
	arn := fmt.Sprintf("arn:aws:sqs:%s:%s:%s", sqsRegion, accountID, queueName)

	attrs := map[string]string{
		attrVisibilityTimeout:             strconv.Itoa(defaultVisibilityTimeout),
		attrMaximumMessageSize:            strconv.Itoa(defaultMaxMessageSize),
		attrMessageRetentionPeriod:        strconv.Itoa(defaultMessageRetentionPeriod),
		attrDelaySeconds:                  strconv.Itoa(defaultDelaySeconds),
		attrReceiveMessageWaitTimeSeconds: strconv.Itoa(defaultWaitTimeSeconds),
		attrCreatedTimestamp:              now,
		attrLastModifiedTimestamp:         now,
		attrQueueArn:                      arn,
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
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.queues[input.QueueName]; exists {
		return nil, ErrQueueAlreadyExists
	}

	isFIFO := strings.HasSuffix(input.QueueName, fifoSuffix)
	attrs := buildDefaultAttributes(input.QueueName, isFIFO)

	maps.Copy(attrs, input.Attributes)

	queueURL := "http://" + input.Endpoint + "/" + accountID + "/" + input.QueueName

	q := &Queue{
		Name:                input.QueueName,
		URL:                 queueURL,
		IsFIFO:              isFIFO,
		Attributes:          attrs,
		DeduplicationIDs:    make(map[string]time.Time),
		deduplicationMsgIDs: make(map[string]string),
	}

	b.queues[input.QueueName] = q

	return &CreateQueueOutput{QueueURL: queueURL}, nil
}

// DeleteQueue removes a queue by its URL.
func (b *InMemoryBackend) DeleteQueue(input *DeleteQueueInput) error {
	b.mu.Lock()
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
	b.mu.RLock()
	defer b.mu.RUnlock()

	var urls []string

	for name, q := range b.queues {
		if input.QueueNamePrefix == "" || strings.HasPrefix(name, input.QueueNamePrefix) {
			urls = append(urls, q.URL)
		}
	}

	return &ListQueuesOutput{QueueURLs: urls}, nil
}

// GetQueueURL returns the URL for a queue by name.
func (b *InMemoryBackend) GetQueueURL(input *GetQueueURLInput) (*GetQueueURLOutput, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	q, ok := b.queues[input.QueueName]
	if !ok {
		return nil, ErrQueueNotFound
	}

	return &GetQueueURLOutput{QueueURL: q.URL}, nil
}

// GetQueueAttributes returns queue attributes, computing dynamic ones on the fly.
func (b *InMemoryBackend) GetQueueAttributes(input *GetQueueAttributesInput) (*GetQueueAttributesOutput, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

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
	b.mu.Lock()
	defer b.mu.Unlock()

	name := queueNameFromInput(input.QueueURL)

	q, ok := b.queues[name]
	if !ok {
		return ErrQueueNotFound
	}

	maps.Copy(q.Attributes, input.Attributes)

	q.Attributes[attrLastModifiedTimestamp] = strconv.FormatInt(time.Now().Unix(), 10)

	return nil
}

// SendMessage adds a message to the specified queue.
func (b *InMemoryBackend) SendMessage(input *SendMessageInput) (*SendMessageOutput, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	name := queueNameFromInput(input.QueueURL)

	q, ok := b.queues[name]
	if !ok {
		return nil, ErrQueueNotFound
	}

	md5Body := computeMD5(input.MessageBody)

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

	return &SendMessageOutput{MessageID: msgID, MD5OfBody: md5Body}, nil
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

// ReceiveMessage retrieves messages from the queue, with optional long-poll wait.
func (b *InMemoryBackend) ReceiveMessage(input *ReceiveMessageInput) (*ReceiveMessageOutput, error) {
	name := queueNameFromInput(input.QueueURL)
	deadline := time.Now().Add(time.Duration(input.WaitTimeSeconds) * time.Second)

	for {
		msgs, err := b.receiveOnce(name, input)
		if err != nil {
			return nil, err
		}

		if len(msgs) > 0 || !time.Now().Before(deadline) {
			return &ReceiveMessageOutput{Messages: msgs}, nil
		}

		time.Sleep(longPollIntervalMs * time.Millisecond)
	}
}

// receiveOnce performs a single receive attempt under the backend lock.
func (b *InMemoryBackend) receiveOnce(name string, input *ReceiveMessageInput) ([]*Message, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	q, ok := b.queues[name]
	if !ok {
		return nil, ErrQueueNotFound
	}

	now := time.Now()
	reQueueExpired(q, now)

	maxMessages := input.MaxNumberOfMessages
	if maxMessages <= 0 {
		maxMessages = 1
	}

	vt := resolveVisibilityTimeout(input.VisibilityTimeout, q)

	return pickMessages(q, maxMessages, vt, now), nil
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

	result := make([]*Message, 0, count)

	for _, msg := range picked {
		receipt := uuid.New().String()
		msg.ReceiptHandle = receipt
		msg.ApproximateReceiveCount++
		msg.Attributes[attrApproxReceiveCount] = strconv.Itoa(msg.ApproximateReceiveCount)

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
	b.mu.Lock()
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
	b.mu.Lock()
	defer b.mu.Unlock()

	name := queueNameFromInput(input.QueueURL)

	q, ok := b.queues[name]
	if !ok {
		return ErrQueueNotFound
	}

	for _, inf := range q.inFlightMessages {
		if inf.ReceiptHandle == input.ReceiptHandle {
			inf.VisibleAt = time.Now().Add(time.Duration(input.VisibilityTimeout) * time.Second)

			return nil
		}
	}

	return ErrReceiptHandleInvalid
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
			ID:        entry.ID,
			MessageID: sendOut.MessageID,
			MD5OfBody: sendOut.MD5OfBody,
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
	b.mu.Lock()
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

// ListAll returns all queues in the backend.
func (b *InMemoryBackend) ListAll() []*Queue {
	b.mu.RLock()
	defer b.mu.RUnlock()

	queues := make([]*Queue, 0, len(b.queues))

	for _, q := range b.queues {
		queues = append(queues, q)
	}

	return queues
}
