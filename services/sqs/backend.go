package sqs

import (
	"context"
	"crypto/md5" //nolint:gosec // MD5 used for SQS wire protocol compatibility, not security
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"

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
	ListDeadLetterSourceQueues(input *ListDeadLetterSourceQueuesInput) (*ListDeadLetterSourceQueuesOutput, error)
	AddPermission(input *AddPermissionInput) error
	RemovePermission(input *RemovePermissionInput) error
	StartMessageMoveTask(input *StartMessageMoveTaskInput) (*StartMessageMoveTaskOutput, error)
	CancelMessageMoveTask(input *CancelMessageMoveTaskInput) (*CancelMessageMoveTaskOutput, error)
	ListMessageMoveTasks(input *ListMessageMoveTasksInput) (*ListMessageMoveTasksOutput, error)
	ListAll() []QueueInfo
}

// moveTaskVisibilityTimeoutSecs is the visibility timeout used when receiving
// messages during a message move task. It provides enough time to send the
// message to the destination and delete it from the source.
const moveTaskVisibilityTimeoutSecs = 30

const (
	janitorInterval      = 30 * time.Second
	moveTaskRetentionTTL = 15 * time.Minute
)

// moveTaskState tracks the live state of a StartMessageMoveTask goroutine.
type moveTaskState struct {
	cancel        context.CancelFunc
	taskHandle    string
	sourceArn     string
	destArn       string
	failureReason string
	status        MoveTaskStatus
	startedAt     int64
	movedCount    int64
	totalCount    int64
	mu            sync.Mutex
	maxPerSec     int32
}

// MetricEmitter emits a CloudWatch metric data point.
// It is implemented by the CloudWatch backend and injected into InMemoryBackend
// so that SQS operations can be forwarded to CloudWatch as metrics.
type MetricEmitter interface {
	EmitMetric(namespace, name string, value float64, unit string) error
}

// MetricEmitterFunc is a function adapter for MetricEmitter.
type MetricEmitterFunc func(namespace, name string, value float64, unit string) error

// EmitMetric implements MetricEmitter.
func (f MetricEmitterFunc) EmitMetric(namespace, name string, value float64, unit string) error {
	return f(namespace, name, value, unit)
}

// sqsMetricNamespace is the CloudWatch namespace used for SQS metrics.
const sqsMetricNamespace = "AWS/SQS"

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	metricEmitter  MetricEmitter
	queues         map[string]*Queue
	moveTasks      map[string]*moveTaskState
	snsUnsubscribe func()
	janitorStop    chan struct{}
	mu             *lockmetrics.RWMutex
	accountID      string
	region         string
}

// SetMetricEmitter sets the emitter used to forward SQS operation metrics to CloudWatch.
func (b *InMemoryBackend) SetMetricEmitter(e MetricEmitter) {
	b.mu.Lock("SetMetricEmitter")
	defer b.mu.Unlock()

	b.metricEmitter = e
}

// emitMetric emits a single metric to CloudWatch if a MetricEmitter is configured.
// It reads the emitter under the lock, then calls it without holding the lock
// to avoid a potential deadlock if the emitter itself takes a lock.
func (b *InMemoryBackend) emitMetric(name string, value float64, unit string) {
	b.mu.RLock("emitMetric")
	e := b.metricEmitter
	b.mu.RUnlock()

	if e == nil {
		return
	}

	// Emit without holding the lock.
	_ = e.EmitMetric(sqsMetricNamespace, name, value, unit)
}

const sqsDefaultMaxResults = 1000

// NewInMemoryBackend creates a new empty InMemoryBackend with default account/region.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion)
}

// NewInMemoryBackendWithConfig creates a new InMemoryBackend with the given account ID and region.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		queues:    make(map[string]*Queue),
		moveTasks: make(map[string]*moveTaskState),
		accountID: accountID,
		region:    region,
		mu:        lockmetrics.New("sqs"),
	}

	b.startJanitor()

	return b
}

func (b *InMemoryBackend) startJanitor() {
	b.janitorStop = make(chan struct{})

	go b.runJanitor()
}

// Close stops the background janitor goroutine and releases associated
// resources.  It is safe to call Close multiple times; subsequent calls are
// no-ops.  The backend must not be used after Close returns.
func (b *InMemoryBackend) Close() {
	b.stopInternalJanitor()
}

// stopInternalJanitor stops the internal janitor goroutine started by
// NewInMemoryBackendWithConfig. Safe to call multiple times.
func (b *InMemoryBackend) stopInternalJanitor() {
	b.mu.Lock("stopInternalJanitor")
	ch := b.janitorStop
	b.mu.Unlock()

	if ch == nil {
		return
	}

	select {
	case <-ch:
		// already closed
	default:
		close(ch)
	}
}

func (b *InMemoryBackend) runJanitor() {
	ticker := time.NewTicker(janitorInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.janitorStop:
			return
		case <-ticker.C:
			b.pruneState(time.Now())
		}
	}
}

func (b *InMemoryBackend) pruneState(now time.Time) {
	b.mu.Lock("pruneState")
	defer b.mu.Unlock()

	dedupPruned := 0
	msgExpired := 0

	for _, q := range b.queues {
		if q.IsFIFO {
			before := len(q.DeduplicationIDs)
			pruneDedup(q, now)
			dedupPruned += before - len(q.DeduplicationIDs)
		}

		before := len(q.messages)
		reQueueExpired(q, now)
		expireRetainedMessages(q, now)
		drainToDLQ(q)
		msgExpired += max(0, before-len(q.messages))
	}

	tasksPruned := 0
	pruneBefore := now.Add(-moveTaskRetentionTTL).UnixMilli()

	for taskHandle, task := range b.moveTasks {
		task.mu.Lock()
		isTerminal := task.status == MoveTaskStatusCompleted ||
			task.status == MoveTaskStatusCancelled ||
			task.status == MoveTaskStatusFailed
		isExpired := task.startedAt <= pruneBefore
		task.mu.Unlock()

		if isTerminal && isExpired {
			delete(b.moveTasks, taskHandle)
			tasksPruned++
		}
	}

	totalItems := dedupPruned + msgExpired + tasksPruned
	telemetry.RecordWorkerItems("sqs", "JanitorSweeper", totalItems)
	telemetry.RecordWorkerTask("sqs", "JanitorSweeper", "success")
}

// queueNameFromInput extracts the queue name from a queue URL.
func queueNameFromInput(queueURL string) string {
	parts := strings.Split(queueURL, "/")
	if len(parts) == 0 {
		return ""
	}

	return parts[len(parts)-1]
}

// effectiveRegion returns the region to scope a backend lookup against.
// Empty input falls back to the backend's default region so single-region
// callers (and existing tests) continue to work without explicit region threading.
func (b *InMemoryBackend) effectiveRegion(region string) string {
	if region != "" {
		return region
	}

	return b.region
}

// queueKey is the composite map key used by b.queues, formed from
// (region, name). Two queues with the same name in different regions occupy
// distinct keys, matching AWS semantics where queue names are scoped per-region.
func queueKey(region, name string) string {
	return region + "/" + name
}

// lookupQueueByName returns the queue stored under (region, name), or false if
// no queue exists in that region with that name.
func (b *InMemoryBackend) lookupQueueByName(region, name string) (*Queue, bool) {
	q, ok := b.queues[queueKey(b.effectiveRegion(region), name)]

	return q, ok
}

// lookupQueueByURL finds a queue by its URL.
//
// When a region is supplied (the caller threaded the SigV4 region from the
// request) the queue MUST live in that region; a queue with the same name in
// a different region is treated as not found, matching real AWS where the
// SigV4 region must match the regional endpoint. Lookup uses the queue name
// extracted from the URL plus the region — we do NOT require the stored
// q.URL to be byte-identical to the caller's queueURL because SDKs and proxy
// hops may rewrite the host/port (e.g. host.docker.internal vs localhost).
//
// When region is empty the lookup falls back to a URL-string scan across all
// regions to support callers that have not yet been wired to thread region
// through. New code should always pass the request region.
func (b *InMemoryBackend) lookupQueueByURL(region, queueURL string) (*Queue, bool) {
	name := queueNameFromInput(queueURL)

	if region != "" {
		q, ok := b.queues[queueKey(region, name)]
		if !ok {
			return nil, false
		}

		return q, true
	}

	for _, q := range b.queues {
		if q.URL == queueURL {
			return q, true
		}
	}

	return nil, false
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

	// DLQ must reside in the same region as the source queue (AWS rule).
	dlq, exists := backend.lookupQueueByName(q.Region, dlqName)
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
	if q, exists := b.queues[queueKey(region, input.QueueName)]; exists {
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
	attrs := buildDefaultAttributes(input.QueueName, b.accountID, region, isFIFO)

	maps.Copy(attrs, input.Attributes)

	queueURL := "http://" + input.Endpoint + "/" + b.accountID + "/" + input.QueueName

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
	}

	b.queues[queueKey(region, input.QueueName)] = q

	applyRedrivePolicy(q, attrs, b)

	return &CreateQueueOutput{QueueURL: queueURL}, nil
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

	delete(b.queues, queueKey(q.Region, q.Name))

	// Cancel any active move tasks that involve this queue (either as source or
	// destination) to prevent goroutine leaks.
	for _, task := range b.moveTasks {
		task.mu.Lock()
		isActive := task.status == MoveTaskStatusRunning || task.status == MoveTaskStatusCancelling
		involves := task.sourceArn == queueARN || task.destArn == queueARN
		task.mu.Unlock()

		if isActive && involves {
			task.cancel()
		}
	}

	return nil
}

// ListQueues returns queue URLs in the requested region, optionally filtered by prefix.
func (b *InMemoryBackend) ListQueues(input *ListQueuesInput) (*ListQueuesOutput, error) {
	b.mu.RLock("ListQueues")
	defer b.mu.RUnlock()

	scope := b.effectiveRegion(input.Region)

	var urls []string

	for _, q := range b.queues {
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

// GetQueueAttributes returns queue attributes, computing dynamic ones on the fly.
func (b *InMemoryBackend) GetQueueAttributes(input *GetQueueAttributesInput) (*GetQueueAttributesOutput, error) {
	b.mu.RLock("GetQueueAttributes")
	defer b.mu.RUnlock()

	name := queueNameFromInput(input.QueueURL)

	q, ok := b.lookupQueueByName(input.Region, name)
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
	now := time.Now()
	delayed := 0

	for _, msg := range q.messages {
		if now.Before(msg.VisibleAt) {
			delayed++
		}
	}

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

	maps.Copy(q.Attributes, input.Attributes)

	if _, hasRedrive := input.Attributes[attrRedrivePolicy]; hasRedrive {
		applyRedrivePolicy(q, input.Attributes, b)
	}

	q.Attributes[attrLastModifiedTimestamp] = strconv.FormatInt(time.Now().Unix(), 10)

	return nil
}

// validateQueueAttributes returns an error if any of the provided queue attributes
// fall outside their allowed ranges (matching AWS SQS validation).
func validateQueueAttributes(attrs map[string]string) error {
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

	// AWS requires DeduplicationScope=messageGroup when FifoThroughputLimit
	// is perMessageGroupId. Enforce the pairing only when both sides are
	// present in the same SetQueueAttributes call.
	if t, hasT := attrs[attrFifoThroughputLimit]; hasT && t == fifoThroughputLimitPerMessageGroupID {
		if s, hasS := attrs[attrDeduplicationScope]; hasS && s != fifoDedupScopePerMessageGroup {
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

// checkFIFOPerGroupRateLimit enforces the 300 TPS per-message-group AWS limit
// for FIFO queues running with FifoThroughputLimit=perMessageGroupId.
//
// Maintains a sliding 1-second window per group, pruning timestamps older
// than the window on each call. Returns ErrOverLimit when the window is
// already full; otherwise appends the new send and returns nil.
//
// Caller must hold b.mu (write). Allocates the per-queue map lazily.
func checkFIFOPerGroupRateLimit(q *Queue, group string, now time.Time) error {
	if group == "" {
		return nil
	}

	if q.fifoSendTimes == nil {
		q.fifoSendTimes = make(map[string][]time.Time)
	}

	cutoff := now.Add(-time.Second)
	prev := q.fifoSendTimes[group]
	kept := prev[:0]
	for _, t := range prev {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}

	if len(kept) >= fifoPerGroupTPS {
		q.fifoSendTimes[group] = kept

		return ErrOverLimit
	}

	q.fifoSendTimes[group] = append(kept, now)

	return nil
}

// buildInitialMessageAttributes returns the system-attribute map stamped on a
// new message at send-time. AWSTraceHeader is the one reserved name SQS
// accepts; we copy its StringValue verbatim so consumers see the original
// X-Ray trace context.
func buildInitialMessageAttributes(
	sentTS string, sysAttrs map[string]MessageAttributeValue,
) map[string]string {
	attrs := map[string]string{
		attrSentTimestamp:      sentTS,
		attrApproxReceiveCount: attrValZero,
	}

	if v, ok := sysAttrs[attrAWSTraceHeader]; ok && v.StringValue != "" {
		attrs[attrAWSTraceHeader] = v.StringValue
	}

	return attrs
}

// fifoPreflight is the outcome of preflightFIFOSend. handled=true means the
// caller should return Output/Err immediately; handled=false means proceed
// with normal send flow.
type fifoPreflight struct {
	Output  *SendMessageOutput
	Err     error
	Handled bool
}

// preflightFIFOSend runs the FIFO-only preconditions a SendMessage must
// satisfy before the message is constructed: parameter validation, per-group
// throughput limiting, and content-based deduplication.
//
// Caller must already hold b.mu (write).
func preflightFIFOSend(q *Queue, input *SendMessageInput, md5Body string, now time.Time) fifoPreflight {
	if err := validateFIFOParams(input, q); err != nil {
		return fifoPreflight{Err: err, Handled: true}
	}

	if q.Attributes[attrFifoThroughputLimit] == fifoThroughputLimitPerMessageGroupID {
		if err := checkFIFOPerGroupRateLimit(q, input.MessageGroupID, now); err != nil {
			return fifoPreflight{Err: err, Handled: true}
		}
	}

	if out, dup := checkDedup(
		q,
		input.MessageGroupID,
		input.MessageDeduplicationID,
		md5Body,
		q.Attributes[attrContentBasedDeduplication],
		now,
	); dup {
		return fifoPreflight{Output: out, Handled: true}
	}

	return fifoPreflight{}
}

// SendMessage adds a message to the specified queue.
func (b *InMemoryBackend) SendMessage(input *SendMessageInput) (*SendMessageOutput, error) {
	if input.MessageBody == "" {
		return nil, ErrInvalidMessageBody
	}

	if input.DelaySeconds < 0 || input.DelaySeconds > maxDelaySeconds {
		return nil, ErrInvalidDelaySeconds
	}

	if err := validateMessageAttributes(input.MessageAttributes); err != nil {
		return nil, err
	}

	md5Body := computeMD5(input.MessageBody)
	md5Attrs := computeMD5OfMessageAttributes(input.MessageAttributes)
	msgID := uuid.New().String()

	b.mu.Lock("SendMessage")
	defer b.mu.Unlock()

	name := queueNameFromInput(input.QueueURL)

	q, ok := b.lookupQueueByName(input.Region, name)
	if !ok {
		return nil, ErrQueueNotFound
	}

	if err := validateMessageSize(input.MessageBody, input.MessageAttributes, q); err != nil {
		return nil, err
	}

	// Capture now once so that pruneDedup, checkDedup, storeDedup, and message
	// timestamps all use the same consistent clock value.
	now := time.Now()

	if q.IsFIFO {
		if pre := preflightFIFOSend(q, input, md5Body, now); pre.Handled {
			return pre.Output, pre.Err
		}
	}

	sentTS := strconv.FormatInt(now.UnixMilli(), 10)

	var seqNum string
	if q.IsFIFO {
		// fifoSeqCounter is guarded by b.mu (write-locked throughout SendMessage).
		// No additional synchronisation is required.
		q.fifoSeqCounter++
		seqNum = fmt.Sprintf("%020d", q.fifoSeqCounter)
	}

	msg := &Message{
		MessageID:              msgID,
		Body:                   input.MessageBody,
		MD5OfBody:              md5Body,
		MD5OfMessageAttributes: md5Attrs,
		MessageGroupID:         input.MessageGroupID,
		MessageDeduplicationID: input.MessageDeduplicationID,
		SequenceNumber:         seqNum,
		SentTimestamp:          now.UnixMilli(),
		MessageAttributes:      input.MessageAttributes,
		Attributes:             buildInitialMessageAttributes(sentTS, input.MessageSystemAttributes),
		VisibleAt:              resolveMessageVisibleAt(now, input.DelaySeconds, q.Attributes[attrDelaySeconds]),
	}

	if q.IsFIFO {
		msg.Attributes[attrSequenceNumber] = seqNum
		if input.MessageGroupID != "" {
			msg.Attributes[attrMessageGroupIDSys] = input.MessageGroupID
		}

		if input.MessageDeduplicationID != "" {
			msg.Attributes[attrMessageDeduplicationIDSys] = input.MessageDeduplicationID
		}

		storeDedup(q, input.MessageGroupID, input.MessageDeduplicationID, md5Body, q.Attributes[attrContentBasedDeduplication], msgID, now)
	}

	q.messages = append(q.messages, msg)

	// Broadcast to all long-polling receivers: close the current generation channel
	// (which unblocks all goroutines waiting on it) and replace it with a new one.
	// Any receiver holding a reference to the old closed channel will wake up, re-check
	// for messages under the lock, and (if no messages are found yet) capture the new
	// channel for the next wait. This provides fair wake-up for all concurrent receivers.
	old := q.notify
	q.notify = make(chan struct{})
	close(old)

	out := &SendMessageOutput{
		MessageID:              msgID,
		MD5OfBody:              md5Body,
		MD5OfMessageAttributes: md5Attrs,
		SequenceNumber:         seqNum,
	}

	// Emit CloudWatch metric for NumberOfMessagesSent (after releasing the lock).
	go b.emitMetric("NumberOfMessagesSent", 1, "Count")

	return out, nil
}

// validateMessageSize checks whether the total message size (body plus
// attribute names, values, and types) exceeds the queue's MaximumMessageSize.
// AWS SQS measures size as the sum of UTF-8 bytes across the body and every
// attribute name / type / value, matching the documented limit.
func validateMessageSize(body string, attrs map[string]MessageAttributeValue, q *Queue) error {
	maxSize := defaultMaxMessageSize

	if v, err := strconv.Atoi(q.Attributes[attrMaximumMessageSize]); err == nil && v > 0 {
		maxSize = v
	}

	total := len(body)
	for name, attr := range attrs {
		total += len(name) + len(attr.DataType) + len(attr.StringValue) + len(attr.BinaryValue)
	}

	if total > maxSize {
		return ErrMessageTooLarge
	}

	return nil
}

// isValidDataTypeBase reports whether base is one of the three AWS-defined
// message attribute base types: String, Number, or Binary.
func isValidDataTypeBase(base string) bool {
	return base == "String" || base == "Number" || base == "Binary"
}

// validateMessageAttributes checks that each message attribute has a recognised
// DataType and that the correct value field is populated.
// AWS rules:
//   - DataType must be "String", "Number", "Binary", or "<base>.<custom-suffix>"
//   - String/Number attributes must supply StringValue
//   - Binary attributes must supply BinaryValue
func validateMessageAttributes(attrs map[string]MessageAttributeValue) error {
	for _, attr := range attrs {
		base, _, _ := strings.Cut(attr.DataType, ".")
		if !isValidDataTypeBase(base) {
			return ErrInvalidMessageAttributeValue
		}

		if base == "Binary" {
			if len(attr.BinaryValue) == 0 {
				return ErrInvalidMessageAttributeValue
			}
		} else {
			if attr.StringValue == "" {
				return ErrInvalidMessageAttributeValue
			}
		}
	}

	return nil
}

// validateFIFOParams validates FIFO-specific parameters for a SendMessage request.
// AWS requires MessageGroupID for all FIFO sends, and MessageDeduplicationID when
// ContentBasedDeduplication is disabled on the queue. FIFO queues do not support
// per-message delays; a non-zero DelaySeconds is rejected.
func validateFIFOParams(input *SendMessageInput, q *Queue) error {
	if input.MessageGroupID == "" {
		return ErrMissingMessageGroupID
	}

	if input.DelaySeconds > 0 {
		return ErrFIFODelayNotSupported
	}

	contentBasedDedup := q.Attributes[attrContentBasedDeduplication]
	if contentBasedDedup != attrValTrue && input.MessageDeduplicationID == "" {
		return ErrMissingDeduplicationID
	}

	return nil
}

// resolveMessageVisibleAt computes the earliest time the message should be visible.
// Message-level delaySeconds takes precedence over the queue-level attribute.
// A zero [time.Time] return value means the message is immediately visible (no delay).
func resolveMessageVisibleAt(now time.Time, msgDelaySeconds int, queueDelayAttr string) time.Time {
	if msgDelaySeconds > 0 {
		return now.Add(time.Duration(msgDelaySeconds) * time.Second)
	}

	if qd, err := strconv.Atoi(queueDelayAttr); err == nil && qd > 0 {
		return now.Add(time.Duration(qd) * time.Second)
	}

	// Zero time means no delay — the message is immediately visible to consumers.
	return time.Time{}
}

// dedupKey returns the deduplication map key, respecting the queue's
// DeduplicationScope attribute. When scope is "queue" (queue-wide), only the
// effective dedup ID is used as the key. The default scope is "messageGroup",
// where the key is scoped per group to allow identical messages in different
// groups within the same 5-minute window.
func dedupKey(q *Queue, groupID, effectiveID string) string {
	if q.Attributes[attrDeduplicationScope] == fifoDedupScopeQueue {
		return effectiveID
	}

	// Default: messageGroup scope — key by group + dedupID.
	return groupID + "|" + effectiveID
}

// checkDedup checks for a duplicate FIFO message and returns the original output if found.
// now is the reference time used for window expiry comparison.
func checkDedup(q *Queue, groupID, dedupID, md5Body, contentBasedDedup string, now time.Time) (*SendMessageOutput, bool) {
	effectiveID := dedupID
	if effectiveID == "" && contentBasedDedup == attrValTrue {
		effectiveID = md5Body
	}

	if effectiveID == "" {
		return nil, false
	}

	key := dedupKey(q, groupID, effectiveID)

	expiry, found := q.DeduplicationIDs[key]
	if !found {
		return nil, false
	}

	if !now.Before(expiry) {
		// Eagerly remove the expired entry inline. This keeps the deduplication map
		// lean without waiting for the next janitor sweep, reducing memory pressure
		// and speeding up subsequent lookups.
		delete(q.DeduplicationIDs, key)
		delete(q.deduplicationMsgIDs, key)

		return nil, false
	}

	origMsgID := q.deduplicationMsgIDs[key]

	return &SendMessageOutput{MessageID: origMsgID, MD5OfBody: md5Body}, true
}

// maxDedupEntriesPerQueue caps the per-queue deduplication maps to bound memory
// in the absence of a janitor sweep. AWS keeps the dedup window at 5 minutes and
// limits transactions per second well below this cap; once exceeded the oldest
// entries (by expiry) are evicted to make room.
const maxDedupEntriesPerQueue = 100_000

// storeDedup records a deduplication entry for a FIFO message. When the dedup
// map is at capacity, the entries closest to expiry are evicted first.
func storeDedup(q *Queue, groupID, dedupID, md5Body, contentBasedDedup, msgID string, now time.Time) {
	effectiveID := dedupID
	if effectiveID == "" && contentBasedDedup == attrValTrue {
		effectiveID = md5Body
	}

	if effectiveID == "" {
		return
	}

	if len(q.DeduplicationIDs) >= maxDedupEntriesPerQueue {
		evictOldestDedup(q, len(q.DeduplicationIDs)-maxDedupEntriesPerQueue+1)
	}

	key := dedupKey(q, groupID, effectiveID)
	q.DeduplicationIDs[key] = now.Add(deduplicationWindowSecs * time.Second)
	q.deduplicationMsgIDs[key] = msgID
}

// evictOldestDedup removes up to n entries with the earliest expiry times.
// Linear scan is acceptable given maxDedupEntriesPerQueue is small enough that
// hitting the cap is the cold path (janitor normally prunes first).
func evictOldestDedup(q *Queue, n int) {
	for ; n > 0 && len(q.DeduplicationIDs) > 0; n-- {
		var oldestKey string
		var oldestExpiry time.Time
		first := true
		for k, exp := range q.DeduplicationIDs {
			if first || exp.Before(oldestExpiry) {
				oldestKey = k
				oldestExpiry = exp
				first = false
			}
		}
		delete(q.DeduplicationIDs, oldestKey)
		delete(q.deduplicationMsgIDs, oldestKey)
	}
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
// Long polling uses a broadcast notify channel: SendMessage closes the current generation
// channel (waking all waiting receivers) and replaces it with a new one. This ensures
// all concurrent long-poll receivers are woken on each message arrival rather than just one.
// A 1-second recheck interval is also applied so that messages which reappear
// due to visibility-timeout expiry (reQueueExpired) are picked up promptly even
// when no new SendMessage occurs.
// validateReceiveInput validates and normalises the receive input, returning an
// error if any parameter is out of range.  It mutates MaxNumberOfMessages so
// that a zero value becomes the AWS default of 1.
func validateReceiveInput(input *ReceiveMessageInput) error {
	if input.WaitTimeSeconds < 0 || input.WaitTimeSeconds > maxReceiveMessageWaitTimeSeconds {
		return ErrInvalidWaitTime
	}

	// AWS accepts MaxNumberOfMessages in [1, 10]. Default to 1 when unset (0).
	if input.MaxNumberOfMessages == 0 {
		input.MaxNumberOfMessages = 1
	}

	if input.MaxNumberOfMessages < 1 || input.MaxNumberOfMessages > maxBatchSize {
		return ErrInvalidMaxMessages
	}

	return nil
}

func (b *InMemoryBackend) ReceiveMessage(input *ReceiveMessageInput) (*ReceiveMessageOutput, error) {
	if err := validateReceiveInput(input); err != nil {
		return nil, err
	}

	// If the caller did not specify a positive WaitTimeSeconds, apply the queue's
	// ReceiveMessageWaitTimeSeconds attribute as the default long-poll duration.
	// This mirrors the AWS behaviour where the queue-level attribute acts as the
	// default wait time for receives that omit the parameter.
	waitSecs := b.resolveWaitSeconds(input.QueueURL, input.WaitTimeSeconds)

	name := queueNameFromInput(input.QueueURL)
	deadline := time.Now().Add(time.Duration(waitSecs) * time.Second)

	const recheckInterval = time.Second

	timer := time.NewTimer(recheckInterval)
	defer timer.Stop()

	for {
		msgs, notifyCh, err := b.receiveOnce(name, input)
		if err != nil {
			return nil, err
		}

		if len(msgs) > 0 {
			count := float64(len(msgs))
			go b.emitMetric("NumberOfMessagesReceived", count, "Count")

			return &ReceiveMessageOutput{Messages: msgs}, nil
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return &ReceiveMessageOutput{}, nil
		}

		sleep := min(remaining, recheckInterval)

		// Stop and drain the timer before Reset to guarantee the channel is empty
		// before the next iteration begins. Both select arms leave the timer in a
		// consistent state: the notifyCh arm already stops and drains; the C arm
		// fires normally and drains. An explicit stop-and-drain here makes the
		// invariant explicit and safe against future loop restructuring.
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}

		timer.Reset(sleep)

		select {
		case <-notifyCh:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
		}
	}
}

// resolveWaitSeconds returns the effective long-poll wait duration.
// If requested is positive it is returned unchanged. Otherwise the queue's
// ReceiveMessageWaitTimeSeconds attribute is used as the default.
func (b *InMemoryBackend) resolveWaitSeconds(queueURL string, requested int) int {
	if requested > 0 {
		return requested
	}

	b.mu.RLock("resolveWaitSeconds")
	defer b.mu.RUnlock()

	if q, ok := b.lookupQueueByURL("", queueURL); ok {
		if v, err := strconv.Atoi(q.Attributes[attrReceiveMessageWaitTimeSeconds]); err == nil && v > 0 {
			return v
		}
	}

	return 0
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

// receiveAttemptTTL is the AWS-specified window for ReceiveRequestAttemptID
// deduplication on FIFO queues (5 minutes).
const receiveAttemptTTL = 5 * time.Minute

// receiveOnce performs a single receive attempt under the backend lock.
func (b *InMemoryBackend) receiveOnce(name string, input *ReceiveMessageInput) ([]*Message, chan struct{}, error) {
	b.mu.Lock("receiveOnce")
	defer b.mu.Unlock()

	q, ok := b.lookupQueueByName(input.Region, name)
	if !ok {
		return nil, nil, ErrQueueNotFound
	}

	now := time.Now()
	reQueueExpired(q, now)
	expireRetainedMessages(q, now)
	drainToDLQ(q)

	if q.IsFIFO {
		pruneDedup(q, now)
		pruneReceiveAttempts(q, now)

		// FIFO exactly-once retry: if the caller repeats with the same
		// ReceiveRequestAttemptID within 5 minutes, return the cached result.
		if id := input.ReceiveRequestAttemptID; id != "" {
			if entry, found := q.receiveAttempts[id]; found && now.Before(entry.expiresAt) {
				return entry.msgs, q.notify, nil
			}
		}
	}

	maxMessages := input.MaxNumberOfMessages
	if maxMessages <= 0 {
		maxMessages = 1
	}
	if maxMessages > maxBatchSize {
		maxMessages = maxBatchSize
	}

	// AWS rejects ReceiveMessage when the queue is already at its in-flight cap
	// (120k for standard, 20k for FIFO) — clients see OverLimit and must wait.
	limit := maxInFlightStandard
	if q.IsFIFO {
		limit = maxInFlightFIFO
	}
	if len(q.inFlightMessages) >= limit {
		return nil, q.notify, ErrOverLimit
	}

	vt := resolveVisibilityTimeout(input.VisibilityTimeout, q)
	msgs := pickMessages(q, b.accountID, maxMessages, vt, now)

	// Cache the result for FIFO ReceiveRequestAttemptID deduplication.
	if q.IsFIFO && input.ReceiveRequestAttemptID != "" && len(msgs) > 0 {
		if q.receiveAttempts == nil {
			q.receiveAttempts = make(map[string]*receiveAttemptEntry)
		}
		q.receiveAttempts[input.ReceiveRequestAttemptID] = &receiveAttemptEntry{
			msgs:      msgs,
			expiresAt: now.Add(receiveAttemptTTL),
		}
	}

	return msgs, q.notify, nil
}

// pruneReceiveAttempts removes expired ReceiveRequestAttemptID cache entries.
// Caller must hold b.mu (write).
func pruneReceiveAttempts(q *Queue, now time.Time) {
	for id, entry := range q.receiveAttempts {
		if !now.Before(entry.expiresAt) {
			delete(q.receiveAttempts, id)
		}
	}
}

// maxInFlightStandard / maxInFlightFIFO are AWS's per-queue caps for messages
// that have been received but not yet deleted or visibility-expired.
const (
	maxInFlightStandard = 120000
	maxInFlightFIFO     = 20000
)

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

// expireRetainedMessages removes messages that have exceeded the queue's
// MessageRetentionPeriod. AWS silently discards such messages; they never
// reach a consumer. The expiry is computed from SentTimestamp.
func expireRetainedMessages(q *Queue, now time.Time) {
	retentionSecs, err := strconv.Atoi(q.Attributes[attrMessageRetentionPeriod])
	if err != nil || retentionSecs <= 0 {
		retentionSecs = defaultMessageRetentionPeriod
	}

	cutoff := now.Add(-time.Duration(retentionSecs) * time.Second)

	fresh := q.messages[:0]

	for _, msg := range q.messages {
		sentAt := time.UnixMilli(msg.SentTimestamp)
		if sentAt.Before(cutoff) {
			continue // silently discard expired message
		}

		fresh = append(fresh, msg)
	}

	q.messages = fresh

	freshInFlight := q.inFlightMessages[:0]

	for _, inf := range q.inFlightMessages {
		sentAt := time.UnixMilli(inf.Msg.SentTimestamp)
		if sentAt.Before(cutoff) {
			continue // silently discard expired in-flight message
		}

		freshInFlight = append(freshInFlight, inf)
	}

	q.inFlightMessages = freshInFlight
}

// buildBlockedGroups returns the set of FIFO message group IDs that currently
// have at least one in-flight message. Messages in a blocked group must not be
// delivered until all earlier in-flight messages for that group are deleted,
// ensuring strict per-group ordering.
func buildBlockedGroups(inflight []*InFlightMessage) map[string]bool {
	blocked := make(map[string]bool)
	for _, inf := range inflight {
		if inf.Msg.MessageGroupID != "" {
			blocked[inf.Msg.MessageGroupID] = true
		}
	}

	return blocked
}

// pickMessages moves up to maxMessages visible (non-delayed) messages from the
// queue to in-flight and returns them. Messages whose VisibleAt is in the future
// are skipped and remain in the queue. The accountID is used to populate the
// SenderId system attribute on first-time receive.
//
// For FIFO queues, messages with the same MessageGroupId are returned in
// insertion order. A group is blocked if it already has an in-flight message,
// ensuring strict per-group ordering while allowing different groups to be
// returned in parallel.
func pickMessages(q *Queue, accountID string, maxMessages, vt int, now time.Time) []*Message {
	// No capacity hint — user-derived values like maxMessages in the
	// capacity slot trigger CodeQL.
	// nolint:prealloc,nolintlint // satisfies CodeQL by removing tainted capacity hint
	result := make([]*Message, 0)
	remaining := make([]*Message, 0, len(q.messages))

	// For FIFO queues, collect the set of message groups that currently have
	// in-flight messages. Messages belonging to a blocked group must not be
	// delivered until all earlier in-flight messages for that group are deleted.
	var blockedGroups map[string]bool
	if q.IsFIFO {
		blockedGroups = buildBlockedGroups(q.inFlightMessages)
	}

	for _, msg := range q.messages {
		if pickMessage(q, msg, accountID, maxMessages, vt, now, blockedGroups, &result) {
			continue
		}

		remaining = append(remaining, msg)
	}

	q.messages = remaining

	return result
}

// pickMessage attempts to pick a single message for delivery. It returns true
// if the message was either picked for delivery or explicitly skipped (blocked
// FIFO group). It returns false when the message should remain in the queue
// because either the result set is full or the message is not yet visible.
func pickMessage(
	q *Queue,
	msg *Message,
	accountID string,
	maxMessages, vt int,
	now time.Time,
	blockedGroups map[string]bool,
	result *[]*Message,
) bool {
	// For FIFO queues, skip messages whose group is currently blocked.
	if q.IsFIFO && msg.MessageGroupID != "" && blockedGroups[msg.MessageGroupID] {
		return false // leave in remaining
	}

	if len(*result) >= maxMessages || now.Before(msg.VisibleAt) {
		return false
	}

	// Mint a new per-receive generation so that a stale receipt handle from a
	// previous receive (e.g. after visibility-timeout expiry) can be detected
	// even if the random UUID collides (astronomically unlikely but possible).
	q.receiveGeneration++
	receipt := fmt.Sprintf("%s:%d:%s", msg.MessageID, q.receiveGeneration, uuid.New().String())
	msg.ReceiptHandle = receipt
	msg.ApproximateReceiveCount++
	msg.Attributes[attrApproxReceiveCount] = strconv.Itoa(msg.ApproximateReceiveCount)

	// Set ApproximateFirstReceiveTimestamp and SenderId on the first receive.
	if msg.ApproximateFirstReceiveTimestamp == 0 {
		msg.ApproximateFirstReceiveTimestamp = now.UnixMilli()
		msg.Attributes[attrApproxFirstReceiveTimestamp] = strconv.FormatInt(
			msg.ApproximateFirstReceiveTimestamp,
			10,
		)
		msg.Attributes[attrSenderID] = accountID
	}

	inf := &InFlightMessage{
		VisibleAt:     now.Add(time.Duration(vt) * time.Second),
		ReceiptHandle: receipt,
		Generation:    q.receiveGeneration,
		Msg:           msg,
	}
	q.inFlightMessages = append(q.inFlightMessages, inf)
	*result = append(*result, msg)

	// Block further messages from this group in this receive call so that
	// we only deliver the earliest message per group at a time.
	if q.IsFIFO && msg.MessageGroupID != "" {
		blockedGroups[msg.MessageGroupID] = true
	}

	return true
}

// DeleteMessage removes an in-flight message by its receipt handle.
func (b *InMemoryBackend) DeleteMessage(input *DeleteMessageInput) error {
	b.mu.Lock("DeleteMessage")
	defer b.mu.Unlock()

	name := queueNameFromInput(input.QueueURL)

	q, ok := b.lookupQueueByName(input.Region, name)
	if !ok {
		return ErrQueueNotFound
	}

	for i, inf := range q.inFlightMessages {
		if inf.ReceiptHandle == input.ReceiptHandle {
			q.inFlightMessages = append(q.inFlightMessages[:i], q.inFlightMessages[i+1:]...)

			go b.emitMetric("NumberOfMessagesDeleted", 1, "Count")

			return nil
		}
	}

	return ErrReceiptHandleInvalid
}

// ChangeMessageVisibility updates the visibility timeout for an in-flight message.
func (b *InMemoryBackend) ChangeMessageVisibility(input *ChangeMessageVisibilityInput) error {
	if input.VisibilityTimeout < 0 || input.VisibilityTimeout > maxVisibilityTimeoutSeconds {
		return ErrInvalidVisibilityTimeout
	}

	b.mu.Lock("ChangeMessageVisibility")
	defer b.mu.Unlock()

	name := queueNameFromInput(input.QueueURL)

	q, ok := b.lookupQueueByName(input.Region, name)
	if !ok {
		return ErrQueueNotFound
	}

	return changeVisibility(q, input.ReceiptHandle, input.VisibilityTimeout)
}

// changeVisibility updates the VisibleAt time for an in-flight message by receipt handle.
// When visibilityTimeout is 0 the message is immediately returned to the visible queue,
// matching the AWS behaviour where a zero timeout makes a message immediately available.
func changeVisibility(q *Queue, receiptHandle string, visibilityTimeout int) error {
	for i, inf := range q.inFlightMessages {
		if inf.ReceiptHandle != receiptHandle {
			continue
		}

		if visibilityTimeout == 0 {
			// Move back to the visible queue immediately.
			inf.Msg.VisibleAt = time.Now()
			q.messages = append(q.messages, inf.Msg)
			q.inFlightMessages = append(q.inFlightMessages[:i], q.inFlightMessages[i+1:]...)

			// Wake long-poll receivers that may be waiting for a message.
			old := q.notify
			q.notify = make(chan struct{})
			close(old)

			return nil
		}

		inf.VisibleAt = time.Now().Add(time.Duration(visibilityTimeout) * time.Second)

		return nil
	}

	return ErrMessageNotInflight
}

// ChangeMessageVisibilityBatch updates visibility for a batch of in-flight messages.
func (b *InMemoryBackend) ChangeMessageVisibilityBatch(
	input *ChangeMessageVisibilityBatchInput,
) (*ChangeMessageVisibilityBatchOutput, error) {
	ids := make([]string, len(input.Entries))
	for i, e := range input.Entries {
		ids[i] = e.ID
	}

	if err := validateBatchEnvelope(ids); err != nil {
		return nil, err
	}

	b.mu.Lock("ChangeMessageVisibilityBatch")
	defer b.mu.Unlock()

	name := queueNameFromInput(input.QueueURL)

	q, ok := b.lookupQueueByName(input.Region, name)
	if !ok {
		return nil, ErrQueueNotFound
	}

	out := &ChangeMessageVisibilityBatchOutput{}

	for _, entry := range input.Entries {
		if err := changeVisibility(q, entry.ReceiptHandle, entry.VisibilityTimeout); err != nil {
			out.Failed = append(out.Failed, BatchErrorEntry{
				ID:          entry.ID,
				Code:        "MessageNotInflight",
				Message:     err.Error(),
				SenderFault: true,
			})
		} else {
			out.Successful = append(out.Successful, BatchResultEntry{ID: entry.ID})
		}
	}

	return out, nil
}

// isValidBatchEntryID reports whether id conforms to the AWS batch entry ID
// format: 1-80 characters from [A-Za-z0-9_-].
func isValidBatchEntryID(id string) bool {
	if id == "" || len(id) > maxQueueNameLength {
		return false
	}

	for _, c := range id {
		if !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
			(c >= '0' && c <= '9') || c == '_' || c == '-') {
			return false
		}
	}

	return true
}

// validateBatchEnvelope checks request-level batch constraints that apply
// identically to SendMessageBatch, DeleteMessageBatch, and
// ChangeMessageVisibilityBatch:
//  1. At least one entry (empty → EmptyBatchRequest)
//  2. At most 10 entries (> 10 → TooManyEntriesInBatchRequest)
//  3. All IDs distinct (duplicates → BatchEntryIdsNotDistinct)
//  4. Each ID matches ^[A-Za-z0-9_-]{1,80}$ (invalid → EmptyBatchRequest)
func validateBatchEnvelope(ids []string) error {
	if len(ids) == 0 {
		return ErrInvalidBatchEntry
	}

	if len(ids) > maxBatchSize {
		return ErrTooManyEntriesInBatch
	}

	seen := make(map[string]struct{}, len(ids))

	for _, id := range ids {
		if !isValidBatchEntryID(id) {
			return ErrInvalidBatchEntry
		}

		if _, dup := seen[id]; dup {
			return ErrBatchEntryIDsNotDistinct
		}

		seen[id] = struct{}{}
	}

	return nil
}


// SendMessageBatch sends a batch of messages to the specified queue.
// Results in the Successful and Failed slices are returned in the same
// order as the corresponding entries in the input slice.
func (b *InMemoryBackend) SendMessageBatch(input *SendMessageBatchInput) (*SendMessageBatchOutput, error) {
	ids := make([]string, len(input.Entries))
	for i, e := range input.Entries {
		ids[i] = e.ID
	}

	if err := validateBatchEnvelope(ids); err != nil {
		return nil, err
	}

	// AWS rejects the entire batch with BatchRequestTooLong when the combined
	// payload size of every entry that is itself within the per-message limit
	// (bodies plus attribute name + type + value bytes) would still exceed the
	// per-queue MaximumMessageSize (default 256 KiB). Entries that are
	// individually oversized are surfaced per-entry by validateMessageSize so
	// existing per-entry-failure semantics are preserved.
	totalBytes := 0
	allEntriesUnderLimit := true
	for _, entry := range input.Entries {
		entryBytes := len(entry.MessageBody)
		for name, attr := range entry.MessageAttributes {
			entryBytes += len(name) + len(attr.DataType) + len(attr.StringValue) + len(attr.BinaryValue)
		}

		if entryBytes > defaultMaxMessageSize {
			allEntriesUnderLimit = false
		}
		totalBytes += entryBytes
	}

	if allEntriesUnderLimit && totalBytes > defaultMaxMessageSize {
		return nil, ErrBatchRequestTooLong
	}

	out := &SendMessageBatchOutput{}

	// Process entries in input order; append results directly so Successful and
	// Failed slices already match the original entry order without sorting.
	for _, entry := range input.Entries {
		sendOut, err := b.SendMessage(&SendMessageInput{
			QueueURL:                input.QueueURL,
			Region:                  input.Region,
			MessageBody:             entry.MessageBody,
			MessageGroupID:          entry.MessageGroupID,
			MessageDeduplicationID:  entry.MessageDeduplicationID,
			DelaySeconds:            entry.DelaySeconds,
			MessageAttributes:       entry.MessageAttributes,
			MessageSystemAttributes: entry.MessageSystemAttributes,
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
			SequenceNumber:         sendOut.SequenceNumber,
		})
	}

	return out, nil
}

// DeleteMessageBatch deletes a batch of messages from the specified queue.
func (b *InMemoryBackend) DeleteMessageBatch(input *DeleteMessageBatchInput) (*DeleteMessageBatchOutput, error) {
	ids := make([]string, len(input.Entries))
	for i, e := range input.Entries {
		ids[i] = e.ID
	}

	if err := validateBatchEnvelope(ids); err != nil {
		return nil, err
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
	q.lastPurgedAt = time.Now()

	// For FIFO queues, purging messages also resets the deduplication state so
	// that producers can re-send messages with the same deduplication IDs.
	if q.IsFIFO {
		q.DeduplicationIDs = make(map[string]time.Time)
		q.deduplicationMsgIDs = make(map[string]string)
	}

	return nil
}

// ListDeadLetterSourceQueues returns the URLs of all queues that have the given
// queue configured as their dead-letter queue via a RedrivePolicy.
func (b *InMemoryBackend) ListDeadLetterSourceQueues(
	input *ListDeadLetterSourceQueuesInput,
) (*ListDeadLetterSourceQueuesOutput, error) {
	b.mu.RLock("ListDeadLetterSourceQueues")
	defer b.mu.RUnlock()

	dlq, exists := b.lookupQueueByURL(input.Region, input.QueueURL)
	if !exists {
		return nil, ErrQueueNotFound
	}

	dlqARN := dlq.Attributes[attrQueueArn]

	var urls []string

	for _, q := range b.queues {
		raw, ok := q.Attributes[attrRedrivePolicy]
		if !ok || raw == "" {
			continue
		}

		var pol redrivePolicy
		if err := json.Unmarshal([]byte(raw), &pol); err != nil {
			continue
		}

		count, err := pol.MaxReceiveCount.Int64()
		if err != nil || count <= 0 {
			continue
		}

		if pol.DeadLetterTargetArn == dlqARN {
			urls = append(urls, q.URL)
		}
	}

	sort.Strings(urls)

	p := page.New(urls, input.NextToken, input.MaxResults, sqsDefaultMaxResults)

	return &ListDeadLetterSourceQueuesOutput{QueueURLs: p.Data, NextToken: p.Next}, nil
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

	q, ok := b.lookupQueueByName(input.Region, name)
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

	q, ok := b.lookupQueueByName(input.Region, name)
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

	q, ok := b.lookupQueueByName(input.Region, name)
	if !ok {
		return nil, ErrQueueNotFound
	}

	if q.Tags == nil {
		return &ListQueueTagsOutput{Tags: tags.New("sqs.queue." + q.Name + ".tags")}, nil
	}

	return &ListQueueTagsOutput{Tags: q.Tags}, nil
}

// TaggedQueueInfo contains a queue's ARN and tag snapshot, for use by the
// Resource Groups Tagging API cross-service listing.
type TaggedQueueInfo struct {
	Tags map[string]string
	ARN  string
}

// TaggedQueues returns a snapshot of all queues with their ARNs and tags.
// Intended for use by the Resource Groups Tagging API provider.
func (b *InMemoryBackend) TaggedQueues() []TaggedQueueInfo {
	b.mu.RLock("TaggedQueues")
	defer b.mu.RUnlock()

	result := make([]TaggedQueueInfo, 0, len(b.queues))

	for _, q := range b.queues {
		var tagMap map[string]string
		if q.Tags != nil {
			tagMap = q.Tags.Clone()
		}

		result = append(result, TaggedQueueInfo{
			ARN:  q.Attributes[attrQueueArn],
			Tags: tagMap,
		})
	}

	return result
}

// TagQueueByARN applies tags to the queue identified by its ARN.
// Returns ErrQueueNotFound if no queue with that ARN exists.
func (b *InMemoryBackend) TagQueueByARN(queueARN string, newTags map[string]string) error {
	b.mu.Lock("TagQueueByARN")
	defer b.mu.Unlock()

	for _, q := range b.queues {
		if q.Attributes[attrQueueArn] == queueARN {
			if q.Tags == nil {
				q.Tags = tags.New("sqs.queue." + q.Name + ".tags")
			}

			q.Tags.Merge(newTags)

			return nil
		}
	}

	return ErrQueueNotFound
}

// UntagQueueByARN removes the specified tag keys from the queue identified by its ARN.
// Returns ErrQueueNotFound if no queue with that ARN exists.
func (b *InMemoryBackend) UntagQueueByARN(queueARN string, tagKeys []string) error {
	b.mu.Lock("UntagQueueByARN")
	defer b.mu.Unlock()

	for _, q := range b.queues {
		if q.Attributes[attrQueueArn] == queueARN {
			if q.Tags != nil {
				q.Tags.DeleteKeys(tagKeys)
			}

			return nil
		}
	}

	return ErrQueueNotFound
}

// ReceiveMessagesLocal is an internal method used by the ESM poller to pull
// messages from a queue without long-polling. It returns up to maxMessages
// visible messages, moving them to in-flight state using the queue's default
// visibility timeout.
func (b *InMemoryBackend) ReceiveMessagesLocal(queueURL string, maxMessages int) ([]*Message, error) {
	out, err := b.ReceiveMessage(&ReceiveMessageInput{
		QueueURL:            queueURL,
		MaxNumberOfMessages: maxMessages,
		WaitTimeSeconds:     0,
		VisibilityTimeout:   noVisibilitySet,
	})
	if err != nil {
		return nil, err
	}

	return out.Messages, nil
}

// DeleteMessagesLocal is an internal method used by the ESM poller to delete
// successfully processed messages by their receipt handles.
func (b *InMemoryBackend) DeleteMessagesLocal(queueURL string, receiptHandles []string) error {
	for _, rh := range receiptHandles {
		if err := b.DeleteMessage(&DeleteMessageInput{
			QueueURL:      queueURL,
			ReceiptHandle: rh,
		}); err != nil {
			return err
		}
	}

	return nil
}

// totalMessages returns the total in-memory message count across all queues
// (visible + in-flight + DLQ retained).
func (b *InMemoryBackend) totalMessages() int {
	b.mu.RLock("totalMessages")
	defer b.mu.RUnlock()

	total := 0
	for _, q := range b.queues {
		total += len(q.messages) + len(q.inFlightMessages)
	}

	return total
}

// Purge removes all queues created before the given cutoff time.
func (b *InMemoryBackend) Purge(ctx context.Context, cutoff time.Time) {
	if ctx.Err() != nil {
		return
	}

	b.mu.Lock("Purge")
	defer b.mu.Unlock()

	for k, q := range b.queues {
		if ctx.Err() != nil {
			return
		}

		createdStr, ok := q.Attributes[attrCreatedTimestamp]
		if !ok {
			continue
		}

		createdUnix, err := strconv.ParseInt(createdStr, 10, 64)
		if err != nil {
			continue
		}

		if time.Unix(createdUnix, 0).Before(cutoff) {
			b.purgeQueue(k, q)
		}
	}
}

// purgeQueue closes and removes a single queue and cancels any move tasks that involve it.
// Caller must hold b.mu.
func (b *InMemoryBackend) purgeQueue(key string, q *Queue) {
	close(q.notify)
	if q.Tags != nil {
		q.Tags.Close()
	}
	delete(b.queues, key)

	queueARN := q.Attributes[attrQueueArn]
	for _, task := range b.moveTasks {
		b.cancelMoveTaskIfInvolved(task, queueARN)
	}
}

// cancelMoveTaskIfInvolved cancels task if it is active and references queueARN.
func (b *InMemoryBackend) cancelMoveTaskIfInvolved(task *moveTaskState, queueARN string) {
	task.mu.Lock()
	isActive := task.status == MoveTaskStatusRunning || task.status == MoveTaskStatusCancelling
	involves := task.sourceArn == queueARN || task.destArn == queueARN
	task.mu.Unlock()

	if isActive && involves {
		task.cancel()
	}
}

// Reset clears all in-memory state from the database. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
// The active SNS subscription listener is kept intact so that SNS→SQS delivery
// continues to work after a reset (wireSNSToSQS is only wired at startup and is
// not re-run after reset).
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	// Close all queue tag stores to prevent resource leaks.
	for _, q := range b.queues {
		if q.Tags != nil {
			q.Tags.Close()
		}
	}

	// Cancel any running move tasks.
	for _, task := range b.moveTasks {
		task.cancel()
	}

	b.queues = make(map[string]*Queue)
	b.moveTasks = make(map[string]*moveTaskState)
}

// AddPermission adds a permission statement to the specified queue.
func (b *InMemoryBackend) AddPermission(input *AddPermissionInput) error {
	if input.Label == "" {
		return ErrInvalidPermissionLabel
	}

	if len(input.Actions) == 0 {
		return ErrInvalidPermissionActions
	}

	if len(input.AWSAccountIDs) == 0 {
		return ErrInvalidPermissionAccountIDs
	}

	b.mu.Lock("AddPermission")
	defer b.mu.Unlock()

	name := queueNameFromInput(input.QueueURL)

	q, ok := b.lookupQueueByName(input.Region, name)
	if !ok {
		return ErrQueueNotFound
	}

	q.Permissions[input.Label] = &QueuePermissionEntry{
		AWSAccountIDs: slices.Clone(input.AWSAccountIDs),
		Actions:       slices.Clone(input.Actions),
	}

	return nil
}

// RemovePermission removes a permission statement from the specified queue.
func (b *InMemoryBackend) RemovePermission(input *RemovePermissionInput) error {
	b.mu.Lock("RemovePermission")
	defer b.mu.Unlock()

	name := queueNameFromInput(input.QueueURL)

	q, ok := b.lookupQueueByName(input.Region, name)
	if !ok {
		return ErrQueueNotFound
	}

	delete(q.Permissions, input.Label)

	return nil
}

// queueURLFromARNLocked returns the URL and ARN of the queue with the given ARN.
// Must be called with b.mu held (either read or write).
func (b *InMemoryBackend) queueURLFromARNLocked(queueARN string) (string, bool) {
	for _, q := range b.queues {
		if q.Attributes[attrQueueArn] == queueARN {
			return q.URL, true
		}
	}

	return "", false
}

// findDefaultMoveDestinationLocked finds the URL of the queue that has a RedrivePolicy
// pointing to the DLQ identified by dlqARN.
// Must be called with b.mu held (either read or write).
func (b *InMemoryBackend) findDefaultMoveDestinationLocked(dlqARN string) (string, bool) {
	for _, q := range b.queues {
		raw, ok := q.Attributes[attrRedrivePolicy]
		if !ok || raw == "" {
			continue
		}

		var pol redrivePolicy

		if err := json.Unmarshal([]byte(raw), &pol); err != nil {
			continue
		}

		if pol.DeadLetterTargetArn == dlqARN {
			return q.URL, true
		}
	}

	return "", false
}

// approximateQueueDepthLocked returns the approximate number of visible messages in the queue with the given name.
// Must be called with b.mu held (either read or write).
func approximateQueueDepthLocked(q *Queue) int64 {
	now := time.Now()
	visible := 0

	for _, msg := range q.messages {
		if !now.Before(msg.VisibleAt) {
			visible++
		}
	}

	return int64(visible)
}

// StartMessageMoveTask starts an asynchronous task that moves messages from the
// source queue (typically a DLQ) to the destination queue.
// If DestinationArn is empty, the backend looks for a queue whose RedrivePolicy
// points to the source ARN and uses that as the destination.
// Returns ErrMoveTaskAlreadyRunning if there is already a RUNNING task for the source ARN.
func (b *InMemoryBackend) StartMessageMoveTask(
	input *StartMessageMoveTaskInput,
) (*StartMessageMoveTaskOutput, error) {
	if input.SourceArn == "" {
		return nil, ErrInvalidSourceArn
	}

	if input.MaxNumberOfMessagesPerSecond < 0 {
		return nil, ErrInvalidMaxMessagesPerSecond
	}

	b.mu.Lock("StartMessageMoveTask")

	// Check for existing running task on the same source ARN (AWS realism).
	// We check task status while holding both b.mu and t.mu to ensure the
	// status snapshot is consistent with the subsequent task insertion.
	for _, t := range b.moveTasks {
		if t.sourceArn == input.SourceArn {
			t.mu.Lock()
			isActive := t.status == MoveTaskStatusRunning || t.status == MoveTaskStatusCancelling
			t.mu.Unlock()

			if isActive {
				b.mu.Unlock()

				return nil, ErrMoveTaskAlreadyRunning
			}
		}
	}

	// Resolve source queue under the lock to avoid TOCTOU races.
	srcURL, ok := b.queueURLFromARNLocked(input.SourceArn)
	if !ok {
		b.mu.Unlock()

		return nil, ErrQueueNotFound
	}

	// Resolve destination queue under the same lock.
	destArn := input.DestinationArn

	var destURL string

	if destArn == "" {
		destURL, ok = b.findDefaultMoveDestinationLocked(input.SourceArn)
		if !ok {
			b.mu.Unlock()

			return nil, ErrQueueNotFound
		}

		// Derive destination ARN from its URL for task metadata.
		destName := queueNameFromInput(destURL)
		destArn = arn.Build("sqs", b.region, b.accountID, destName)
	} else {
		destURL, ok = b.queueURLFromARNLocked(destArn)
		if !ok {
			b.mu.Unlock()

			return nil, ErrQueueNotFound
		}
	}

	// Snapshot queue depth under the lock so the estimate is consistent.
	srcQueue, _ := b.lookupQueueByURL("", srcURL)
	totalCount := approximateQueueDepthLocked(srcQueue)

	taskHandle := uuid.New().String()

	ctx, cancel := context.WithCancel(
		context.Background(),
	)

	state := &moveTaskState{
		cancel:     cancel,
		taskHandle: taskHandle,
		sourceArn:  input.SourceArn,
		destArn:    destArn,
		status:     MoveTaskStatusRunning,
		maxPerSec:  input.MaxNumberOfMessagesPerSecond,
		startedAt:  time.Now().UnixMilli(),
		totalCount: totalCount,
	}

	b.moveTasks[taskHandle] = state
	b.mu.Unlock()

	go b.runMoveTask(ctx, state, srcURL, destURL)

	return &StartMessageMoveTaskOutput{TaskHandle: taskHandle}, nil
}

// from the source queue one at a time and writes them to the destination queue
// until the source is empty, the context is cancelled, or a fatal error occurs.
func (b *InMemoryBackend) runMoveTask(ctx context.Context, state *moveTaskState, srcURL, destURL string) {
	defer func() {
		state.mu.Lock()

		switch state.status {
		case MoveTaskStatusRunning:
			state.status = MoveTaskStatusCompleted
		case MoveTaskStatusCancelling:
			// CancelMessageMoveTask set status to CANCELLING and called cancel().
			// Whether the context was done (cancelled) or the queue was drained,
			// the task is now stopped — report CANCELLED either way.
			state.status = MoveTaskStatusCancelled
		case MoveTaskStatusCompleted, MoveTaskStatusCancelled, MoveTaskStatusFailed:
			// Terminal state already set (e.g. by an error path within the loop).
		}

		state.mu.Unlock()
	}()

	// Use a Ticker for MaxNumberOfMessagesPerSecond so the interval accounts for
	// the receive+send work time, maintaining the configured rate accurately.
	// A per-iteration Timer would add receive+send latency to each period, reducing
	// effective throughput below the requested rate.
	var rateTicker *time.Ticker
	var rateC <-chan time.Time

	if state.maxPerSec > 0 {
		interval := time.Second / time.Duration(state.maxPerSec)
		rateTicker = time.NewTicker(interval)
		rateC = rateTicker.C
		defer rateTicker.Stop()
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if rateC != nil {
			select {
			case <-ctx.Done():
				return
			case <-rateC:
			}
		}

		// Receive one message from the source queue.
		out, err := b.ReceiveMessage(&ReceiveMessageInput{
			QueueURL:            srcURL,
			MaxNumberOfMessages: 1,
			VisibilityTimeout:   moveTaskVisibilityTimeoutSecs,
		})
		if err != nil {
			state.mu.Lock()
			state.status = MoveTaskStatusFailed
			state.failureReason = "failed to receive message from source queue: " + err.Error()
			state.mu.Unlock()

			return
		}

		if len(out.Messages) == 0 {
			// Source queue is empty; task is complete.
			return
		}

		msg := out.Messages[0]

		// Send the message to the destination queue.
		// Preserve MessageGroupID and MessageDeduplicationID for FIFO queues so that
		// moved messages retain their original message group and deduplication identities.
		_, sendErr := b.SendMessage(&SendMessageInput{
			QueueURL:               destURL,
			MessageBody:            msg.Body,
			MessageAttributes:      msg.MessageAttributes,
			MessageGroupID:         msg.MessageGroupID,
			MessageDeduplicationID: msg.MessageDeduplicationID,
		})
		if sendErr != nil {
			// Re-make message visible so it is not lost.
			_ = b.ChangeMessageVisibility(&ChangeMessageVisibilityInput{
				QueueURL:          srcURL,
				ReceiptHandle:     msg.ReceiptHandle,
				VisibilityTimeout: 0,
			})
			state.mu.Lock()
			state.status = MoveTaskStatusFailed
			state.failureReason = "failed to send message to destination queue: " + sendErr.Error()
			state.mu.Unlock()

			return
		}

		// Delete the original from the source queue.
		_ = b.DeleteMessage(&DeleteMessageInput{
			QueueURL:      srcURL,
			ReceiptHandle: msg.ReceiptHandle,
		})

		state.mu.Lock()
		state.movedCount++
		state.mu.Unlock()
	}
}

// CancelMessageMoveTask cancels an active message move task.
// Returns ErrMoveTaskNotRunning if the task is not in RUNNING or CANCELLING state,
// matching AWS behaviour ("A message move task with the specified task handle is not running.").
func (b *InMemoryBackend) CancelMessageMoveTask(
	input *CancelMessageMoveTaskInput,
) (*CancelMessageMoveTaskOutput, error) {
	b.mu.RLock("CancelMessageMoveTask")
	state, ok := b.moveTasks[input.TaskHandle]
	b.mu.RUnlock()

	if !ok {
		return nil, ErrTaskHandleInvalid
	}

	state.mu.Lock()

	// Per AWS: cancelling a task that is not RUNNING returns ResourceInConflict.
	if state.status != MoveTaskStatusRunning && state.status != MoveTaskStatusCancelling {
		state.mu.Unlock()

		return nil, ErrMoveTaskNotRunning
	}

	state.status = MoveTaskStatusCancelling
	// Capture movedCount under the lock before releasing it, to avoid a race
	// with the goroutine that increments movedCount concurrently.
	moved := state.movedCount
	state.mu.Unlock()

	state.cancel()

	return &CancelMessageMoveTaskOutput{
		ApproximateNumberOfMessagesMoved: moved,
	}, nil
}

// listMessageMoveTasksDefaultMaxResults is the default number of results returned by
// ListMessageMoveTasks when MaxResults is not specified, matching AWS behaviour.
const listMessageMoveTasksDefaultMaxResults = 1

// listMessageMoveTasksMaxAllowed is the maximum number of results AWS allows.
const listMessageMoveTasksMaxAllowed = 10

// ListMessageMoveTasks returns message move tasks for the given source ARN.
//
// Per AWS semantics:
//   - If MaxResults is 0 (not set), it defaults to 1 (the most recent task).
//   - The maximum allowed value for MaxResults is 10.
//   - Results are sorted newest-first (descending by startedAt timestamp).
//   - TaskHandle is only populated for tasks in RUNNING status.
func (b *InMemoryBackend) ListMessageMoveTasks(
	input *ListMessageMoveTasksInput,
) (*ListMessageMoveTasksOutput, error) {
	b.mu.RLock("ListMessageMoveTasks")

	var tasks []*moveTaskState

	for _, t := range b.moveTasks {
		if input.SourceArn == "" || t.sourceArn == input.SourceArn {
			tasks = append(tasks, t)
		}
	}

	b.mu.RUnlock()

	// Sort descending by startedAt: higher timestamps are newer, so index[0] is the
	// most recent task. This matches the AWS default of returning the most recent task first.
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].startedAt > tasks[j].startedAt // descending (larger = newer)
	})

	// Apply MaxResults: default to 1 when unset; cap at the AWS maximum of 10.
	limit := int(input.MaxResults)
	if limit <= 0 {
		limit = listMessageMoveTasksDefaultMaxResults
	}

	if limit > listMessageMoveTasksMaxAllowed {
		limit = listMessageMoveTasksMaxAllowed
	}

	if len(tasks) > limit {
		tasks = tasks[:limit]
	}

	results := make([]MessageMoveTask, 0, len(tasks))

	for _, t := range tasks {
		t.mu.Lock()
		movedCount := t.movedCount
		totalCount := t.totalCount
		status := t.status
		startedAt := t.startedAt
		maxPerSec := t.maxPerSec
		taskHandle := t.taskHandle
		sourceArn := t.sourceArn
		destArn := t.destArn
		failureReason := t.failureReason
		t.mu.Unlock()

		task := MessageMoveTask{
			SourceArn:                        sourceArn,
			DestinationArn:                   destArn,
			Status:                           status,
			ApproximateNumberOfMessagesMoved: movedCount,
			StartedTimestamp:                 startedAt,
		}

		// Per AWS: TaskHandle is only populated for RUNNING tasks.
		if status == MoveTaskStatusRunning {
			task.TaskHandle = taskHandle
		}

		if totalCount > 0 {
			task.ApproximateNumberOfMessagesToMove = &totalCount
		}

		if maxPerSec > 0 {
			task.MaxNumberOfMessagesPerSecond = &maxPerSec
		}

		if failureReason != "" {
			task.FailureReason = &failureReason
		}

		results = append(results, task)
	}

	return &ListMessageMoveTasksOutput{Results: results}, nil
}
