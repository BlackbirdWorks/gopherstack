package awsconfig

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	s3pkg "github.com/blackbirdworks/gopherstack/services/s3"
)

// errCodeInternalError is the AWS-style error code recorded for a delivery
// failure this backend cannot attribute to a more specific real AWS error
// code (missing S3/SNS integration, a body-encoding failure, an SNS publish
// failure).
const errCodeInternalError = "InternalError"

var (
	errS3WriterNotConfigured = awserr.New(
		"no S3 integration configured for this AWS Config delivery channel", awserr.ErrInvalidParameter,
	)
	errSNSPublisherNotConfigured = awserr.New(
		"no SNS integration configured for this AWS Config delivery channel", awserr.ErrInvalidParameter,
	)
)

// deliveryChannelStatusState is the persisted per-channel delivery outcome
// backing DescribeDeliveryChannelStatus, keyed by channel name and kept off
// DeliveryChannel itself (see that field's comment on InMemoryBackend in
// store.go). Never carries a History entry: gopherstack has no periodic
// configuration-history delivery loop, only on-demand snapshot delivery via
// DeliverConfigSnapshot, so ConfigHistoryDeliveryInfo stays nil rather than
// mirroring the snapshot outcome (that would be a delivery that never
// happened) -- see PARITY.md.
type deliveryChannelStatusState struct {
	Snapshot    *exportDeliveryState `json:"snapshot,omitempty"`
	Stream      *streamDeliveryState `json:"stream,omitempty"`
	ChannelName string               `json:"channelName"`
}

// exportDeliveryState is deliveryChannelStatusState's internal twin of the
// wire ConfigExportDeliveryInfo, using time.Time instead of *float64 so zero
// means "never happened" without an extra bool.
type exportDeliveryState struct {
	LastAttemptTime    time.Time `json:"lastAttemptTime"`
	LastSuccessfulTime time.Time `json:"lastSuccessfulTime"`
	NextDeliveryTime   time.Time `json:"nextDeliveryTime"`
	LastErrorCode      string    `json:"lastErrorCode,omitempty"`
	LastErrorMessage   string    `json:"lastErrorMessage,omitempty"`
	LastStatus         string    `json:"lastStatus,omitempty"`
}

// streamDeliveryState is deliveryChannelStatusState's internal twin of the
// wire ConfigStreamDeliveryInfo.
type streamDeliveryState struct {
	LastStatusChangeTime time.Time `json:"lastStatusChangeTime"`
	LastErrorCode        string    `json:"lastErrorCode,omitempty"`
	LastErrorMessage     string    `json:"lastErrorMessage,omitempty"`
	LastStatus           string    `json:"lastStatus,omitempty"`
}

// epochPtr converts t to the wire *float64 epoch-seconds form (pkgs/awstime),
// nil when t is unset -- DescribeDeliveryChannelStatus must omit fields no
// delivery has produced yet rather than fabricate a zero timestamp.
func epochPtr(t time.Time) *float64 {
	if t.IsZero() {
		return nil
	}

	v := awstime.Epoch(t)

	return &v
}

func (s *exportDeliveryState) toWire() *ConfigExportDeliveryInfo {
	if s == nil {
		return nil
	}

	return &ConfigExportDeliveryInfo{
		LastAttemptTime:    epochPtr(s.LastAttemptTime),
		LastErrorCode:      s.LastErrorCode,
		LastErrorMessage:   s.LastErrorMessage,
		LastStatus:         s.LastStatus,
		LastSuccessfulTime: epochPtr(s.LastSuccessfulTime),
		NextDeliveryTime:   epochPtr(s.NextDeliveryTime),
	}
}

func (s *streamDeliveryState) toWire() *ConfigStreamDeliveryInfo {
	if s == nil {
		return nil
	}

	return &ConfigStreamDeliveryInfo{
		LastErrorCode:        s.LastErrorCode,
		LastErrorMessage:     s.LastErrorMessage,
		LastStatus:           s.LastStatus,
		LastStatusChangeTime: epochPtr(s.LastStatusChangeTime),
	}
}

// DescribeDeliveryChannelStatus returns statuses for delivery channels.
// If names is empty, all channels are returned. Each delivery-info slot is
// nil until that kind of delivery has actually happened (gopherstack-ru0y):
// no delivery has ever been fabricated as SUCCESS.
func (b *InMemoryBackend) DescribeDeliveryChannelStatus(names []string) []DeliveryChannelStatus {
	b.mu.RLock("DescribeDeliveryChannelStatus")
	defer b.mu.RUnlock()

	var channelNames []string

	if len(names) == 0 {
		for _, c := range b.channels.All() {
			channelNames = append(channelNames, c.Name)
		}
	} else {
		for _, name := range names {
			if b.channels.Has(name) {
				channelNames = append(channelNames, name)
			}
		}
	}

	out := make([]DeliveryChannelStatus, 0, len(channelNames))

	for _, name := range channelNames {
		status := DeliveryChannelStatus{Name: name}

		if state, ok := b.deliveryStatus.Get(name); ok {
			status.ConfigSnapshotDeliveryInfo = state.Snapshot.toWire()
			status.ConfigStreamDeliveryInfo = state.Stream.toWire()
		}

		out = append(out, status)
	}

	return out
}

// snapshotDeliveryInputs is the state DeliverConfigSnapshot captures under
// the backend lock and then delivers (S3 write, SNS publish) without it --
// see services/lambda/lifecycle.go for the same capture-then-release pattern.
type snapshotDeliveryInputs struct {
	now        time.Time
	s3w        S3Writer
	snsPub     SNSPublisher
	snapshotID string
	bucket     string
	keyPrefix  string
	snsArn     string
	frequency  string
	accountID  string
	region     string
	items      []*ResourceConfigItem
}

// prepareConfigSnapshotDeliveryLocked validates DeliverConfigSnapshot's
// preconditions and captures everything needed to perform the delivery.
// Caller must hold b.mu.
func (b *InMemoryBackend) prepareConfigSnapshotDeliveryLocked(channelName string) (snapshotDeliveryInputs, error) {
	if !b.channels.Has(channelName) {
		return snapshotDeliveryInputs{}, fmt.Errorf("%w: %s", ErrNoSuchDeliveryChannel, channelName)
	}

	if b.recorders.Len() == 0 {
		return snapshotDeliveryInputs{}, fmt.Errorf(
			"%w: no configuration recorder configured", ErrNoAvailableConfigurationRecorder,
		)
	}

	running := false

	for _, r := range b.recorders.All() {
		if r.Status == recorderStatusActive {
			running = true

			break
		}
	}

	if !running {
		return snapshotDeliveryInputs{}, fmt.Errorf(
			"%w: no configuration recorder is running", ErrNoRunningConfigurationRecorder,
		)
	}

	b.captureCounter++
	now := b.now()
	snapshotID := fmt.Sprintf("%08x-0000-0000-0000-%012d", now.Unix(), b.captureCounter)

	channel, _ := b.channels.Get(channelName)

	freq := ""
	if channel.ConfigSnapshotDeliveryProperties != nil {
		freq = channel.ConfigSnapshotDeliveryProperties.DeliveryFrequency
	}

	return snapshotDeliveryInputs{
		snapshotID: snapshotID,
		bucket:     channel.S3Bucket,
		keyPrefix:  channel.S3KeyPrefix,
		snsArn:     channel.SNSArn,
		frequency:  freq,
		items:      b.resourceConfigs.Snapshot(),
		s3w:        b.s3Writer,
		snsPub:     b.snsPublisher,
		accountID:  b.accountID,
		region:     b.region,
		now:        now,
	}, nil
}

// snapshotDeliveryResult is what deliverConfigSnapshotIO produces for
// DeliverConfigSnapshot to record back onto b.deliveryStatus under the lock.
type snapshotDeliveryResult struct {
	snapshotState *exportDeliveryState
	streamState   *streamDeliveryState
}

// deliverConfigSnapshotIO performs the actual snapshot delivery: builds the
// gzipped ConfigSnapshot body, writes it to S3, and (on a successful write)
// publishes the stream notification to SNS. Called with no lock held.
func deliverConfigSnapshotIO(ctx context.Context, in snapshotDeliveryInputs) snapshotDeliveryResult {
	key := buildConfigSnapshotKey(in.accountID, in.region, in.keyPrefix, in.snapshotID, in.now)
	body, bodyErr := buildConfigSnapshotBody(in.snapshotID, in.items)

	snap := &exportDeliveryState{LastAttemptTime: in.now}

	var s3Err error

	switch {
	case bodyErr != nil:
		s3Err = bodyErr
		snap.LastErrorCode, snap.LastErrorMessage = errCodeInternalError, bodyErr.Error()
	case in.s3w == nil:
		s3Err = errS3WriterNotConfigured
		snap.LastErrorCode, snap.LastErrorMessage = errCodeInternalError, errS3WriterNotConfigured.Error()
	default:
		if putErr := in.s3w.PutObjectBytes(ctx, in.bucket, key, body); putErr != nil {
			s3Err = putErr
			snap.LastErrorCode, snap.LastErrorMessage = classifyS3Error(putErr)
		}
	}

	if s3Err != nil {
		snap.LastStatus = deliveryStatusFailure

		return snapshotDeliveryResult{snapshotState: snap}
	}

	snap.LastStatus = deliveryStatusSuccess
	snap.LastSuccessfulTime = in.now

	if d, ok := deliveryFrequencyDuration(in.frequency); ok {
		snap.NextDeliveryTime = in.now.Add(d)
	}

	return snapshotDeliveryResult{
		snapshotState: snap,
		streamState:   publishConfigStreamNotification(in, key),
	}
}

// publishConfigStreamNotification publishes the ConfigurationSnapshotDeliveryCompleted
// notification once a snapshot has actually been delivered to S3. When no
// SNS topic is configured, real AWS Config still reports the stream slot as
// Not_Applicable rather than omitting it (configservice@v1.68.4
// types/types.go:855-859, ConfigStreamDeliveryInfo.LastStatus doc comment:
// "If the SNS delivery is turned off, the last status will be
// Not_Applicable") -- so a delivery event exists here and this only returns
// nil when no delivery was attempted at all (deliverConfigSnapshotIO's S3
// failure path).
func publishConfigStreamNotification(in snapshotDeliveryInputs, key string) *streamDeliveryState {
	stream := &streamDeliveryState{LastStatusChangeTime: in.now}

	if in.snsArn == "" {
		stream.LastStatus = deliveryStatusNotApplicable

		return stream
	}

	if in.snsPub == nil {
		stream.LastStatus = deliveryStatusFailure
		stream.LastErrorCode, stream.LastErrorMessage = errCodeInternalError, errSNSPublisherNotConfigured.Error()

		return stream
	}

	msg := buildSnapshotSNSMessage(in.snapshotID, in.bucket, key, in.now)

	if pubErr := in.snsPub.PublishToTopic(in.snsArn, msg); pubErr != nil {
		stream.LastStatus = deliveryStatusFailure
		stream.LastErrorCode, stream.LastErrorMessage = errCodeInternalError, pubErr.Error()

		return stream
	}

	stream.LastStatus = deliveryStatusSuccess

	return stream
}

// classifyS3Error maps an S3Writer error to an AWS-style (code, message)
// pair. NoSuchBucket is s3pkg's own sentinel (services/s3/errors.go:15,
// ErrNoSuchBucket = awserr.New("NoSuchBucket", ...)); anything else is
// reported as a generic delivery failure rather than guessing a code.
func classifyS3Error(err error) (string, string) {
	if errors.Is(err, s3pkg.ErrNoSuchBucket) {
		return "NoSuchBucket", err.Error()
	}

	return errCodeInternalError, err.Error()
}

// Duration equivalents of MaximumExecutionFrequency's enum values
// (configservice@v1.68.4 types/enums.go:331-340), used by
// deliveryFrequencyDuration to compute ConfigExportDeliveryInfo.NextDeliveryTime.
const (
	deliveryFrequencyOneHourDuration         = time.Hour
	deliveryFrequencyThreeHoursDuration      = 3 * time.Hour
	deliveryFrequencySixHoursDuration        = 6 * time.Hour
	deliveryFrequencyTwelveHoursDuration     = 12 * time.Hour
	deliveryFrequencyTwentyFourHoursDuration = 24 * time.Hour
)

// deliveryFrequencyDuration maps a DeliverySnapshotProperties.DeliveryFrequency
// value to its duration.
func deliveryFrequencyDuration(freq string) (time.Duration, bool) {
	switch freq {
	case "One_Hour":
		return deliveryFrequencyOneHourDuration, true
	case "Three_Hours":
		return deliveryFrequencyThreeHoursDuration, true
	case "Six_Hours":
		return deliveryFrequencySixHoursDuration, true
	case "Twelve_Hours":
		return deliveryFrequencyTwelveHoursDuration, true
	case "TwentyFour_Hours":
		return deliveryFrequencyTwentyFourHoursDuration, true
	default:
		return 0, false
	}
}

// configSnapshotFileVersion is the fileVersion this repo's delivered
// ConfigSnapshot files declare (see configSnapshotEnvelope's doc comment).
const configSnapshotFileVersion = "1.0"

// configSnapshotEnvelope is the on-S3 JSON envelope for a delivered
// configuration snapshot. Verified against
// https://docs.aws.amazon.com/config/latest/developerguide/example-s3-snapshot.md
// ("Example Configuration Snapshot"): top-level fileVersion/requestId/
// configurationItems -- the top-level id field is "requestId", not
// "configSnapshotId", even though it carries the same value
// DeliverConfigSnapshotOutput.ConfigSnapshotId returns. Each configuration
// item only carries the fields this backend actually tracks
// (ResourceConfigItem: resourceType/resourceId/configuration/
// configurationItemCaptureTime) rather than the full real ConfigurationItem
// shape (arn/accountId/tags/relationships/... are not modeled here) -- see
// PARITY.md.
type configSnapshotEnvelope struct {
	FileVersion        string               `json:"fileVersion"`
	RequestID          string               `json:"requestId"`
	ConfigurationItems []ResourceConfigItem `json:"configurationItems"`
}

// buildConfigSnapshotBody gzip-compresses a ConfigSnapshot file body,
// mirroring services/cloudtrail/delivery.go's logFileBody.
func buildConfigSnapshotBody(snapshotID string, items []*ResourceConfigItem) ([]byte, error) {
	envelope := configSnapshotEnvelope{
		FileVersion:        configSnapshotFileVersion,
		RequestID:          snapshotID,
		ConfigurationItems: make([]ResourceConfigItem, 0, len(items)),
	}

	for _, item := range items {
		envelope.ConfigurationItems = append(envelope.ConfigurationItems, *item)
	}

	encoded, err := json.Marshal(envelope)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer

	gz := gzip.NewWriter(&buf)

	if _, writeErr := gz.Write(encoded); writeErr != nil {
		return nil, writeErr
	}

	if closeErr := gz.Close(); closeErr != nil {
		return nil, closeErr
	}

	return buf.Bytes(), nil
}

// buildConfigSnapshotKey builds the S3 object key for a delivered
// configuration snapshot. Verified against the "Example Configuration
// Snapshot Delivery Notification" s3ObjectKey (same doc URL as
// buildSnapshotSNSMessage's doc comment):
// "AWSLogs/123456789012/Config/us-east-2/2016/9/27/ConfigSnapshot/
// 123456789012_Config_us-east-2_ConfigSnapshot_20160927T183939Z_
// 16da64e4-cb65-4846-b061-e6c3ba43cb96.json.gz" -- year/month/day segments
// are NOT zero-padded, unlike CloudTrail's log file key
// (services/cloudtrail/delivery.go's logFileKey).
func buildConfigSnapshotKey(accountID, region, keyPrefix, snapshotID string, t time.Time) string {
	ts := t.UTC()

	key := fmt.Sprintf(
		"AWSLogs/%s/Config/%s/%d/%d/%d/ConfigSnapshot/%s_Config_%s_ConfigSnapshot_%s_%s.json.gz",
		accountID, region, ts.Year(), ts.Month(), ts.Day(),
		accountID, region, ts.Format("20060102T150405Z"), snapshotID,
	)

	if keyPrefix != "" {
		key = strings.TrimSuffix(keyPrefix, "/") + "/" + key
	}

	return key
}

// configSnapshotDeliveryCompletedMessageType/configSnapshotNotificationRecordVersion
// are the messageType/recordVersion values AWS Config publishes on snapshot
// delivery (see buildSnapshotSNSMessage's doc comment).
const (
	configSnapshotDeliveryCompletedMessageType = "ConfigurationSnapshotDeliveryCompleted"
	configSnapshotNotificationRecordVersion    = "1.1"
)

// snapshotSNSMessage mirrors the "Message" body of AWS Config's
// ConfigurationSnapshotDeliveryCompleted SNS notification. Verified against
// https://docs.aws.amazon.com/config/latest/developerguide/example-configuration-snapshot-notification.md
// ("Example Configuration Snapshot Delivery Notification"): configSnapshotId/
// s3ObjectKey/s3Bucket/notificationCreationTime/messageType/recordVersion.
type snapshotSNSMessage struct {
	ConfigSnapshotID         string `json:"configSnapshotId"`
	S3ObjectKey              string `json:"s3ObjectKey"`
	S3Bucket                 string `json:"s3Bucket"`
	NotificationCreationTime string `json:"notificationCreationTime"`
	MessageType              string `json:"messageType"`
	RecordVersion            string `json:"recordVersion"`
}

func buildSnapshotSNSMessage(snapshotID, bucket, key string, t time.Time) string {
	msg := snapshotSNSMessage{
		ConfigSnapshotID:         snapshotID,
		S3ObjectKey:              key,
		S3Bucket:                 bucket,
		NotificationCreationTime: t.UTC().Format("2006-01-02T15:04:05.000Z"),
		MessageType:              configSnapshotDeliveryCompletedMessageType,
		RecordVersion:            configSnapshotNotificationRecordVersion,
	}

	encoded, err := json.Marshal(msg)
	if err != nil {
		return ""
	}

	return string(encoded)
}

// DeliverConfigSnapshot triggers an on-demand snapshot delivery through the
// named delivery channel and returns a generated snapshot ID, matching real
// AWS Config's DeliverConfigSnapshotOutput.ConfigSnapshotId. Errors (verified
// against aws-sdk-go-v2/service/configservice's DeliverConfigSnapshot
// deserializer, awsAwsjson11_deserializeOpErrorDeliverConfigSnapshot, which
// declares exactly NoSuchDeliveryChannelException/
// NoAvailableConfigurationRecorderException/
// NoRunningConfigurationRecorderException and nothing S3/SNS-shaped):
//   - NoSuchDeliveryChannelException when the named channel does not exist
//   - NoAvailableConfigurationRecorderException when no configuration recorder
//     has ever been created
//   - NoRunningConfigurationRecorderException when recorders exist but none is
//     currently ACTIVE
//
// Because the declared error set has no S3/SNS-shaped exception, a delivery
// failure (missing bucket, no S3/SNS integration wired) does NOT fail this
// call -- it still returns the generated snapshot ID with a nil error,
// exactly like real AWS Config's real async delivery model, and the failure
// is recorded instead on DescribeDeliveryChannelStatus's
// ConfigSnapshotDeliveryInfo for the caller to observe.
func (b *InMemoryBackend) DeliverConfigSnapshot(ctx context.Context, channelName string) (string, error) {
	b.mu.Lock("DeliverConfigSnapshot")
	in, err := b.prepareConfigSnapshotDeliveryLocked(channelName)
	b.mu.Unlock()

	if err != nil {
		return "", err
	}

	result := deliverConfigSnapshotIO(ctx, in)

	b.mu.Lock("DeliverConfigSnapshot:recordOutcome")
	defer b.mu.Unlock()

	state, ok := b.deliveryStatus.Get(channelName)
	if !ok {
		state = &deliveryChannelStatusState{ChannelName: channelName}
	}

	state.Snapshot = result.snapshotState
	if result.streamState != nil {
		state.Stream = result.streamState
	}

	b.deliveryStatus.Put(state)

	return in.snapshotID, nil
}
