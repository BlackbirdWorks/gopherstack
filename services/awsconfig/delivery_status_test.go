package awsconfig_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	configservicesdk "github.com/aws/aws-sdk-go-v2/service/configservice"
	"github.com/aws/aws-sdk-go-v2/service/configservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
	s3pkg "github.com/blackbirdworks/gopherstack/services/s3"
)

// fakeS3Writer is a minimal awsconfig.S3Writer for tests. A bucket name
// listed in failBuckets returns s3pkg.ErrNoSuchBucket, the same sentinel a
// real S3Writer integration would surface for a missing bucket.
type fakeS3Writer struct {
	objects     map[string][]byte
	failBuckets map[string]bool
	mu          sync.Mutex
}

func (f *fakeS3Writer) PutObjectBytes(_ context.Context, bucket, key string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.failBuckets[bucket] {
		return s3pkg.ErrNoSuchBucket
	}

	if f.objects == nil {
		f.objects = make(map[string][]byte)
	}

	f.objects[bucket+"/"+key] = data

	return nil
}

func (f *fakeS3Writer) keys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]string, 0, len(f.objects))
	for k := range f.objects {
		out = append(out, k)
	}

	return out
}

// fakeSNSPublisher is a minimal awsconfig.SNSPublisher for tests.
type fakeSNSPublisher struct {
	failTopics map[string]bool
	published  []fakeSNSMessage
	mu         sync.Mutex
}

type fakeSNSMessage struct {
	topicARN string
	message  string
}

var errFakeSNSPublishFailed = errors.New("fakeSNSPublisher: simulated publish failure")

func (f *fakeSNSPublisher) PublishToTopic(topicARN, message string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.failTopics[topicARN] {
		return errFakeSNSPublishFailed
	}

	f.published = append(f.published, fakeSNSMessage{topicARN: topicARN, message: message})

	return nil
}

func TestDescribeDeliveryChannelStatus_NoDeliveryYet(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	require.NoError(t, b.PutDeliveryChannel("chan", "my-bucket", "", "", nil))

	statuses := b.DescribeDeliveryChannelStatus(nil)
	require.Len(t, statuses, 1)

	status := statuses[0]
	assert.Equal(t, "chan", status.Name)
	assert.Nil(t, status.ConfigHistoryDeliveryInfo)
	assert.Nil(t, status.ConfigSnapshotDeliveryInfo)
	assert.Nil(t, status.ConfigStreamDeliveryInfo)
}

func TestDeliverConfigSnapshot_S3Delivery(t *testing.T) {
	t.Parallel()

	fixedNow := time.Date(2026, 9, 11, 18, 39, 39, 0, time.UTC)

	tests := []struct {
		setupChannel func(b *awsconfig.InMemoryBackend)
		wantStatus   string
		name         string
		wantErrCode  string
		wantKeyPart  string
	}{
		{
			name: "success",
			setupChannel: func(b *awsconfig.InMemoryBackend) {
				require.NoError(t, b.PutDeliveryChannel("chan", "good-bucket", "", "", nil))
			},
			wantStatus:  "Success",
			wantKeyPart: "AWSLogs/123456789012/Config/us-east-1/2026/9/11/ConfigSnapshot/",
		},
		{
			name: "missing_bucket",
			setupChannel: func(b *awsconfig.InMemoryBackend) {
				require.NoError(t, b.PutDeliveryChannel("chan", "missing-bucket", "", "", nil))
			},
			wantStatus:  "Failure",
			wantErrCode: "NoSuchBucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			b.SetClock(func() time.Time { return fixedNow })

			s3w := &fakeS3Writer{failBuckets: map[string]bool{"missing-bucket": true}}
			b.SetS3Writer(s3w)

			tt.setupChannel(b)
			require.NoError(t, b.PutConfigurationRecorder("rec", "arn:aws:iam::123456789012:role/r", nil))
			require.NoError(t, b.StartConfigurationRecorder("rec"))

			id, err := b.DeliverConfigSnapshot(t.Context(), "chan")
			require.NoError(t, err)
			assert.NotEmpty(t, id)

			statuses := b.DescribeDeliveryChannelStatus([]string{"chan"})
			require.Len(t, statuses, 1)
			snap := statuses[0].ConfigSnapshotDeliveryInfo
			require.NotNil(t, snap)
			assert.Equal(t, tt.wantStatus, snap.LastStatus)
			require.NotNil(t, snap.LastAttemptTime)

			if tt.wantErrCode != "" {
				assert.Equal(t, tt.wantErrCode, snap.LastErrorCode)
				assert.Nil(t, snap.LastSuccessfulTime)
			} else {
				require.NotNil(t, snap.LastSuccessfulTime)

				found := false

				for _, k := range s3w.keys() {
					if strings.Contains(k, tt.wantKeyPart) && strings.HasSuffix(k, ".json.gz") &&
						strings.Contains(k, id) {
						found = true
					}
				}

				assert.True(t, found, "expected an S3 key containing %q and %q, got %v", tt.wantKeyPart, id, s3w.keys())
			}
		})
	}
}

func TestDeliverConfigSnapshot_SNSStreamDelivery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		snsTopic       string
		name           string
		wantStreamStat string
		failPublish    bool
	}{
		{
			name:           "no_topic_configured",
			wantStreamStat: "Not_Applicable",
		},
		{
			name:           "topic_configured_publish_succeeds",
			snsTopic:       "arn:aws:sns:us-east-1:123456789012:topic",
			wantStreamStat: "Success",
		},
		{
			name:           "topic_configured_publish_fails",
			snsTopic:       "arn:aws:sns:us-east-1:123456789012:topic",
			failPublish:    true,
			wantStreamStat: "Failure",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			b.SetS3Writer(&fakeS3Writer{})

			pub := &fakeSNSPublisher{failTopics: map[string]bool{}}
			if tt.failPublish {
				pub.failTopics[tt.snsTopic] = true
			}

			b.SetSNSPublisher(pub)

			require.NoError(t, b.PutDeliveryChannel("chan", "bucket", tt.snsTopic, "", nil))
			require.NoError(t, b.PutConfigurationRecorder("rec", "arn:aws:iam::123456789012:role/r", nil))
			require.NoError(t, b.StartConfigurationRecorder("rec"))

			_, err := b.DeliverConfigSnapshot(t.Context(), "chan")
			require.NoError(t, err)

			statuses := b.DescribeDeliveryChannelStatus([]string{"chan"})
			require.Len(t, statuses, 1)
			stream := statuses[0].ConfigStreamDeliveryInfo
			require.NotNil(t, stream)
			assert.Equal(t, tt.wantStreamStat, stream.LastStatus)
			require.NotNil(t, stream.LastStatusChangeTime)

			// ConfigHistoryDeliveryInfo is never populated: DeliverConfigSnapshot
			// only delivers a snapshot, never a periodic history file.
			assert.Nil(t, statuses[0].ConfigHistoryDeliveryInfo)
		})
	}
}

func TestDeliverConfigSnapshot_NoS3WriterConfigured(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	require.NoError(t, b.PutDeliveryChannel("chan", "bucket", "", "", nil))
	require.NoError(t, b.PutConfigurationRecorder("rec", "arn:aws:iam::123456789012:role/r", nil))
	require.NoError(t, b.StartConfigurationRecorder("rec"))

	id, err := b.DeliverConfigSnapshot(t.Context(), "chan")
	require.NoError(t, err)
	assert.NotEmpty(t, id)

	statuses := b.DescribeDeliveryChannelStatus([]string{"chan"})
	require.Len(t, statuses, 1)
	snap := statuses[0].ConfigSnapshotDeliveryInfo
	require.NotNil(t, snap)
	assert.Equal(t, "Failure", snap.LastStatus)
	assert.NotEmpty(t, snap.LastErrorCode)
}

func TestDeliverConfigSnapshot_NextDeliveryTimeFromFrequency(t *testing.T) {
	t.Parallel()

	fixedNow := time.Date(2026, 9, 11, 0, 0, 0, 0, time.UTC)

	b := awsconfig.NewInMemoryBackend()
	b.SetClock(func() time.Time { return fixedNow })
	b.SetS3Writer(&fakeS3Writer{})

	require.NoError(t, b.PutDeliveryChannel("chan", "bucket", "", "", &awsconfig.DeliverySnapshotProperties{
		DeliveryFrequency: "Six_Hours",
	}))
	require.NoError(t, b.PutConfigurationRecorder("rec", "arn:aws:iam::123456789012:role/r", nil))
	require.NoError(t, b.StartConfigurationRecorder("rec"))

	_, err := b.DeliverConfigSnapshot(t.Context(), "chan")
	require.NoError(t, err)

	statuses := b.DescribeDeliveryChannelStatus([]string{"chan"})
	require.Len(t, statuses, 1)
	snap := statuses[0].ConfigSnapshotDeliveryInfo
	require.NotNil(t, snap)
	require.NotNil(t, snap.NextDeliveryTime)
	assert.Equal(t, fixedNow.Add(6*time.Hour).Unix(), int64(*snap.NextDeliveryTime))
}

func TestDeliverConfigSnapshot_RestoreAfterSnapshotKeepsState(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	b.SetS3Writer(&fakeS3Writer{})

	require.NoError(t, b.PutDeliveryChannel("chan", "bucket", "", "", nil))
	require.NoError(t, b.PutConfigurationRecorder("rec", "arn:aws:iam::123456789012:role/r", nil))
	require.NoError(t, b.StartConfigurationRecorder("rec"))

	_, err := b.DeliverConfigSnapshot(t.Context(), "chan")
	require.NoError(t, err)

	before := b.DescribeDeliveryChannelStatus([]string{"chan"})
	require.Len(t, before, 1)
	require.NotNil(t, before[0].ConfigSnapshotDeliveryInfo)

	data := b.Snapshot(t.Context())
	require.NotEmpty(t, data)

	restored := awsconfig.NewInMemoryBackend()
	require.NoError(t, restored.Restore(t.Context(), data))

	after := restored.DescribeDeliveryChannelStatus([]string{"chan"})
	require.Len(t, after, 1)
	require.NotNil(t, after[0].ConfigSnapshotDeliveryInfo)
	assert.Equal(t, before[0].ConfigSnapshotDeliveryInfo.LastStatus, after[0].ConfigSnapshotDeliveryInfo.LastStatus)
	require.NotNil(t, after[0].ConfigSnapshotDeliveryInfo.LastSuccessfulTime)
	assert.InDelta(
		t,
		*before[0].ConfigSnapshotDeliveryInfo.LastSuccessfulTime,
		*after[0].ConfigSnapshotDeliveryInfo.LastSuccessfulTime,
		0.001,
	)
}

// TestDeliverConfigSnapshot_RealClient drives PutDeliveryChannel then
// DeliverConfigSnapshot through the real aws-sdk-go-v2 configservice client,
// then confirms DescribeDeliveryChannelStatus reports a real, populated
// ConfigSnapshotDeliveryInfo (lowerCamel wire keys, confirmed at
// deserializers.go:15453 awsAwsjson11_deserializeDocumentConfigExportDeliveryInfo)
// instead of the pre-fix always-nil-tracking gap (gopherstack-ru0y).
func TestDeliverConfigSnapshot_RealClient(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	b.SetS3Writer(&fakeS3Writer{})

	h := awsconfig.NewHandler(b)
	client := newTestAWSConfigSDKClient(t, h)

	_, err := client.PutDeliveryChannel(t.Context(), &configservicesdk.PutDeliveryChannelInput{
		DeliveryChannel: &types.DeliveryChannel{
			Name:         aws.String("default"),
			S3BucketName: aws.String("my-bucket"),
		},
	})
	require.NoError(t, err)

	require.NoError(t, b.PutConfigurationRecorder("rec", "arn:aws:iam::123456789012:role/r", nil))
	require.NoError(t, b.StartConfigurationRecorder("rec"))

	deliverOut, err := client.DeliverConfigSnapshot(
		t.Context(), &configservicesdk.DeliverConfigSnapshotInput{DeliveryChannelName: aws.String("default")},
	)
	require.NoError(t, err)
	require.NotNil(t, deliverOut.ConfigSnapshotId)

	out, err := client.DescribeDeliveryChannelStatus(
		t.Context(), &configservicesdk.DescribeDeliveryChannelStatusInput{},
	)
	require.NoError(t, err)
	require.Len(t, out.DeliveryChannelsStatus, 1)

	status := out.DeliveryChannelsStatus[0]
	require.NotNil(t, status.ConfigSnapshotDeliveryInfo)
	assert.Equal(t, "Success", string(status.ConfigSnapshotDeliveryInfo.LastStatus))
	require.NotNil(t, status.ConfigSnapshotDeliveryInfo.LastSuccessfulTime)
	assert.Nil(t, status.ConfigHistoryDeliveryInfo)
}
