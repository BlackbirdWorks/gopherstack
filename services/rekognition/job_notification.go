package rekognition

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/sns"
)

// notificationChannelReq mirrors types.NotificationChannel; both members are required.
type notificationChannelReq struct {
	SNSTopicArn string `json:"SNSTopicArn"`
	RoleArn     string `json:"RoleArn"`
}

func (n *notificationChannelReq) validate() error {
	if n == nil {
		return nil
	}

	if n.SNSTopicArn == "" || n.RoleArn == "" {
		return fmt.Errorf("%w: NotificationChannel requires SNSTopicArn and RoleArn", ErrValidation)
	}

	return nil
}

func (n *notificationChannelReq) topic() string {
	if n == nil {
		return ""
	}

	return n.SNSTopicArn
}

var asyncJobAPINames = map[string]string{ //nolint:gochecknoglobals // static lookup table
	"celebrity_recognition": "StartCelebrityRecognition",
	"label_detection":       "StartLabelDetection",
	"person_tracking":       "StartPersonTracking",
	"segment_detection":     "StartSegmentDetection",
	"face_detection":        "StartFaceDetection",
	"face_search":           "StartFaceSearch",
	"text_detection":        "StartTextDetection",
	"content_moderation":    "StartContentModeration",
}

// JobNotifier publishes a job completion message to an SNS topic.
type JobNotifier interface {
	Publish(topicARN, message string) error
}

type snsHandlerProvider interface {
	GetSNSHandler() service.Registerable
}

// snsNotifier resolves the SNS backend lazily, so provider init order does not matter.
type snsNotifier struct{ cfg snsHandlerProvider }

func (n snsNotifier) Publish(topicARN, message string) error {
	h, ok := n.cfg.GetSNSHandler().(*sns.Handler)
	if !ok || h == nil || h.Backend == nil {
		return nil
	}

	_, err := h.Backend.Publish(topicARN, message, "", "", nil)

	return err
}

// SetJobNotifier wires the completion-notification sink.
func (b *InMemoryBackend) SetJobNotifier(n JobNotifier) {
	b.mu.Lock("SetJobNotifier")
	defer b.mu.Unlock()

	b.notifier = n
}

type jobNotification struct {
	topic   string
	message []byte
}

// buildJobNotification renders the documented Rekognition Video completion message.
func buildJobNotification(jobID string, p StartAsyncJobParams) jobNotification {
	msg := map[string]any{
		"JobId":     jobID,
		"Status":    jobStatusSucceeded,
		"API":       asyncJobAPINames[p.JobType],
		"JobTag":    p.JobTag,
		"Timestamp": time.Now().UnixMilli(),
		"Video":     map[string]string{"S3ObjectName": p.VideoS3Name, "S3Bucket": p.VideoS3Bucket},
	}
	data, _ := json.Marshal(msg)

	return jobNotification{topic: p.NotificationTopicARN, message: data}
}

func (b *InMemoryBackend) publishJobNotification(n jobNotification) {
	b.mu.RLock("publishJobNotification")
	notifier := b.notifier
	b.mu.RUnlock()

	if notifier == nil {
		return
	}

	if err := notifier.Publish(n.topic, string(n.message)); err != nil {
		ctx := context.Background()
		logger.Load(ctx).WarnContext(
			ctx, "rekognition: job completion notification failed", "topic", n.topic, "error", err,
		)
	}
}
