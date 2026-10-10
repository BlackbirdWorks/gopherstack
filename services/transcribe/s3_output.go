package transcribe

import (
	"bytes"
	"context"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

const (
	roleAgent    = "AGENT"
	roleCustomer = "CUSTOMER"
)

const millisPerSyntheticChar = 50

const outputWriteFailure = "The S3 bucket that you specified is not accessible or does not exist."

// S3Writer stores a finished job's output object.
type S3Writer interface {
	PutObject(ctx context.Context, bucket, key string, data []byte) error
}

type siblingServices interface {
	GetS3Handler() service.Registerable
}

type backendS3Writer struct{ backend s3backend.StorageBackend }

func (w backendS3Writer) PutObject(ctx context.Context, bucket, key string, data []byte) error {
	_, err := w.backend.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(data),
	})

	return err
}

// SetS3Writer overrides the S3 destination for job output.
func (b *InMemoryBackend) SetS3Writer(w S3Writer) {
	b.mu.Lock("SetS3Writer")
	defer b.mu.Unlock()

	b.s3Writer = w
}

// SetAppConfig records the service.AppContext.Config for lazy S3 lookup.
func (b *InMemoryBackend) SetAppConfig(cfg any) {
	b.mu.Lock("SetAppConfig")
	defer b.mu.Unlock()

	b.appConfig = cfg
}

func (b *InMemoryBackend) appConfigValue() any {
	b.mu.RLock("appConfigValue")
	defer b.mu.RUnlock()

	return b.appConfig
}

// resolveS3Writer returns the explicit writer or the emulated S3; the caller holds b.mu.
func (b *InMemoryBackend) resolveS3Writer() (S3Writer, bool) {
	if b.s3Writer != nil {
		return b.s3Writer, true
	}

	s, ok := b.appConfig.(siblingServices)
	if !ok {
		return nil, false
	}

	h, ok := s.GetS3Handler().(*s3backend.S3Handler)
	if !ok || h == nil || h.Backend == nil {
		return nil, false
	}

	return backendS3Writer{backend: h.Backend}, true
}

// putOutputLocked writes data when a customer bucket is named and S3 is reachable.
// It reports false only when the write was attempted and failed.
func (b *InMemoryBackend) putOutputLocked(bucket, key string, data []byte) bool {
	if bucket == "" {
		return true
	}

	w, ok := b.resolveS3Writer()
	if !ok {
		return true
	}

	return w.PutObject(context.Background(), bucket, key, data) == nil
}

// outputObjectKey resolves OutputKey: empty or a trailing "/" folder gets <jobName>.json appended.
func outputObjectKey(outputKey, jobName string) string {
	if outputKey == "" || strings.HasSuffix(outputKey, "/") {
		return outputKey + jobName + ".json"
	}

	return outputKey
}

// splitOutputLocation splits an OutputLocation s3:// URI into bucket and resolved object key.
func splitOutputLocation(location, jobName string) (string, string) {
	rest := strings.TrimPrefix(location, "s3://")
	bucket, key, _ := strings.Cut(rest, "/")

	return bucket, outputObjectKey(key, jobName)
}

type callAnalyticsDocument struct {
	JobName            string                       `json:"JobName"`
	AccountID          string                       `json:"AccountId"`
	JobStatus          string                       `json:"JobStatus"`
	LanguageCode       string                       `json:"LanguageCode"`
	ChannelDefinitions []ChannelDefinition          `json:"ChannelDefinitions,omitempty"`
	Participants       []callAnalyticsPerson        `json:"Participants"`
	Transcript         []callAnalyticsSegment       `json:"Transcript"`
	Characteristics    callAnalyticsCharacteristics `json:"ConversationCharacteristics"`
}

type callAnalyticsPerson struct {
	ParticipantRole string `json:"ParticipantRole"`
}

type callAnalyticsSegment struct {
	ParticipantRole string `json:"ParticipantRole"`
	Content         string `json:"Content"`
	ID              string `json:"Id"`
	Sentiment       string `json:"Sentiment"`
	Begin           int    `json:"BeginOffsetMillis"`
	End             int    `json:"EndOffsetMillis"`
}

type callAnalyticsCharacteristics struct {
	TotalConversationDurationMillis int `json:"TotalConversationDurationMillis"`
}

func synthesizeCallAnalyticsJSON(job *CallAnalyticsJob) []byte {
	text := deriveTranscriptText(job.CallAnalyticsJobName, job.Media.MediaFileURI)
	doc := callAnalyticsDocument{
		JobName:            job.CallAnalyticsJobName,
		AccountID:          defaultAccountID,
		JobStatus:          jobStatusCompleted,
		LanguageCode:       job.LanguageCode,
		ChannelDefinitions: job.ChannelDefinitions,
		Participants:       []callAnalyticsPerson{{ParticipantRole: roleAgent}, {ParticipantRole: roleCustomer}},
		Transcript: []callAnalyticsSegment{
			{
				ParticipantRole: roleAgent,
				Content:         text,
				ID:              "0",
				End:             len(text) * millisPerSyntheticChar,
				Sentiment:       "NEUTRAL",
			},
		},
		Characteristics: callAnalyticsCharacteristics{
			TotalConversationDurationMillis: len(text) * millisPerSyntheticChar,
		},
	}
	data, _ := marshalJSON(doc)

	return data
}
