package comprehend

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

const tarFileMode = 0o644

const outputWriteFailure = "Unable to write job output to the specified S3 location."

// S3Writer stores a finished job's output archive.
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

const (
	jobTypeClassification    = "document-classification-job"
	jobTypeEntities          = "entities-detection-job"
	jobTypeKeyPhrases        = "key-phrases-detection-job"
	jobTypeSentiment         = "sentiment-detection-job"
	jobTypePII               = "pii-entities-detection-job"
	jobTypeTopics            = "topics-detection-job"
	jobTypeTargetedSentiment = "targeted-sentiment-detection-job"
	jobTypeLanguage          = "dominant-language-detection-job"
	jobTypeEvents            = "events-detection-job"
)

func jobOutputCode(jobType string) string {
	switch jobType {
	case jobTypeClassification:
		return "CLN"
	case jobTypeEntities:
		return "NER"
	case jobTypeKeyPhrases:
		return "KP"
	case jobTypeSentiment:
		return "SENTIMENT"
	case jobTypePII:
		return "PII"
	case jobTypeTopics:
		return "TOPICS"
	case jobTypeTargetedSentiment:
		return "TS"
	case jobTypeLanguage:
		return "LANGUAGE"
	case jobTypeEvents:
		return "EVENTS"
	default:
		return ""
	}
}

// deliverJobOutputLocked writes <S3Uri>/<account>-<code>-<jobid>/output/output.tar.gz for a completed job.
func (b *InMemoryBackend) deliverJobOutputLocked(job *Job) {
	uri, _ := job.OutputDataConfig["S3Uri"].(string)
	bucket, prefix, ok := splitS3URI(uri)
	if !ok {
		return
	}

	w, ok := b.resolveS3Writer()
	if !ok {
		return
	}

	archive, err := buildOutputArchive(job)
	if err == nil {
		folder := fmt.Sprintf("%s-%s-%s", b.accountID, jobOutputCode(job.JobType), job.JobID)
		err = w.PutObject(context.Background(), bucket, path.Join(prefix, folder, "output", "output.tar.gz"), archive)
	}

	if err != nil {
		job.JobStatus = statusFailed
		job.FailureReason = outputWriteFailure
	}
}

func splitS3URI(uri string) (string, string, bool) {
	rest, found := strings.CutPrefix(uri, "s3://")
	if !found {
		return "", "", false
	}

	bucket, prefix, _ := strings.Cut(rest, "/")
	if bucket == "" {
		return "", "", false
	}

	return bucket, strings.Trim(prefix, "/"), true
}

// outputFiles returns the per-job-type files placed in output.tar.gz.
func outputFiles(job *Job) map[string][]byte {
	input, _ := job.InputDataConfig["S3Uri"].(string)

	switch job.JobType {
	case jobTypeTopics:
		return map[string][]byte{
			"doc-topics.csv":   []byte("docname,topic,proportion\n" + input + ",0,1.0\n"),
			"topics-terms.csv": []byte("topic,term,weight\n0,document,1.0\n"),
		}
	case jobTypeClassification:
		line, _ := json.Marshal(map[string]any{
			"File": input, "Classes": []map[string]any{{"Name": "DEFAULT", "Score": defaultScore}},
		})

		return map[string][]byte{"predictions.jsonl": append(line, '\n')}
	}

	line, _ := json.Marshal(jobResultLine(job, input))

	return map[string][]byte{"output": append(line, '\n')}
}

func jobResultLine(job *Job, input string) map[string]any {
	line := map[string]any{"File": input, "Line": 0}

	switch job.JobType {
	case jobTypeEntities:
		line["Entities"] = []any{}
	case jobTypeKeyPhrases:
		line["KeyPhrases"] = []any{}
	case jobTypeSentiment:
		sentiment, pos, neg, neu, mix := sentimentResult(0, 0)
		line["Sentiment"] = sentiment
		line["SentimentScore"] = sentimentScoreMap(pos, neg, neu, mix)
	case jobTypePII:
		line["Entities"] = []any{}
	case jobTypeTargetedSentiment:
		line["Entities"] = []any{}
	case jobTypeLanguage:
		line["Languages"] = []map[string]any{{"LanguageCode": job.LanguageCode, "Score": defaultScore}}
	case jobTypeEvents:
		line["Entities"] = []any{}
		line["Events"] = []any{}
	}

	return line
}

func buildOutputArchive(job *Job) ([]byte, error) {
	var buf bytes.Buffer

	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)

	files := outputFiles(job)
	names := make([]string, 0, len(files))

	for name := range files {
		names = append(names, name)
	}

	slices.Sort(names)

	for _, name := range names {
		data := files[name]
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: tarFileMode, Size: int64(len(data))}); err != nil {
			return nil, err
		}

		if _, err := tw.Write(data); err != nil {
			return nil, err
		}
	}

	if err := tw.Close(); err != nil {
		return nil, err
	}

	if err := gz.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
