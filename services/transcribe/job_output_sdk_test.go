package transcribe_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	transcribesdk "github.com/aws/aws-sdk-go-v2/service/transcribe"
	sdktypes "github.com/aws/aws-sdk-go-v2/service/transcribe/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	"github.com/blackbirdworks/gopherstack/services/transcribe"
)

type appConfig struct {
	s3       service.Registerable
	endpoint string
}

func (c *appConfig) GetS3Handler() service.Registerable { return c.s3 }
func (c *appConfig) GetEndpoint() string                { return c.endpoint }

type fakeClock struct {
	now time.Time
	mu  sync.Mutex
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = c.now.Add(d)
}

type jobEnv struct {
	tr      *transcribesdk.Client
	s3      *s3.Client
	backend *transcribe.InMemoryBackend
	clock   *fakeClock
	url     string
}

const jobDelay = time.Minute

func newJobEnv(t *testing.T, engine transcribe.Engine) *jobEnv {
	t.Helper()

	cfg := &appConfig{s3: s3backend.NewHandler(s3backend.NewInMemoryBackend(nil))}
	backend := transcribe.NewInMemoryBackend()
	backend.SetAppConfig(cfg)

	clock := &fakeClock{now: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	backend.SetClock(clock.Now)
	backend.SetLifecycleDelay(jobDelay)

	if engine != nil {
		backend.SetEngine(engine)
	}

	h := transcribe.NewHandler(backend)
	h.EnableRegions()
	t.Cleanup(func() { h.Shutdown(context.WithoutCancel(t.Context())) })

	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	require.NoError(t, registry.Register(cfg.s3))

	e := echo.New()
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg.endpoint = srv.URL

	awsCfg, err := awscfg.LoadDefaultConfig(t.Context(), awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	require.NoError(t, err)

	env := &jobEnv{backend: backend, clock: clock, url: srv.URL}
	env.tr = transcribesdk.NewFromConfig(
		awsCfg,
		func(o *transcribesdk.Options) { o.BaseEndpoint = aws.String(srv.URL) },
	)
	env.s3 = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.UsePathStyle = true
	})

	for _, b := range []string{"media-in", "media-out"} {
		_, err = env.s3.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String(b)})
		require.NoError(t, err)
	}

	return env
}

func (e *jobEnv) put(t *testing.T, key string, data []byte) {
	t.Helper()

	_, err := e.s3.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String("media-in"), Key: aws.String(key), Body: bytes.NewReader(data),
	})
	require.NoError(t, err)
}

func (e *jobEnv) getOut(t *testing.T, key string) []byte {
	t.Helper()

	out, err := e.s3.GetObject(t.Context(), &s3.GetObjectInput{Bucket: aws.String("media-out"), Key: aws.String(key)})
	require.NoError(t, err)

	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	require.NoError(t, err)

	return data
}

func (e *jobEnv) job(t *testing.T, name string) *sdktypes.TranscriptionJob {
	t.Helper()

	out, err := e.tr.GetTranscriptionJob(t.Context(), &transcribesdk.GetTranscriptionJobInput{
		TranscriptionJobName: aws.String(name),
	})
	require.NoError(t, err)

	return out.TranscriptionJob
}

func (e *jobEnv) fetch(t *testing.T, url string) (int, []byte) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, data
}

func pcmTone(rate, channels int, seconds float64) []byte {
	frames := int(float64(rate) * seconds)
	buf := make([]byte, 0, frames*channels*2)

	for i := range frames {
		v := int16(8000 * math.Sin(2*math.Pi*440*float64(i)/float64(rate)))
		for range channels {
			buf = binary.LittleEndian.AppendUint16(buf, uint16(v))
		}
	}

	return buf
}

func wavFile(channels int, seconds float64) []byte {
	const rate = 16000

	pcm := pcmTone(rate, channels, seconds)
	hdr := make([]byte, 0, 44)
	hdr = append(hdr, "RIFF"...)
	hdr = binary.LittleEndian.AppendUint32(hdr, uint32(36+len(pcm)))
	hdr = append(hdr, "WAVEfmt "...)
	hdr = binary.LittleEndian.AppendUint32(hdr, 16)
	hdr = binary.LittleEndian.AppendUint16(hdr, 1)
	hdr = binary.LittleEndian.AppendUint16(hdr, uint16(channels))
	hdr = binary.LittleEndian.AppendUint32(hdr, uint32(rate))
	hdr = binary.LittleEndian.AppendUint32(hdr, uint32(rate*channels*2))
	hdr = binary.LittleEndian.AppendUint16(hdr, uint16(channels*2))
	hdr = binary.LittleEndian.AppendUint16(hdr, 16)
	hdr = append(hdr, "data"...)
	hdr = binary.LittleEndian.AppendUint32(hdr, uint32(len(pcm)))

	return append(hdr, pcm...)
}

type docShape struct {
	JobName   string `json:"jobName"`
	AccountID string `json:"accountId"`
	Status    string `json:"status"`
	Results   struct {
		SpeakerLabels *struct {
			Speakers int `json:"speakers"`
		} `json:"speaker_labels"`
		ChannelLabels *struct {
			Channels []struct {
				Label string           `json:"channel_label"`
				Items []jsontext.Value `json:"items"`
			} `json:"channels"`
			Number int `json:"number_of_channels"`
		} `json:"channel_labels"`
		Transcripts []struct {
			Transcript string `json:"transcript"`
		} `json:"transcripts"`
		Items []struct {
			StartTime    string `json:"start_time"`
			EndTime      string `json:"end_time"`
			Type         string `json:"type"`
			Alternatives []struct {
				Content    string `json:"content"`
				Confidence string `json:"confidence"`
			} `json:"alternatives"`
			ID int `json:"id"`
		} `json:"items"`
		AudioSegments []jsontext.Value `json:"audio_segments"`
	} `json:"results"`
}

func parseDoc(t *testing.T, data []byte) docShape {
	t.Helper()

	var d docShape
	require.NoError(t, json.Unmarshal(data, &d, jsonx.V1()), string(data))

	return d
}

func TestJobOutput_NoEngine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		media      func() []byte
		name       string
		bucket     string
		key        string
		wantURI    string
		wantStatus sdktypes.TranscriptionJobStatus
		format     sdktypes.MediaFormat
		wantReason string
		rate       int32
	}{
		{
			name: "custom bucket", bucket: "media-out", key: "t/", wantURI: "s3://media-out/t/job.json",
			wantStatus: sdktypes.TranscriptionJobStatusCompleted,
			media:      func() []byte { return wavFile(1, 0.5) }, format: sdktypes.MediaFormatWav,
		},
		{
			name: "service managed", wantStatus: sdktypes.TranscriptionJobStatusCompleted,
			media: func() []byte { return wavFile(1, 0.5) }, format: sdktypes.MediaFormatWav, rate: 16000,
		},
		{
			name: "missing media", bucket: "media-out", wantStatus: sdktypes.TranscriptionJobStatusFailed,
			wantReason: "can't be accessed", format: sdktypes.MediaFormatWav,
		},
		{
			name: "unknown format bytes", bucket: "media-out", wantStatus: sdktypes.TranscriptionJobStatusFailed,
			media: func() []byte { return []byte("this is not audio at all") }, format: sdktypes.MediaFormatMp3,
			wantReason: "Unsupported media format",
		},
		{
			name: "format mismatch", bucket: "media-out", wantStatus: sdktypes.TranscriptionJobStatusFailed,
			media: func() []byte { return wavFile(1, 0.1) }, format: sdktypes.MediaFormatFlac,
			wantReason: "does not match the detected media format",
		},
		{
			name: "sample rate mismatch", bucket: "media-out", wantStatus: sdktypes.TranscriptionJobStatusFailed,
			media: func() []byte { return wavFile(1, 0.1) }, format: sdktypes.MediaFormatWav, rate: 44100,
			wantReason: "does not match the detected sample rate",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newJobEnv(t, nil)
			if tt.media != nil {
				env.put(t, "audio.wav", tt.media())
			}

			in := &transcribesdk.StartTranscriptionJobInput{
				TranscriptionJobName: aws.String("job"), LanguageCode: sdktypes.LanguageCodeEnUs,
				Media:       &sdktypes.Media{MediaFileUri: aws.String("s3://media-in/audio.wav")},
				MediaFormat: tt.format,
			}
			if tt.bucket != "" {
				in.OutputBucketName = aws.String(tt.bucket)
				in.OutputKey = aws.String(tt.key)
			}

			if tt.rate != 0 {
				in.MediaSampleRateHertz = aws.Int32(tt.rate)
			}

			started, err := env.tr.StartTranscriptionJob(t.Context(), in)
			require.NoError(t, err)
			assert.Equal(t, sdktypes.TranscriptionJobStatusInProgress, started.TranscriptionJob.TranscriptionJobStatus)
			assert.Equal(t, sdktypes.TranscriptionJobStatusInProgress, env.job(t, "job").TranscriptionJobStatus)

			env.clock.Advance(jobDelay)

			job := env.job(t, "job")
			require.Equal(t, tt.wantStatus, job.TranscriptionJobStatus, aws.ToString(job.FailureReason))

			if tt.wantStatus == sdktypes.TranscriptionJobStatusFailed {
				assert.Contains(t, aws.ToString(job.FailureReason), tt.wantReason)
				assert.Nil(t, job.Transcript.TranscriptFileUri)

				return
			}

			uri := aws.ToString(job.Transcript.TranscriptFileUri)

			var raw []byte
			if tt.bucket != "" {
				assert.Equal(t, tt.wantURI, uri)
				raw = env.getOut(t, "t/job.json")
			} else {
				assert.True(
					t,
					strings.HasPrefix(uri, env.url+"/_gopherstack/transcribe/output/us-east-1/transcription/job/"),
					uri,
				)
				code, body := env.fetch(t, uri)
				require.Equal(t, http.StatusOK, code)
				raw = body

				code, _ = env.fetch(t, strings.Replace(uri, "/job/", "/job/x", 1))
				assert.Equal(t, http.StatusForbidden, code)
			}

			doc := parseDoc(t, raw)
			assert.Equal(t, "job", doc.JobName)
			assert.Equal(t, "123456789012", doc.AccountID)
			assert.Equal(t, "COMPLETED", doc.Status)
			require.Len(t, doc.Results.Transcripts, 1)
			assert.Empty(t, doc.Results.Transcripts[0].Transcript)
			assert.Empty(t, doc.Results.Items)
			assert.Contains(t, string(raw), `"items":[]`)
		})
	}
}

type scriptedEngine struct {
	words map[string][]transcribe.Word
	langs map[string]bool
}

func (e *scriptedEngine) SupportsLanguage(code string) bool { return e.langs[code] }

func (e *scriptedEngine) Recognize(_ context.Context, _ string, _ int, _ []byte) ([]transcribe.Word, error) {
	return e.words["tone"], nil
}

func TestJobOutput_Engine(t *testing.T) {
	t.Parallel()

	words := map[string][]transcribe.Word{"tone": {
		{Text: "hello", Start: 0.1, End: 0.4, Confidence: 0.9},
		{Text: "world", Start: 0.5, End: 0.9, Confidence: 1},
		{Text: "again", Start: 3.0, End: 3.4, Confidence: 0.75},
	}}
	engine := &scriptedEngine{words: words, langs: map[string]bool{"en-US": true}}

	tests := []struct {
		settings   *sdktypes.Settings
		name       string
		language   sdktypes.LanguageCode
		wantReason string
		wantText   string
		redaction  bool
		subtitles  bool
	}{
		{name: "words", language: sdktypes.LanguageCodeEnUs, wantText: "hello world again"},
		{name: "subtitles", language: sdktypes.LanguageCodeEnUs, subtitles: true, wantText: "hello world again"},
		{
			name: "channels", language: sdktypes.LanguageCodeEnUs, wantText: "hello hello world world again again",
			settings: &sdktypes.Settings{ChannelIdentification: aws.Bool(true)},
		},
		{
			name:       "unsupported language",
			language:   sdktypes.LanguageCodeFrFr,
			wantReason: "not supported by the emulator",
		},
		{
			name: "diarization", language: sdktypes.LanguageCodeEnUs, wantReason: "Speaker partitioning",
			settings: &sdktypes.Settings{ShowSpeakerLabels: aws.Bool(true), MaxSpeakerLabels: aws.Int32(2)},
		},
		{
			name: "redaction", language: sdktypes.LanguageCodeEnUs, redaction: true,
			wantReason: "Content redaction",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newJobEnv(t, engine)
			env.put(t, "audio.wav", wavFile(2, 0.3))

			in := &transcribesdk.StartTranscriptionJobInput{
				TranscriptionJobName: aws.String("job"), LanguageCode: tt.language, Settings: tt.settings,
				Media:            &sdktypes.Media{MediaFileUri: aws.String("s3://media-in/audio.wav")},
				OutputBucketName: aws.String("media-out"),
			}
			if tt.redaction {
				in.ContentRedaction = &sdktypes.ContentRedaction{
					RedactionType: sdktypes.RedactionTypePii, RedactionOutput: sdktypes.RedactionOutputRedacted,
				}
			}

			if tt.subtitles {
				in.Subtitles = &sdktypes.Subtitles{
					Formats:          []sdktypes.SubtitleFormat{"vtt", "srt"},
					OutputStartIndex: aws.Int32(1),
				}
			}

			_, err := env.tr.StartTranscriptionJob(t.Context(), in)
			require.NoError(t, err)

			var job *sdktypes.TranscriptionJob

			require.Eventually(t, func() bool {
				job = env.job(t, "job")

				return job.TranscriptionJobStatus != sdktypes.TranscriptionJobStatusInProgress
			}, 10*time.Second, 5*time.Millisecond)

			if tt.wantReason != "" {
				require.Equal(t, sdktypes.TranscriptionJobStatusFailed, job.TranscriptionJobStatus)
				assert.Contains(t, aws.ToString(job.FailureReason), tt.wantReason)

				return
			}

			require.Equal(
				t,
				sdktypes.TranscriptionJobStatusCompleted,
				job.TranscriptionJobStatus,
				aws.ToString(job.FailureReason),
			)

			doc := parseDoc(t, env.getOut(t, "job.json"))
			require.Len(t, doc.Results.Transcripts, 1)
			assert.Equal(t, tt.wantText, doc.Results.Transcripts[0].Transcript)

			if tt.settings != nil {
				require.NotNil(t, doc.Results.ChannelLabels)
				assert.Equal(t, 2, doc.Results.ChannelLabels.Number)
				assert.Len(t, doc.Results.ChannelLabels.Channels[1].Items, 3)

				return
			}

			require.Len(t, doc.Results.Items, 3)
			assert.Equal(t, "0.10", doc.Results.Items[0].StartTime)
			assert.Equal(t, "0.9", doc.Results.Items[0].Alternatives[0].Confidence)
			assert.Equal(t, "1.0", doc.Results.Items[1].Alternatives[0].Confidence)
			assert.Len(t, doc.Results.AudioSegments, 2)

			if tt.subtitles {
				assert.Equal(
					t,
					[]string{"s3://media-out/job.vtt", "s3://media-out/job.srt"},
					job.Subtitles.SubtitleFileUris,
				)
				assert.Contains(t, string(env.getOut(t, "job.vtt")),
					"WEBVTT\n\n00:00:00.100 --> 00:00:00.900\nhello world")
				assert.Contains(t, string(env.getOut(t, "job.srt")),
					"1\n00:00:00,100 --> 00:00:00,900\nhello world")
			}
		})
	}
}

func TestJobOutput_NoEngineFeatures(t *testing.T) {
	t.Parallel()

	env := newJobEnv(t, nil)
	env.put(t, "audio.wav", wavFile(2, 0.2))

	_, err := env.tr.StartTranscriptionJob(t.Context(), &transcribesdk.StartTranscriptionJobInput{
		TranscriptionJobName: aws.String("job"), LanguageCode: sdktypes.LanguageCodeEnUs,
		Media:            &sdktypes.Media{MediaFileUri: aws.String("s3://media-in/audio.wav")},
		OutputBucketName: aws.String("media-out"),
		Settings: &sdktypes.Settings{
			ShowSpeakerLabels: aws.Bool(true), MaxSpeakerLabels: aws.Int32(2),
		},
		Subtitles:        &sdktypes.Subtitles{Formats: []sdktypes.SubtitleFormat{"vtt"}},
		ContentRedaction: &sdktypes.ContentRedaction{RedactionType: "PII", RedactionOutput: "redacted"},
	})
	require.NoError(t, err)
	env.clock.Advance(jobDelay)

	job := env.job(t, "job")
	require.Equal(t, sdktypes.TranscriptionJobStatusCompleted, job.TranscriptionJobStatus)

	doc := parseDoc(t, env.getOut(t, "job.json"))
	require.NotNil(t, doc.Results.SpeakerLabels)
	assert.Zero(t, doc.Results.SpeakerLabels.Speakers)
	assert.Equal(t, "s3://media-out/job.redacted.json", aws.ToString(job.Transcript.RedactedTranscriptFileUri))
	assert.Equal(t, "WEBVTT\n\n", string(env.getOut(t, "job.vtt")))
	assert.JSONEq(t, string(env.getOut(t, "job.json")), string(env.getOut(t, "job.redacted.json")))
}

func TestJobOutput_MedicalAndCallAnalytics(t *testing.T) {
	t.Parallel()

	env := newJobEnv(t, nil)
	env.put(t, "audio.wav", wavFile(2, 0.2))
	media := &sdktypes.Media{MediaFileUri: aws.String("s3://media-in/audio.wav")}

	_, err := env.tr.StartMedicalTranscriptionJob(t.Context(), &transcribesdk.StartMedicalTranscriptionJobInput{
		MedicalTranscriptionJobName: aws.String("med"), LanguageCode: sdktypes.LanguageCodeEnUs, Media: media,
		OutputBucketName: aws.String(
			"media-out",
		), Specialty: sdktypes.SpecialtyPrimarycare, Type: sdktypes.TypeDictation,
	})
	require.NoError(t, err)

	_, err = env.tr.StartCallAnalyticsJob(t.Context(), &transcribesdk.StartCallAnalyticsJobInput{
		CallAnalyticsJobName: aws.String("ca"), Media: media, OutputLocation: aws.String("s3://media-out/calls/"),
		ChannelDefinitions: []sdktypes.ChannelDefinition{
			{ChannelId: 0, ParticipantRole: sdktypes.ParticipantRoleAgent},
			{ChannelId: 1, ParticipantRole: sdktypes.ParticipantRoleCustomer},
		},
	})
	require.NoError(t, err)

	_, err = env.tr.StartCallAnalyticsJob(t.Context(), &transcribesdk.StartCallAnalyticsJobInput{
		CallAnalyticsJobName: aws.String("ca-managed"), Media: media,
	})
	require.NoError(t, err)

	env.clock.Advance(jobDelay)

	med, err := env.tr.GetMedicalTranscriptionJob(t.Context(), &transcribesdk.GetMedicalTranscriptionJobInput{
		MedicalTranscriptionJobName: aws.String("med"),
	})
	require.NoError(t, err)
	require.Equal(t, sdktypes.TranscriptionJobStatusCompleted, med.MedicalTranscriptionJob.TranscriptionJobStatus)
	assert.Equal(t, "s3://media-out/med.json", aws.ToString(med.MedicalTranscriptionJob.Transcript.TranscriptFileUri))
	assert.Equal(t, "COMPLETED", parseDoc(t, env.getOut(t, "med.json")).Status)

	ca, err := env.tr.GetCallAnalyticsJob(t.Context(), &transcribesdk.GetCallAnalyticsJobInput{
		CallAnalyticsJobName: aws.String("ca"),
	})
	require.NoError(t, err)
	require.Equal(t, sdktypes.CallAnalyticsJobStatusCompleted, ca.CallAnalyticsJob.CallAnalyticsJobStatus)
	assert.Equal(t, "s3://media-out/calls/ca.json", aws.ToString(ca.CallAnalyticsJob.Transcript.TranscriptFileUri))

	var caDoc struct {
		JobName         string `json:"JobName"`
		Participants    []any  `json:"Participants"`
		Transcript      []any  `json:"Transcript"`
		Characteristics struct {
			Total int `json:"TotalConversationDurationMillis"`
		} `json:"ConversationCharacteristics"`
	}
	require.NoError(t, json.Unmarshal(env.getOut(t, "calls/ca.json"), &caDoc, jsonx.V1()))
	assert.Equal(t, "ca", caDoc.JobName)
	assert.Len(t, caDoc.Participants, 2)
	assert.Empty(t, caDoc.Transcript)
	assert.Equal(t, 200, caDoc.Characteristics.Total)

	managed, err := env.tr.GetCallAnalyticsJob(t.Context(), &transcribesdk.GetCallAnalyticsJobInput{
		CallAnalyticsJobName: aws.String("ca-managed"),
	})
	require.NoError(t, err)

	code, body := env.fetch(t, aws.ToString(managed.CallAnalyticsJob.Transcript.TranscriptFileUri))
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, string(body), `"JobName":"ca-managed"`)
}

func TestJobOutput_MissingMediaFailsMedicalAndCallAnalytics(t *testing.T) {
	t.Parallel()

	env := newJobEnv(t, nil)
	media := &sdktypes.Media{MediaFileUri: aws.String("s3://media-in/nope.wav")}

	_, err := env.tr.StartMedicalTranscriptionJob(t.Context(), &transcribesdk.StartMedicalTranscriptionJobInput{
		MedicalTranscriptionJobName: aws.String("med"), LanguageCode: sdktypes.LanguageCodeEnUs, Media: media,
		OutputBucketName: aws.String(
			"media-out",
		), Specialty: sdktypes.SpecialtyPrimarycare, Type: sdktypes.TypeDictation,
	})
	require.NoError(t, err)

	_, err = env.tr.StartCallAnalyticsJob(t.Context(), &transcribesdk.StartCallAnalyticsJobInput{
		CallAnalyticsJobName: aws.String("ca"), Media: media,
	})
	require.NoError(t, err)
	env.clock.Advance(jobDelay)

	med, err := env.tr.GetMedicalTranscriptionJob(t.Context(), &transcribesdk.GetMedicalTranscriptionJobInput{
		MedicalTranscriptionJobName: aws.String("med"),
	})
	require.NoError(t, err)
	assert.Equal(t, sdktypes.TranscriptionJobStatusFailed, med.MedicalTranscriptionJob.TranscriptionJobStatus)
	assert.NotEmpty(t, aws.ToString(med.MedicalTranscriptionJob.FailureReason))

	ca, err := env.tr.GetCallAnalyticsJob(t.Context(), &transcribesdk.GetCallAnalyticsJobInput{
		CallAnalyticsJobName: aws.String("ca"),
	})
	require.NoError(t, err)
	assert.Equal(t, sdktypes.CallAnalyticsJobStatusFailed, ca.CallAnalyticsJob.CallAnalyticsJobStatus)
	assert.NotEmpty(t, aws.ToString(ca.CallAnalyticsJob.FailureReason))
}

func TestJobOutput_ManagedOutputSurvivesRestore(t *testing.T) {
	t.Parallel()

	env := newJobEnv(t, nil)
	env.put(t, "audio.wav", wavFile(1, 0.2))

	_, err := env.tr.StartTranscriptionJob(t.Context(), &transcribesdk.StartTranscriptionJobInput{
		TranscriptionJobName: aws.String("job"), LanguageCode: sdktypes.LanguageCodeEnUs,
		Media:     &sdktypes.Media{MediaFileUri: aws.String("s3://media-in/audio.wav")},
		Subtitles: &sdktypes.Subtitles{Formats: []sdktypes.SubtitleFormat{"srt"}},
	})
	require.NoError(t, err)
	env.clock.Advance(jobDelay)

	job := env.job(t, "job")
	require.Equal(t, sdktypes.TranscriptionJobStatusCompleted, job.TranscriptionJobStatus)

	uri := aws.ToString(job.Transcript.TranscriptFileUri)
	srtURI := job.Subtitles.SubtitleFileUris[0]
	assert.True(t, strings.HasSuffix(srtURI, "/job.srt"), srtURI)

	restored := transcribe.NewInMemoryBackend()
	require.NoError(t, restored.Restore(t.Context(), env.backend.Snapshot(t.Context())))

	parts := strings.Split(strings.TrimPrefix(uri, env.url+"/_gopherstack/transcribe/output/"), "/")
	require.Len(t, parts, 5)

	data, ok := restored.ManagedOutput(parts[1], parts[2], parts[3], parts[4])
	require.True(t, ok)
	assert.Equal(t, "COMPLETED", parseDoc(t, data).Status)

	_, ok = restored.ManagedOutput(parts[1], parts[2], "wrong", parts[4])
	assert.False(t, ok)

	_, body := env.fetch(t, srtURI)
	assert.Empty(t, body)

	code, _ := env.fetch(t, env.url+"/_gopherstack/transcribe/output/us-east-1/transcription/nope/tok/asrOutput.json")
	assert.Equal(t, http.StatusForbidden, code)
}
