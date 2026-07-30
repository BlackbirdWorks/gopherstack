package transcribe

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

type transcriptionJobNameInput struct {
	TranscriptionJobName string `json:"TranscriptionJobName"`
}

type transcriptionJobOutput struct {
	Tags                      []transcribeTag               `json:"Tags,omitempty"`
	Settings                  *TranscriptionSettings        `json:"Settings,omitempty"`
	ModelSettings             *ModelSettings                `json:"ModelSettings,omitempty"`
	JobExecutionSettings      *JobExecutionSettings         `json:"JobExecutionSettings,omitempty"`
	ContentRedaction          *ContentRedaction             `json:"ContentRedaction,omitempty"`
	Subtitles                 *SubtitlesOutput              `json:"Subtitles,omitempty"`
	Media                     *Media                        `json:"Media,omitempty"`
	LanguageIDSettings        map[string]LanguageIDSettings `json:"LanguageIdSettings,omitempty"`
	Transcript                transcriptOutput              `json:"Transcript"`
	CreationTime              *float64                      `json:"CreationTime,omitempty"`
	StartTime                 *float64                      `json:"StartTime,omitempty"`
	CompletionTime            *float64                      `json:"CompletionTime,omitempty"`
	TranscriptionJobName      string                        `json:"TranscriptionJobName"`
	TranscriptionJobStatus    string                        `json:"TranscriptionJobStatus"`
	LanguageCode              string                        `json:"LanguageCode,omitempty"`
	MediaFormat               string                        `json:"MediaFormat,omitempty"`
	FailureReason             string                        `json:"FailureReason,omitempty"`
	LanguageOptions           []string                      `json:"LanguageOptions,omitempty"`
	LanguageCodes             []LanguageCodeItem            `json:"LanguageCodes,omitempty"`
	ToxicityDetection         []ToxicityDetectionSettings   `json:"ToxicityDetection,omitempty"`
	IdentifiedLanguageScore   float32                       `json:"IdentifiedLanguageScore,omitempty"`
	MediaSampleRateHertz      int32                         `json:"MediaSampleRateHertz,omitempty"`
	IdentifyLanguage          bool                          `json:"IdentifyLanguage,omitempty"`
	IdentifyMultipleLanguages bool                          `json:"IdentifyMultipleLanguages,omitempty"`
}

type startTranscriptionJobOutput struct {
	TranscriptionJob transcriptionJobOutput `json:"TranscriptionJob"`
}

type getTranscriptionJobOutput struct {
	TranscriptionJob transcriptionJobOutput `json:"TranscriptionJob"`
}

type handleStartTranscriptionJobInput struct {
	Settings                  *TranscriptionSettings        `json:"Settings"`
	Tags                      []transcribeTag               `json:"Tags"`
	Subtitles                 *SubtitlesInput               `json:"Subtitles"`
	ContentRedaction          *ContentRedaction             `json:"ContentRedaction"`
	ModelSettings             *ModelSettings                `json:"ModelSettings"`
	JobExecutionSettings      *JobExecutionSettings         `json:"JobExecutionSettings"`
	LanguageIDSettings        map[string]LanguageIDSettings `json:"LanguageIdSettings"`
	Media                     Media                         `json:"Media"`
	MediaFormat               string                        `json:"MediaFormat"`
	OutputEncryptionKMSKeyID  string                        `json:"OutputEncryptionKMSKeyId"`
	OutputKey                 string                        `json:"OutputKey"`
	OutputBucketName          string                        `json:"OutputBucketName"`
	TranscriptionJobName      string                        `json:"TranscriptionJobName"`
	LanguageCode              string                        `json:"LanguageCode"`
	LanguageOptions           []string                      `json:"LanguageOptions"`
	ToxicityDetection         []ToxicityDetectionSettings   `json:"ToxicityDetection"`
	MediaSampleRateHertz      int32                         `json:"MediaSampleRateHertz"`
	IdentifyMultipleLanguages bool                          `json:"IdentifyMultipleLanguages"`
	IdentifyLanguage          bool                          `json:"IdentifyLanguage"`
}

func (h *Handler) handleStartTranscriptionJob(
	_ context.Context,
	in *handleStartTranscriptionJobInput,
) (*startTranscriptionJobOutput, error) {
	subtitlesOut := (*SubtitlesOutput)(nil)
	if in.Subtitles != nil {
		subtitlesOut = &SubtitlesOutput{Formats: in.Subtitles.Formats, OutputStartIndex: in.Subtitles.OutputStartIndex}
	}

	job, err := h.Backend.StartTranscriptionJob(&TranscriptionJob{
		JobName:                   in.TranscriptionJobName,
		LanguageCode:              in.LanguageCode,
		Media:                     in.Media,
		MediaFormat:               in.MediaFormat,
		MediaSampleRateHertz:      in.MediaSampleRateHertz,
		OutputBucketName:          in.OutputBucketName,
		OutputKey:                 in.OutputKey,
		OutputEncryptionKMSKeyID:  in.OutputEncryptionKMSKeyID,
		Settings:                  in.Settings,
		ModelSettings:             in.ModelSettings,
		JobExecutionSettings:      in.JobExecutionSettings,
		ContentRedaction:          in.ContentRedaction,
		Subtitles:                 subtitlesOut,
		IdentifyLanguage:          in.IdentifyLanguage,
		IdentifyMultipleLanguages: in.IdentifyMultipleLanguages,
		LanguageOptions:           in.LanguageOptions,
		LanguageIDSettings:        in.LanguageIDSettings,
		ToxicityDetection:         in.ToxicityDetection,
		Tags:                      tagsToMap(in.Tags),
	})
	if err != nil {
		return nil, err
	}

	transcriptURI := buildTranscriptURI(job)

	return &startTranscriptionJobOutput{
		TranscriptionJob: buildTranscriptionJobOutput(job, transcriptURI),
	}, nil
}

func (h *Handler) handleGetTranscriptionJob(
	_ context.Context,
	in *transcriptionJobNameInput,
) (*getTranscriptionJobOutput, error) {
	job, err := h.Backend.GetTranscriptionJob(in.TranscriptionJobName)
	if err != nil {
		return nil, err
	}

	transcriptURI := buildTranscriptURI(job)

	return &getTranscriptionJobOutput{
		TranscriptionJob: buildTranscriptionJobOutput(job, transcriptURI),
	}, nil
}

func buildTranscriptURI(job *TranscriptionJob) string {
	if job.OutputBucketName != "" {
		key := job.OutputKey
		if key == "" {
			key = job.JobName + ".json"
		}

		return "s3://" + job.OutputBucketName + "/" + key
	}

	return "s3://synthetic-transcripts/" + job.JobName + ".json"
}

func buildTranscriptionJobOutput(job *TranscriptionJob, transcriptURI string) transcriptionJobOutput {
	out := transcriptionJobOutput{
		TranscriptionJobName:      job.JobName,
		TranscriptionJobStatus:    job.JobStatus,
		LanguageCode:              job.LanguageCode,
		MediaFormat:               job.MediaFormat,
		MediaSampleRateHertz:      job.MediaSampleRateHertz,
		FailureReason:             job.FailureReason,
		Settings:                  job.Settings,
		ModelSettings:             job.ModelSettings,
		JobExecutionSettings:      job.JobExecutionSettings,
		ContentRedaction:          job.ContentRedaction,
		Subtitles:                 job.Subtitles,
		IdentifyLanguage:          job.IdentifyLanguage,
		IdentifyMultipleLanguages: job.IdentifyMultipleLanguages,
		LanguageOptions:           job.LanguageOptions,
		LanguageCodes:             job.LanguageCodes,
		LanguageIDSettings:        job.LanguageIDSettings,
		ToxicityDetection:         job.ToxicityDetection,
		IdentifiedLanguageScore:   job.IdentifiedLanguageScore,
		Tags:                      tagsFromMap(job.Tags),
		Transcript: transcriptOutput{
			TranscriptFileURI: transcriptURI,
		},
	}

	if job.Media.MediaFileURI != "" {
		m := job.Media
		out.Media = &m
	}

	if job.ContentRedaction != nil {
		redacted := "s3://synthetic-transcripts/" + job.JobName + "-redacted.json"
		out.Transcript.RedactedTranscriptFileURI = &redacted
	}

	if !job.CreationTime.IsZero() {
		s := awstime.Epoch(job.CreationTime)
		out.CreationTime = &s
	}

	if !job.StartTime.IsZero() {
		s := awstime.Epoch(job.StartTime)
		out.StartTime = &s
	}

	if !job.CompletionTime.IsZero() {
		s := awstime.Epoch(job.CompletionTime)
		out.CompletionTime = &s
	}

	return out
}

type transcriptionJobSummary struct {
	ContentRedaction          *ContentRedaction           `json:"ContentRedaction,omitempty"`
	ModelSettings             *ModelSettings              `json:"ModelSettings,omitempty"`
	CreationTime              *float64                    `json:"CreationTime,omitempty"`
	CompletionTime            *float64                    `json:"CompletionTime,omitempty"`
	StartTime                 *float64                    `json:"StartTime,omitempty"`
	LanguageCode              string                      `json:"LanguageCode,omitempty"`
	TranscriptionJobName      string                      `json:"TranscriptionJobName"`
	TranscriptionJobStatus    string                      `json:"TranscriptionJobStatus"`
	FailureReason             string                      `json:"FailureReason,omitempty"`
	OutputLocationType        string                      `json:"OutputLocationType,omitempty"`
	ToxicityDetection         []ToxicityDetectionSettings `json:"ToxicityDetection,omitempty"`
	LanguageCodes             []LanguageCodeItem          `json:"LanguageCodes,omitempty"`
	IdentifiedLanguageScore   float32                     `json:"IdentifiedLanguageScore,omitempty"`
	IdentifyLanguage          bool                        `json:"IdentifyLanguage,omitempty"`
	IdentifyMultipleLanguages bool                        `json:"IdentifyMultipleLanguages,omitempty"`
}

type listTranscriptionJobsOutput struct {
	NextToken                 string                    `json:"NextToken,omitempty"`
	TranscriptionJobSummaries []transcriptionJobSummary `json:"TranscriptionJobSummaries"`
}

type handleListTranscriptionJobsInput struct {
	Status          string `json:"Status"`
	JobNameContains string `json:"JobNameContains"`
	NextToken       string `json:"NextToken"`
	MaxResults      int32  `json:"MaxResults"`
}

func (h *Handler) handleListTranscriptionJobs(
	_ context.Context,
	in *handleListTranscriptionJobsInput,
) (*listTranscriptionJobsOutput, error) {
	jobs, nextToken := h.Backend.ListTranscriptionJobs(in.Status, in.JobNameContains, in.NextToken, in.MaxResults)

	summaries := make([]transcriptionJobSummary, 0, len(jobs))
	for _, j := range jobs {
		s := transcriptionJobSummary{
			TranscriptionJobName:      j.JobName,
			TranscriptionJobStatus:    j.JobStatus,
			LanguageCode:              j.LanguageCode,
			FailureReason:             j.FailureReason,
			ContentRedaction:          j.ContentRedaction,
			ModelSettings:             j.ModelSettings,
			LanguageCodes:             j.LanguageCodes,
			ToxicityDetection:         j.ToxicityDetection,
			IdentifiedLanguageScore:   j.IdentifiedLanguageScore,
			IdentifyLanguage:          j.IdentifyLanguage,
			IdentifyMultipleLanguages: j.IdentifyMultipleLanguages,
			OutputLocationType:        outputLocationType(j.OutputBucketName),
		}

		if !j.CreationTime.IsZero() {
			ts := awstime.Epoch(j.CreationTime)
			s.CreationTime = &ts
		}

		if !j.StartTime.IsZero() {
			ts := awstime.Epoch(j.StartTime)
			s.StartTime = &ts
		}

		if !j.CompletionTime.IsZero() {
			ts := awstime.Epoch(j.CompletionTime)
			s.CompletionTime = &ts
		}

		summaries = append(summaries, s)
	}

	return &listTranscriptionJobsOutput{
		TranscriptionJobSummaries: summaries,
		NextToken:                 nextToken,
	}, nil
}

func (h *Handler) handleDeleteTranscriptionJob(
	_ context.Context,
	in *transcriptionJobNameInput,
) (*struct{}, error) {
	if err := h.Backend.DeleteTranscriptionJob(in.TranscriptionJobName); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}
