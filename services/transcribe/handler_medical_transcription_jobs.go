package transcribe

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// --- GetMedicalTranscriptionJob ---

type getMedicalTranscriptionJobInput struct {
	MedicalTranscriptionJobName string `json:"MedicalTranscriptionJobName"`
}

type medicalTranscriptionJobOutput struct {
	CompletionTime                   *float64                      `json:"CompletionTime,omitempty"`
	Media                            *Media                        `json:"Media,omitempty"`
	Transcript                       *transcriptOutput             `json:"Transcript,omitempty"`
	Settings                         *MedicalTranscriptionSettings `json:"Settings,omitempty"`
	CreationTime                     *float64                      `json:"CreationTime,omitempty"`
	StartTime                        *float64                      `json:"StartTime,omitempty"`
	TranscriptionJobStatus           string                        `json:"TranscriptionJobStatus"`
	MedicalTranscriptionJobName      string                        `json:"MedicalTranscriptionJobName"`
	LanguageCode                     string                        `json:"LanguageCode,omitempty"`
	Specialty                        string                        `json:"Specialty,omitempty"`
	Type                             string                        `json:"Type,omitempty"`
	MediaFormat                      string                        `json:"MediaFormat,omitempty"`
	MedicalContentIdentificationType string                        `json:"MedicalContentIdentificationType,omitempty"`
	FailureReason                    string                        `json:"FailureReason,omitempty"`
	Tags                             []transcribeTag               `json:"Tags,omitempty"`
	MediaSampleRateHertz             int32                         `json:"MediaSampleRateHertz,omitempty"`
}

func buildMedicalTranscriptURI(job *MedicalTranscriptionJob) string {
	if job.OutputBucketName != "" {
		key := job.OutputKey
		if key == "" {
			key = job.MedicalTranscriptionJobName + ".json"
		}

		return "s3://" + job.OutputBucketName + "/" + key
	}

	return "s3://synthetic-transcripts/" + job.MedicalTranscriptionJobName + ".json"
}

func buildMedicalTranscriptionJobOutput(job *MedicalTranscriptionJob) *medicalTranscriptionJobOutput {
	out := &medicalTranscriptionJobOutput{
		MedicalTranscriptionJobName:      job.MedicalTranscriptionJobName,
		TranscriptionJobStatus:           job.TranscriptionJobStatus,
		LanguageCode:                     job.LanguageCode,
		Specialty:                        job.Specialty,
		Type:                             job.Type,
		MediaFormat:                      job.MediaFormat,
		MedicalContentIdentificationType: job.MedicalContentIdentificationType,
		FailureReason:                    job.FailureReason,
		MediaSampleRateHertz:             job.MediaSampleRateHertz,
		Settings:                         job.Settings,
		Tags:                             tagsFromMap(job.Tags),
		Transcript:                       &transcriptOutput{TranscriptFileURI: buildMedicalTranscriptURI(job)},
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
	if job.Media.MediaFileURI != "" {
		m := job.Media
		out.Media = &m
	}

	return out
}

type getMedicalTranscriptionJobOutput struct {
	MedicalTranscriptionJob *medicalTranscriptionJobOutput `json:"MedicalTranscriptionJob"`
}

func (h *Handler) handleGetMedicalTranscriptionJob(
	_ context.Context,
	in *getMedicalTranscriptionJobInput,
) (*getMedicalTranscriptionJobOutput, error) {
	job, err := h.Backend.GetMedicalTranscriptionJob(in.MedicalTranscriptionJobName)
	if err != nil {
		return nil, err
	}

	return &getMedicalTranscriptionJobOutput{
		MedicalTranscriptionJob: buildMedicalTranscriptionJobOutput(job),
	}, nil
}

// --- StartMedicalTranscriptionJob ---

type startMedicalTranscriptionJobInput struct {
	Settings                         *MedicalTranscriptionSettings `json:"Settings"`
	Media                            Media                         `json:"Media"`
	MedicalTranscriptionJobName      string                        `json:"MedicalTranscriptionJobName"`
	LanguageCode                     string                        `json:"LanguageCode"`
	Specialty                        string                        `json:"Specialty"`
	Type                             string                        `json:"Type"`
	MediaFormat                      string                        `json:"MediaFormat"`
	OutputBucketName                 string                        `json:"OutputBucketName"`
	OutputKey                        string                        `json:"OutputKey"`
	MedicalContentIdentificationType string                        `json:"MedicalContentIdentificationType"`
	Tags                             []transcribeTag               `json:"Tags"`
	MediaSampleRateHertz             int32                         `json:"MediaSampleRateHertz"`
}

type startMedicalTranscriptionJobOutput struct {
	MedicalTranscriptionJob *medicalTranscriptionJobOutput `json:"MedicalTranscriptionJob"`
}

func (h *Handler) handleStartMedicalTranscriptionJob(
	_ context.Context,
	in *startMedicalTranscriptionJobInput,
) (*startMedicalTranscriptionJobOutput, error) {
	job, err := h.Backend.StartMedicalTranscriptionJob(&MedicalTranscriptionJob{
		MedicalTranscriptionJobName:      in.MedicalTranscriptionJobName,
		LanguageCode:                     in.LanguageCode,
		Media:                            in.Media,
		Specialty:                        in.Specialty,
		Type:                             in.Type,
		MediaFormat:                      in.MediaFormat,
		MediaSampleRateHertz:             in.MediaSampleRateHertz,
		OutputBucketName:                 in.OutputBucketName,
		OutputKey:                        in.OutputKey,
		Settings:                         in.Settings,
		MedicalContentIdentificationType: in.MedicalContentIdentificationType,
		Tags:                             tagsToMap(in.Tags),
	})
	if err != nil {
		return nil, err
	}

	return &startMedicalTranscriptionJobOutput{
		MedicalTranscriptionJob: buildMedicalTranscriptionJobOutput(job),
	}, nil
}

// --- ListMedicalTranscriptionJobs ---

type listMedicalTranscriptionJobsInput struct {
	Status          string `json:"Status"`
	JobNameContains string `json:"JobNameContains"`
	NextToken       string `json:"NextToken"`
	MaxResults      int32  `json:"MaxResults"`
}

// medicalTranscriptionJobSummary mirrors the real MedicalTranscriptionJobSummary
// shape, a strict subset of the full MedicalTranscriptionJob fields.
type medicalTranscriptionJobSummary struct {
	CompletionTime              *float64 `json:"CompletionTime,omitempty"`
	CreationTime                *float64 `json:"CreationTime,omitempty"`
	StartTime                   *float64 `json:"StartTime,omitempty"`
	MedicalTranscriptionJobName string   `json:"MedicalTranscriptionJobName"`
	TranscriptionJobStatus      string   `json:"TranscriptionJobStatus"`
	LanguageCode                string   `json:"LanguageCode,omitempty"`
	FailureReason               string   `json:"FailureReason,omitempty"`
	OutputLocationType          string   `json:"OutputLocationType,omitempty"`
	Specialty                   string   `json:"Specialty,omitempty"`
	Type                        string   `json:"Type,omitempty"`
	ContentIdentificationType   string   `json:"ContentIdentificationType,omitempty"`
}

func buildMedicalTranscriptionJobSummary(job *MedicalTranscriptionJob) medicalTranscriptionJobSummary {
	s := medicalTranscriptionJobSummary{
		MedicalTranscriptionJobName: job.MedicalTranscriptionJobName,
		TranscriptionJobStatus:      job.TranscriptionJobStatus,
		LanguageCode:                job.LanguageCode,
		FailureReason:               job.FailureReason,
		OutputLocationType:          outputLocationType(job.OutputBucketName),
		Specialty:                   job.Specialty,
		Type:                        job.Type,
		ContentIdentificationType:   job.MedicalContentIdentificationType,
	}
	if !job.CreationTime.IsZero() {
		t := awstime.Epoch(job.CreationTime)
		s.CreationTime = &t
	}
	if !job.StartTime.IsZero() {
		t := awstime.Epoch(job.StartTime)
		s.StartTime = &t
	}
	if !job.CompletionTime.IsZero() {
		t := awstime.Epoch(job.CompletionTime)
		s.CompletionTime = &t
	}

	return s
}

type listMedicalTranscriptionJobsOutput struct {
	NextToken                        string                           `json:"NextToken,omitempty"`
	MedicalTranscriptionJobSummaries []medicalTranscriptionJobSummary `json:"MedicalTranscriptionJobSummaries"`
}

func (h *Handler) handleListMedicalTranscriptionJobs(
	_ context.Context,
	in *listMedicalTranscriptionJobsInput,
) (*listMedicalTranscriptionJobsOutput, error) {
	jobs, nextToken := h.Backend.ListMedicalTranscriptionJobs(
		in.Status, in.JobNameContains, in.NextToken, in.MaxResults,
	)

	summaries := make([]medicalTranscriptionJobSummary, 0, len(jobs))
	for i := range jobs {
		summaries = append(summaries, buildMedicalTranscriptionJobSummary(&jobs[i]))
	}

	return &listMedicalTranscriptionJobsOutput{
		MedicalTranscriptionJobSummaries: summaries,
		NextToken:                        nextToken,
	}, nil
}

// --- DeleteMedicalTranscriptionJob ---

type deleteMedicalTranscriptionJobInput struct {
	MedicalTranscriptionJobName string `json:"MedicalTranscriptionJobName"`
}

func (h *Handler) handleDeleteMedicalTranscriptionJob(
	_ context.Context,
	in *deleteMedicalTranscriptionJobInput,
) (*struct{}, error) {
	if err := h.Backend.DeleteMedicalTranscriptionJob(in.MedicalTranscriptionJobName); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}
