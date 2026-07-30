package transcribe

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// --- GetMedicalScribeJob ---

type getMedicalScribeJobInput struct {
	MedicalScribeJobName string `json:"MedicalScribeJobName"`
}

// medicalScribeOutputLocations holds the S3 URIs for a completed Medical Scribe
// job's results, mirroring the real MedicalScribeOutput shape
// (ClinicalDocumentUri/TranscriptFileUri).
type medicalScribeOutputLocations struct {
	ClinicalDocumentURI string `json:"ClinicalDocumentUri"`
	TranscriptFileURI   string `json:"TranscriptFileUri"`
}

type medicalScribeJobOutput struct {
	Settings                       *MedicalScribeSettings           `json:"Settings,omitempty"`
	Media                          *Media                           `json:"Media,omitempty"`
	ClinicalNoteGenerationSettings *ClinicalNoteGenerationSettings  `json:"ClinicalNoteGenerationSettings,omitempty"`
	MedicalScribeOutput            *medicalScribeOutputLocations    `json:"MedicalScribeOutput,omitempty"`
	Tags                           []transcribeTag                  `json:"Tags,omitempty"`
	CreationTime                   *float64                         `json:"CreationTime,omitempty"`
	StartTime                      *float64                         `json:"StartTime,omitempty"`
	CompletionTime                 *float64                         `json:"CompletionTime,omitempty"`
	MedicalScribeJobName           string                           `json:"MedicalScribeJobName"`
	MedicalScribeJobStatus         string                           `json:"MedicalScribeJobStatus"`
	LanguageCode                   string                           `json:"LanguageCode,omitempty"`
	DataAccessRoleArn              string                           `json:"DataAccessRoleArn,omitempty"`
	FailureReason                  string                           `json:"FailureReason,omitempty"`
	ChannelDefinitions             []MedicalScribeChannelDefinition `json:"ChannelDefinitions,omitempty"`
}

// buildMedicalScribeOutputLocations synthesizes the S3 result locations for a
// completed Medical Scribe job, matching the pattern used for other job
// families' synthetic Transcript output.
func buildMedicalScribeOutputLocations(job *MedicalScribeJob) *medicalScribeOutputLocations {
	if job.MedicalScribeJobStatus != jobStatusCompleted {
		return nil
	}

	bucket := job.OutputBucketName
	if bucket == "" {
		bucket = "synthetic-transcripts"
	}

	return &medicalScribeOutputLocations{
		ClinicalDocumentURI: "s3://" + bucket + "/" + job.MedicalScribeJobName + "-clinical-document.json",
		TranscriptFileURI:   "s3://" + bucket + "/" + job.MedicalScribeJobName + "-transcript.json",
	}
}

func buildMedicalScribeJobOutput(job *MedicalScribeJob) *medicalScribeJobOutput {
	out := &medicalScribeJobOutput{
		MedicalScribeJobName:           job.MedicalScribeJobName,
		MedicalScribeJobStatus:         job.MedicalScribeJobStatus,
		LanguageCode:                   job.LanguageCode,
		DataAccessRoleArn:              job.DataAccessRoleArn,
		FailureReason:                  job.FailureReason,
		Settings:                       job.Settings,
		ChannelDefinitions:             job.ChannelDefinitions,
		ClinicalNoteGenerationSettings: job.ClinicalNoteGenerationSettings,
		Tags:                           tagsFromMap(job.Tags),
		MedicalScribeOutput:            buildMedicalScribeOutputLocations(job),
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

type getMedicalScribeJobOutput struct {
	MedicalScribeJob *medicalScribeJobOutput `json:"MedicalScribeJob"`
}

func (h *Handler) handleGetMedicalScribeJob(
	_ context.Context,
	in *getMedicalScribeJobInput,
) (*getMedicalScribeJobOutput, error) {
	job, err := h.Backend.GetMedicalScribeJob(in.MedicalScribeJobName)
	if err != nil {
		return nil, err
	}

	return &getMedicalScribeJobOutput{
		MedicalScribeJob: buildMedicalScribeJobOutput(job),
	}, nil
}

// --- StartMedicalScribeJob ---

type startMedicalScribeJobInput struct {
	Settings                       *MedicalScribeSettings           `json:"Settings"`
	Media                          Media                            `json:"Media"`
	ClinicalNoteGenerationSettings *ClinicalNoteGenerationSettings  `json:"ClinicalNoteGenerationSettings"`
	Tags                           []transcribeTag                  `json:"Tags"`
	MedicalScribeJobName           string                           `json:"MedicalScribeJobName"`
	DataAccessRoleArn              string                           `json:"DataAccessRoleArn"`
	OutputBucketName               string                           `json:"OutputBucketName"`
	ChannelDefinitions             []MedicalScribeChannelDefinition `json:"ChannelDefinitions"`
}

type startMedicalScribeJobOutput struct {
	MedicalScribeJob *medicalScribeJobOutput `json:"MedicalScribeJob"`
}

func (h *Handler) handleStartMedicalScribeJob(
	_ context.Context,
	in *startMedicalScribeJobInput,
) (*startMedicalScribeJobOutput, error) {
	job, err := h.Backend.StartMedicalScribeJob(&MedicalScribeJob{
		MedicalScribeJobName:           in.MedicalScribeJobName,
		Media:                          in.Media,
		DataAccessRoleArn:              in.DataAccessRoleArn,
		OutputBucketName:               in.OutputBucketName,
		ChannelDefinitions:             in.ChannelDefinitions,
		Settings:                       in.Settings,
		ClinicalNoteGenerationSettings: in.ClinicalNoteGenerationSettings,
		Tags:                           tagsToMap(in.Tags),
	})
	if err != nil {
		return nil, err
	}

	return &startMedicalScribeJobOutput{
		MedicalScribeJob: buildMedicalScribeJobOutput(job),
	}, nil
}

// --- ListMedicalScribeJobs ---

type listMedicalScribeJobsInput struct {
	Status          string `json:"Status"`
	JobNameContains string `json:"JobNameContains"`
	NextToken       string `json:"NextToken"`
	MaxResults      int32  `json:"MaxResults"`
}

// medicalScribeJobSummary mirrors the real MedicalScribeJobSummary shape, which is
// a strict subset of the full MedicalScribeJob fields (no Settings/Media/Tags/etc).
type medicalScribeJobSummary struct {
	CompletionTime         *float64 `json:"CompletionTime,omitempty"`
	CreationTime           *float64 `json:"CreationTime,omitempty"`
	StartTime              *float64 `json:"StartTime,omitempty"`
	MedicalScribeJobName   string   `json:"MedicalScribeJobName"`
	MedicalScribeJobStatus string   `json:"MedicalScribeJobStatus"`
	LanguageCode           string   `json:"LanguageCode,omitempty"`
	FailureReason          string   `json:"FailureReason,omitempty"`
}

func buildMedicalScribeJobSummary(job *MedicalScribeJob) medicalScribeJobSummary {
	s := medicalScribeJobSummary{
		MedicalScribeJobName:   job.MedicalScribeJobName,
		MedicalScribeJobStatus: job.MedicalScribeJobStatus,
		LanguageCode:           job.LanguageCode,
		FailureReason:          job.FailureReason,
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

type listMedicalScribeJobsOutput struct {
	NextToken                 string                    `json:"NextToken,omitempty"`
	MedicalScribeJobSummaries []medicalScribeJobSummary `json:"MedicalScribeJobSummaries"`
}

func (h *Handler) handleListMedicalScribeJobs(
	_ context.Context,
	in *listMedicalScribeJobsInput,
) (*listMedicalScribeJobsOutput, error) {
	jobs, nextToken := h.Backend.ListMedicalScribeJobs(in.Status, in.JobNameContains, in.NextToken, in.MaxResults)

	summaries := make([]medicalScribeJobSummary, 0, len(jobs))
	for i := range jobs {
		summaries = append(summaries, buildMedicalScribeJobSummary(&jobs[i]))
	}

	return &listMedicalScribeJobsOutput{
		MedicalScribeJobSummaries: summaries,
		NextToken:                 nextToken,
	}, nil
}

// --- DeleteMedicalScribeJob ---

type deleteMedicalScribeJobInput struct {
	MedicalScribeJobName string `json:"MedicalScribeJobName"`
}

func (h *Handler) handleDeleteMedicalScribeJob(
	_ context.Context,
	in *deleteMedicalScribeJobInput,
) (*struct{}, error) {
	if err := h.Backend.DeleteMedicalScribeJob(in.MedicalScribeJobName); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}
