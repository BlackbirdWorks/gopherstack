package transcribe

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// --- GetMedicalScribeJob ---

type getMedicalScribeJobInput struct {
	MedicalScribeJobName string `json:"MedicalScribeJobName"`
}

type medicalScribeJobOutput struct {
	Settings                       *MedicalScribeSettings           `json:"Settings,omitempty"`
	Media                          *Media                           `json:"Media,omitempty"`
	ClinicalNoteGenerationSettings *ClinicalNoteGenerationSettings  `json:"ClinicalNoteGenerationSettings,omitempty"`
	Tags                           []transcribeTag                  `json:"Tags,omitempty"`
	CreationTime                   *float64                         `json:"CreationTime,omitempty"`
	StartTime                      *float64                         `json:"StartTime,omitempty"`
	CompletionTime                 *float64                         `json:"CompletionTime,omitempty"`
	MedicalScribeJobName           string                           `json:"MedicalScribeJobName"`
	MedicalScribeJobStatus         string                           `json:"MedicalScribeJobStatus"`
	LanguageCode                   string                           `json:"LanguageCode,omitempty"`
	DataAccessRoleArn              string                           `json:"DataAccessRoleArn,omitempty"`
	OutputBucketName               string                           `json:"OutputBucketName,omitempty"`
	FailureReason                  string                           `json:"FailureReason,omitempty"`
	ChannelDefinitions             []MedicalScribeChannelDefinition `json:"ChannelDefinitions,omitempty"`
}

func buildMedicalScribeJobOutput(job *MedicalScribeJob) *medicalScribeJobOutput {
	out := &medicalScribeJobOutput{
		MedicalScribeJobName:           job.MedicalScribeJobName,
		MedicalScribeJobStatus:         job.MedicalScribeJobStatus,
		LanguageCode:                   job.LanguageCode,
		DataAccessRoleArn:              job.DataAccessRoleArn,
		OutputBucketName:               job.OutputBucketName,
		FailureReason:                  job.FailureReason,
		Settings:                       job.Settings,
		ChannelDefinitions:             job.ChannelDefinitions,
		ClinicalNoteGenerationSettings: job.ClinicalNoteGenerationSettings,
		Tags:                           tagsFromMap(job.Tags),
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
	Status    string `json:"Status"`
	NextToken string `json:"NextToken"`
}

type listMedicalScribeJobsOutput struct {
	NextToken                 string                   `json:"NextToken,omitempty"`
	MedicalScribeJobSummaries []medicalScribeJobOutput `json:"MedicalScribeJobSummaries"`
}

func (h *Handler) handleListMedicalScribeJobs(
	_ context.Context,
	in *listMedicalScribeJobsInput,
) (*listMedicalScribeJobsOutput, error) {
	jobs, nextToken := h.Backend.ListMedicalScribeJobs(in.Status, in.NextToken)

	summaries := make([]medicalScribeJobOutput, 0, len(jobs))
	for i := range jobs {
		summaries = append(summaries, *buildMedicalScribeJobOutput(&jobs[i]))
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
