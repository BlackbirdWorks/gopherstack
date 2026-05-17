package transcribe

import (
	"context"
	"time"
)

// --- GetCallAnalyticsJob ---

type getCallAnalyticsJobInput struct {
	CallAnalyticsJobName string `json:"CallAnalyticsJobName"`
}

type callAnalyticsJobOutput struct {
	Tags                   map[string]string      `json:"Tags,omitempty"`
	Settings               *CallAnalyticsSettings `json:"Settings,omitempty"`
	Media                  *Media                 `json:"Media,omitempty"`
	CreationTime           *string                `json:"CreationTime,omitempty"`
	StartTime              *string                `json:"StartTime,omitempty"`
	CompletionTime         *string                `json:"CompletionTime,omitempty"`
	CallAnalyticsJobName   string                 `json:"CallAnalyticsJobName"`
	CallAnalyticsJobStatus string                 `json:"CallAnalyticsJobStatus"`
	LanguageCode           string                 `json:"LanguageCode,omitempty"`
	FailureReason          string                 `json:"FailureReason,omitempty"`
	DataAccessRoleArn      string                 `json:"DataAccessRoleArn,omitempty"`
	MediaFormat            string                 `json:"MediaFormat,omitempty"`
	ChannelDefinitions     []ChannelDefinition    `json:"ChannelDefinitions,omitempty"`
	MediaSampleRateHertz   int32                  `json:"MediaSampleRateHertz,omitempty"`
}

func buildCallAnalyticsJobOutput(job *CallAnalyticsJob) *callAnalyticsJobOutput {
	out := &callAnalyticsJobOutput{
		CallAnalyticsJobName:   job.CallAnalyticsJobName,
		CallAnalyticsJobStatus: job.CallAnalyticsJobStatus,
		LanguageCode:           job.LanguageCode,
		FailureReason:          job.FailureReason,
		DataAccessRoleArn:      job.DataAccessRoleArn,
		ChannelDefinitions:     job.ChannelDefinitions,
		Settings:               job.Settings,
		MediaFormat:            job.MediaFormat,
		MediaSampleRateHertz:   job.MediaSampleRateHertz,
		Tags:                   job.Tags,
	}
	if !job.CreationTime.IsZero() {
		s := job.CreationTime.Format(time.RFC3339)
		out.CreationTime = &s
	}
	if !job.StartTime.IsZero() {
		s := job.StartTime.Format(time.RFC3339)
		out.StartTime = &s
	}
	if !job.CompletionTime.IsZero() {
		s := job.CompletionTime.Format(time.RFC3339)
		out.CompletionTime = &s
	}
	if job.Media.MediaFileURI != "" || job.Media.RedactedMediaFileURI != "" {
		m := job.Media
		out.Media = &m
	}

	return out
}

type getCallAnalyticsJobOutput struct {
	CallAnalyticsJob *callAnalyticsJobOutput `json:"CallAnalyticsJob"`
}

func (h *Handler) handleGetCallAnalyticsJob(
	_ context.Context,
	in *getCallAnalyticsJobInput,
) (*getCallAnalyticsJobOutput, error) {
	job, err := h.Backend.GetCallAnalyticsJob(in.CallAnalyticsJobName)
	if err != nil {
		return nil, err
	}

	return &getCallAnalyticsJobOutput{
		CallAnalyticsJob: buildCallAnalyticsJobOutput(job),
	}, nil
}

// --- StartCallAnalyticsJob ---

type startCallAnalyticsJobInput struct {
	Settings             *CallAnalyticsSettings `json:"Settings"`
	Media                Media                  `json:"Media"`
	Tags                 map[string]string      `json:"Tags"`
	CallAnalyticsJobName string                 `json:"CallAnalyticsJobName"`
	LanguageCode         string                 `json:"LanguageCode"`
	DataAccessRoleArn    string                 `json:"DataAccessRoleArn"`
	ChannelDefinitions   []ChannelDefinition    `json:"ChannelDefinitions"`
}

type startCallAnalyticsJobOutput struct {
	CallAnalyticsJob *callAnalyticsJobOutput `json:"CallAnalyticsJob"`
}

func (h *Handler) handleStartCallAnalyticsJob(
	_ context.Context,
	in *startCallAnalyticsJobInput,
) (*startCallAnalyticsJobOutput, error) {
	job, err := h.Backend.StartCallAnalyticsJob(&CallAnalyticsJob{
		CallAnalyticsJobName: in.CallAnalyticsJobName,
		LanguageCode:         in.LanguageCode,
		Media:                in.Media,
		DataAccessRoleArn:    in.DataAccessRoleArn,
		ChannelDefinitions:   in.ChannelDefinitions,
		Settings:             in.Settings,
		Tags:                 in.Tags,
	})
	if err != nil {
		return nil, err
	}

	return &startCallAnalyticsJobOutput{
		CallAnalyticsJob: buildCallAnalyticsJobOutput(job),
	}, nil
}

// --- ListCallAnalyticsJobs ---

type listCallAnalyticsJobsInput struct {
	Status    string `json:"Status"`
	NextToken string `json:"NextToken"`
}

type callAnalyticsJobSummary struct {
	CreationTime           *string `json:"CreationTime,omitempty"`
	CompletionTime         *string `json:"CompletionTime,omitempty"`
	CallAnalyticsJobName   string  `json:"CallAnalyticsJobName"`
	CallAnalyticsJobStatus string  `json:"CallAnalyticsJobStatus"`
	LanguageCode           string  `json:"LanguageCode,omitempty"`
	FailureReason          string  `json:"FailureReason,omitempty"`
}

type listCallAnalyticsJobsOutput struct {
	NextToken                 string                    `json:"NextToken,omitempty"`
	CallAnalyticsJobSummaries []callAnalyticsJobSummary `json:"CallAnalyticsJobSummaries"`
}

func (h *Handler) handleListCallAnalyticsJobs(
	_ context.Context,
	in *listCallAnalyticsJobsInput,
) (*listCallAnalyticsJobsOutput, error) {
	jobs, nextToken := h.Backend.ListCallAnalyticsJobs(in.Status, in.NextToken)

	summaries := make([]callAnalyticsJobSummary, 0, len(jobs))
	for _, j := range jobs {
		s := callAnalyticsJobSummary{
			CallAnalyticsJobName:   j.CallAnalyticsJobName,
			CallAnalyticsJobStatus: j.CallAnalyticsJobStatus,
			LanguageCode:           j.LanguageCode,
			FailureReason:          j.FailureReason,
		}
		if !j.CreationTime.IsZero() {
			ts := j.CreationTime.Format(time.RFC3339)
			s.CreationTime = &ts
		}
		if !j.CompletionTime.IsZero() {
			ts := j.CompletionTime.Format(time.RFC3339)
			s.CompletionTime = &ts
		}
		summaries = append(summaries, s)
	}

	return &listCallAnalyticsJobsOutput{
		CallAnalyticsJobSummaries: summaries,
		NextToken:                 nextToken,
	}, nil
}

// --- GetCallAnalyticsCategory ---

type getCallAnalyticsCategoryInput struct {
	CategoryName string `json:"CategoryName"`
}

type getCallAnalyticsCategoryOutput struct {
	CategoryProperties *callAnalyticsCategoryProperties `json:"CategoryProperties"`
}

func (h *Handler) handleGetCallAnalyticsCategory(
	_ context.Context,
	in *getCallAnalyticsCategoryInput,
) (*getCallAnalyticsCategoryOutput, error) {
	cat, err := h.Backend.GetCallAnalyticsCategory(in.CategoryName)
	if err != nil {
		return nil, err
	}

	return &getCallAnalyticsCategoryOutput{
		CategoryProperties: &callAnalyticsCategoryProperties{
			CategoryName: cat.CategoryName,
			InputType:    cat.InputType,
		},
	}, nil
}

// --- UpdateCallAnalyticsCategory ---

type updateCallAnalyticsCategoryInput struct {
	CategoryName string              `json:"CategoryName"`
	InputType    string              `json:"InputType"`
	Rules        []CallAnalyticsRule `json:"Rules"`
}

type updateCallAnalyticsCategoryOutput struct {
	CategoryProperties *callAnalyticsCategoryProperties `json:"CategoryProperties"`
}

func (h *Handler) handleUpdateCallAnalyticsCategory(
	_ context.Context,
	in *updateCallAnalyticsCategoryInput,
) (*updateCallAnalyticsCategoryOutput, error) {
	cat, err := h.Backend.UpdateCallAnalyticsCategory(&CallAnalyticsCategory{
		CategoryName: in.CategoryName,
		InputType:    in.InputType,
		Rules:        in.Rules,
	})
	if err != nil {
		return nil, err
	}

	return &updateCallAnalyticsCategoryOutput{
		CategoryProperties: &callAnalyticsCategoryProperties{
			CategoryName: cat.CategoryName,
			InputType:    cat.InputType,
		},
	}, nil
}

// --- ListCallAnalyticsCategories ---

type listCallAnalyticsCategoriesInput struct {
	NextToken string `json:"NextToken"`
}

type listCallAnalyticsCategoriesOutput struct {
	NextToken  string                            `json:"NextToken,omitempty"`
	Categories []callAnalyticsCategoryProperties `json:"Categories"`
}

func (h *Handler) handleListCallAnalyticsCategories(
	_ context.Context,
	in *listCallAnalyticsCategoriesInput,
) (*listCallAnalyticsCategoriesOutput, error) {
	cats, nextToken := h.Backend.ListCallAnalyticsCategories(in.NextToken)

	result := make([]callAnalyticsCategoryProperties, 0, len(cats))
	for _, c := range cats {
		result = append(result, callAnalyticsCategoryProperties{
			CategoryName: c.CategoryName,
			InputType:    c.InputType,
		})
	}

	return &listCallAnalyticsCategoriesOutput{
		Categories: result,
		NextToken:  nextToken,
	}, nil
}

// --- GetMedicalScribeJob ---

type getMedicalScribeJobInput struct {
	MedicalScribeJobName string `json:"MedicalScribeJobName"`
}

type medicalScribeJobOutput struct {
	Settings                       *MedicalScribeSettings           `json:"Settings,omitempty"`
	Media                          *Media                           `json:"Media,omitempty"`
	ClinicalNoteGenerationSettings *ClinicalNoteGenerationSettings  `json:"ClinicalNoteGenerationSettings,omitempty"`
	Tags                           map[string]string                `json:"Tags,omitempty"`
	CreationTime                   *string                          `json:"CreationTime,omitempty"`
	StartTime                      *string                          `json:"StartTime,omitempty"`
	CompletionTime                 *string                          `json:"CompletionTime,omitempty"`
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
		Tags:                           job.Tags,
	}
	if !job.CreationTime.IsZero() {
		s := job.CreationTime.Format(time.RFC3339)
		out.CreationTime = &s
	}
	if !job.StartTime.IsZero() {
		s := job.StartTime.Format(time.RFC3339)
		out.StartTime = &s
	}
	if !job.CompletionTime.IsZero() {
		s := job.CompletionTime.Format(time.RFC3339)
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
	Tags                           map[string]string                `json:"Tags"`
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
		Tags:                           in.Tags,
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

// --- GetMedicalTranscriptionJob ---

type getMedicalTranscriptionJobInput struct {
	MedicalTranscriptionJobName string `json:"MedicalTranscriptionJobName"`
}

type medicalTranscriptionJobOutput struct {
	Settings                         *MedicalTranscriptionSettings `json:"Settings,omitempty"`
	Media                            *Media                        `json:"Media,omitempty"`
	Tags                             map[string]string             `json:"Tags,omitempty"`
	CreationTime                     *string                       `json:"CreationTime,omitempty"`
	StartTime                        *string                       `json:"StartTime,omitempty"`
	CompletionTime                   *string                       `json:"CompletionTime,omitempty"`
	MedicalTranscriptionJobName      string                        `json:"MedicalTranscriptionJobName"`
	TranscriptionJobStatus           string                        `json:"TranscriptionJobStatus"`
	LanguageCode                     string                        `json:"LanguageCode,omitempty"`
	Specialty                        string                        `json:"Specialty,omitempty"`
	Type                             string                        `json:"Type,omitempty"`
	MediaFormat                      string                        `json:"MediaFormat,omitempty"`
	OutputBucketName                 string                        `json:"OutputBucketName,omitempty"`
	OutputKey                        string                        `json:"OutputKey,omitempty"`
	MedicalContentIdentificationType string                        `json:"MedicalContentIdentificationType,omitempty"`
	FailureReason                    string                        `json:"FailureReason,omitempty"`
	MediaSampleRateHertz             int32                         `json:"MediaSampleRateHertz,omitempty"`
}

func buildMedicalTranscriptionJobOutput(job *MedicalTranscriptionJob) *medicalTranscriptionJobOutput {
	out := &medicalTranscriptionJobOutput{
		MedicalTranscriptionJobName:      job.MedicalTranscriptionJobName,
		TranscriptionJobStatus:           job.TranscriptionJobStatus,
		LanguageCode:                     job.LanguageCode,
		Specialty:                        job.Specialty,
		Type:                             job.Type,
		MediaFormat:                      job.MediaFormat,
		OutputBucketName:                 job.OutputBucketName,
		OutputKey:                        job.OutputKey,
		MedicalContentIdentificationType: job.MedicalContentIdentificationType,
		FailureReason:                    job.FailureReason,
		MediaSampleRateHertz:             job.MediaSampleRateHertz,
		Settings:                         job.Settings,
		Tags:                             job.Tags,
	}
	if !job.CreationTime.IsZero() {
		s := job.CreationTime.Format(time.RFC3339)
		out.CreationTime = &s
	}
	if !job.StartTime.IsZero() {
		s := job.StartTime.Format(time.RFC3339)
		out.StartTime = &s
	}
	if !job.CompletionTime.IsZero() {
		s := job.CompletionTime.Format(time.RFC3339)
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
	Tags                             map[string]string             `json:"Tags"`
	MedicalTranscriptionJobName      string                        `json:"MedicalTranscriptionJobName"`
	LanguageCode                     string                        `json:"LanguageCode"`
	Specialty                        string                        `json:"Specialty"`
	Type                             string                        `json:"Type"`
	MediaFormat                      string                        `json:"MediaFormat"`
	OutputBucketName                 string                        `json:"OutputBucketName"`
	OutputKey                        string                        `json:"OutputKey"`
	MedicalContentIdentificationType string                        `json:"MedicalContentIdentificationType"`
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
		Tags:                             in.Tags,
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
	Status    string `json:"Status"`
	NextToken string `json:"NextToken"`
}

type listMedicalTranscriptionJobsOutput struct {
	NextToken                        string                          `json:"NextToken,omitempty"`
	MedicalTranscriptionJobSummaries []medicalTranscriptionJobOutput `json:"MedicalTranscriptionJobSummaries"`
}

func (h *Handler) handleListMedicalTranscriptionJobs(
	_ context.Context,
	in *listMedicalTranscriptionJobsInput,
) (*listMedicalTranscriptionJobsOutput, error) {
	jobs, nextToken := h.Backend.ListMedicalTranscriptionJobs(in.Status, in.NextToken)

	summaries := make([]medicalTranscriptionJobOutput, 0, len(jobs))
	for i := range jobs {
		summaries = append(summaries, *buildMedicalTranscriptionJobOutput(&jobs[i]))
	}

	return &listMedicalTranscriptionJobsOutput{
		MedicalTranscriptionJobSummaries: summaries,
		NextToken:                        nextToken,
	}, nil
}

// --- GetVocabulary ---

type getVocabularyInput struct {
	VocabularyName string `json:"VocabularyName"`
}

type getVocabularyOutput struct {
	VocabularyName  string `json:"VocabularyName"`
	LanguageCode    string `json:"LanguageCode"`
	VocabularyState string `json:"VocabularyState"`
}

func (h *Handler) handleGetVocabulary(
	_ context.Context,
	in *getVocabularyInput,
) (*getVocabularyOutput, error) {
	v, err := h.Backend.GetVocabulary(in.VocabularyName)
	if err != nil {
		return nil, err
	}

	return &getVocabularyOutput{
		VocabularyName:  v.VocabularyName,
		LanguageCode:    v.LanguageCode,
		VocabularyState: v.VocabularyState,
	}, nil
}

// --- UpdateVocabulary ---

type updateVocabularyInput struct {
	VocabularyName    string   `json:"VocabularyName"`
	LanguageCode      string   `json:"LanguageCode"`
	VocabularyFileURI string   `json:"VocabularyFileURI"`
	Phrases           []string `json:"Phrases"`
}

type updateVocabularyOutput struct {
	VocabularyName  string `json:"VocabularyName"`
	LanguageCode    string `json:"LanguageCode"`
	VocabularyState string `json:"VocabularyState"`
}

func (h *Handler) handleUpdateVocabulary(
	_ context.Context,
	in *updateVocabularyInput,
) (*updateVocabularyOutput, error) {
	v, err := h.Backend.UpdateVocabulary(&Vocabulary{
		VocabularyName:    in.VocabularyName,
		LanguageCode:      in.LanguageCode,
		Phrases:           in.Phrases,
		VocabularyFileURI: in.VocabularyFileURI,
	})
	if err != nil {
		return nil, err
	}

	return &updateVocabularyOutput{
		VocabularyName:  v.VocabularyName,
		LanguageCode:    v.LanguageCode,
		VocabularyState: v.VocabularyState,
	}, nil
}

// --- DeleteVocabulary ---

type deleteVocabularyInput struct {
	VocabularyName string `json:"VocabularyName"`
}

func (h *Handler) handleDeleteVocabulary(
	_ context.Context,
	in *deleteVocabularyInput,
) (*struct{}, error) {
	if err := h.Backend.DeleteVocabulary(in.VocabularyName); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

// --- ListVocabularies ---

type listVocabulariesInput struct {
	StateEquals string `json:"StateEquals"`
	NextToken   string `json:"NextToken"`
}

type vocabularySummary struct {
	VocabularyName  string `json:"VocabularyName"`
	LanguageCode    string `json:"LanguageCode"`
	VocabularyState string `json:"VocabularyState"`
}

type listVocabulariesOutput struct {
	NextToken    string              `json:"NextToken,omitempty"`
	Vocabularies []vocabularySummary `json:"Vocabularies"`
}

func (h *Handler) handleListVocabularies(
	_ context.Context,
	in *listVocabulariesInput,
) (*listVocabulariesOutput, error) {
	vocabs, nextToken := h.Backend.ListVocabularies(in.StateEquals, in.NextToken)

	result := make([]vocabularySummary, 0, len(vocabs))
	for _, v := range vocabs {
		result = append(result, vocabularySummary{
			VocabularyName:  v.VocabularyName,
			LanguageCode:    v.LanguageCode,
			VocabularyState: v.VocabularyState,
		})
	}

	return &listVocabulariesOutput{
		Vocabularies: result,
		NextToken:    nextToken,
	}, nil
}

// --- GetVocabularyFilter ---

type getVocabularyFilterInput struct {
	VocabularyFilterName string `json:"VocabularyFilterName"`
}

type vocabularyFilterOutput struct {
	VocabularyFilterName string `json:"VocabularyFilterName"`
	LanguageCode         string `json:"LanguageCode"`
}

type getVocabularyFilterOutput struct {
	VocabularyFilterName string `json:"VocabularyFilterName"`
	LanguageCode         string `json:"LanguageCode"`
}

func (h *Handler) handleGetVocabularyFilter(
	_ context.Context,
	in *getVocabularyFilterInput,
) (*getVocabularyFilterOutput, error) {
	f, err := h.Backend.GetVocabularyFilter(in.VocabularyFilterName)
	if err != nil {
		return nil, err
	}

	return &getVocabularyFilterOutput{
		VocabularyFilterName: f.VocabularyFilterName,
		LanguageCode:         f.LanguageCode,
	}, nil
}

// --- UpdateVocabularyFilter ---

type updateVocabularyFilterInput struct {
	VocabularyFilterName    string   `json:"VocabularyFilterName"`
	LanguageCode            string   `json:"LanguageCode"`
	VocabularyFilterFileURI string   `json:"VocabularyFilterFileURI"`
	Words                   []string `json:"Words"`
}

func (h *Handler) handleUpdateVocabularyFilter(
	_ context.Context,
	in *updateVocabularyFilterInput,
) (*vocabularyFilterOutput, error) {
	f, err := h.Backend.UpdateVocabularyFilter(&VocabularyFilter{
		VocabularyFilterName:    in.VocabularyFilterName,
		LanguageCode:            in.LanguageCode,
		Words:                   in.Words,
		VocabularyFilterFileURI: in.VocabularyFilterFileURI,
	})
	if err != nil {
		return nil, err
	}

	return &vocabularyFilterOutput{
		VocabularyFilterName: f.VocabularyFilterName,
		LanguageCode:         f.LanguageCode,
	}, nil
}

// --- DeleteVocabularyFilter ---

type deleteVocabularyFilterInput struct {
	VocabularyFilterName string `json:"VocabularyFilterName"`
}

func (h *Handler) handleDeleteVocabularyFilter(
	_ context.Context,
	in *deleteVocabularyFilterInput,
) (*struct{}, error) {
	if err := h.Backend.DeleteVocabularyFilter(in.VocabularyFilterName); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

// --- ListVocabularyFilters ---

type listVocabularyFiltersInput struct {
	NextToken string `json:"NextToken"`
}

type listVocabularyFiltersOutput struct {
	NextToken         string                   `json:"NextToken,omitempty"`
	VocabularyFilters []vocabularyFilterOutput `json:"VocabularyFilters"`
}

func (h *Handler) handleListVocabularyFilters(
	_ context.Context,
	in *listVocabularyFiltersInput,
) (*listVocabularyFiltersOutput, error) {
	filters, nextToken := h.Backend.ListVocabularyFilters(in.NextToken)

	result := make([]vocabularyFilterOutput, 0, len(filters))
	for _, f := range filters {
		result = append(result, vocabularyFilterOutput{
			VocabularyFilterName: f.VocabularyFilterName,
			LanguageCode:         f.LanguageCode,
		})
	}

	return &listVocabularyFiltersOutput{
		VocabularyFilters: result,
		NextToken:         nextToken,
	}, nil
}

// --- GetMedicalVocabulary ---

type getMedicalVocabularyInput struct {
	VocabularyName string `json:"VocabularyName"`
}

type getMedicalVocabularyOutput struct {
	VocabularyName  string `json:"VocabularyName"`
	LanguageCode    string `json:"LanguageCode"`
	VocabularyState string `json:"VocabularyState"`
}

func (h *Handler) handleGetMedicalVocabulary(
	_ context.Context,
	in *getMedicalVocabularyInput,
) (*getMedicalVocabularyOutput, error) {
	v, err := h.Backend.GetMedicalVocabulary(in.VocabularyName)
	if err != nil {
		return nil, err
	}

	return &getMedicalVocabularyOutput{
		VocabularyName:  v.VocabularyName,
		LanguageCode:    v.LanguageCode,
		VocabularyState: v.VocabularyState,
	}, nil
}

// --- UpdateMedicalVocabulary ---

type updateMedicalVocabularyInput struct {
	VocabularyName    string `json:"VocabularyName"`
	LanguageCode      string `json:"LanguageCode"`
	VocabularyFileURI string `json:"VocabularyFileURI"`
}

type updateMedicalVocabularyOutput struct {
	VocabularyName  string `json:"VocabularyName"`
	LanguageCode    string `json:"LanguageCode"`
	VocabularyState string `json:"VocabularyState"`
}

func (h *Handler) handleUpdateMedicalVocabulary(
	_ context.Context,
	in *updateMedicalVocabularyInput,
) (*updateMedicalVocabularyOutput, error) {
	v, err := h.Backend.UpdateMedicalVocabulary(
		in.VocabularyName,
		in.LanguageCode,
		in.VocabularyFileURI,
	)
	if err != nil {
		return nil, err
	}

	return &updateMedicalVocabularyOutput{
		VocabularyName:  v.VocabularyName,
		LanguageCode:    v.LanguageCode,
		VocabularyState: v.VocabularyState,
	}, nil
}

// --- DeleteMedicalVocabulary ---

type deleteMedicalVocabularyInput struct {
	VocabularyName string `json:"VocabularyName"`
}

func (h *Handler) handleDeleteMedicalVocabulary(
	_ context.Context,
	in *deleteMedicalVocabularyInput,
) (*struct{}, error) {
	if err := h.Backend.DeleteMedicalVocabulary(in.VocabularyName); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

// --- ListMedicalVocabularies ---

type listMedicalVocabulariesInput struct {
	StateEquals string `json:"StateEquals"`
	NextToken   string `json:"NextToken"`
}

type medicalVocabularySummary struct {
	VocabularyName  string `json:"VocabularyName"`
	LanguageCode    string `json:"LanguageCode"`
	VocabularyState string `json:"VocabularyState"`
}

type listMedicalVocabulariesOutput struct {
	NextToken    string                     `json:"NextToken,omitempty"`
	Vocabularies []medicalVocabularySummary `json:"Vocabularies"`
}

func (h *Handler) handleListMedicalVocabularies(
	_ context.Context,
	in *listMedicalVocabulariesInput,
) (*listMedicalVocabulariesOutput, error) {
	vocabs, nextToken := h.Backend.ListMedicalVocabularies(in.StateEquals, in.NextToken)

	result := make([]medicalVocabularySummary, 0, len(vocabs))
	for _, v := range vocabs {
		result = append(result, medicalVocabularySummary{
			VocabularyName:  v.VocabularyName,
			LanguageCode:    v.LanguageCode,
			VocabularyState: v.VocabularyState,
		})
	}

	return &listMedicalVocabulariesOutput{
		Vocabularies: result,
		NextToken:    nextToken,
	}, nil
}

// --- DescribeLanguageModel ---

type describeLanguageModelInput struct {
	ModelName string `json:"ModelName"`
}

type languageModelOutput struct {
	InputDataConfig *InputDataConfig `json:"InputDataConfig,omitempty"`
	ModelName       string           `json:"ModelName"`
	BaseModelName   string           `json:"BaseModelName"`
	LanguageCode    string           `json:"LanguageCode"`
	ModelStatus     string           `json:"ModelStatus"`
}

type describeLanguageModelOutput struct {
	LanguageModel *languageModelOutput `json:"LanguageModel"`
}

func (h *Handler) handleDescribeLanguageModel(
	_ context.Context,
	in *describeLanguageModelInput,
) (*describeLanguageModelOutput, error) {
	m, err := h.Backend.DescribeLanguageModel(in.ModelName)
	if err != nil {
		return nil, err
	}

	return &describeLanguageModelOutput{
		LanguageModel: &languageModelOutput{
			ModelName:       m.ModelName,
			BaseModelName:   m.BaseModelName,
			LanguageCode:    m.LanguageCode,
			ModelStatus:     m.ModelStatus,
			InputDataConfig: m.InputDataConfig,
		},
	}, nil
}

// --- ListLanguageModels ---

type listLanguageModelsInput struct {
	NextToken    string `json:"NextToken"`
	StatusEquals string `json:"StatusEquals"`
}

type listLanguageModelsOutput struct {
	NextToken string                `json:"NextToken,omitempty"`
	Models    []languageModelOutput `json:"Models"`
}

func (h *Handler) handleListLanguageModels(
	_ context.Context,
	in *listLanguageModelsInput,
) (*listLanguageModelsOutput, error) {
	models, nextToken := h.Backend.ListLanguageModels(in.StatusEquals, in.NextToken)

	result := make([]languageModelOutput, 0, len(models))
	for _, m := range models {
		result = append(result, languageModelOutput{
			ModelName:       m.ModelName,
			BaseModelName:   m.BaseModelName,
			LanguageCode:    m.LanguageCode,
			ModelStatus:     m.ModelStatus,
			InputDataConfig: m.InputDataConfig,
		})
	}

	return &listLanguageModelsOutput{
		Models:    result,
		NextToken: nextToken,
	}, nil
}

// --- Tags (no-op stubs) ---

type tagResourceInput struct {
	Tags        map[string]string `json:"Tags"`
	ResourceArn string            `json:"ResourceArn"`
}

func (h *Handler) handleTagResource(
	_ context.Context,
	in *tagResourceInput,
) (*struct{}, error) {
	if err := h.Backend.TagResource(in.ResourceArn, in.Tags); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type untagResourceInput struct {
	ResourceArn string   `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
}

func (h *Handler) handleUntagResource(
	_ context.Context,
	in *untagResourceInput,
) (*struct{}, error) {
	if err := h.Backend.UntagResource(in.ResourceArn, in.TagKeys); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type listTagsForResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
}

type listTagsForResourceOutput struct {
	Tags        map[string]string `json:"Tags"`
	ResourceArn string            `json:"ResourceArn,omitempty"`
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	tags, err := h.Backend.ListTagsForResource(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &listTagsForResourceOutput{
		ResourceArn: in.ResourceArn,
		Tags:        tags,
	}, nil
}

// allSupportedOps returns all 43 supported operations in sorted order.
func allSupportedOps() []string {
	return []string{
		"CreateCallAnalyticsCategory",
		"CreateLanguageModel",
		"CreateMedicalVocabulary",
		"CreateVocabulary",
		"CreateVocabularyFilter",
		"DeleteCallAnalyticsCategory",
		"DeleteCallAnalyticsJob",
		"DeleteLanguageModel",
		"DeleteMedicalScribeJob",
		"DeleteMedicalTranscriptionJob",
		"DeleteMedicalVocabulary",
		"DeleteTranscriptionJob",
		"DeleteVocabulary",
		"DeleteVocabularyFilter",
		"DescribeLanguageModel",
		"GetCallAnalyticsCategory",
		"GetCallAnalyticsJob",
		"GetMedicalScribeJob",
		"GetMedicalTranscriptionJob",
		"GetMedicalVocabulary",
		"GetTranscriptionJob",
		"GetVocabulary",
		"GetVocabularyFilter",
		"ListCallAnalyticsCategories",
		"ListCallAnalyticsJobs",
		"ListLanguageModels",
		"ListMedicalScribeJobs",
		"ListMedicalTranscriptionJobs",
		"ListMedicalVocabularies",
		"ListTagsForResource",
		"ListTranscriptionJobs",
		"ListVocabularies",
		"ListVocabularyFilters",
		"StartCallAnalyticsJob",
		"StartMedicalScribeJob",
		"StartMedicalTranscriptionJob",
		"StartTranscriptionJob",
		"TagResource",
		"UntagResource",
		"UpdateCallAnalyticsCategory",
		"UpdateMedicalVocabulary",
		"UpdateVocabulary",
		"UpdateVocabularyFilter",
	}
}
