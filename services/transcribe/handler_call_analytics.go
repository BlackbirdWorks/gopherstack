package transcribe

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// --- CreateCallAnalyticsCategory ---

type createCallAnalyticsCategoryInput struct {
	Tags         []transcribeTag     `json:"Tags"`
	CategoryName string              `json:"CategoryName"`
	InputType    string              `json:"InputType"`
	Rules        []CallAnalyticsRule `json:"Rules"`
}

type callAnalyticsCategoryProperties struct {
	CategoryName string `json:"CategoryName"`
	InputType    string `json:"InputType,omitempty"`
}

type createCallAnalyticsCategoryOutput struct {
	CategoryProperties *callAnalyticsCategoryProperties `json:"CategoryProperties"`
}

func (h *Handler) handleCreateCallAnalyticsCategory(
	_ context.Context,
	in *createCallAnalyticsCategoryInput,
) (*createCallAnalyticsCategoryOutput, error) {
	cat, err := h.Backend.CreateCallAnalyticsCategory(&CallAnalyticsCategory{
		CategoryName: in.CategoryName,
		InputType:    in.InputType,
		Rules:        in.Rules,
		Tags:         tagsToMap(in.Tags),
	})
	if err != nil {
		return nil, err
	}

	return &createCallAnalyticsCategoryOutput{
		CategoryProperties: &callAnalyticsCategoryProperties{
			CategoryName: cat.CategoryName,
			InputType:    cat.InputType,
		},
	}, nil
}

// --- DeleteCallAnalyticsCategory ---

type deleteCallAnalyticsCategoryInput struct {
	CategoryName string `json:"CategoryName"`
}

func (h *Handler) handleDeleteCallAnalyticsCategory(
	_ context.Context,
	in *deleteCallAnalyticsCategoryInput,
) (*struct{}, error) {
	if err := h.Backend.DeleteCallAnalyticsCategory(in.CategoryName); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

// --- GetCallAnalyticsJob ---

type getCallAnalyticsJobInput struct {
	CallAnalyticsJobName string `json:"CallAnalyticsJobName"`
}

type callAnalyticsJobOutput struct {
	Tags                   []transcribeTag        `json:"Tags,omitempty"`
	Settings               *CallAnalyticsSettings `json:"Settings,omitempty"`
	Media                  *Media                 `json:"Media,omitempty"`
	Transcript             *transcriptOutput      `json:"Transcript,omitempty"`
	CreationTime           *float64               `json:"CreationTime,omitempty"`
	StartTime              *float64               `json:"StartTime,omitempty"`
	CompletionTime         *float64               `json:"CompletionTime,omitempty"`
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
		Tags:                   tagsFromMap(job.Tags),
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
	if job.Media.MediaFileURI != "" || job.Media.RedactedMediaFileURI != "" {
		m := job.Media
		out.Media = &m
	}

	if job.CallAnalyticsJobStatus == jobStatusCompleted {
		out.Transcript = &transcriptOutput{
			TranscriptFileURI: "s3://synthetic-transcripts/" + job.CallAnalyticsJobName + ".json",
		}
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
	Tags                 []transcribeTag        `json:"Tags"`
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
		Tags:                 tagsToMap(in.Tags),
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
	CreationTime           *float64 `json:"CreationTime,omitempty"`
	CompletionTime         *float64 `json:"CompletionTime,omitempty"`
	CallAnalyticsJobName   string   `json:"CallAnalyticsJobName"`
	CallAnalyticsJobStatus string   `json:"CallAnalyticsJobStatus"`
	LanguageCode           string   `json:"LanguageCode,omitempty"`
	FailureReason          string   `json:"FailureReason,omitempty"`
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
			ts := awstime.Epoch(j.CreationTime)
			s.CreationTime = &ts
		}
		if !j.CompletionTime.IsZero() {
			ts := awstime.Epoch(j.CompletionTime)
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

// --- DeleteCallAnalyticsJob ---

type deleteCallAnalyticsJobInput struct {
	CallAnalyticsJobName string `json:"CallAnalyticsJobName"`
}

func (h *Handler) handleDeleteCallAnalyticsJob(
	_ context.Context,
	in *deleteCallAnalyticsJobInput,
) (*struct{}, error) {
	if err := h.Backend.DeleteCallAnalyticsJob(in.CallAnalyticsJobName); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}
