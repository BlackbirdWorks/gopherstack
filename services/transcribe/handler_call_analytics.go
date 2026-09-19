package transcribe

import (
	"context"
	"strings"

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
	CreateTime     *float64            `json:"CreateTime,omitempty"`
	LastUpdateTime *float64            `json:"LastUpdateTime,omitempty"`
	Tags           []transcribeTag     `json:"Tags,omitempty"`
	CategoryName   string              `json:"CategoryName"`
	InputType      string              `json:"InputType,omitempty"`
	Rules          []CallAnalyticsRule `json:"Rules,omitempty"`
}

func buildCallAnalyticsCategoryProperties(cat *CallAnalyticsCategory) *callAnalyticsCategoryProperties {
	out := &callAnalyticsCategoryProperties{
		CategoryName: cat.CategoryName,
		InputType:    cat.InputType,
		Rules:        cat.Rules,
		Tags:         tagsFromMap(cat.Tags),
	}
	if !cat.CreateTime.IsZero() {
		t := awstime.Epoch(cat.CreateTime)
		out.CreateTime = &t
	}
	if !cat.LastUpdateTime.IsZero() {
		t := awstime.Epoch(cat.LastUpdateTime)
		out.LastUpdateTime = &t
	}

	return out
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
		CategoryProperties: buildCallAnalyticsCategoryProperties(cat),
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
			TranscriptFileURI: resolveCallAnalyticsOutputLocation(job.OutputLocation, job.CallAnalyticsJobName),
		}
	}

	return out
}

// resolveCallAnalyticsOutputLocation applies StartCallAnalyticsJobInput.OutputLocation's
// three documented forms (transcribe@v1.64.0 api_op_StartCallAnalyticsJob.go): a
// bucket-only URI or a folder URI (trailing "/") both get the job name appended as the
// default file name; a URI that already names a file is used as-is. Empty means the
// caller didn't specify one, so the job lands in the service-managed bucket.
func resolveCallAnalyticsOutputLocation(outputLocation, jobName string) string {
	if outputLocation == "" {
		return "s3://synthetic-transcripts/" + jobName + ".json"
	}

	if strings.HasSuffix(outputLocation, "/") {
		return outputLocation + jobName + ".json"
	}

	if !strings.Contains(strings.TrimPrefix(outputLocation, "s3://"), "/") {
		return outputLocation + "/" + jobName + ".json"
	}

	return outputLocation
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
	OutputLocation       string                 `json:"OutputLocation"`
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
		OutputLocation:       in.OutputLocation,
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
	Status          string `json:"Status"`
	JobNameContains string `json:"JobNameContains"`
	NextToken       string `json:"NextToken"`
	MaxResults      int32  `json:"MaxResults"`
}

type callAnalyticsJobSummary struct {
	CreationTime           *float64 `json:"CreationTime,omitempty"`
	CompletionTime         *float64 `json:"CompletionTime,omitempty"`
	StartTime              *float64 `json:"StartTime,omitempty"`
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
	jobs, nextToken := h.Backend.ListCallAnalyticsJobs(in.Status, in.JobNameContains, in.NextToken, in.MaxResults)

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
		CategoryProperties: buildCallAnalyticsCategoryProperties(cat),
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
		CategoryProperties: buildCallAnalyticsCategoryProperties(cat),
	}, nil
}

// --- ListCallAnalyticsCategories ---

type listCallAnalyticsCategoriesInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type listCallAnalyticsCategoriesOutput struct {
	NextToken  string                            `json:"NextToken,omitempty"`
	Categories []callAnalyticsCategoryProperties `json:"Categories"`
}

func (h *Handler) handleListCallAnalyticsCategories(
	_ context.Context,
	in *listCallAnalyticsCategoriesInput,
) (*listCallAnalyticsCategoriesOutput, error) {
	cats, nextToken := h.Backend.ListCallAnalyticsCategories(in.NextToken, in.MaxResults)

	result := make([]callAnalyticsCategoryProperties, 0, len(cats))
	for i := range cats {
		result = append(result, *buildCallAnalyticsCategoryProperties(&cats[i]))
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
