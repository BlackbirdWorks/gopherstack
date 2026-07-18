package translate

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// --- Translation Jobs ---

func (h *Handler) startTextTranslationJob(input map[string]any) (map[string]any, error) {
	jobName, _ := input["JobName"].(string)
	dataAccessRoleARN, _ := input["DataAccessRoleArn"].(string)
	sourceLang, _ := input[keySourceLanguageCode].(string)

	targetLangs := strSliceField(input, keyTargetLanguageCodes)
	terminologyNames := strSliceField(input, "TerminologyNames")
	parallelDataNames := strSliceField(input, "ParallelDataNames")

	inputCfg, _ := input["InputDataConfig"].(map[string]any)
	outputCfg, _ := input["OutputDataConfig"].(map[string]any)
	settings, _ := input["Settings"].(map[string]any)
	tags := extractTags(input)

	job, err := h.Backend.StartTextTranslationJob(
		jobName, dataAccessRoleARN, sourceLang,
		targetLangs, terminologyNames, parallelDataNames,
		inputCfg, outputCfg, settings, tags,
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyJobID:     job.JobID,
		keyJobStatus: job.JobStatus,
	}, nil
}

func (h *Handler) stopTextTranslationJob(input map[string]any) (map[string]any, error) {
	jobID, _ := input[keyJobID].(string)
	if jobID == "" {
		return nil, fmt.Errorf("%w: JobId is required", ErrValidation)
	}

	job, err := h.Backend.StopTextTranslationJob(jobID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyJobID:     job.JobID,
		keyJobStatus: job.JobStatus,
	}, nil
}

func (h *Handler) describeTextTranslationJob(input map[string]any) (map[string]any, error) {
	jobID, _ := input[keyJobID].(string)
	if jobID == "" {
		return nil, fmt.Errorf("%w: JobId is required", ErrValidation)
	}

	job, err := h.Backend.DescribeTextTranslationJob(jobID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"TextTranslationJobProperties": jobToMap(job),
	}, nil
}

func (h *Handler) listTextTranslationJobs(input map[string]any) (map[string]any, error) {
	maxResults := maxResultsField(input)
	nextToken, _ := input["NextToken"].(string)

	var statusFilter string
	if f, ok := input["Filter"].(map[string]any); ok {
		statusFilter, _ = f[keyJobStatus].(string)
	}

	list, outToken := h.Backend.ListTextTranslationJobs(statusFilter, maxResults, nextToken)

	jobs := make([]map[string]any, 0, len(list))
	for _, job := range list {
		jobs = append(jobs, jobToMap(job))
	}

	result := map[string]any{
		"TextTranslationJobPropertiesList": jobs,
	}

	if outToken != "" {
		result["NextToken"] = outToken
	}

	return result, nil
}

func jobToMap(job *TranslationJob) map[string]any {
	m := map[string]any{
		keyJobID:               job.JobID,
		"JobName":              job.JobName,
		keyJobStatus:           job.JobStatus,
		"DataAccessRoleArn":    job.DataAccessRoleARN,
		keySourceLanguageCode:  job.SourceLanguage,
		keyTargetLanguageCodes: job.TargetLanguages,
		"SubmittedTime":        awstime.Epoch(job.SubmittedAt),
		"JobDetails": map[string]any{
			"TranslatedDocumentsCount": 0,
			"DocumentsWithErrorsCount": 0,
			"InputDocumentsCount":      0,
		},
	}

	if !job.EndAt.IsZero() {
		m["EndTime"] = awstime.Epoch(job.EndAt)
	}

	if job.Message != "" {
		m["Message"] = job.Message
	}

	if job.Settings != nil {
		m["Settings"] = job.Settings
	}

	if job.InputDataConfig != nil {
		m["InputDataConfig"] = job.InputDataConfig
	}

	if job.OutputDataConfig != nil {
		m["OutputDataConfig"] = job.OutputDataConfig
	}

	if len(job.TerminologyNames) > 0 {
		m["TerminologyNames"] = job.TerminologyNames
	}

	if len(job.ParallelDataNames) > 0 {
		m["ParallelDataNames"] = job.ParallelDataNames
	}

	return m
}
