package translate

import (
	"fmt"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// --- Translation Jobs ---

// startTextTranslationJobRequired validates the top-level required members of
// StartTextTranslationJobRequest (DataAccessRoleArn, InputDataConfig,
// OutputDataConfig, SourceLanguageCode, TargetLanguageCodes -- confirmed
// against StartTextTranslationJobRequest's "required" list in the smithy
// model) and InputDataConfig/OutputDataConfig's own required members
// (S3Uri+ContentType, S3Uri respectively). StartTextTranslationJob models
// both InvalidRequestException and InvalidParameterValueException; missing
// structural members use ErrValidation (InvalidRequestException), matching
// the convention CreateParallelData/UpdateParallelData already use for the
// same class of "request is incomplete" failure.
func startTextTranslationJobRequired(
	dataAccessRoleARN, sourceLang string,
	targetLangs []string,
	inputCfg, outputCfg map[string]any,
) error {
	if dataAccessRoleARN == "" {
		return fmt.Errorf("%w: DataAccessRoleArn is required", ErrValidation)
	}

	if inputCfg == nil || strField(inputCfg, "S3Uri") == "" || strField(inputCfg, "ContentType") == "" {
		return fmt.Errorf("%w: InputDataConfig.S3Uri and InputDataConfig.ContentType are required", ErrValidation)
	}

	if outputCfg == nil || strField(outputCfg, "S3Uri") == "" {
		return fmt.Errorf("%w: OutputDataConfig.S3Uri is required", ErrValidation)
	}

	if sourceLang == "" {
		return fmt.Errorf("%w: SourceLanguageCode is required", ErrValidation)
	}

	if len(targetLangs) == 0 {
		return fmt.Errorf("%w: TargetLanguageCodes is required", ErrValidation)
	}

	return nil
}

// validateLanguagePair rejects a SourceLanguageCode ("auto" always passes --
// it is resolved per-document at translation time, not a real language code
// to validate) or target code(s) outside Translate's supported language list
// (handler_languages.go's knownLanguages, the same list ListLanguages
// serves).
func validateLanguagePair(sourceLang string, targetLangs []string) error {
	if sourceLang != sourceLangAuto && !isKnownLanguageCode(sourceLang) {
		return fmt.Errorf("%w: source language %q is not supported", ErrUnsupportedLanguagePair, sourceLang)
	}

	for _, t := range targetLangs {
		if !isKnownLanguageCode(t) {
			return fmt.Errorf("%w: target language %q is not supported", ErrUnsupportedLanguagePair, t)
		}
	}

	return nil
}

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

	if err := startTextTranslationJobRequired(
		dataAccessRoleARN, sourceLang, targetLangs, inputCfg, outputCfg,
	); err != nil {
		return nil, err
	}

	if err := validateLanguagePair(sourceLang, targetLangs); err != nil {
		return nil, err
	}

	// StartTextTranslationJob's API reference documents "Brevity: not
	// supported" for batch jobs, unlike TranslateText/TranslateDocument
	// (handler_translation.go), which both allow Brevity: ON.
	if err := validSettingsEnums(settings, false); err != nil {
		return nil, err
	}

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
		// StopTextTranslationJob models neither InvalidRequestException nor
		// InvalidParameterValueException (api-2.json) -- only
		// ResourceNotFoundException, TooManyRequestsException, and
		// InternalServerException -- so an empty JobId surfaces the same way
		// a well-formed-but-absent one does.
		return nil, fmt.Errorf("%w: JobId is required", ErrNotFound)
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
		// Same rationale as stopTextTranslationJob above: DescribeTextTranslationJob
		// has no validation exception modeled at all.
		return nil, fmt.Errorf("%w: JobId is required", ErrNotFound)
	}

	job, err := h.Backend.DescribeTextTranslationJob(jobID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"TextTranslationJobProperties": jobToMap(job),
	}, nil
}

// validJobStatusesTable is the JobStatus enum (types.JobStatus), used to
// validate ListTextTranslationJobs' Filter.JobStatus.
//
//nolint:gochecknoglobals // read-only package-level lookup table, apigatewayv2-style
var validJobStatusesTable = sync.OnceValue(func() map[string]bool {
	return map[string]bool{
		jobStatusSubmitted: true, jobStatusInProgress: true, jobStatusCompleted: true,
		"COMPLETED_WITH_ERROR": true, jobStatusFailed: true,
		jobStatusStopRequested: true, jobStatusStopped: true,
	}
})

// epochTimeFromFilter reads key from a decoded JSON-RPC Filter object as a
// unixTimestamp (JSON number of seconds since the epoch, awsjson1.1's
// timestamp wire format -- see pkgs/awstime doc comment), returning nil when
// the key is absent or not a number.
func epochTimeFromFilter(f map[string]any, key string) *time.Time {
	sec, ok := f[key].(float64)
	if !ok {
		return nil
	}

	t := time.UnixMilli(int64(sec * float64(time.Second/time.Millisecond))).UTC()

	return &t
}

// textTranslationJobFilterFromMap decodes a ListTextTranslationJobsInput
// Filter object into a TextTranslationJobFilter, rejecting Filter.JobStatus
// values outside the real enum and requests that set more than one of
// JobName/JobStatus/SubmittedAfterTime/SubmittedBeforeTime -- per
// api_op_ListTextTranslationJobs.go's Filter doc comment: "Filters include
// job name, job status, and submission time. You can only set one filter at
// a time".
func textTranslationJobFilterFromMap(f map[string]any) (TextTranslationJobFilter, error) {
	var filter TextTranslationJobFilter

	setCount := 0

	if name, nameOK := f["JobName"].(string); nameOK && name != "" {
		filter.JobName = name
		setCount++
	}

	if status, statusOK := f[keyJobStatus].(string); statusOK && status != "" {
		if !validJobStatusesTable()[status] {
			return filter, fmt.Errorf("%w: Filter.JobStatus %q is not a valid job status", ErrInvalidFilter, status)
		}

		filter.JobStatus = status
		setCount++
	}

	if after := epochTimeFromFilter(f, "SubmittedAfterTime"); after != nil {
		filter.SubmittedAfterTime = after
		setCount++
	}

	if before := epochTimeFromFilter(f, "SubmittedBeforeTime"); before != nil {
		filter.SubmittedBeforeTime = before
		setCount++
	}

	if setCount > 1 {
		return filter, fmt.Errorf("%w: you can only set one filter at a time", ErrInvalidFilter)
	}

	return filter, nil
}

func (h *Handler) listTextTranslationJobs(input map[string]any) (map[string]any, error) {
	maxResults := maxResultsField(input)
	nextToken, _ := input["NextToken"].(string)

	var filter TextTranslationJobFilter

	if f, ok := input["Filter"].(map[string]any); ok {
		var err error

		filter, err = textTranslationJobFilterFromMap(f)
		if err != nil {
			return nil, err
		}
	}

	list, outToken := h.Backend.ListTextTranslationJobs(filter, maxResults, nextToken)

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
