package translate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	translateTargetPrefix = "AWSShineFrontendService_20170701."
	translateContentType  = "application/x-amz-json-1.1"
	unknownOperation      = "Unknown"

	keyName               = "Name"
	keyJobID              = "JobId"
	keyResourceARN        = "ResourceArn"
	keyJobStatus          = "JobStatus"
	keyStatus             = "Status"
	keySourceLanguageCode = "SourceLanguageCode"
	keyTargetLanguageCode = "TargetLanguageCode"
	keyLanguageCode       = "LanguageCode"
	keyLanguageName       = "LanguageName"
)

type opFunc func(map[string]any) (map[string]any, error)

// Handler serves Amazon Translate JSON operations.
type Handler struct {
	Backend *InMemoryBackend
	ops     map[string]opFunc
}

// NewHandler creates a Translate handler backed by in-memory state.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// Reset clears backend state.
func (h *Handler) Reset() { h.Backend.Reset() }

// Name returns service name.
func (h *Handler) Name() string { return "Translate" }

// ChaosServiceName returns service key for fault matching.
func (h *Handler) ChaosServiceName() string { return "translate" }

// ChaosOperations returns fault-injectable operations.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns configured service region.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// MatchPriority returns header matching priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// RouteMatcher matches Translate X-Amz-Target headers.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), translateTargetPrefix)
	}
}

// ExtractOperation returns the operation name from the request target.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	if !strings.HasPrefix(target, translateTargetPrefix) {
		return unknownOperation
	}

	return strings.TrimPrefix(target, translateTargetPrefix)
}

// ExtractResource retrieves the primary resource name.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var values map[string]any
	if unmarshalErr := json.Unmarshal(body, &values); unmarshalErr != nil {
		return ""
	}

	for _, key := range []string{keyName, keyJobID, keyResourceARN} {
		if v, ok := values[key].(string); ok && v != "" {
			return v
		}
	}

	return ""
}

// GetSupportedOperations returns all implemented operation names.
func (h *Handler) GetSupportedOperations() []string {
	ops := make([]string, 0, len(h.ops))
	for name := range h.ops {
		ops = append(ops, name)
	}

	return ops
}

// Handler returns the Echo HTTP handler.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()), h.Name(), translateContentType,
			h.GetSupportedOperations(), h.dispatch, h.handleError,
		)
	}
}

func (h *Handler) dispatch(_ context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
	if !ok {
		return nil, fmt.Errorf("%w: operation %q", ErrValidation, action)
	}

	var input map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &input); err != nil {
			return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
		}
	}

	if input == nil {
		input = make(map[string]any)
	}

	output, err := fn(input)
	if err != nil {
		return nil, err
	}

	return json.Marshal(output)
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	code := "InternalServerException"
	status := http.StatusInternalServerError

	switch {
	case errors.Is(err, ErrNotFound):
		code, status = "ResourceNotFoundException", http.StatusBadRequest
	case errors.Is(err, ErrConflict):
		code, status = "ResourceInUseException", http.StatusBadRequest
	case errors.Is(err, ErrValidation):
		code, status = "InvalidRequestException", http.StatusBadRequest
	}

	c.Response().Header().Set("Content-Type", translateContentType)

	return c.JSON(status, map[string]string{
		"__type":  code,
		"message": err.Error(),
	})
}

func (h *Handler) buildOps() map[string]opFunc {
	return map[string]opFunc{
		"ImportTerminology":          h.importTerminology,
		"GetTerminology":             h.getTerminology,
		"DeleteTerminology":          h.deleteTerminology,
		"ListTerminologies":          h.listTerminologies,
		"CreateParallelData":         h.createParallelData,
		"GetParallelData":            h.getParallelData,
		"UpdateParallelData":         h.updateParallelData,
		"DeleteParallelData":         h.deleteParallelData,
		"ListParallelData":           h.listParallelData,
		"StartTextTranslationJob":    h.startTextTranslationJob,
		"StopTextTranslationJob":     h.stopTextTranslationJob,
		"DescribeTextTranslationJob": h.describeTextTranslationJob,
		"ListTextTranslationJobs":    h.listTextTranslationJobs,
		"TranslateText":              h.translateText,
		"TranslateDocument":          h.translateDocument,
		"ListLanguages":              h.listLanguages,
		"TagResource":                h.tagResource,
		"UntagResource":              h.untagResource,
		"ListTagsForResource":        h.listTagsForResource,
	}
}

// --- Terminology ---

func (h *Handler) importTerminology(input map[string]any) (map[string]any, error) {
	name, _ := input[keyName].(string)
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	description, _ := input["Description"].(string)

	var data *TerminologyData
	if td, ok := input["TerminologyData"].(map[string]any); ok {
		data = &TerminologyData{
			Format: strField(td, "Format"),
		}

		if fileStr, fok := td["File"].(string); fok {
			data.File = []byte(fileStr)
		}
	}

	if data == nil {
		data = &TerminologyData{Format: "CSV"}
	}

	encKey := extractEncryptionKey(input)
	tags := extractTags(input)

	term, err := h.Backend.ImportTerminology(name, description, data, encKey, tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"TerminologyProperties": terminologyToMap(term),
	}, nil
}

func (h *Handler) getTerminology(input map[string]any) (map[string]any, error) {
	name, _ := input[keyName].(string)
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	term, err := h.Backend.GetTerminology(name)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"TerminologyProperties": terminologyToMap(term),
		"TerminologyDataLocation": map[string]any{
			"RepositoryType": "S3",
			"Location":       "s3://gopherstack-translate/terminology/" + name,
		},
	}, nil
}

func (h *Handler) deleteTerminology(input map[string]any) (map[string]any, error) {
	name, _ := input[keyName].(string)
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if err := h.Backend.DeleteTerminology(name); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) listTerminologies(input map[string]any) (map[string]any, error) {
	maxResults := maxResultsField(input)
	nextToken, _ := input["NextToken"].(string)

	list, outToken := h.Backend.ListTerminologies(maxResults, nextToken)

	props := make([]map[string]any, 0, len(list))
	for _, t := range list {
		props = append(props, terminologyToMap(t))
	}

	result := map[string]any{
		"TerminologyPropertiesList": props,
	}

	if outToken != "" {
		result["NextToken"] = outToken
	}

	return result, nil
}

// --- Parallel Data ---

func (h *Handler) createParallelData(input map[string]any) (map[string]any, error) {
	name, _ := input[keyName].(string)
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	description, _ := input["Description"].(string)
	cfg := extractParallelDataConfig(input)
	encKey := extractEncryptionKey(input)
	tags := extractTags(input)

	pd, err := h.Backend.CreateParallelData(name, description, cfg, encKey, tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyName:   pd.Name,
		keyStatus: pd.Status,
	}, nil
}

func (h *Handler) getParallelData(input map[string]any) (map[string]any, error) {
	name, _ := input[keyName].(string)
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	pd, err := h.Backend.GetParallelData(name)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"ParallelDataProperties": parallelDataToMap(pd),
		"DataLocation": map[string]any{
			"RepositoryType": "S3",
			"Location":       "s3://gopherstack-translate/parallel-data/" + name,
		},
	}, nil
}

func (h *Handler) updateParallelData(input map[string]any) (map[string]any, error) {
	name, _ := input[keyName].(string)
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	description, _ := input["Description"].(string)
	cfg := extractParallelDataConfig(input)

	pd, err := h.Backend.UpdateParallelData(name, description, cfg)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyName:                     pd.Name,
		keyStatus:                   pd.Status,
		"LatestUpdateAttemptStatus": "ACTIVE",
	}, nil
}

func (h *Handler) deleteParallelData(input map[string]any) (map[string]any, error) {
	name, _ := input[keyName].(string)
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	pd, err := h.Backend.DeleteParallelData(name)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyName:   pd.Name,
		keyStatus: pd.Status,
	}, nil
}

func (h *Handler) listParallelData(input map[string]any) (map[string]any, error) {
	maxResults := maxResultsField(input)
	nextToken, _ := input["NextToken"].(string)

	list, outToken := h.Backend.ListParallelData(maxResults, nextToken)

	props := make([]map[string]any, 0, len(list))
	for _, pd := range list {
		props = append(props, parallelDataToMap(pd))
	}

	result := map[string]any{
		"ParallelDataPropertiesList": props,
	}

	if outToken != "" {
		result["NextToken"] = outToken
	}

	return result, nil
}

// --- Translation Jobs ---

func (h *Handler) startTextTranslationJob(input map[string]any) (map[string]any, error) {
	jobName, _ := input["JobName"].(string)
	dataAccessRoleARN, _ := input["DataAccessRoleArn"].(string)
	sourceLang, _ := input[keySourceLanguageCode].(string)

	targetLangs := strSliceField(input, "TargetLanguageCodes")
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

// --- Translation ---

func (h *Handler) translateText(input map[string]any) (map[string]any, error) {
	text, _ := input["Text"].(string)
	if text == "" {
		return nil, fmt.Errorf("%w: Text is required", ErrValidation)
	}

	sourceLang, _ := input[keySourceLanguageCode].(string)
	targetLang, _ := input[keyTargetLanguageCode].(string)

	if targetLang == "" {
		return nil, fmt.Errorf("%w: TargetLanguageCode is required", ErrValidation)
	}

	if sourceLang == "" {
		sourceLang = "auto"
	}

	termNames := strSliceField(input, "TerminologyNames")
	translated := applyTranslation(text, sourceLang, targetLang, h.Backend.LookupTerminologies(termNames))

	return map[string]any{
		"TranslatedText":      translated,
		keySourceLanguageCode: sourceLang,
		keyTargetLanguageCode: targetLang,
		"AppliedSettings":     map[string]any{},
	}, nil
}

func (h *Handler) translateDocument(input map[string]any) (map[string]any, error) {
	sourceLang, _ := input[keySourceLanguageCode].(string)
	targetLang, _ := input[keyTargetLanguageCode].(string)

	if targetLang == "" {
		return nil, fmt.Errorf("%w: TargetLanguageCode is required", ErrValidation)
	}

	if sourceLang == "" {
		sourceLang = "auto"
	}

	var content string
	if doc, ok := input["Document"].(map[string]any); ok {
		content, _ = doc["Content"].(string)
	}

	termNames := strSliceField(input, "TerminologyNames")
	translated := applyTranslation(content, sourceLang, targetLang, h.Backend.LookupTerminologies(termNames))

	return map[string]any{
		"TranslatedDocument":  map[string]any{"Content": translated},
		keySourceLanguageCode: sourceLang,
		keyTargetLanguageCode: targetLang,
		"AppliedSettings":     map[string]any{},
	}, nil
}

// --- Languages ---

func (h *Handler) listLanguages(input map[string]any) (map[string]any, error) {
	maxResults := maxResultsField(input)
	if maxResults <= 0 {
		maxResults = 500
	}

	languages := knownLanguages()
	if maxResults < len(languages) {
		languages = languages[:maxResults]
	}

	return map[string]any{
		"Languages":           languages,
		"DisplayLanguageCode": "en",
	}, nil
}

// --- Tags ---

func (h *Handler) tagResource(input map[string]any) (map[string]any, error) {
	resourceARN, _ := input[keyResourceARN].(string)
	if resourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrValidation)
	}

	tags := extractTagsFromSlice(input, "Tags")

	if err := h.Backend.TagResource(resourceARN, tags); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) untagResource(input map[string]any) (map[string]any, error) {
	resourceARN, _ := input[keyResourceARN].(string)
	if resourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrValidation)
	}

	keys := strSliceField(input, "TagKeys")

	if err := h.Backend.UntagResource(resourceARN, keys); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) listTagsForResource(input map[string]any) (map[string]any, error) {
	resourceARN, _ := input[keyResourceARN].(string)
	if resourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrValidation)
	}

	tags, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		return nil, err
	}

	tagSlice := make([]map[string]any, 0, len(tags))

	for k, v := range tags {
		tagSlice = append(tagSlice, map[string]any{"Key": k, "Value": v})
	}

	return map[string]any{
		"Tags": tagSlice,
	}, nil
}

// --- Helpers ---

func terminologyToMap(t *Terminology) map[string]any {
	m := map[string]any{
		"Arn":                 t.ARN,
		keyName:               t.Name,
		"Description":         t.Description,
		"Directionality":      t.Directionality,
		"Format":              t.Format,
		"SizeBytes":           t.SizeBytes,
		"TermCount":           t.TermCount,
		"CreatedAt":           awstime.Epoch(t.CreatedAt),
		"LastUpdatedAt":       awstime.Epoch(t.LastUpdatedAt),
		keySourceLanguageCode: t.SourceLanguage,
	}

	if t.EncryptionKey != nil {
		m["EncryptionKey"] = map[string]any{
			"Id":   t.EncryptionKey.ID,
			"Type": t.EncryptionKey.Type,
		}
	}

	return m
}

func parallelDataToMap(pd *ParallelData) map[string]any {
	m := map[string]any{
		"Arn":                 pd.ARN,
		keyName:               pd.Name,
		"Description":         pd.Description,
		keyStatus:             pd.Status,
		keySourceLanguageCode: pd.SourceLanguage,
		"TargetLanguageCodes": pd.TargetLanguages,
		"CreatedAt":           awstime.Epoch(pd.CreatedAt),
		"LastUpdatedAt":       awstime.Epoch(pd.LastUpdatedAt),
	}

	if pd.ParallelDataConfig != nil {
		m["ParallelDataConfig"] = map[string]any{
			"S3Uri":  pd.ParallelDataConfig.S3URI,
			"Format": pd.ParallelDataConfig.Format,
		}
	}

	return m
}

func jobToMap(job *TranslationJob) map[string]any {
	m := map[string]any{
		keyJobID:              job.JobID,
		"JobName":             job.JobName,
		keyJobStatus:          job.JobStatus,
		"DataAccessRoleArn":   job.DataAccessRoleARN,
		keySourceLanguageCode: job.SourceLanguage,
		"TargetLanguageCodes": job.TargetLanguages,
		"SubmittedTime":       awstime.Epoch(job.SubmittedAt),
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

func extractEncryptionKey(input map[string]any) *EncryptionKey {
	ek, ok := input["EncryptionKey"].(map[string]any)
	if !ok {
		return nil
	}

	return &EncryptionKey{
		ID:   strField(ek, "Id"),
		Type: strField(ek, "Type"),
	}
}

func extractParallelDataConfig(input map[string]any) *ParallelDataConfig {
	cfg, ok := input["ParallelDataConfig"].(map[string]any)
	if !ok {
		return nil
	}

	return &ParallelDataConfig{
		S3URI:  strField(cfg, "S3Uri"),
		Format: strField(cfg, "Format"),
	}
}

func extractTags(input map[string]any) map[string]string {
	return extractTagsFromSlice(input, "Tags")
}

func extractTagsFromSlice(input map[string]any, key string) map[string]string {
	raw, ok := input[key].([]any)
	if !ok {
		return nil
	}

	tags := make(map[string]string, len(raw))

	for _, item := range raw {
		m, mok := item.(map[string]any)
		if !mok {
			continue
		}

		k, _ := m["Key"].(string)
		v, _ := m["Value"].(string)

		if k != "" {
			tags[k] = v
		}
	}

	return tags
}

func strField(m map[string]any, key string) string {
	v, _ := m[key].(string)

	return v
}

func maxResultsField(m map[string]any) int {
	switch v := m["MaxResults"].(type) {
	case float64:
		return int(v)
	case int:
		return v
	}

	return 0
}

func strSliceField(m map[string]any, key string) []string {
	raw, ok := m[key].([]any)
	if !ok {
		return nil
	}

	out := make([]string, 0, len(raw))

	for _, v := range raw {
		if s, sok := v.(string); sok {
			out = append(out, s)
		}
	}

	return out
}

func knownLanguages() []map[string]any {
	return []map[string]any{
		{keyLanguageCode: "af", keyLanguageName: "Afrikaans"},
		{keyLanguageCode: "sq", keyLanguageName: "Albanian"},
		{keyLanguageCode: "am", keyLanguageName: "Amharic"},
		{keyLanguageCode: "ar", keyLanguageName: "Arabic"},
		{keyLanguageCode: "hy", keyLanguageName: "Armenian"},
		{keyLanguageCode: "az", keyLanguageName: "Azerbaijani"},
		{keyLanguageCode: "bn", keyLanguageName: "Bengali"},
		{keyLanguageCode: "bs", keyLanguageName: "Bosnian"},
		{keyLanguageCode: "bg", keyLanguageName: "Bulgarian"},
		{keyLanguageCode: "ca", keyLanguageName: "Catalan"},
		{keyLanguageCode: "zh", keyLanguageName: "Chinese (Simplified)"},
		{keyLanguageCode: "zh-TW", keyLanguageName: "Chinese (Traditional)"},
		{keyLanguageCode: "hr", keyLanguageName: "Croatian"},
		{keyLanguageCode: "cs", keyLanguageName: "Czech"},
		{keyLanguageCode: "da", keyLanguageName: "Danish"},
		{keyLanguageCode: "fa-AF", keyLanguageName: "Dari"},
		{keyLanguageCode: "nl", keyLanguageName: "Dutch"},
		{keyLanguageCode: "en", keyLanguageName: "English"},
		{keyLanguageCode: "et", keyLanguageName: "Estonian"},
		{keyLanguageCode: "fa", keyLanguageName: "Farsi (Persian)"},
		{keyLanguageCode: "tl", keyLanguageName: "Filipino, Tagalog"},
		{keyLanguageCode: "fi", keyLanguageName: "Finnish"},
		{keyLanguageCode: "fr", keyLanguageName: "French"},
		{keyLanguageCode: "fr-CA", keyLanguageName: "French (Canada)"},
		{keyLanguageCode: "ka", keyLanguageName: "Georgian"},
		{keyLanguageCode: "de", keyLanguageName: "German"},
		{keyLanguageCode: "el", keyLanguageName: "Greek"},
		{keyLanguageCode: "gu", keyLanguageName: "Gujarati"},
		{keyLanguageCode: "ht", keyLanguageName: "Haitian Creole"},
		{keyLanguageCode: "ha", keyLanguageName: "Hausa"},
		{keyLanguageCode: "he", keyLanguageName: "Hebrew"},
		{keyLanguageCode: "hi", keyLanguageName: "Hindi"},
		{keyLanguageCode: "hu", keyLanguageName: "Hungarian"},
		{keyLanguageCode: "id", keyLanguageName: "Indonesian"},
		{keyLanguageCode: "ga", keyLanguageName: "Irish"},
		{keyLanguageCode: "it", keyLanguageName: "Italian"},
		{keyLanguageCode: "ja", keyLanguageName: "Japanese"},
		{keyLanguageCode: "kn", keyLanguageName: "Kannada"},
		{keyLanguageCode: "kk", keyLanguageName: "Kazakh"},
		{keyLanguageCode: "ko", keyLanguageName: "Korean"},
		{keyLanguageCode: "lv", keyLanguageName: "Latvian"},
		{keyLanguageCode: "lt", keyLanguageName: "Lithuanian"},
		{keyLanguageCode: "mk", keyLanguageName: "Macedonian"},
		{keyLanguageCode: "ms", keyLanguageName: "Malay"},
		{keyLanguageCode: "ml", keyLanguageName: "Malayalam"},
		{keyLanguageCode: "mt", keyLanguageName: "Maltese"},
		{keyLanguageCode: "mr", keyLanguageName: "Marathi"},
		{keyLanguageCode: "mn", keyLanguageName: "Mongolian"},
		{keyLanguageCode: "no", keyLanguageName: "Norwegian"},
		{keyLanguageCode: "ps", keyLanguageName: "Pashto"},
		{keyLanguageCode: "pl", keyLanguageName: "Polish"},
		{keyLanguageCode: "pt", keyLanguageName: "Portuguese (Brazil)"},
		{keyLanguageCode: "pt-PT", keyLanguageName: "Portuguese (Portugal)"},
		{keyLanguageCode: "pa", keyLanguageName: "Punjabi"},
		{keyLanguageCode: "ro", keyLanguageName: "Romanian"},
		{keyLanguageCode: "ru", keyLanguageName: "Russian"},
		{keyLanguageCode: "sr", keyLanguageName: "Serbian"},
		{keyLanguageCode: "si", keyLanguageName: "Sinhala"},
		{keyLanguageCode: "sk", keyLanguageName: "Slovak"},
		{keyLanguageCode: "sl", keyLanguageName: "Slovenian"},
		{keyLanguageCode: "so", keyLanguageName: "Somali"},
		{keyLanguageCode: "es", keyLanguageName: "Spanish"},
		{keyLanguageCode: "es-MX", keyLanguageName: "Spanish (Mexico)"},
		{keyLanguageCode: "sw", keyLanguageName: "Swahili"},
		{keyLanguageCode: "sv", keyLanguageName: "Swedish"},
		{keyLanguageCode: "ta", keyLanguageName: "Tamil"},
		{keyLanguageCode: "te", keyLanguageName: "Telugu"},
		{keyLanguageCode: "th", keyLanguageName: "Thai"},
		{keyLanguageCode: "tr", keyLanguageName: "Turkish"},
		{keyLanguageCode: "uk", keyLanguageName: "Ukrainian"},
		{keyLanguageCode: "ur", keyLanguageName: "Urdu"},
		{keyLanguageCode: "uz", keyLanguageName: "Uzbek"},
		{keyLanguageCode: "vi", keyLanguageName: "Vietnamese"},
		{keyLanguageCode: "cy", keyLanguageName: "Welsh"},
	}
}

// applyTranslation applies terminology substitutions and a simple language transform.
// Terminologies take priority; remaining text gets a minimal transform to avoid echo.
func applyTranslation(text, sourceLang, targetLang string, terms []*Terminology) string {
	if text == "" || sourceLang == targetLang {
		return text
	}

	// Apply terminology CSV substitutions first.
	for _, term := range terms {
		if term.TerminologyData == nil || len(term.TerminologyData.File) == 0 {
			continue
		}
		text = applyCSVTerminology(text, term.TerminologyData.File)
	}

	// Simple lang-to-prefix map for a handful of common targets, so output
	// differs visibly from the source without requiring a real translation engine.
	langPrefixes := map[string]string{
		"es": "es: ", "fr": "fr: ", "de": "de: ", "ja": "ja: ", "zh": "zh: ",
		"pt": "pt: ", "it": "it: ", "ru": "ru: ", "ar": "ar: ", "ko": "ko: ",
		"nl": "nl: ", "pl": "pl: ", "sv": "sv: ", "tr": "tr: ", "hi": "hi: ",
	}

	// Only add prefix if text doesn't already have one (avoids double-prefixing on retry).
	prefix := langPrefixes[targetLang]
	if prefix != "" && !strings.HasPrefix(text, prefix) {
		text = prefix + text
	}

	return text
}

// applyCSVTerminology parses a simple two-column CSV (source,target) and replaces
// occurrences of source terms in text with the corresponding target terms.
func applyCSVTerminology(text string, csvBytes []byte) string {
	const csvColumns = 2 // source,target
	for line := range strings.SplitSeq(string(csvBytes), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, ",", csvColumns)
		if len(parts) != csvColumns {
			continue
		}
		src := strings.TrimSpace(parts[0])
		tgt := strings.TrimSpace(parts[1])
		if src != "" && tgt != "" {
			text = strings.ReplaceAll(text, src, tgt)
		}
	}

	return text
}
