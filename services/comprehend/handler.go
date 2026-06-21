package comprehend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	comprehendTargetPrefix     = "Comprehend_20171127."
	comprehendContentType      = "application/x-amz-json-1.1"
	unknownOperation           = "Unknown"
	fieldDocumentClassifierARN = "DocumentClassifierArn"
	fieldEntityRecognizerARN   = "EntityRecognizerArn"
	fieldEndpointARN           = "EndpointArn"
	fieldFlywheelARN           = "FlywheelArn"
	fieldDatasetARN            = "DatasetArn"
	fieldJobID                 = "JobId"
	fieldJobStatus             = "JobStatus"
	fieldLanguageCode          = "LanguageCode"
	fieldText                  = "Text"
	fieldScore                 = "Score"
	fieldEntities              = "Entities"
	fieldLabels                = "Labels"
	fieldName                  = "Name"
	fieldBeginOffset           = "BeginOffset"
	fieldEndOffset             = "EndOffset"
	lowSentimentScore          = 0.01
	neutralSentimentScore      = 0.97
)

type operation func(map[string]any) (map[string]any, error)

type resourceSpec struct {
	resourceType string
	nameField    string
	arnField     string
	objectField  string
	listField    string
}

type jobSpec struct {
	jobType     string
	objectField string
	listField   string
}

// Handler serves Amazon Comprehend JSON operations.
type Handler struct {
	Backend *InMemoryBackend
	ops     map[string]operation
}

// NewHandler creates Comprehend handler backed by in-memory state.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOperations()

	return h
}

// Reset clears backend state.
func (h *Handler) Reset() { h.Backend.Reset() }

// Name returns service name.
func (h *Handler) Name() string { return "Comprehend" }

// ChaosServiceName returns service key for fault matching.
func (h *Handler) ChaosServiceName() string { return "comprehend" }

// ChaosOperations returns fault-injectable operations.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns configured service region.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// MatchPriority returns header matching priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// RouteMatcher matches Comprehend X-Amz-Target headers.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), comprehendTargetPrefix)
	}
}

// ExtractOperation returns operation in request target.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	if !strings.HasPrefix(target, comprehendTargetPrefix) {
		return unknownOperation
	}

	return strings.TrimPrefix(target, comprehendTargetPrefix)
}

// ExtractResource retrieves common ARN and job identifier fields.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}
	var values map[string]any
	if unmarshalErr := json.Unmarshal(body, &values); unmarshalErr != nil {
		return ""
	}
	for _, key := range []string{
		"ResourceArn", fieldDocumentClassifierARN, fieldEntityRecognizerARN, fieldEndpointARN, fieldFlywheelARN,
		fieldDatasetARN, fieldJobID,
	} {
		if value, ok := values[key].(string); ok && value != "" {
			return value
		}
	}

	return ""
}

// GetSupportedOperations reports implemented operations.
func (h *Handler) GetSupportedOperations() []string {
	operations := make([]string, 0, len(h.ops))
	for name := range h.ops {
		operations = append(operations, name)
	}

	return operations
}

// Handler returns Echo JSON target dispatcher.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()), h.Name(), comprehendContentType,
			h.GetSupportedOperations(), h.dispatch, h.handleError,
		)
	}
}

func (h *Handler) dispatch(_ context.Context, action string, body []byte) ([]byte, error) {
	handler, ok := h.ops[action]
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
	output, err := handler(input)
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

	payload, marshalErr := json.Marshal(service.JSONErrorResponse{Type: code, Message: err.Error()})
	if marshalErr != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}

	c.Response().Header().Set("Content-Type", comprehendContentType)

	return c.JSONBlob(status, payload)
}

func (h *Handler) buildOperations() map[string]operation {
	ops := map[string]operation{
		"DetectSentiment":             h.detectSentiment,
		"DetectEntities":              h.detectEntities,
		"DetectKeyPhrases":            h.detectKeyPhrases,
		"DetectPiiEntities":           h.detectPIIEntities,
		"DetectSyntax":                h.detectSyntax,
		"DetectDominantLanguage":      h.detectDominantLanguage,
		"DetectToxicContent":          h.detectToxicContent,
		"BatchDetectSentiment":        h.batch(h.detectSentiment),
		"BatchDetectEntities":         h.batch(h.detectEntities),
		"BatchDetectKeyPhrases":       h.batch(h.detectKeyPhrases),
		"BatchDetectPiiEntities":      h.batch(h.detectPIIEntities),
		"BatchDetectSyntax":           h.batch(h.detectSyntax),
		"BatchDetectDominantLanguage": h.batch(h.detectDominantLanguage),
		"TagResource":                 h.tagResource,
		"UntagResource":               h.untagResource,
		"ListTagsForResource":         h.listTags,
	}
	for prefix, spec := range asyncJobSpecs() {
		ops["Start"+prefix] = h.startJob(spec)
		ops["Describe"+prefix] = h.describeJob(spec)
		ops["List"+prefix+"s"] = h.listJobs(spec)
		ops["Stop"+prefix] = h.stopJob(spec)
	}
	for prefix, spec := range resourceSpecs() {
		ops["Create"+prefix] = h.createResource(spec)
		ops["Describe"+prefix] = h.describeResource(spec)
		ops["List"+prefix+"s"] = h.listResources(spec)
		ops["Delete"+prefix] = h.deleteResource(spec)
		if spec.resourceType == resourceTypeEndpoint || spec.resourceType == resourceTypeFlywheel {
			ops["Update"+prefix] = h.updateResource(spec)
		}
	}
	ops["StartFlywheelIteration"] = h.startIteration
	ops["GetFlywheelIteration"] = h.getIteration
	ops["DescribeFlywheelIteration"] = h.getIteration
	ops["ListFlywheelIterationHistory"] = h.listIterations

	ops["BatchDetectTargetedSentiment"] = h.batch(h.detectTargetedSentiment)
	ops["ClassifyDocument"] = h.classifyDocument
	ops["ContainsPiiEntities"] = h.containsPIIEntities
	ops["DeleteResourcePolicy"] = h.deleteResourcePolicy
	ops["DescribeResourcePolicy"] = h.describeResourcePolicy
	ops["DetectTargetedSentiment"] = h.detectTargetedSentiment
	ops["ImportModel"] = h.importModel
	ops["ListDocumentClassifierSummaries"] = h.listDocumentClassifierSummaries
	ops["ListEntityRecognizerSummaries"] = h.listEntityRecognizerSummaries
	ops["PutResourcePolicy"] = h.putResourcePolicy
	ops["StopTrainingDocumentClassifier"] = h.stopTrainingDocumentClassifier
	ops["StopTrainingEntityRecognizer"] = h.stopTrainingEntityRecognizer

	return ops
}

func asyncJobSpecs() map[string]jobSpec {
	return map[string]jobSpec{
		"DocumentClassificationJob": {
			jobType:     "document-classification-job",
			objectField: "DocumentClassificationJobProperties",
			listField:   "DocumentClassificationJobPropertiesList",
		},
		"EntitiesDetectionJob": {
			jobType:     "entities-detection-job",
			objectField: "EntitiesDetectionJobProperties",
			listField:   "EntitiesDetectionJobPropertiesList",
		},
		"KeyPhrasesDetectionJob": {
			jobType:     "key-phrases-detection-job",
			objectField: "KeyPhrasesDetectionJobProperties",
			listField:   "KeyPhrasesDetectionJobPropertiesList",
		},
		"SentimentDetectionJob": {
			jobType:     "sentiment-detection-job",
			objectField: "SentimentDetectionJobProperties",
			listField:   "SentimentDetectionJobPropertiesList",
		},
		"PiiEntitiesDetectionJob": {
			jobType:     "pii-entities-detection-job",
			objectField: "PiiEntitiesDetectionJobProperties",
			listField:   "PiiEntitiesDetectionJobPropertiesList",
		},
		"TopicsDetectionJob": {
			jobType:     "topics-detection-job",
			objectField: "TopicsDetectionJobProperties",
			listField:   "TopicsDetectionJobPropertiesList",
		},
		"TargetedSentimentDetectionJob": {
			jobType:     "targeted-sentiment-detection-job",
			objectField: "TargetedSentimentDetectionJobProperties",
			listField:   "TargetedSentimentDetectionJobPropertiesList",
		},
		"DominantLanguageDetectionJob": {
			jobType:     "dominant-language-detection-job",
			objectField: "DominantLanguageDetectionJobProperties",
			listField:   "DominantLanguageDetectionJobPropertiesList",
		},
		"EventsDetectionJob": {
			jobType:     "events-detection-job",
			objectField: "EventsDetectionJobProperties",
			listField:   "EventsDetectionJobPropertiesList",
		},
	}
}

func resourceSpecs() map[string]resourceSpec {
	return map[string]resourceSpec{
		"DocumentClassifier": {
			resourceType: resourceTypeDocClassifier,
			nameField:    "DocumentClassifierName",
			arnField:     fieldDocumentClassifierARN,
			objectField:  "DocumentClassifierProperties",
			listField:    "DocumentClassifierPropertiesList",
		},
		"DocumentClassifierVersion": {
			resourceType: resourceTypeDocClassifierVersion,
			nameField:    fieldDocumentClassifierARN,
			arnField:     fieldDocumentClassifierARN,
			objectField:  "DocumentClassifierProperties",
			listField:    "DocumentClassifierPropertiesList",
		},
		"EntityRecognizer": {
			resourceType: resourceTypeEntityRecognizer,
			nameField:    "RecognizerName",
			arnField:     fieldEntityRecognizerARN,
			objectField:  "EntityRecognizerProperties",
			listField:    "EntityRecognizerPropertiesList",
		},
		"EntityRecognizerVersion": {
			resourceType: resourceTypeEntityRecognizerVer,
			nameField:    fieldEntityRecognizerARN,
			arnField:     fieldEntityRecognizerARN,
			objectField:  "EntityRecognizerProperties",
			listField:    "EntityRecognizerPropertiesList",
		},
		"Endpoint": {
			resourceType: resourceTypeEndpoint,
			nameField:    "EndpointName",
			arnField:     fieldEndpointARN,
			objectField:  "EndpointProperties",
			listField:    "EndpointPropertiesList",
		},
		"Flywheel": {
			resourceType: resourceTypeFlywheel,
			nameField:    "FlywheelName",
			arnField:     fieldFlywheelARN,
			objectField:  "FlywheelProperties",
			listField:    "FlywheelPropertiesList",
		},
		"Dataset": {
			resourceType: resourceTypeDataset,
			nameField:    "DatasetName",
			arnField:     fieldDatasetARN,
			objectField:  "DatasetProperties",
			listField:    "DatasetPropertiesList",
		},
	}
}

func (h *Handler) startJob(spec jobSpec) operation {
	return func(input map[string]any) (map[string]any, error) {
		job, err := h.Backend.StartJob(spec.jobType, stringValue(input, "JobName", ""), input)
		if err != nil {
			return nil, err
		}

		return map[string]any{fieldJobID: job.JobID, "JobArn": job.JobArn, fieldJobStatus: job.JobStatus}, nil
	}
}

func (h *Handler) describeJob(spec jobSpec) operation {
	return func(input map[string]any) (map[string]any, error) {
		job, err := h.Backend.DescribeJob(stringValue(input, fieldJobID, ""), spec.jobType)
		if err != nil {
			return nil, err
		}

		return map[string]any{spec.objectField: jobMap(job)}, nil
	}
}

func (h *Handler) listJobs(spec jobSpec) operation {
	return func(_ map[string]any) (map[string]any, error) {
		jobs := h.Backend.ListJobs(spec.jobType)
		items := make([]map[string]any, 0, len(jobs))
		for _, job := range jobs {
			items = append(items, jobMap(job))
		}

		return map[string]any{spec.listField: items}, nil
	}
}

func (h *Handler) stopJob(spec jobSpec) operation {
	return func(input map[string]any) (map[string]any, error) {
		job, err := h.Backend.StopJob(stringValue(input, fieldJobID, ""), spec.jobType)
		if err != nil {
			return nil, err
		}

		return map[string]any{fieldJobID: job.JobID, fieldJobStatus: job.JobStatus}, nil
	}
}

func jobMap(job *Job) map[string]any {
	return map[string]any{
		fieldJobID: job.JobID, "JobArn": job.JobArn, "JobName": job.JobName, fieldJobStatus: job.JobStatus,
		fieldLanguageCode: job.LanguageCode,
		"SubmitTime":      awstime.Epoch(job.SubmitTime),
		"EndTime":         awstime.Epoch(job.EndTime),
		"FailureReason":   job.FailureReason, "InputDataConfig": job.InputDataConfig,
		"OutputDataConfig": job.OutputDataConfig, "DataAccessRoleArn": job.DataAccessRoleArn,
		fieldDocumentClassifierARN: job.DocumentClassifierArn, fieldEntityRecognizerARN: job.EntityRecognizerArn,
		"TargetEventTypes": job.TargetEventTypes,
	}
}

func (h *Handler) createResource(spec resourceSpec) operation {
	return func(input map[string]any) (map[string]any, error) {
		resource, err := h.Backend.CreateResource(
			spec.resourceType, stringValue(input, spec.nameField, ""), stringValue(input, "VersionName", ""),
			input, inputTags(input),
		)
		if err != nil {
			return nil, err
		}

		return map[string]any{spec.arnField: resource.Arn}, nil
	}
}

func (h *Handler) describeResource(spec resourceSpec) operation {
	return func(input map[string]any) (map[string]any, error) {
		resource, err := h.Backend.GetResource(stringValue(input, spec.arnField, ""), spec.resourceType)
		if err != nil {
			return nil, err
		}

		return map[string]any{spec.objectField: resourceMap(resource, spec)}, nil
	}
}

func (h *Handler) listResources(spec resourceSpec) operation {
	return func(_ map[string]any) (map[string]any, error) {
		resources := h.Backend.ListResources(spec.resourceType)
		items := make([]map[string]any, 0, len(resources))
		for _, resource := range resources {
			items = append(items, resourceMap(resource, spec))
		}

		return map[string]any{spec.listField: items}, nil
	}
}

func (h *Handler) updateResource(spec resourceSpec) operation {
	return func(input map[string]any) (map[string]any, error) {
		_, err := h.Backend.UpdateResource(stringValue(input, spec.arnField, ""), spec.resourceType, input)

		return map[string]any{}, err
	}
}

func (h *Handler) deleteResource(spec resourceSpec) operation {
	return func(input map[string]any) (map[string]any, error) {
		err := h.Backend.DeleteResource(stringValue(input, spec.arnField, ""), spec.resourceType)

		return map[string]any{}, err
	}
}

func resourceMap(resource *Resource, spec resourceSpec) map[string]any {
	out := cloneMap(resource.Configuration)
	out[spec.arnField] = resource.Arn
	out["Status"] = resource.Status
	out["SubmitTime"] = awstime.Epoch(resource.CreatedAt)
	out["EndTime"] = awstime.Epoch(resource.UpdatedAt)
	if resource.VersionName != "" {
		out["VersionName"] = resource.VersionName
	}

	return out
}

func (h *Handler) startIteration(input map[string]any) (map[string]any, error) {
	iteration, err := h.Backend.StartFlywheelIteration(stringValue(input, fieldFlywheelARN, ""))
	if err != nil {
		return nil, err
	}

	return map[string]any{"FlywheelIterationId": iteration.FlywheelIterationID}, nil
}

func (h *Handler) getIteration(input map[string]any) (map[string]any, error) {
	iteration, err := h.Backend.GetFlywheelIteration(stringValue(input, "FlywheelIterationId", ""))
	if err != nil {
		return nil, err
	}

	return map[string]any{"FlywheelIterationProperties": iterationMap(iteration)}, nil
}

func (h *Handler) listIterations(input map[string]any) (map[string]any, error) {
	iterations := h.Backend.ListFlywheelIterations(stringValue(input, fieldFlywheelARN, ""))
	items := make([]map[string]any, 0, len(iterations))
	for _, iteration := range iterations {
		items = append(items, iterationMap(iteration))
	}

	return map[string]any{"FlywheelIterationPropertiesList": items}, nil
}

func iterationMap(iteration *FlywheelIteration) map[string]any {
	return map[string]any{
		fieldFlywheelARN:          iteration.FlywheelArn,
		"FlywheelIterationId":     iteration.FlywheelIterationID,
		"FlywheelIterationStatus": iteration.FlywheelIterationStatus,
		"CreationTime":            awstime.Epoch(iteration.CreationTime),
		"EndTime":                 awstime.Epoch(iteration.EndTime),
		"Message":                 iteration.Message,
	}
}

func (h *Handler) tagResource(input map[string]any) (map[string]any, error) {
	err := h.Backend.TagResource(stringValue(input, "ResourceArn", ""), inputTags(input))

	return map[string]any{}, err
}

func (h *Handler) untagResource(input map[string]any) (map[string]any, error) {
	keys := stringSliceValue(input, "TagKeys")
	err := h.Backend.UntagResource(stringValue(input, "ResourceArn", ""), keys)

	return map[string]any{}, err
}

func (h *Handler) listTags(input map[string]any) (map[string]any, error) {
	tags, err := h.Backend.ListTags(stringValue(input, "ResourceArn", ""))
	if err != nil {
		return nil, err
	}

	return map[string]any{"ResourceArn": input["ResourceArn"], "Tags": tags}, nil
}

func inputTags(input map[string]any) []Tag {
	raw, _ := input["Tags"].([]any)
	tags := make([]Tag, 0, len(raw))
	for _, item := range raw {
		entry, _ := item.(map[string]any)
		tags = append(tags, Tag{Key: stringValue(entry, "Key", ""), Value: stringValue(entry, "Value", "")})
	}

	return tags
}

func positiveWordList() []string {
	return []string{
		"great", "love", "excellent", "wonderful", "amazing", "good", "happy", "best",
		"fantastic", "awesome", "beautiful", "perfect", "superb", "outstanding", "brilliant",
		"delightful", "pleased", "enjoy", "liked", "satisfied",
	}
}

func negativeWordList() []string {
	return []string{
		"bad", "hate", "terrible", "awful", "horrible", "worst", "angry", "sad",
		"disappointing", "poor", "dreadful", "disgusting", "upset", "frustrating",
		"dislike", "failed", "useless", "broken", "wrong", "awful",
	}
}

const (
	sentimentMixedScore   = 0.45
	sentimentBaseScore    = 0.92
	sentimentMinScore     = 0.01
	sentimentNeutralScore = 0.06
	sentimentMaxScore     = 0.99
	sentimentMixedMin     = 0.05
)

func countSentimentWords(wordSet map[string]bool, lower string, words []string) int {
	count := 0
	for _, w := range words {
		if wordSet[w] || strings.Contains(lower, w) {
			count++
		}
	}

	return count
}

func sentimentResult(posCount, negCount int) (string, float64, float64, float64, float64) {
	switch {
	case posCount > 0 && negCount > 0:
		return "MIXED", sentimentMixedScore, sentimentMixedScore, sentimentMixedMin, sentimentMixedMin
	case posCount > 0:
		ps := min(sentimentBaseScore+float64(posCount)*sentimentMinScore, sentimentMaxScore)

		return "POSITIVE", ps, sentimentMinScore, sentimentNeutralScore, sentimentMinScore
	case negCount > 0:
		ns := min(sentimentBaseScore+float64(negCount)*sentimentMinScore, sentimentMaxScore)

		return "NEGATIVE", sentimentMinScore, ns, sentimentNeutralScore, sentimentMinScore
	default:
		return "NEUTRAL", lowSentimentScore, lowSentimentScore, neutralSentimentScore, lowSentimentScore
	}
}

func (h *Handler) detectSentiment(input map[string]any) (map[string]any, error) {
	text, err := documentText(input)
	if err != nil {
		return nil, err
	}
	lower := strings.ToLower(text)
	words := strings.Fields(lower)
	wordSet := make(map[string]bool, len(words))
	for _, w := range words {
		wordSet[strings.Trim(w, ".,!?;:")] = true
	}

	posCount := countSentimentWords(wordSet, lower, positiveWordList())
	negCount := countSentimentWords(wordSet, lower, negativeWordList())
	sentiment, posScore, negScore, neuScore, mixScore := sentimentResult(posCount, negCount)

	return map[string]any{
		"Sentiment": sentiment,
		"SentimentScore": map[string]float64{
			"Positive": posScore, "Negative": negScore,
			"Neutral": neuScore, "Mixed": mixScore,
		},
	}, nil
}

func orgSuffixList() []string {
	return []string{
		" inc", " inc.", " corp", " corp.", " ltd", " ltd.", " llc", " co.", " company",
		" university", " institute", " foundation", " association", " corporation",
		" group", " holdings", " technologies", " solutions",
	}
}

func locSuffixList() []string {
	return []string{
		" street", " avenue", " road", " drive", " boulevard", " lane", " way",
		" city", " town", " village", " county", " state", " country", " nation",
		" river", " lake", " mountain", " park",
	}
}

func locPrefixList() []string {
	return []string{
		"mount ", "lake ", "north ", "south ", "east ", "west ", "new ",
	}
}

func quantityWordList() []string {
	return []string{
		"thousand", "million", "billion", "percent", "kg", "lb", "km", "mile",
	}
}

func dateWordList() []string {
	return []string{
		"january", "february", "march", "april", "may", "june", "july", "august",
		"september", "october", "november", "december", "monday", "tuesday",
		"wednesday", "thursday", "friday", "saturday", "sunday", "yesterday",
		"tomorrow", "today",
	}
}

func entityType(word, textLower string) string {
	wl := strings.ToLower(word)
	for _, sfx := range orgSuffixList() {
		if strings.HasSuffix(textLower, wl+sfx) || strings.Contains(textLower, wl+sfx+" ") {
			return "ORGANIZATION"
		}
	}
	for _, sfx := range locSuffixList() {
		if strings.HasSuffix(wl, strings.TrimSpace(sfx)) {
			return "LOCATION"
		}
	}
	for _, pfx := range locPrefixList() {
		if strings.HasPrefix(wl, strings.TrimSpace(pfx)) {
			return "LOCATION"
		}
	}
	for _, q := range quantityWordList() {
		if strings.Contains(wl, q) {
			return "QUANTITY"
		}
	}
	for _, d := range dateWordList() {
		if strings.EqualFold(word, d) {
			return "DATE"
		}
	}

	return "PERSON"
}

func (h *Handler) detectEntities(input map[string]any) (map[string]any, error) {
	text, err := documentText(input)
	if err != nil {
		return nil, err
	}
	textLower := strings.ToLower(text)
	entities := make([]map[string]any, 0)
	for word := range strings.FieldsSeq(text) {
		cleaned := strings.Trim(word, ".,!?;:")
		if cleaned == "" {
			continue
		}
		r, _ := utf8.DecodeRuneInString(cleaned)
		if !unicode.IsUpper(r) {
			continue
		}
		kind := entityType(cleaned, textLower)
		entities = append(entities, matchResult(text, cleaned, kind))
	}

	return map[string]any{"Entities": entities}, nil
}

func (h *Handler) detectKeyPhrases(input map[string]any) (map[string]any, error) {
	text, err := documentText(input)
	if err != nil {
		return nil, err
	}
	phrase := strings.TrimSpace(text)
	if index := strings.IndexAny(phrase, ".!?"); index >= 0 {
		phrase = phrase[:index]
	}

	return map[string]any{"KeyPhrases": []map[string]any{matchResult(text, phrase, "")}}, nil
}

func (h *Handler) detectPIIEntities(input map[string]any) (map[string]any, error) {
	text, err := documentText(input)
	if err != nil {
		return nil, err
	}
	patterns := []struct {
		expression *regexp.Regexp
		kind       string
	}{
		{regexp.MustCompile(`[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}`), "EMAIL"},
		{regexp.MustCompile(`\b\d{3}-\d{2}-\d{4}\b`), "SSN"},
	}
	entities := make([]map[string]any, 0)
	for _, pattern := range patterns {
		for _, match := range pattern.expression.FindAllString(text, -1) {
			entities = append(entities, matchResult(text, match, pattern.kind))
		}
	}

	return map[string]any{"Entities": entities}, nil
}

func (h *Handler) detectSyntax(input map[string]any) (map[string]any, error) {
	text, err := documentText(input)
	if err != nil {
		return nil, err
	}
	tokens := make([]map[string]any, 0)
	searchFrom := 0
	for index, token := range strings.Fields(text) {
		idx := strings.Index(text[searchFrom:], token)
		if idx < 0 {
			continue
		}
		begin := searchFrom + idx
		end := begin + len(token)
		tokens = append(tokens, map[string]any{
			"TokenId": index + 1, fieldText: token, fieldBeginOffset: begin, fieldEndOffset: end,
			"PartOfSpeech": map[string]any{"Tag": "NOUN", fieldScore: defaultScore},
		})
		searchFrom = end
	}

	return map[string]any{"SyntaxTokens": tokens}, nil
}

func (h *Handler) detectDominantLanguage(input map[string]any) (map[string]any, error) {
	text, err := documentText(input)
	if err != nil {
		return nil, err
	}

	lang := dominantLanguage(text)

	return map[string]any{
		"Languages": []map[string]any{{fieldLanguageCode: lang, fieldScore: defaultScore}},
	}, nil
}

const asciiMaxChar = 127

type scriptCounts struct {
	cjk, cyrillic, arabic, devanagari, hebrew, latin, nonASCII int
}

func isCJK(r rune) bool {
	return (r >= 0x4E00 && r <= 0x9FFF) || // CJK Unified Ideographs
		(r >= 0x3040 && r <= 0x30FF) || // Hiragana/Katakana
		(r >= 0xAC00 && r <= 0xD7AF) // Hangul
}

func isLatinLetter(r rune) bool {
	return (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z')
}

func classifyRune(r rune, c *scriptCounts) {
	switch {
	case isCJK(r):
		c.cjk++
	case r >= 0x0400 && r <= 0x04FF: // Cyrillic
		c.cyrillic++
	case r >= 0x0600 && r <= 0x06FF: // Arabic
		c.arabic++
	case r >= 0x0590 && r <= 0x05FF: // Hebrew
		c.hebrew++
	case r >= 0x0900 && r <= 0x097F: // Devanagari
		c.devanagari++
	case isLatinLetter(r):
		c.latin++
	case r > asciiMaxChar:
		c.nonASCII++
	}
}

func countScripts(text string) scriptCounts {
	var c scriptCounts
	for _, r := range text {
		classifyRune(r, &c)
	}

	return c
}

func dominantLanguage(text string) string {
	c := countScripts(text)
	total := c.cjk + c.cyrillic + c.arabic + c.devanagari + c.hebrew + c.nonASCII
	if total == 0 {
		return "en"
	}
	switch {
	case c.cjk*2 > total:
		return "zh"
	case c.cyrillic*2 > total:
		return "ru"
	case c.arabic*2 > total:
		return "ar"
	case c.devanagari*2 > total:
		return "hi"
	case c.hebrew*2 > total:
		return "he"
	case c.nonASCII > c.latin:
		return "fr"
	default:
		return "en"
	}
}

func (h *Handler) detectToxicContent(input map[string]any) (map[string]any, error) {
	segments, _ := input["TextSegments"].([]any)
	result := make([]map[string]any, 0, len(segments))
	for _, segment := range segments {
		entry, _ := segment.(map[string]any)
		text := stringValue(entry, fieldText, "")
		score := 0.01
		if strings.Contains(strings.ToLower(text), "hate") {
			score = 0.99
		}
		result = append(result, map[string]any{
			"Toxicity":  score,
			fieldLabels: []map[string]any{{fieldName: "HATE_SPEECH", fieldScore: score}},
		})
	}

	return map[string]any{"ResultList": result}, nil
}

func (h *Handler) batch(detector operation) operation {
	return func(input map[string]any) (map[string]any, error) {
		texts, _ := input["TextList"].([]any)
		results := make([]map[string]any, 0, len(texts))
		for index, rawText := range texts {
			result, err := detector(map[string]any{fieldText: rawText, fieldLanguageCode: input[fieldLanguageCode]})
			if err != nil {
				return nil, err
			}
			result["Index"] = index
			results = append(results, result)
		}

		return map[string]any{"ResultList": results, "ErrorList": []any{}}, nil
	}
}

func documentText(input map[string]any) (string, error) {
	text := stringValue(input, fieldText, "")
	if strings.TrimSpace(text) == "" {
		return "", fmt.Errorf("%w: Text is required", ErrValidation)
	}

	return text, nil
}

func matchResult(text, match, kind string) map[string]any {
	begin := strings.Index(text, match)
	begin = max(begin, 0)
	out := map[string]any{
		fieldText: match, fieldScore: defaultScore, fieldBeginOffset: begin, fieldEndOffset: begin + len(match),
	}
	if kind != "" {
		out["Type"] = kind
	}

	return out
}

func (h *Handler) classifyDocument(input map[string]any) (map[string]any, error) {
	text, err := documentText(input)
	if err != nil {
		return nil, err
	}

	lower := strings.ToLower(text)
	className := "OTHER"
	switch {
	case strings.Contains(lower, "finance") || strings.Contains(lower, "money"):
		className = "FINANCE"
	case strings.Contains(lower, "sports") || strings.Contains(lower, "game"):
		className = "SPORTS"
	case strings.Contains(lower, "tech") || strings.Contains(lower, "computer"):
		className = "TECHNOLOGY"
	}

	return map[string]any{
		"Classes": []map[string]any{
			{fieldName: className, fieldScore: defaultScore},
		},
		fieldLabels: []map[string]any{
			{fieldName: className, fieldScore: defaultScore},
		},
	}, nil
}

func (h *Handler) containsPIIEntities(input map[string]any) (map[string]any, error) {
	text, err := documentText(input)
	if err != nil {
		return nil, err
	}
	patterns := []struct {
		expression *regexp.Regexp
		kind       string
	}{
		{regexp.MustCompile(`[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}`), "EMAIL"},
		{regexp.MustCompile(`\b\d{3}-\d{2}-\d{4}\b`), "SSN"},
	}
	seen := make(map[string]bool)
	labels := []map[string]any{}
	for _, pattern := range patterns {
		if pattern.expression.MatchString(text) && !seen[pattern.kind] {
			seen[pattern.kind] = true
			labels = append(labels, map[string]any{fieldName: pattern.kind, fieldScore: defaultScore})
		}
	}

	return map[string]any{
		fieldLabels: labels,
	}, nil
}

func (h *Handler) detectTargetedSentiment(input map[string]any) (map[string]any, error) {
	text, err := documentText(input)
	if err != nil {
		return nil, err
	}

	entities := make([]map[string]any, 0)
	lower := strings.ToLower(text)
	sentiment := "NEUTRAL"
	switch {
	case strings.Contains(lower, "great") || strings.Contains(lower, "love") || strings.Contains(lower, "excellent"):
		sentiment = "POSITIVE"
	case strings.Contains(lower, "bad") || strings.Contains(lower, "hate") || strings.Contains(lower, "terrible"):
		sentiment = "NEGATIVE"
	}

	for word := range strings.FieldsSeq(text) {
		cleaned := strings.Trim(word, ".,!?")
		if cleaned != "" && unicode.IsUpper(rune(cleaned[0])) {
			entity := matchResult(text, cleaned, "PERSON")
			// Targeted sentiment specific fields
			mentions := []map[string]any{
				{
					"Score": defaultScore, "Text": cleaned, "Type": "PERSON",
					fieldBeginOffset: entity[fieldBeginOffset], fieldEndOffset: entity[fieldEndOffset],
					"MentionSentiment": map[string]any{
						"Sentiment": sentiment,
						"SentimentScore": map[string]float64{
							"Positive": lowSentimentScore, "Negative": lowSentimentScore,
							"Neutral": neutralSentimentScore, "Mixed": lowSentimentScore,
						},
					},
				},
			}
			entity["Mentions"] = mentions
			entities = append(entities, entity)
		}
	}

	return map[string]any{
		fieldEntities: entities,
	}, nil
}

func (h *Handler) deleteResourcePolicy(input map[string]any) (map[string]any, error) {
	err := h.Backend.DeleteResourcePolicy(
		stringValue(input, "ResourceArn", ""),
		stringValue(input, "PolicyRevisionId", ""),
	)

	return map[string]any{}, err
}

func (h *Handler) describeResourcePolicy(input map[string]any) (map[string]any, error) {
	policy, revision, err := h.Backend.GetResourcePolicy(stringValue(input, "ResourceArn", ""))
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"ResourcePolicy":   policy,
		"CreationTime":     awstime.Epoch(time.Now().UTC()),
		"LastModifiedTime": awstime.Epoch(time.Now().UTC()),
		"PolicyRevisionId": revision,
	}, nil
}

func (h *Handler) putResourcePolicy(input map[string]any) (map[string]any, error) {
	revision, err := h.Backend.PutResourcePolicy(
		stringValue(input, "ResourceArn", ""),
		stringValue(input, "ResourcePolicy", ""),
		stringValue(input, "PolicyRevisionId", ""),
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"PolicyRevisionId": revision,
	}, nil
}

func (h *Handler) importModel(input map[string]any) (map[string]any, error) {
	// ImportModel effectively creates a resource modeled after a source model.
	// For simplicity in emulation, we map it to creating a new DocumentClassifier (or EntityRecognizer).
	// We'll assume DocumentClassifier by default as AWS docs specify it can be either based on source.
	resource, err := h.Backend.CreateResource(
		resourceTypeDocClassifier,
		stringValue(input, "ModelName", ""),
		stringValue(input, "VersionName", ""),
		input,
		inputTags(input),
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"ModelArn": resource.Arn,
	}, nil
}

func (h *Handler) listDocumentClassifierSummaries(_ map[string]any) (map[string]any, error) {
	resources := h.Backend.ListResources(resourceTypeDocClassifier)
	items := make([]map[string]any, 0, len(resources))
	for _, resource := range resources {
		items = append(items, map[string]any{
			"DocumentClassifierName": resource.Name,
			"NumberOfVersions":       1,
			"LatestVersionCreatedAt": awstime.Epoch(resource.CreatedAt),
			"LatestVersionName":      resource.VersionName,
			"LatestVersionStatus":    resource.Status,
		})
	}

	return map[string]any{
		"DocumentClassifierSummariesList": items,
	}, nil
}

func (h *Handler) listEntityRecognizerSummaries(_ map[string]any) (map[string]any, error) {
	resources := h.Backend.ListResources(resourceTypeEntityRecognizer)
	items := make([]map[string]any, 0, len(resources))
	for _, resource := range resources {
		items = append(items, map[string]any{
			"RecognizerName":         resource.Name,
			"NumberOfVersions":       1,
			"LatestVersionCreatedAt": awstime.Epoch(resource.CreatedAt),
			"LatestVersionName":      resource.VersionName,
			"LatestVersionStatus":    resource.Status,
		})
	}

	return map[string]any{
		"EntityRecognizerSummariesList": items,
	}, nil
}

func (h *Handler) stopTrainingDocumentClassifier(input map[string]any) (map[string]any, error) {
	err := h.Backend.StopTrainingResource(stringValue(input, fieldDocumentClassifierARN, ""), resourceTypeDocClassifier)

	return map[string]any{}, err
}

func (h *Handler) stopTrainingEntityRecognizer(input map[string]any) (map[string]any, error) {
	err := h.Backend.StopTrainingResource(
		stringValue(input, fieldEntityRecognizerARN, ""),
		resourceTypeEntityRecognizer,
	)

	return map[string]any{}, err
}
