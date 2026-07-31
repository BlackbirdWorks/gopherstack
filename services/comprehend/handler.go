package comprehend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

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
	// noDelete marks a resource family that has no real Delete*/SDK operation at
	// all (verified against aws-sdk-go-v2/service/comprehend.Client: Dataset has
	// no Client.DeleteDataset -- real Comprehend datasets are immutable once
	// created, unlike DocumentClassifier/EntityRecognizer/Endpoint/Flywheel,
	// which all have a real Delete op). buildOperations skips generating
	// "Delete"+prefix when this is set, instead of advertising/dispatching a
	// fabricated operation -- see gopherstack-vhw2 category A.
	noDelete bool
}

// jobSpec describes one async job family's wire shape. The *Properties
// shapes are NOT uniform across job families in the real API -- e.g.
// DocumentClassificationJobProperties has DocumentClassifierArn+FlywheelArn
// but no LanguageCode, while EntitiesDetectionJobProperties has
// EntityRecognizerArn+FlywheelArn+LanguageCode, and
// PiiEntitiesDetectionJobProperties has Mode+RedactionConfig but no
// VolumeKmsKeyId/VpcConfig at all. These booleans were field-diffed against
// each family's real Properties struct in aws-sdk-go-v2/service/comprehend/types
// (see jobMap in handler_jobs.go) so each Describe/List response only ever
// carries fields the real shape actually has.
type jobSpec struct {
	jobType                  string
	objectField              string
	listField                string
	hasLanguageCode          bool
	hasDocumentClassifierArn bool
	hasEntityRecognizerArn   bool
	hasFlywheelArn           bool
	hasVolumeKmsKeyID        bool
	hasVpcConfig             bool
	hasTargetEventTypes      bool
	hasPiiMode               bool
	hasNumberOfTopics        bool
	// noStop marks a job family that has no real Stop*Job SDK operation at all
	// (verified against aws-sdk-go-v2/service/comprehend.Client: only 7 of the 9
	// job families have a Stop method -- DocumentClassificationJob and
	// TopicsDetectionJob, once started, cannot be cancelled via the real API).
	// buildOperations skips generating "Stop"+prefix when this is set, instead
	// of advertising/dispatching a fabricated operation -- see
	// gopherstack-vhw2 category A.
	noStop bool
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
	case errors.Is(err, ErrJobNotFound):
		code, status = "JobNotFoundException", http.StatusBadRequest
	case errors.Is(err, ErrTooManyTags):
		code, status = "TooManyTagsException", http.StatusBadRequest
	case errors.Is(err, ErrBatchSizeLimitExceeded):
		code, status = "BatchSizeLimitExceededException", http.StatusBadRequest
	case errors.Is(err, ErrTextSizeLimitExceeded):
		code, status = "TextSizeLimitExceededException", http.StatusBadRequest
	case errors.Is(err, ErrUnsupportedLanguage):
		code, status = "UnsupportedLanguageException", http.StatusBadRequest
	case errors.Is(err, ErrKmsKeyValidation):
		code, status = "KmsKeyValidationException", http.StatusBadRequest
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
		"DetectSentiment":        h.detectSentiment,
		"DetectEntities":         h.detectEntities,
		"DetectKeyPhrases":       h.detectKeyPhrases,
		"DetectPiiEntities":      h.detectPIIEntities,
		"DetectSyntax":           h.detectSyntax,
		"DetectDominantLanguage": h.detectDominantLanguage,
		"DetectToxicContent":     h.detectToxicContent,
		"BatchDetectSentiment":   h.batch(h.detectSentiment, generalLanguageCodes),
		"BatchDetectEntities":    h.batch(h.detectEntities, generalLanguageCodes),
		"BatchDetectKeyPhrases":  h.batch(h.detectKeyPhrases, generalLanguageCodes),
		// Deliberately no "BatchDetectPiiEntities" entry: real Comprehend has no
		// such operation (verified against aws-sdk-go-v2/service/comprehend.Client
		// -- PII entity detection has no Batch* form at all, unlike Sentiment/
		// Entities/KeyPhrases/Syntax/DominantLanguage, which all do). A prior pass
		// fabricated it by pattern-matching the other five Batch* ops -- see
		// gopherstack-vhw2 category A.
		"BatchDetectSyntax":           h.batch(h.detectSyntax, syntaxLanguageCodes),
		"BatchDetectDominantLanguage": h.batch(h.detectDominantLanguage, nil),
		"TagResource":                 h.tagResource,
		"UntagResource":               h.untagResource,
		"ListTagsForResource":         h.listTags,
	}
	for prefix, spec := range asyncJobSpecs() {
		ops["Start"+prefix] = h.startJob(spec)
		ops["Describe"+prefix] = h.describeJob(spec)
		ops["List"+prefix+"s"] = h.listJobs(spec)
		if !spec.noStop {
			ops["Stop"+prefix] = h.stopJob(spec)
		}
	}
	for prefix, spec := range resourceSpecs() {
		ops["Create"+prefix] = h.createResource(spec)
		ops["Describe"+prefix] = h.describeResource(spec)
		ops["List"+prefix+"s"] = h.listResources(spec)
		if !spec.noDelete {
			ops["Delete"+prefix] = h.deleteResource(spec)
		}
		if spec.resourceType == resourceTypeEndpoint || spec.resourceType == resourceTypeFlywheel {
			ops["Update"+prefix] = h.updateResource(spec)
		}
	}
	ops["StartFlywheelIteration"] = h.startIteration
	// Deliberately no "GetFlywheelIteration" entry: the real SDK operation is
	// named DescribeFlywheelIteration (aws-sdk-go-v2/service/comprehend.Client
	// has Client.DescribeFlywheelIteration, no Client.GetFlywheelIteration at
	// all). A prior pass registered both names pointing at the same handler;
	// "GetFlywheelIteration" was a fabricated alias, not a real op or a real
	// gopherstack-only route -- removed rather than kept, since the real name
	// was already present and wired to the identical handler (same resolution
	// as lambda's InvokeFunction/Invoke) -- see gopherstack-vhw2 category A.
	ops["DescribeFlywheelIteration"] = h.getIteration
	ops["ListFlywheelIterationHistory"] = h.listIterations

	ops["BatchDetectTargetedSentiment"] = h.batch(h.detectTargetedSentiment, englishOnlyLanguageCodes)
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

// comprehendPaginate slices items using an integer-offset NextToken and returns
// the page and the token for the following page (empty when exhausted).
// maxResults ≤ 0 means no limit.
func comprehendPaginate[T any](items []T, nextToken string, maxResults int) ([]T, string) {
	if len(items) == 0 {
		return items, ""
	}

	start := 0
	if nextToken != "" {
		if idx, err := strconv.Atoi(nextToken); err == nil && idx > 0 && idx < len(items) {
			start = idx
		}
	}

	if maxResults <= 0 {
		return items[start:], ""
	}

	end := start + maxResults
	if end >= len(items) {
		return items[start:], ""
	}

	return items[start:end], strconv.Itoa(end)
}

// paginationParams extracts NextToken and MaxResults from the JSON body input.
func paginationParams(input map[string]any) (string, int) {
	tok, _ := input["NextToken"].(string)
	maxResults := 0
	switch v := input["MaxResults"].(type) {
	case float64:
		maxResults = int(v)
	case int:
		maxResults = v
	}

	return tok, maxResults
}
