package personalize

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	personalizeTargetPrefix        = "AmazonPersonalize."
	personalizeRuntimeTargetPrefix = "AmazonPersonalizeRuntime."
	personalizeContentType         = "application/x-amz-json-1.1"

	keyDatasetGroupArn          = "datasetGroupArn"
	keyDatasetArn               = "datasetArn"
	keySchemaArn                = "schemaArn"
	keySolutionArn              = "solutionArn"
	keySolutionVersionArn       = "solutionVersionArn"
	keyCampaignArn              = "campaignArn"
	keyRecommenderArn           = "recommenderArn"
	keyMetricAttributionArn     = "metricAttributionArn"
	keyRoleArn                  = "roleArn"
	keyDomain                   = "domain"
	keyName                     = "name"
	keyRecipeArn                = "recipeArn"
	keyRecipeType               = "recipeType"
	keyCreationDateTime         = "creationDateTime"
	keyLastUpdatedDateTime      = "lastUpdatedDateTime"
	keyJobName                  = "jobName"
	keyJobOutput                = "jobOutput"
	keyStatus                   = "status"
	keyEventType                = "eventType"
	keyPerformIncrementalUpdate = "performIncrementalUpdate"

	keyBatchInferenceJobArn = "batchInferenceJobArn"
	keyBatchSegmentJobArn   = "batchSegmentJobArn"
	keyDataDeletionJobArn   = "dataDeletionJobArn"
	keyDatasetImportJobArn  = "datasetImportJobArn"
	keyDatasetExportJobArn  = "datasetExportJobArn"
	keyEventTrackerArn      = "eventTrackerArn"
	keyFilterArn            = "filterArn"

	recipeTypeUserPersonalization = "USER_PERSONALIZATION"
)

type opFunc func(map[string]any) (map[string]any, error)

// Handler serves Amazon Personalize JSON operations.
type Handler struct {
	Backend *InMemoryBackend
	ops     map[string]opFunc
}

// NewHandler creates a Personalize handler backed by in-memory state.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// Name returns service name.
func (h *Handler) Name() string { return "Personalize" }

// Reset clears all backend state for the /_gopherstack/reset test hook.
func (h *Handler) Reset() { h.Backend.Reset() }

// ChaosServiceName returns service key for fault matching.
func (h *Handler) ChaosServiceName() string { return "personalize" }

// ChaosOperations returns fault-injectable operations.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns configured service region.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// MatchPriority returns header matching priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// RouteMatcher matches Personalize and Personalize Runtime X-Amz-Target headers.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")

		return strings.HasPrefix(target, personalizeTargetPrefix) ||
			strings.HasPrefix(target, personalizeRuntimeTargetPrefix)
	}
}

// ExtractOperation returns the operation name from the request target.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	if op, ok := strings.CutPrefix(target, personalizeRuntimeTargetPrefix); ok {
		return op
	}

	return strings.TrimPrefix(target, personalizeTargetPrefix)
}

// ExtractResource returns an empty string (no generic resource identifier).
func (h *Handler) ExtractResource(_ *echo.Context) string { return "" }

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
			c, logger.Load(c.Request().Context()), h.Name(), personalizeContentType,
			h.GetSupportedOperations(), h.dispatch, h.handleError,
		)
	}
}

func (h *Handler) dispatch(_ context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
	if !ok {
		return nil, fmt.Errorf("%w: operation %q not implemented", ErrValidation, action)
	}

	var input map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &input); err != nil {
			return nil, fmt.Errorf("%w: invalid JSON", ErrValidation)
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
	errType := "InternalServerException"
	status := http.StatusInternalServerError

	switch {
	case errors.Is(err, ErrNotFound):
		errType, status = "ResourceNotFoundException", http.StatusBadRequest
	case errors.Is(err, ErrAlreadyExists):
		errType, status = "ResourceAlreadyExistsException", http.StatusBadRequest
	case errors.Is(err, ErrValidation):
		errType, status = "InvalidInputException", http.StatusBadRequest
	case errors.Is(err, ErrInUse):
		errType, status = "ResourceInUseException", http.StatusBadRequest
	}

	c.Response().Header().Set("Content-Type", personalizeContentType)

	payload, marshalErr := json.Marshal(map[string]string{"__type": errType, "message": err.Error()})
	if marshalErr != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}

	return c.JSONBlob(status, payload)
}

func (h *Handler) buildOps() map[string]opFunc {
	return map[string]opFunc{
		// DatasetGroup
		"CreateDatasetGroup":   h.createDatasetGroup,
		"DescribeDatasetGroup": h.describeDatasetGroup,
		"DeleteDatasetGroup":   h.deleteDatasetGroup,
		"ListDatasetGroups":    h.listDatasetGroups,
		// Dataset
		"CreateDataset":   h.createDataset,
		"DescribeDataset": h.describeDataset,
		"UpdateDataset":   h.updateDataset,
		"DeleteDataset":   h.deleteDataset,
		"ListDatasets":    h.listDatasets,
		// Schema
		"CreateSchema":   h.createSchema,
		"DescribeSchema": h.describeSchema,
		"DeleteSchema":   h.deleteSchema,
		"ListSchemas":    h.listSchemas,
		// Solution
		"CreateSolution":   h.createSolution,
		"DescribeSolution": h.describeSolution,
		"UpdateSolution":   h.updateSolution,
		"DeleteSolution":   h.deleteSolution,
		"ListSolutions":    h.listSolutions,
		// SolutionVersion
		"CreateSolutionVersion":       h.createSolutionVersion,
		"DescribeSolutionVersion":     h.describeSolutionVersion,
		"ListSolutionVersions":        h.listSolutionVersions,
		"StopSolutionVersionCreation": h.stopSolutionVersionCreation,
		"GetSolutionMetrics":          h.getSolutionMetrics,
		// Campaign
		"CreateCampaign":   h.createCampaign,
		"DescribeCampaign": h.describeCampaign,
		"UpdateCampaign":   h.updateCampaign,
		"DeleteCampaign":   h.deleteCampaign,
		"ListCampaigns":    h.listCampaigns,
		// EventTracker
		"CreateEventTracker":   h.createEventTracker,
		"DescribeEventTracker": h.describeEventTracker,
		"DeleteEventTracker":   h.deleteEventTracker,
		"ListEventTrackers":    h.listEventTrackers,
		// Filter
		"CreateFilter":   h.createFilter,
		"DescribeFilter": h.describeFilter,
		"DeleteFilter":   h.deleteFilter,
		"ListFilters":    h.listFilters,
		// Recommender
		"CreateRecommender":   h.createRecommender,
		"DescribeRecommender": h.describeRecommender,
		"UpdateRecommender":   h.updateRecommender,
		"DeleteRecommender":   h.deleteRecommender,
		"ListRecommenders":    h.listRecommenders,
		"StartRecommender":    h.startRecommender,
		"StopRecommender":     h.stopRecommender,
		// MetricAttribution
		"CreateMetricAttribution":      h.createMetricAttribution,
		"DescribeMetricAttribution":    h.describeMetricAttribution,
		"UpdateMetricAttribution":      h.updateMetricAttribution,
		"DeleteMetricAttribution":      h.deleteMetricAttribution,
		"ListMetricAttributions":       h.listMetricAttributions,
		"ListMetricAttributionMetrics": h.listMetricAttributionMetrics,
		// DatasetImportJob
		"CreateDatasetImportJob":   h.createDatasetImportJob,
		"DescribeDatasetImportJob": h.describeDatasetImportJob,
		"ListDatasetImportJobs":    h.listDatasetImportJobs,
		// DatasetExportJob
		"CreateDatasetExportJob":   h.createDatasetExportJob,
		"DescribeDatasetExportJob": h.describeDatasetExportJob,
		"ListDatasetExportJobs":    h.listDatasetExportJobs,
		// BatchInferenceJob
		"CreateBatchInferenceJob":   h.createBatchInferenceJob,
		"DescribeBatchInferenceJob": h.describeBatchInferenceJob,
		"ListBatchInferenceJobs":    h.listBatchInferenceJobs,
		// BatchSegmentJob
		"CreateBatchSegmentJob":   h.createBatchSegmentJob,
		"DescribeBatchSegmentJob": h.describeBatchSegmentJob,
		"ListBatchSegmentJobs":    h.listBatchSegmentJobs,
		// DataDeletionJob
		"CreateDataDeletionJob":   h.createDataDeletionJob,
		"DescribeDataDeletionJob": h.describeDataDeletionJob,
		"ListDataDeletionJobs":    h.listDataDeletionJobs,
		// Recipe (read-only)
		"DescribeRecipe": h.describeRecipe,
		"ListRecipes":    h.listRecipes,
		// FeatureTransformation (read-only)
		"DescribeFeatureTransformation": h.describeFeatureTransformation,
		// Algorithm (read-only)
		"DescribeAlgorithm": h.describeAlgorithm,
		// Tags
		"TagResource":         h.tagResource,
		"UntagResource":       h.untagResource,
		"ListTagsForResource": h.listTagsForResource,
		// Personalize Runtime
		"GetRecommendations":     h.getRecommendations,
		"GetPersonalizedRanking": h.getPersonalizedRanking,
	}
}

// --- Input helpers ---

func extractTags(input map[string]any) map[string]string {
	return extractTagsFromSlice(input, "tags")
}

func extractTagsFromSlice(input map[string]any, key string) map[string]string {
	raw, ok := input[key].([]any)
	if !ok {
		return nil
	}

	tags := make(map[string]string, len(raw))
	for _, item := range raw {
		entry, isMap := item.(map[string]any)
		if !isMap {
			continue
		}

		k, _ := entry["tagKey"].(string)
		v, _ := entry["tagValue"].(string)
		if k != "" {
			tags[k] = v
		}
	}

	return tags
}

// boolFieldDefault returns the bool value of key if present in m, otherwise
// def. Unlike a plain type assertion, this distinguishes "absent from the
// request" (use def) from "explicitly present" (use its value), which
// matters for wire fields whose real-API default is true.
func boolFieldDefault(m map[string]any, key string, def bool) bool {
	if v, ok := m[key].(bool); ok {
		return v
	}

	return def
}

func intField(m map[string]any, key string) int {
	switch v := m[key].(type) {
	case float64:
		return int(v)
	case int:
		return v
	}

	return 0
}

func int32Field(m map[string]any, key string) int32 {
	switch v := m[key].(type) {
	case float64:
		const maxInt32 float64 = 1<<31 - 1
		const minInt32 float64 = -1 << 31
		if v > maxInt32 || v < minInt32 {
			return 0
		}

		return int32(v)
	case int:
		return int32(v) //nolint:gosec // caller provides JSON-decoded number in int32 range
	case int32:
		return v
	}

	return 0
}

// rawMap returns m[key] as a map[string]any, or nil if absent/wrong type.
func rawMap(m map[string]any, key string) map[string]any {
	raw, _ := m[key].(map[string]any)

	return raw
}

// decodeConfig converts a generically-decoded JSON sub-object into a typed
// config struct via a marshal/unmarshal round trip -- dispatch() already
// decoded the whole request body into map[string]any, so this is how a
// specific sub-object (solutionConfig/campaignConfig/recommenderConfig) gets
// deep-typed field access instead of staying an opaque map.
func decodeConfig[T any](raw map[string]any) *T {
	if raw == nil {
		return nil
	}

	b, marshalErr := json.Marshal(raw)
	if marshalErr != nil {
		return nil
	}

	var cfg T
	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil
	}

	return &cfg
}

func strSlice(m map[string]any, key string) []string {
	raw, ok := m[key].([]any)
	if !ok {
		return nil
	}

	out := make([]string, 0, len(raw))
	for _, v := range raw {
		if s, isStr := v.(string); isStr {
			out = append(out, s)
		}
	}

	return out
}
