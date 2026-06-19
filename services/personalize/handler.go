package personalize

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	personalizeTargetPrefix        = "AmazonPersonalize."
	personalizeRuntimeTargetPrefix = "AmazonPersonalizeRuntime."
	personalizeContentType         = "application/x-amz-json-1.1"

	keyDatasetGroupArn      = "datasetGroupArn"
	keyDatasetArn           = "datasetArn"
	keySchemaArn            = "schemaArn"
	keySolutionArn          = "solutionArn"
	keySolutionVersionArn   = "solutionVersionArn"
	keyCampaignArn          = "campaignArn"
	keyRecommenderArn       = "recommenderArn"
	keyMetricAttributionArn = "metricAttributionArn"
	keyRoleArn              = "roleArn"
	keyDomain               = "domain"
	keyName                 = "name"
	keyRecipeArn            = "recipeArn"
	keyRecipeType           = "recipeType"
	keyCreationDateTime     = "creationDateTime"
	keyLastUpdatedDateTime  = "lastUpdatedDateTime"
	keyJobName              = "jobName"
	keyJobOutput            = "jobOutput"
	keyStatus               = "status"

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
	if strings.HasPrefix(target, personalizeRuntimeTargetPrefix) {
		return strings.TrimPrefix(target, personalizeRuntimeTargetPrefix)
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
		"DeleteSolutionVersion":       h.deleteSolutionVersion,
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

// --- DatasetGroup ---

func (h *Handler) createDatasetGroup(input map[string]any) (map[string]any, error) {
	name, _ := input["name"].(string)
	domain, _ := input["domain"].(string)
	kmsKeyArn, _ := input["kmsKeyArn"].(string)
	roleArn, _ := input["roleArn"].(string)
	tags := extractTags(input)

	dg, err := h.Backend.CreateDatasetGroup(name, domain, kmsKeyArn, roleArn, tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyDatasetGroupArn: dg.DatasetGroupArn,
		keyDomain:          dg.Domain,
	}, nil
}

func (h *Handler) describeDatasetGroup(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["datasetGroupArn"].(string)

	dg, err := h.Backend.DescribeDatasetGroup(nameOrArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"datasetGroup": datasetGroupToMap(dg),
	}, nil
}

func (h *Handler) deleteDatasetGroup(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["datasetGroupArn"].(string)

	return map[string]any{}, h.Backend.DeleteDatasetGroup(nameOrArn)
}

func (h *Handler) listDatasetGroups(input map[string]any) (map[string]any, error) {
	maxResults := intField(input, "maxResults")
	nextToken, _ := input["nextToken"].(string)

	list, outToken := h.Backend.ListDatasetGroups(maxResults, nextToken)

	summaries := make([]map[string]any, 0, len(list))
	for _, dg := range list {
		summaries = append(summaries, datasetGroupToMap(dg))
	}

	result := map[string]any{"datasetGroups": summaries}
	if outToken != "" {
		result["nextToken"] = outToken
	}

	return result, nil
}

// --- Dataset ---

func (h *Handler) createDataset(input map[string]any) (map[string]any, error) {
	name, _ := input["name"].(string)
	datasetGroupArn, _ := input["datasetGroupArn"].(string)
	datasetType, _ := input["datasetType"].(string)
	schemaArn, _ := input["schemaArn"].(string)
	tags := extractTags(input)

	ds, err := h.Backend.CreateDataset(name, datasetGroupArn, datasetType, schemaArn, tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyDatasetArn: ds.DatasetArn}, nil
}

func (h *Handler) describeDataset(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["datasetArn"].(string)

	ds, err := h.Backend.DescribeDataset(nameOrArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"dataset": datasetToMap(ds)}, nil
}

func (h *Handler) updateDataset(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["datasetArn"].(string)
	schemaArn, _ := input["schemaArn"].(string)

	ds, err := h.Backend.UpdateDataset(nameOrArn, schemaArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyDatasetArn: ds.DatasetArn}, nil
}

func (h *Handler) deleteDataset(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["datasetArn"].(string)

	return map[string]any{}, h.Backend.DeleteDataset(nameOrArn)
}

func (h *Handler) listDatasets(input map[string]any) (map[string]any, error) {
	datasetGroupArn, _ := input["datasetGroupArn"].(string)
	maxResults := intField(input, "maxResults")
	nextToken, _ := input["nextToken"].(string)

	list, outToken := h.Backend.ListDatasets(datasetGroupArn, maxResults, nextToken)

	summaries := make([]map[string]any, 0, len(list))
	for _, ds := range list {
		summaries = append(summaries, datasetToMap(ds))
	}

	result := map[string]any{"datasets": summaries}
	if outToken != "" {
		result["nextToken"] = outToken
	}

	return result, nil
}

// --- Schema ---

func (h *Handler) createSchema(input map[string]any) (map[string]any, error) {
	name, _ := input["name"].(string)
	schema, _ := input["schema"].(string)
	domain, _ := input["domain"].(string)

	s, err := h.Backend.CreateSchema(name, schema, domain)
	if err != nil {
		return nil, err
	}

	return map[string]any{keySchemaArn: s.SchemaArn}, nil
}

func (h *Handler) describeSchema(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["schemaArn"].(string)

	s, err := h.Backend.DescribeSchema(nameOrArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"schema": schemaToMap(s)}, nil
}

func (h *Handler) deleteSchema(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["schemaArn"].(string)

	return map[string]any{}, h.Backend.DeleteSchema(nameOrArn)
}

func (h *Handler) listSchemas(input map[string]any) (map[string]any, error) {
	maxResults := intField(input, "maxResults")
	nextToken, _ := input["nextToken"].(string)

	list, outToken := h.Backend.ListSchemas(maxResults, nextToken)

	summaries := make([]map[string]any, 0, len(list))
	for _, s := range list {
		summaries = append(summaries, schemaToMap(s))
	}

	result := map[string]any{"schemas": summaries}
	if outToken != "" {
		result["nextToken"] = outToken
	}

	return result, nil
}

// --- Solution ---

func (h *Handler) createSolution(input map[string]any) (map[string]any, error) {
	name, _ := input["name"].(string)
	datasetGroupArn, _ := input["datasetGroupArn"].(string)
	recipeArn, _ := input["recipeArn"].(string)
	performAutoML, _ := input["performAutoML"].(bool)
	performHPO, _ := input["performHPO"].(bool)
	tags := extractTags(input)

	sol, err := h.Backend.CreateSolution(name, datasetGroupArn, recipeArn, performAutoML, performHPO, tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{keySolutionArn: sol.SolutionArn}, nil
}

func (h *Handler) describeSolution(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["solutionArn"].(string)

	sol, err := h.Backend.DescribeSolution(nameOrArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"solution": solutionToMap(sol)}, nil
}

func (h *Handler) updateSolution(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["solutionArn"].(string)
	performAutoML, _ := input["performAutoML"].(bool)
	performHPO, _ := input["performHPO"].(bool)

	sol, err := h.Backend.UpdateSolution(nameOrArn, performAutoML, performHPO)
	if err != nil {
		return nil, err
	}

	return map[string]any{keySolutionArn: sol.SolutionArn}, nil
}

func (h *Handler) deleteSolution(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["solutionArn"].(string)

	return map[string]any{}, h.Backend.DeleteSolution(nameOrArn)
}

func (h *Handler) listSolutions(input map[string]any) (map[string]any, error) {
	datasetGroupArn, _ := input["datasetGroupArn"].(string)
	maxResults := intField(input, "maxResults")
	nextToken, _ := input["nextToken"].(string)

	list, outToken := h.Backend.ListSolutions(datasetGroupArn, maxResults, nextToken)

	summaries := make([]map[string]any, 0, len(list))
	for _, sol := range list {
		summaries = append(summaries, solutionToMap(sol))
	}

	result := map[string]any{"solutions": summaries}
	if outToken != "" {
		result["nextToken"] = outToken
	}

	return result, nil
}

// --- SolutionVersion ---

func (h *Handler) createSolutionVersion(input map[string]any) (map[string]any, error) {
	solutionArn, _ := input["solutionArn"].(string)
	trainingMode, _ := input["trainingMode"].(string)
	tags := extractTags(input)

	sv, err := h.Backend.CreateSolutionVersion(solutionArn, trainingMode, tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{keySolutionVersionArn: sv.SolutionVersionArn}, nil
}

func (h *Handler) describeSolutionVersion(input map[string]any) (map[string]any, error) {
	svArn, _ := input["solutionVersionArn"].(string)

	sv, err := h.Backend.DescribeSolutionVersion(svArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"solutionVersion": solutionVersionToMap(sv)}, nil
}

func (h *Handler) deleteSolutionVersion(input map[string]any) (map[string]any, error) {
	svArn, _ := input["solutionVersionArn"].(string)

	return map[string]any{}, h.Backend.DeleteSolutionVersion(svArn)
}

func (h *Handler) listSolutionVersions(input map[string]any) (map[string]any, error) {
	solutionArn, _ := input["solutionArn"].(string)
	maxResults := intField(input, "maxResults")
	nextToken, _ := input["nextToken"].(string)

	list, outToken := h.Backend.ListSolutionVersions(solutionArn, maxResults, nextToken)

	summaries := make([]map[string]any, 0, len(list))
	for _, sv := range list {
		summaries = append(summaries, solutionVersionToMap(sv))
	}

	result := map[string]any{"solutionVersions": summaries}
	if outToken != "" {
		result["nextToken"] = outToken
	}

	return result, nil
}

func (h *Handler) stopSolutionVersionCreation(input map[string]any) (map[string]any, error) {
	svArn, _ := input["solutionVersionArn"].(string)

	return map[string]any{}, h.Backend.StopSolutionVersionCreation(svArn)
}

func (h *Handler) getSolutionMetrics(input map[string]any) (map[string]any, error) {
	svArn, _ := input["solutionVersionArn"].(string)

	return h.Backend.GetSolutionMetrics(svArn)
}

// --- Campaign ---

func (h *Handler) createCampaign(input map[string]any) (map[string]any, error) {
	name, _ := input["name"].(string)
	solutionVersionArn, _ := input["solutionVersionArn"].(string)
	minProvisionedTPS := int32Field(input, "minProvisionedTPS")
	tags := extractTags(input)

	c, err := h.Backend.CreateCampaign(name, solutionVersionArn, minProvisionedTPS, tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyCampaignArn: c.CampaignArn}, nil
}

func (h *Handler) describeCampaign(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["campaignArn"].(string)

	c, err := h.Backend.DescribeCampaign(nameOrArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"campaign": campaignToMap(c)}, nil
}

func (h *Handler) updateCampaign(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["campaignArn"].(string)
	solutionVersionArn, _ := input["solutionVersionArn"].(string)
	minProvisionedTPS := int32Field(input, "minProvisionedTPS")

	c, err := h.Backend.UpdateCampaign(nameOrArn, solutionVersionArn, minProvisionedTPS)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyCampaignArn: c.CampaignArn}, nil
}

func (h *Handler) deleteCampaign(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["campaignArn"].(string)

	return map[string]any{}, h.Backend.DeleteCampaign(nameOrArn)
}

func (h *Handler) listCampaigns(input map[string]any) (map[string]any, error) {
	solutionArn, _ := input["solutionArn"].(string)
	maxResults := intField(input, "maxResults")
	nextToken, _ := input["nextToken"].(string)

	list, outToken := h.Backend.ListCampaigns(solutionArn, maxResults, nextToken)

	summaries := make([]map[string]any, 0, len(list))
	for _, c := range list {
		summaries = append(summaries, campaignToMap(c))
	}

	result := map[string]any{"campaigns": summaries}
	if outToken != "" {
		result["nextToken"] = outToken
	}

	return result, nil
}

// --- EventTracker ---

func (h *Handler) createEventTracker(input map[string]any) (map[string]any, error) {
	name, _ := input["name"].(string)
	datasetGroupArn, _ := input["datasetGroupArn"].(string)
	tags := extractTags(input)

	et, err := h.Backend.CreateEventTracker(name, datasetGroupArn, tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"eventTrackerArn": et.EventTrackerArn,
		"trackingId":      et.TrackingID,
	}, nil
}

func (h *Handler) describeEventTracker(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["eventTrackerArn"].(string)

	et, err := h.Backend.DescribeEventTracker(nameOrArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"eventTracker": eventTrackerToMap(et)}, nil
}

func (h *Handler) deleteEventTracker(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["eventTrackerArn"].(string)

	return map[string]any{}, h.Backend.DeleteEventTracker(nameOrArn)
}

func (h *Handler) listEventTrackers(input map[string]any) (map[string]any, error) {
	datasetGroupArn, _ := input["datasetGroupArn"].(string)
	maxResults := intField(input, "maxResults")
	nextToken, _ := input["nextToken"].(string)

	list, outToken := h.Backend.ListEventTrackers(datasetGroupArn, maxResults, nextToken)

	summaries := make([]map[string]any, 0, len(list))
	for _, et := range list {
		summaries = append(summaries, eventTrackerToMap(et))
	}

	result := map[string]any{"eventTrackers": summaries}
	if outToken != "" {
		result["nextToken"] = outToken
	}

	return result, nil
}

// --- Filter ---

func (h *Handler) createFilter(input map[string]any) (map[string]any, error) {
	name, _ := input["name"].(string)
	datasetGroupArn, _ := input["datasetGroupArn"].(string)
	filterExpression, _ := input["filterExpression"].(string)
	tags := extractTags(input)

	f, err := h.Backend.CreateFilter(name, datasetGroupArn, filterExpression, tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{"filterArn": f.FilterArn}, nil
}

func (h *Handler) describeFilter(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["filterArn"].(string)

	f, err := h.Backend.DescribeFilter(nameOrArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"filter": filterToMap(f)}, nil
}

func (h *Handler) deleteFilter(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["filterArn"].(string)

	return map[string]any{}, h.Backend.DeleteFilter(nameOrArn)
}

func (h *Handler) listFilters(input map[string]any) (map[string]any, error) {
	datasetGroupArn, _ := input["datasetGroupArn"].(string)
	maxResults := intField(input, "maxResults")
	nextToken, _ := input["nextToken"].(string)

	list, outToken := h.Backend.ListFilters(datasetGroupArn, maxResults, nextToken)

	summaries := make([]map[string]any, 0, len(list))
	for _, f := range list {
		summaries = append(summaries, filterToMap(f))
	}

	result := map[string]any{"filters": summaries}
	if outToken != "" {
		result["nextToken"] = outToken
	}

	return result, nil
}

// --- Recommender ---

func (h *Handler) createRecommender(input map[string]any) (map[string]any, error) {
	name, _ := input["name"].(string)
	datasetGroupArn, _ := input["datasetGroupArn"].(string)
	recipeArn, _ := input["recipeArn"].(string)
	minRPS := int32(0)
	if cfg, ok := input["recommenderConfig"].(map[string]any); ok {
		minRPS = int32Field(cfg, "minRecommendationRequestsPerSecond")
	}
	tags := extractTags(input)

	r, err := h.Backend.CreateRecommender(name, datasetGroupArn, recipeArn, minRPS, tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyRecommenderArn: r.RecommenderArn}, nil
}

func (h *Handler) describeRecommender(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["recommenderArn"].(string)

	r, err := h.Backend.DescribeRecommender(nameOrArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"recommender": recommenderToMap(r)}, nil
}

func (h *Handler) updateRecommender(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["recommenderArn"].(string)
	minRPS := int32(0)
	if cfg, ok := input["recommenderConfig"].(map[string]any); ok {
		minRPS = int32Field(cfg, "minRecommendationRequestsPerSecond")
	}

	r, err := h.Backend.UpdateRecommender(nameOrArn, minRPS)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyRecommenderArn: r.RecommenderArn}, nil
}

func (h *Handler) deleteRecommender(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["recommenderArn"].(string)

	return map[string]any{}, h.Backend.DeleteRecommender(nameOrArn)
}

func (h *Handler) listRecommenders(input map[string]any) (map[string]any, error) {
	datasetGroupArn, _ := input["datasetGroupArn"].(string)
	maxResults := intField(input, "maxResults")
	nextToken, _ := input["nextToken"].(string)

	list, outToken := h.Backend.ListRecommenders(datasetGroupArn, maxResults, nextToken)

	summaries := make([]map[string]any, 0, len(list))
	for _, r := range list {
		summaries = append(summaries, recommenderToMap(r))
	}

	result := map[string]any{"recommenders": summaries}
	if outToken != "" {
		result["nextToken"] = outToken
	}

	return result, nil
}

func (h *Handler) startRecommender(input map[string]any) (map[string]any, error) {
	recommenderArn, _ := input["recommenderArn"].(string)

	r, err := h.Backend.StartRecommender(recommenderArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyRecommenderArn: r.RecommenderArn}, nil
}

func (h *Handler) stopRecommender(input map[string]any) (map[string]any, error) {
	recommenderArn, _ := input["recommenderArn"].(string)

	r, err := h.Backend.StopRecommender(recommenderArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyRecommenderArn: r.RecommenderArn}, nil
}

// --- MetricAttribution ---

func (h *Handler) createMetricAttribution(input map[string]any) (map[string]any, error) {
	name, _ := input["name"].(string)
	datasetGroupArn, _ := input["datasetGroupArn"].(string)
	metricsOutputConfig, _ := input["metricsOutputConfig"].(map[string]any)
	tags := extractTags(input)

	ma, err := h.Backend.CreateMetricAttribution(name, datasetGroupArn, metricsOutputConfig, tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyMetricAttributionArn: ma.MetricAttributionArn}, nil
}

func (h *Handler) describeMetricAttribution(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["metricAttributionArn"].(string)

	ma, err := h.Backend.DescribeMetricAttribution(nameOrArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"metricAttribution": metricAttributionToMap(ma)}, nil
}

func (h *Handler) updateMetricAttribution(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["metricAttributionArn"].(string)
	metricsOutputConfig, _ := input["metricsOutputConfig"].(map[string]any)

	ma, err := h.Backend.UpdateMetricAttribution(nameOrArn, metricsOutputConfig)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyMetricAttributionArn: ma.MetricAttributionArn}, nil
}

func (h *Handler) deleteMetricAttribution(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["metricAttributionArn"].(string)

	return map[string]any{}, h.Backend.DeleteMetricAttribution(nameOrArn)
}

func (h *Handler) listMetricAttributions(input map[string]any) (map[string]any, error) {
	datasetGroupArn, _ := input["datasetGroupArn"].(string)
	maxResults := intField(input, "maxResults")
	nextToken, _ := input["nextToken"].(string)

	list, outToken := h.Backend.ListMetricAttributions(datasetGroupArn, maxResults, nextToken)

	summaries := make([]map[string]any, 0, len(list))
	for _, ma := range list {
		summaries = append(summaries, metricAttributionToMap(ma))
	}

	result := map[string]any{"metricAttributions": summaries}
	if outToken != "" {
		result["nextToken"] = outToken
	}

	return result, nil
}

func (h *Handler) listMetricAttributionMetrics(input map[string]any) (map[string]any, error) {
	metricAttributionArn, _ := input["metricAttributionArn"].(string)
	maxResults := intField(input, "maxResults")
	nextToken, _ := input["nextToken"].(string)

	metrics, outToken, err := h.Backend.ListMetricAttributionMetrics(metricAttributionArn, maxResults, nextToken)
	if err != nil {
		return nil, err
	}

	result := map[string]any{"metrics": metrics}
	if outToken != "" {
		result["nextToken"] = outToken
	}

	return result, nil
}

// --- DatasetImportJob ---

func (h *Handler) createDatasetImportJob(input map[string]any) (map[string]any, error) {
	jobName, _ := input["jobName"].(string)
	datasetArn, _ := input["datasetArn"].(string)
	roleArn, _ := input["roleArn"].(string)
	dataSource, _ := input["dataSource"].(map[string]any)
	tags := extractTags(input)

	job, err := h.Backend.CreateDatasetImportJob(jobName, datasetArn, roleArn, dataSource, tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{"datasetImportJobArn": job.DatasetImportJobArn}, nil
}

func (h *Handler) describeDatasetImportJob(input map[string]any) (map[string]any, error) {
	jobArn, _ := input["datasetImportJobArn"].(string)

	job, err := h.Backend.DescribeDatasetImportJob(jobArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"datasetImportJob": datasetImportJobToMap(job)}, nil
}

func (h *Handler) listDatasetImportJobs(input map[string]any) (map[string]any, error) {
	datasetArn, _ := input["datasetArn"].(string)
	maxResults := intField(input, "maxResults")
	nextToken, _ := input["nextToken"].(string)

	list, outToken := h.Backend.ListDatasetImportJobs(datasetArn, maxResults, nextToken)

	summaries := make([]map[string]any, 0, len(list))
	for _, job := range list {
		summaries = append(summaries, datasetImportJobToMap(job))
	}

	result := map[string]any{"datasetImportJobs": summaries}
	if outToken != "" {
		result["nextToken"] = outToken
	}

	return result, nil
}

// --- DatasetExportJob ---

func (h *Handler) createDatasetExportJob(input map[string]any) (map[string]any, error) {
	jobName, _ := input["jobName"].(string)
	datasetArn, _ := input["datasetArn"].(string)
	roleArn, _ := input["roleArn"].(string)
	jobOutput, _ := input["jobOutput"].(map[string]any)
	tags := extractTags(input)

	job, err := h.Backend.CreateDatasetExportJob(jobName, datasetArn, roleArn, jobOutput, tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{"datasetExportJobArn": job.DatasetExportJobArn}, nil
}

func (h *Handler) describeDatasetExportJob(input map[string]any) (map[string]any, error) {
	jobArn, _ := input["datasetExportJobArn"].(string)

	job, err := h.Backend.DescribeDatasetExportJob(jobArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"datasetExportJob": datasetExportJobToMap(job)}, nil
}

func (h *Handler) listDatasetExportJobs(input map[string]any) (map[string]any, error) {
	datasetArn, _ := input["datasetArn"].(string)
	maxResults := intField(input, "maxResults")
	nextToken, _ := input["nextToken"].(string)

	list, outToken := h.Backend.ListDatasetExportJobs(datasetArn, maxResults, nextToken)

	summaries := make([]map[string]any, 0, len(list))
	for _, job := range list {
		summaries = append(summaries, datasetExportJobToMap(job))
	}

	result := map[string]any{"datasetExportJobs": summaries}
	if outToken != "" {
		result["nextToken"] = outToken
	}

	return result, nil
}

// --- BatchInferenceJob ---

func (h *Handler) createBatchInferenceJob(input map[string]any) (map[string]any, error) {
	jobName, _ := input["jobName"].(string)
	solutionVersionArn, _ := input["solutionVersionArn"].(string)
	roleArn, _ := input["roleArn"].(string)
	jobInput, _ := input["jobInput"].(map[string]any)
	jobOutput, _ := input["jobOutput"].(map[string]any)
	tags := extractTags(input)

	job, err := h.Backend.CreateBatchInferenceJob(jobName, solutionVersionArn, roleArn, jobInput, jobOutput, tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{"batchInferenceJobArn": job.BatchInferenceJobArn}, nil
}

func (h *Handler) describeBatchInferenceJob(input map[string]any) (map[string]any, error) {
	jobArn, _ := input["batchInferenceJobArn"].(string)

	job, err := h.Backend.DescribeBatchInferenceJob(jobArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"batchInferenceJob": batchInferenceJobToMap(job)}, nil
}

func (h *Handler) listBatchInferenceJobs(input map[string]any) (map[string]any, error) {
	solutionVersionArn, _ := input["solutionVersionArn"].(string)
	maxResults := intField(input, "maxResults")
	nextToken, _ := input["nextToken"].(string)

	list, outToken := h.Backend.ListBatchInferenceJobs(solutionVersionArn, maxResults, nextToken)

	summaries := make([]map[string]any, 0, len(list))
	for _, job := range list {
		summaries = append(summaries, batchInferenceJobToMap(job))
	}

	result := map[string]any{"batchInferenceJobs": summaries}
	if outToken != "" {
		result["nextToken"] = outToken
	}

	return result, nil
}

// --- BatchSegmentJob ---

func (h *Handler) createBatchSegmentJob(input map[string]any) (map[string]any, error) {
	jobName, _ := input["jobName"].(string)
	solutionVersionArn, _ := input["solutionVersionArn"].(string)
	roleArn, _ := input["roleArn"].(string)
	jobInput, _ := input["jobInput"].(map[string]any)
	jobOutput, _ := input["jobOutput"].(map[string]any)
	tags := extractTags(input)

	job, err := h.Backend.CreateBatchSegmentJob(jobName, solutionVersionArn, roleArn, jobInput, jobOutput, tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{"batchSegmentJobArn": job.BatchSegmentJobArn}, nil
}

func (h *Handler) describeBatchSegmentJob(input map[string]any) (map[string]any, error) {
	jobArn, _ := input["batchSegmentJobArn"].(string)

	job, err := h.Backend.DescribeBatchSegmentJob(jobArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"batchSegmentJob": batchSegmentJobToMap(job)}, nil
}

func (h *Handler) listBatchSegmentJobs(input map[string]any) (map[string]any, error) {
	solutionVersionArn, _ := input["solutionVersionArn"].(string)
	maxResults := intField(input, "maxResults")
	nextToken, _ := input["nextToken"].(string)

	list, outToken := h.Backend.ListBatchSegmentJobs(solutionVersionArn, maxResults, nextToken)

	summaries := make([]map[string]any, 0, len(list))
	for _, job := range list {
		summaries = append(summaries, batchSegmentJobToMap(job))
	}

	result := map[string]any{"batchSegmentJobs": summaries}
	if outToken != "" {
		result["nextToken"] = outToken
	}

	return result, nil
}

// --- DataDeletionJob ---

func (h *Handler) createDataDeletionJob(input map[string]any) (map[string]any, error) {
	jobName, _ := input["jobName"].(string)
	datasetGroupArn, _ := input["datasetGroupArn"].(string)
	roleArn, _ := input["roleArn"].(string)
	dataSource, _ := input["dataSource"].(map[string]any)
	tags := extractTags(input)

	job, err := h.Backend.CreateDataDeletionJob(jobName, datasetGroupArn, roleArn, dataSource, tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{"dataDeletionJobArn": job.DataDeletionJobArn}, nil
}

func (h *Handler) describeDataDeletionJob(input map[string]any) (map[string]any, error) {
	jobArn, _ := input["dataDeletionJobArn"].(string)

	job, err := h.Backend.DescribeDataDeletionJob(jobArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"dataDeletionJob": dataDeletionJobToMap(job)}, nil
}

func (h *Handler) listDataDeletionJobs(input map[string]any) (map[string]any, error) {
	datasetGroupArn, _ := input["datasetGroupArn"].(string)
	maxResults := intField(input, "maxResults")
	nextToken, _ := input["nextToken"].(string)

	list, outToken := h.Backend.ListDataDeletionJobs(datasetGroupArn, maxResults, nextToken)

	summaries := make([]map[string]any, 0, len(list))
	for _, job := range list {
		summaries = append(summaries, dataDeletionJobToMap(job))
	}

	result := map[string]any{"dataDeletionJobs": summaries}
	if outToken != "" {
		result["nextToken"] = outToken
	}

	return result, nil
}

// --- Recipe (read-only) ---

func getBuiltinRecipes() []map[string]any {
	return []map[string]any{
		{
			keyName:       "aws-user-personalization",
			keyRecipeArn:  "arn:aws:personalize:::recipe/aws-user-personalization",
			keyStatus:     statusActive,
			keyRecipeType: recipeTypeUserPersonalization,
		},
		{
			keyName:       "aws-popularity-count",
			keyRecipeArn:  "arn:aws:personalize:::recipe/aws-popularity-count",
			keyStatus:     statusActive,
			keyRecipeType: recipeTypeUserPersonalization,
		},
		{
			keyName:       "aws-hrnn",
			keyRecipeArn:  "arn:aws:personalize:::recipe/aws-hrnn",
			keyStatus:     statusActive,
			keyRecipeType: recipeTypeUserPersonalization,
		},
		{
			keyName:       "aws-hrnn-coldstart",
			keyRecipeArn:  "arn:aws:personalize:::recipe/aws-hrnn-coldstart",
			keyStatus:     statusActive,
			keyRecipeType: recipeTypeUserPersonalization,
		},
		{
			keyName:       "aws-hrnn-metadata",
			keyRecipeArn:  "arn:aws:personalize:::recipe/aws-hrnn-metadata",
			keyStatus:     statusActive,
			keyRecipeType: recipeTypeUserPersonalization,
		},
		{
			keyName:       "aws-similar-items",
			keyRecipeArn:  "arn:aws:personalize:::recipe/aws-similar-items",
			keyStatus:     statusActive,
			keyRecipeType: "RELATED_ITEMS",
		},
		{
			keyName:       "aws-sims",
			keyRecipeArn:  "arn:aws:personalize:::recipe/aws-sims",
			keyStatus:     statusActive,
			keyRecipeType: "RELATED_ITEMS",
		},
		{
			keyName:       "aws-personalized-ranking",
			keyRecipeArn:  "arn:aws:personalize:::recipe/aws-personalized-ranking",
			keyStatus:     statusActive,
			keyRecipeType: "PERSONALIZED_RANKING",
		},
	}
}

func (h *Handler) describeRecipe(input map[string]any) (map[string]any, error) {
	recipeArn, _ := input[keyRecipeArn].(string)

	for _, r := range getBuiltinRecipes() {
		if r[keyRecipeArn] == recipeArn {
			return map[string]any{"recipe": r}, nil
		}
	}

	return nil, fmt.Errorf("%w: recipe %q not found", ErrNotFound, recipeArn)
}

func (h *Handler) listRecipes(input map[string]any) (map[string]any, error) {
	recipes := getBuiltinRecipes()
	maxResults := intField(input, "maxResults")
	if maxResults <= 0 {
		maxResults = len(recipes)
	}

	if maxResults < len(recipes) {
		recipes = recipes[:maxResults]
	}

	return map[string]any{"recipes": recipes}, nil
}

// --- Algorithm (read-only) ---

func (h *Handler) describeAlgorithm(input map[string]any) (map[string]any, error) {
	algorithmArn, _ := input["algorithmArn"].(string)

	return map[string]any{
		"algorithm": map[string]any{
			"algorithmArn":         algorithmArn,
			keyName:                "user-personalization",
			keyStatus:              statusActive,
			keyCreationDateTime:    awstime.Epoch(time.Now().UTC()),
			keyLastUpdatedDateTime: awstime.Epoch(time.Now().UTC()),
		},
	}, nil
}

// --- FeatureTransformation (read-only) ---

func (h *Handler) describeFeatureTransformation(input map[string]any) (map[string]any, error) {
	ftArn, _ := input["featureTransformationArn"].(string)

	ft, err := h.Backend.GetFeatureTransformation(ftArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"featureTransformation": map[string]any{
			"featureTransformationArn": ft.ARN,
			keyName:                    ft.Name,
			keyStatus:                  ft.Status,
			keyCreationDateTime:        awstime.Epoch(ft.CreationTime),
			keyLastUpdatedDateTime:     awstime.Epoch(ft.LastUpdated),
		},
	}, nil
}

// --- Personalize Runtime ---

const defaultNumRecommendations = 25

func (h *Handler) getRecommendations(input map[string]any) (map[string]any, error) {
	campaignArn, _ := input["campaignArn"].(string)
	userID, _ := input["userId"].(string)
	numResults := intField(input, "numResults")
	if numResults <= 0 {
		numResults = defaultNumRecommendations
	}

	seed := campaignArn + "|" + userID
	items := syntheticItemList(seed, numResults)

	return map[string]any{
		"recommendationId": uuid.NewString(),
		"itemList":         items,
	}, nil
}

func (h *Handler) getPersonalizedRanking(input map[string]any) (map[string]any, error) {
	campaignArn, _ := input["campaignArn"].(string)
	userID, _ := input["userId"].(string)

	rawList, _ := input["inputList"].([]any)
	inputIDs := make([]string, 0, len(rawList))
	for _, v := range rawList {
		if s, ok := v.(string); ok {
			inputIDs = append(inputIDs, s)
		}
	}

	seed := campaignArn + "|" + userID
	ranked := make([]map[string]any, 0, len(inputIDs))
	for i, itemID := range inputIDs {
		score := deterministicScore(seed+itemID, len(inputIDs)-i)
		ranked = append(ranked, map[string]any{
			"itemId": itemID,
			"score":  score,
		})
	}

	return map[string]any{
		"recommendationId":    uuid.NewString(),
		"personalizedRanking": ranked,
	}, nil
}

func syntheticItemList(seed string, n int) []map[string]any {
	items := make([]map[string]any, n)
	for i := range items {
		itemID := fmt.Sprintf("item-%d", i+1)
		items[i] = map[string]any{
			"itemId": itemID,
			"score":  deterministicScore(seed+itemID, n-i),
		}
	}

	return items
}

func deterministicScore(seed string, rank int) float64 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(seed))
	base := float64(h.Sum32()%1000) / 1000.0
	decay := float64(rank) / 100.0
	score := base - decay
	if score < 0 {
		score = 0
	}

	return score
}

// --- Tags ---

func (h *Handler) tagResource(input map[string]any) (map[string]any, error) {
	resourceArn, _ := input["resourceArn"].(string)
	tags := extractTagsFromSlice(input, "tags")

	return map[string]any{}, h.Backend.TagResource(resourceArn, tags)
}

func (h *Handler) untagResource(input map[string]any) (map[string]any, error) {
	resourceArn, _ := input["resourceArn"].(string)
	keys := strSlice(input, "tagKeys")

	return map[string]any{}, h.Backend.UntagResource(resourceArn, keys)
}

func (h *Handler) listTagsForResource(input map[string]any) (map[string]any, error) {
	resourceArn, _ := input["resourceArn"].(string)

	tags, err := h.Backend.ListTagsForResource(resourceArn)
	if err != nil {
		return nil, err
	}

	tagSlice := make([]map[string]any, 0, len(tags))
	for k, v := range tags {
		tagSlice = append(tagSlice, map[string]any{"tagKey": k, "tagValue": v})
	}

	return map[string]any{"tags": tagSlice}, nil
}

// --- Map helpers ---

func datasetGroupToMap(dg *DatasetGroup) map[string]any {
	return map[string]any{
		keyDatasetGroupArn:     dg.DatasetGroupArn,
		keyName:                dg.Name,
		keyDomain:              dg.Domain,
		"kmsKeyArn":            dg.KmsKeyArn,
		keyRoleArn:             dg.RoleArn,
		keyStatus:              dg.Status,
		keyCreationDateTime:    awstime.Epoch(dg.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(dg.LastUpdatedDateTime),
	}
}

func datasetToMap(ds *Dataset) map[string]any {
	return map[string]any{
		keyDatasetArn:          ds.DatasetArn,
		keyDatasetGroupArn:     ds.DatasetGroupArn,
		keySchemaArn:           ds.SchemaArn,
		keyName:                ds.Name,
		"datasetType":          ds.DatasetType,
		keyStatus:              ds.Status,
		keyCreationDateTime:    awstime.Epoch(ds.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(ds.LastUpdatedDateTime),
	}
}

func schemaToMap(s *Schema) map[string]any {
	return map[string]any{
		keySchemaArn:           s.SchemaArn,
		keyName:                s.Name,
		"schema":               s.Schema,
		keyDomain:              s.Domain,
		keyCreationDateTime:    awstime.Epoch(s.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(s.LastUpdatedDateTime),
	}
}

func solutionToMap(sol *Solution) map[string]any {
	return map[string]any{
		keySolutionArn:         sol.SolutionArn,
		keyName:                sol.Name,
		keyDatasetGroupArn:     sol.DatasetGroupArn,
		keyRecipeArn:           sol.RecipeArn,
		"performAutoML":        sol.PerformAutoML,
		"performHPO":           sol.PerformHPO,
		keyStatus:              sol.Status,
		keyCreationDateTime:    awstime.Epoch(sol.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(sol.LastUpdatedDateTime),
	}
}

func solutionVersionToMap(sv *SolutionVersion) map[string]any {
	return map[string]any{
		keySolutionVersionArn:  sv.SolutionVersionArn,
		keySolutionArn:         sv.SolutionArn,
		keyStatus:              sv.Status,
		"trainingMode":         sv.TrainingMode,
		"trainingHours":        sv.TrainingHours,
		keyCreationDateTime:    awstime.Epoch(sv.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(sv.LastUpdatedDateTime),
	}
}

func campaignToMap(c *Campaign) map[string]any {
	return map[string]any{
		keyCampaignArn:         c.CampaignArn,
		keyName:                c.Name,
		keySolutionVersionArn:  c.SolutionVersionArn,
		"minProvisionedTPS":    c.MinProvisionedTPS,
		keyStatus:              c.Status,
		keyCreationDateTime:    awstime.Epoch(c.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(c.LastUpdatedDateTime),
	}
}

func eventTrackerToMap(et *EventTracker) map[string]any {
	return map[string]any{
		"eventTrackerArn":      et.EventTrackerArn,
		keyName:                et.Name,
		keyDatasetGroupArn:     et.DatasetGroupArn,
		"trackingId":           et.TrackingID,
		keyStatus:              et.Status,
		keyCreationDateTime:    awstime.Epoch(et.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(et.LastUpdatedDateTime),
	}
}

func filterToMap(f *Filter) map[string]any {
	return map[string]any{
		"filterArn":            f.FilterArn,
		keyName:                f.Name,
		keyDatasetGroupArn:     f.DatasetGroupArn,
		"filterExpression":     f.FilterExpression,
		keyStatus:              f.Status,
		keyCreationDateTime:    awstime.Epoch(f.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(f.LastUpdatedDateTime),
	}
}

func recommenderToMap(r *Recommender) map[string]any {
	return map[string]any{
		keyRecommenderArn:  r.RecommenderArn,
		keyName:            r.Name,
		keyDatasetGroupArn: r.DatasetGroupArn,
		keyRecipeArn:       r.RecipeArn,
		keyStatus:          r.Status,
		"recommenderConfig": map[string]any{
			"minRecommendationRequestsPerSecond": r.MinRecommendationRequestsPerSecond,
		},
		keyCreationDateTime:    awstime.Epoch(r.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(r.LastUpdatedDateTime),
	}
}

func metricAttributionToMap(ma *MetricAttribution) map[string]any {
	return map[string]any{
		keyMetricAttributionArn: ma.MetricAttributionArn,
		keyName:                 ma.Name,
		keyDatasetGroupArn:      ma.DatasetGroupArn,
		"metricsOutputConfig":   ma.MetricsOutputConfig,
		keyStatus:               ma.Status,
		keyCreationDateTime:     awstime.Epoch(ma.CreationDateTime),
		keyLastUpdatedDateTime:  awstime.Epoch(ma.LastUpdatedDateTime),
	}
}

func datasetImportJobToMap(job *DatasetImportJob) map[string]any {
	return map[string]any{
		"datasetImportJobArn":  job.DatasetImportJobArn,
		keyJobName:             job.JobName,
		keyDatasetArn:          job.DatasetArn,
		keyRoleArn:             job.RoleArn,
		"dataSource":           job.DataSource,
		keyStatus:              job.Status,
		keyCreationDateTime:    awstime.Epoch(job.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(job.LastUpdatedDateTime),
	}
}

func datasetExportJobToMap(job *DatasetExportJob) map[string]any {
	return map[string]any{
		"datasetExportJobArn":  job.DatasetExportJobArn,
		keyJobName:             job.JobName,
		keyDatasetArn:          job.DatasetArn,
		keyRoleArn:             job.RoleArn,
		keyJobOutput:           job.JobOutput,
		keyStatus:              job.Status,
		keyCreationDateTime:    awstime.Epoch(job.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(job.LastUpdatedDateTime),
	}
}

func batchInferenceJobToMap(job *BatchInferenceJob) map[string]any {
	return map[string]any{
		"batchInferenceJobArn": job.BatchInferenceJobArn,
		keyJobName:             job.JobName,
		keySolutionVersionArn:  job.SolutionVersionArn,
		keyRoleArn:             job.RoleArn,
		"jobInput":             job.JobInput,
		keyJobOutput:           job.JobOutput,
		keyStatus:              job.Status,
		keyCreationDateTime:    awstime.Epoch(job.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(job.LastUpdatedDateTime),
	}
}

func batchSegmentJobToMap(job *BatchSegmentJob) map[string]any {
	return map[string]any{
		"batchSegmentJobArn":   job.BatchSegmentJobArn,
		keyJobName:             job.JobName,
		keySolutionVersionArn:  job.SolutionVersionArn,
		keyRoleArn:             job.RoleArn,
		"jobInput":             job.JobInput,
		keyJobOutput:           job.JobOutput,
		keyStatus:              job.Status,
		keyCreationDateTime:    awstime.Epoch(job.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(job.LastUpdatedDateTime),
	}
}

func dataDeletionJobToMap(job *DataDeletionJob) map[string]any {
	return map[string]any{
		"dataDeletionJobArn":   job.DataDeletionJobArn,
		keyJobName:             job.JobName,
		keyDatasetGroupArn:     job.DatasetGroupArn,
		keyRoleArn:             job.RoleArn,
		"dataSource":           job.DataSource,
		keyStatus:              job.Status,
		"numDeleted":           job.NumDeleted,
		keyCreationDateTime:    awstime.Epoch(job.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(job.LastUpdatedDateTime),
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

func intField(m map[string]any, key string) int { //nolint:unparam // key always "maxResults" by design
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
