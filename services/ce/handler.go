package ce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/labstack/echo/v5"
)

const (
	ceTargetPrefix          = "AWSInsightsIndexService."
	defaultStartDate        = "2024-01-01"
	defaultEndDate          = "2024-02-01"
	defaultForecastStart    = "2024-02-01"
	defaultForecastEnd      = "2024-03-01"
	defaultGranularity      = "MONTHLY"
	handlerZeroAmount       = "0.0000"
	handlerSavingsPlansType = "COMPUTE_SP"
	handlerRegionDefault    = config.DefaultRegion
	handlerCoverPct         = "65.0000"
	handlerROI              = "25.0000"
	handlerSPUtilPct        = "85.0000"
	handlerCurrencyCode     = "USD"

	anomalyActualSpendMultiplier   = 1.2  // actual spend is 20% above impact
	anomalyExpectedSpendMultiplier = 0.9  // expected spend is 10% below impact
	anomalyImpactPercentage        = 25.0 // synthetic impact percentage
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the Echo HTTP handler for Cost Explorer (Ce) operations.
type Handler struct {
	Backend *InMemoryBackend
	ops     map[string]service.JSONOpFunc
}

// NewHandler creates a new Cost Explorer handler backed by backend.
// backend must not be nil.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// Name returns the service name.
func (h *Handler) Name() string { return "Ce" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateCostCategoryDefinition",
		"DeleteCostCategoryDefinition",
		"DescribeCostCategoryDefinition",
		"ListCostCategoryDefinitions",
		"UpdateCostCategoryDefinition",
		"CreateAnomalyMonitor",
		"DeleteAnomalyMonitor",
		"GetAnomalyMonitors",
		"UpdateAnomalyMonitor",
		"CreateAnomalySubscription",
		"DeleteAnomalySubscription",
		"GetAnomalySubscriptions",
		"UpdateAnomalySubscription",
		"GetCostAndUsage",
		"GetCostForecast",
		"GetUsageForecast",
		"GetDimensionValues",
		"GetTags",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
		"GetAnomalies",
		"GetApproximateUsageRecords",
		"GetCommitmentPurchaseAnalysis",
		"GetCostAndUsageComparisons",
		"GetCostAndUsageWithResources",
		"GetCostCategories",
		"GetCostComparisonDrivers",
		"GetReservationCoverage",
		"GetReservationPurchaseRecommendation",
		"GetReservationUtilization",
		"GetRightsizingRecommendation",
		"GetSavingsPlanPurchaseRecommendationDetails",
		"GetSavingsPlansCoverage",
		"GetSavingsPlansPurchaseRecommendation",
		"GetSavingsPlansUtilization",
		"GetSavingsPlansUtilizationDetails",
		"ListCommitmentPurchaseAnalyses",
		"ListCostAllocationTagBackfillHistory",
		"ListCostAllocationTags",
		"ListCostCategoryResourceAssociations",
		"ListSavingsPlansPurchaseRecommendationGeneration",
		"ProvideAnomalyFeedback",
		"StartCommitmentPurchaseAnalysis",
		"StartCostAllocationTagBackfill",
		"StartSavingsPlansPurchaseRecommendationGeneration",
		"UpdateCostAllocationTagsStatus",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "ce" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches Cost Explorer requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), ceTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the Cost Explorer action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	return strings.TrimPrefix(target, ceTargetPrefix)
}

// ExtractResource extracts the resource identifier from the request (not used for Ce).
func (h *Handler) ExtractResource(_ *echo.Context) string {
	return ""
}

// Handler returns the Echo handler function for Cost Explorer requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"Ce", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

// buildOps assembles the full action -> handler dispatch table by merging each
// op-family's fragment (defined alongside its handlers in handler_<family>.go).
func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	ops := make(map[string]service.JSONOpFunc)

	maps.Copy(ops, h.buildCostCategoryOps())
	maps.Copy(ops, h.buildAnomalyOps())
	maps.Copy(ops, h.buildCostUsageOps())
	maps.Copy(ops, h.buildTagOps())
	maps.Copy(ops, h.buildReservationOps())
	maps.Copy(ops, h.buildSavingsPlansOps())
	maps.Copy(ops, h.buildCommitmentOps())
	maps.Copy(ops, h.buildCostAllocationTagOps())

	return ops
}

// ceRegion returns the request-scoped region from ctx, falling back to the handler default.
func ceRegion(ctx context.Context) string {
	if r := awsmeta.Region(ctx); r != "" {
		return r
	}

	return handlerRegionDefault
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnknownAction, action)
	}

	result, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, ErrUnknownMonitor):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "UnknownMonitorException",
			Message: err.Error(),
		})

		// Real AWS CE returns HTTP 400 for every modeled client-fault exception (verified
		// against API_DeleteAnomalyMonitor/API_UpdateAnomalyMonitor/API_GetAnomalyMonitors),
		// not the generic 404/409 an unstyled REST mapping might suggest.
		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, ErrUnknownSubscription):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "UnknownSubscriptionException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, ErrNotFound):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "ResourceNotFoundException",
			Message: err.Error(),
		})

		// See API_DescribeCostCategoryDefinition: ResourceNotFoundException is documented
		// as HTTP 400, not 404.
		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, ErrAlreadyExists):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "ServiceQuotaExceededException",
			Message: err.Error(),
		})

		// See API_CreateCostCategoryDefinition: ServiceQuotaExceededException is documented
		// as HTTP 400, not 409.
		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, ErrValidation):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "ValidationError",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	// "ValidationError" is CE's documented CommonErrors type for malformed/
	// missing-required-member requests (see ErrValidation above) -- it is not
	// tied to any one operation's model, so it fits here too.
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "ValidationError",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	default:
		// costexplorer@v1.67.4's types/errors.go declares no internal-server/
		// generic-server exception at all -- left untyped rather than inventing one.
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}
}
