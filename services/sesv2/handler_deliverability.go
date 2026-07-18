package sesv2

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/labstack/echo/v5"
)

type createDeliverabilityTestReportInput struct {
	ReportName       string `json:"ReportName"`
	FromEmailAddress string `json:"FromEmailAddress"`
}

type createDeliverabilityTestReportOutput struct {
	ReportID                 string `json:"ReportId"`
	DeliverabilityTestStatus string `json:"DeliverabilityTestStatus"`
}

func (h *Handler) handleCreateDeliverabilityTestReport(c *echo.Context) (any, error) {
	var in createDeliverabilityTestReportInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	report, err := h.Backend.CreateDeliverabilityTestReport(in.ReportName, in.FromEmailAddress)
	if err != nil {
		return nil, err
	}

	return &createDeliverabilityTestReportOutput{
		ReportID:                 report.ReportID,
		DeliverabilityTestStatus: report.DeliverabilityTestStatus,
	}, nil
}

// deliverability handlers

func (h *Handler) handleGetDeliverabilityDashboardOptions() (any, error) {
	opts, err := h.Backend.GetDeliverabilityDashboardOptions()
	if err != nil {
		return nil, err
	}

	return opts, nil
}

func (h *Handler) handlePutDeliverabilityDashboardOption() (any, error) {
	if err := h.Backend.PutDeliverabilityDashboardOption(); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleGetDeliverabilityTestReport(reportID string) (any, error) {
	r, err := h.Backend.GetDeliverabilityTestReport(reportID)
	if err != nil {
		return nil, err
	}

	// IspPlacements and OverallPlacement are required members of
	// GetDeliverabilityTestReportOutput; gopherstack doesn't model per-ISP
	// placement data, so report an empty list (a nil/absent OverallPlacement
	// decodes as its pointer zero value, which real SDK clients accept).
	return map[string]any{
		"DeliverabilityTestReport": toDeliverabilityTestReportItemOutput(r),
		"IspPlacements":            []any{},
	}, nil
}

func (h *Handler) handleListDeliverabilityTestReports(c *echo.Context) (any, error) {
	nextToken := c.QueryParam("NextToken")
	pg := h.Backend.ListDeliverabilityTestReports(nextToken, 0)

	items := make([]deliverabilityTestReportItemOutput, 0, len(pg.Data))
	for _, r := range pg.Data {
		items = append(items, toDeliverabilityTestReportItemOutput(r))
	}

	return map[string]any{
		"DeliverabilityTestReports": items,
		keyNextToken:                pg.Next,
	}, nil
}

// handleGetDomainDeliverabilityCampaign serves
// GET /v2/email/deliverability-dashboard/campaigns/{domain}/{campaignId}.
// After stripping the /v2/email/ prefix the path yields exactly 4 segments
// (0: "deliverability-dashboard", 1: "campaigns", 2: domain, 3: campaignId),
// so the campaign ID lives at segments[3] -- matching every other handler in
// this file that reads a 4-segment path. (Bug fix: was `>= 5`/`segments[4]`.)
func (h *Handler) handleGetDomainDeliverabilityCampaign(
	c *echo.Context,
	domain string,
) (any, error) {
	segments := strings.Split(strings.TrimPrefix(c.Request().URL.Path, sesv2PathPrefix), "/")
	campaignID := ""

	if len(segments) >= 4 { //nolint:mnd // URL segment index is self-documenting in context
		campaignID = segments[3]
	}

	result, err := h.Backend.GetDomainDeliverabilityCampaign(domain, campaignID)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (h *Handler) handleGetDomainStatisticsReport(c *echo.Context, domain string) (any, error) {
	startDate := c.QueryParam("StartDate")
	endDate := c.QueryParam("EndDate")

	result, err := h.Backend.GetDomainStatisticsReport(domain, startDate, endDate)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (h *Handler) handleListDomainDeliverabilityCampaigns(
	c *echo.Context,
	domain string,
) (any, error) {
	startDate := c.QueryParam("StartDate")
	endDate := c.QueryParam("EndDate")
	nextToken := c.QueryParam("NextToken")

	items, next, err := h.Backend.ListDomainDeliverabilityCampaigns(
		startDate,
		endDate,
		domain,
		nextToken,
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"DomainDeliverabilityCampaigns": items,
		keyNextToken:                    next,
	}, nil
}

func (h *Handler) handleGetEmailAddressInsights(email string) (any, error) {
	result, err := h.Backend.GetEmailAddressInsights(email)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (h *Handler) handleListRecommendations(c *echo.Context) (any, error) {
	nextToken := c.QueryParam("NextToken")

	items, next, err := h.Backend.ListRecommendations(nextToken, 0)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"Recommendations": items,
		keyNextToken:      next,
	}, nil
}

// reputation entity handlers

func (h *Handler) handleGetReputationEntity(entityID string) (any, error) {
	result, err := h.Backend.GetReputationEntity(entityID)
	if err != nil {
		return nil, err
	}

	return map[string]any{"ReputationEntity": result}, nil
}

func (h *Handler) handleListReputationEntities(c *echo.Context) (any, error) {
	nextToken := c.QueryParam("NextToken")

	items, next, err := h.Backend.ListReputationEntities(nextToken, 0)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"ReputationEntities": items,
		keyNextToken:         next,
	}, nil
}

type updateReputationEntityCustomerManagedStatusInput struct {
	// SendingStatus is the field name used by the AWS SDK.
	SendingStatus string `json:"SendingStatus"`
	// CustomerManagedStatus is accepted as an alias for callers that post it directly.
	CustomerManagedStatus string `json:"CustomerManagedStatus"`
}

func (h *Handler) handleUpdateReputationEntityCustomerManagedStatus(
	c *echo.Context,
	entityID string,
) (any, error) {
	var in updateReputationEntityCustomerManagedStatusInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	status := in.SendingStatus
	if status == "" {
		status = in.CustomerManagedStatus
	}

	if err := h.Backend.UpdateReputationEntityCustomerManagedStatus(entityID, status); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

type updateReputationEntityPolicyInput struct {
	// ReputationEntityPolicy is the field name used by the AWS SDK.
	ReputationEntityPolicy string `json:"ReputationEntityPolicy"`
	// Policy is accepted as an alias for callers that post it directly.
	Policy string `json:"Policy"`
}

func (h *Handler) handleUpdateReputationEntityPolicy(
	c *echo.Context,
	entityID string,
) (any, error) {
	var in updateReputationEntityPolicyInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	policy := in.ReputationEntityPolicy
	if policy == "" {
		policy = in.Policy
	}

	if err := h.Backend.UpdateReputationEntityPolicy(entityID, policy); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}
