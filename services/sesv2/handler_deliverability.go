package sesv2

import (
	"encoding/json"
	"fmt"

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

type putDeliverabilityDashboardOptionInput struct {
	SubscribedDomains []struct {
		Domain string `json:"Domain"`
	} `json:"SubscribedDomains"`
	DashboardEnabled bool `json:"DashboardEnabled"`
}

func (h *Handler) handlePutDeliverabilityDashboardOption(c *echo.Context) (any, error) {
	var in putDeliverabilityDashboardOptionInput
	if err := decodeSESv2Body(c, &in); err != nil {
		return nil, err
	}

	domains := make([]string, 0, len(in.SubscribedDomains))
	for _, d := range in.SubscribedDomains {
		domains = append(domains, d.Domain)
	}

	if err := h.Backend.PutDeliverabilityDashboardOption(in.DashboardEnabled, domains); err != nil {
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
// GET /v2/email/deliverability-dashboard/campaigns/{CampaignId}. The real
// SDK op takes only a campaign ID -- no domain -- confirmed against
// GetDomainDeliverabilityCampaignInput in aws-sdk-go-v2/service/sesv2.
func (h *Handler) handleGetDomainDeliverabilityCampaign(campaignID string) (any, error) {
	result, err := h.Backend.GetDomainDeliverabilityCampaign(campaignID)
	if err != nil {
		return nil, err
	}

	return map[string]any{"DomainDeliverabilityCampaign": result}, nil
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

type getEmailAddressInsightsInput struct {
	EmailAddress string `json:"EmailAddress"`
}

// handleGetEmailAddressInsights serves POST /v2/email/email-address-insights
// (EmailAddress travels in the JSON body, not the URL).
func (h *Handler) handleGetEmailAddressInsights(c *echo.Context) (any, error) {
	var in getEmailAddressInsightsInput
	if err := decodeSESv2Body(c, &in); err != nil {
		return nil, err
	}

	result, err := h.Backend.GetEmailAddressInsights(in.EmailAddress)
	if err != nil {
		return nil, err
	}

	return result, nil
}

type listRecommendationsInput struct {
	Filter    map[string]string `json:"Filter"`
	NextToken string            `json:"NextToken"`
	PageSize  int32             `json:"PageSize"`
}

// handleListRecommendations serves POST /v2/email/vdm/recommendations.
func (h *Handler) handleListRecommendations(c *echo.Context) (any, error) {
	var in listRecommendationsInput
	if err := decodeSESv2Body(c, &in); err != nil {
		return nil, err
	}

	items, next, err := h.Backend.ListRecommendations(in.Filter, in.NextToken, int(in.PageSize))
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

type listReputationEntitiesInput struct {
	Filter    map[string]string `json:"Filter"`
	NextToken string            `json:"NextToken"`
	PageSize  int32             `json:"PageSize"`
}

// handleListReputationEntities serves POST /v2/email/reputation/entities.
func (h *Handler) handleListReputationEntities(c *echo.Context) (any, error) {
	var in listReputationEntitiesInput
	if err := decodeSESv2Body(c, &in); err != nil {
		return nil, err
	}

	items, next, err := h.Backend.ListReputationEntities(in.Filter, in.NextToken, int(in.PageSize))
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
