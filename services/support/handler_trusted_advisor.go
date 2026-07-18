package support

import (
	"context"
	"fmt"
)

type describeTrustedAdvisorChecksInput struct {
	Language string `json:"language"`
}

type trustedAdvisorCheckView struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Category    string   `json:"category"`
	Metadata    []string `json:"metadata"`
}

type describeTrustedAdvisorChecksOutput struct {
	Checks []trustedAdvisorCheckView `json:"checks"`
}

func (h *Handler) handleDescribeTrustedAdvisorChecks(
	_ context.Context,
	in *describeTrustedAdvisorChecksInput,
) (*describeTrustedAdvisorChecksOutput, error) {
	if !validLanguage(in.Language) {
		return nil, fmt.Errorf("%w: language is required and must be supported", ErrValidation)
	}
	checks := h.Backend.DescribeTrustedAdvisorChecks()

	views := make([]trustedAdvisorCheckView, 0, len(checks))
	for _, c := range checks {
		views = append(views, trustedAdvisorCheckView(c))
	}

	return &describeTrustedAdvisorChecksOutput{Checks: views}, nil
}

type describeTrustedAdvisorCheckRefreshStatusesInput struct {
	CheckIDs []string `json:"checkIds"`
}

type trustedAdvisorCheckRefreshStatusView struct {
	CheckID                    string `json:"checkId"`
	Status                     string `json:"status"`
	MillisUntilNextRefreshable int64  `json:"millisUntilNextRefreshable"`
}

type describeTrustedAdvisorCheckRefreshStatusesOutput struct {
	Statuses []trustedAdvisorCheckRefreshStatusView `json:"statuses"`
}

func (h *Handler) handleDescribeTrustedAdvisorCheckRefreshStatuses(
	_ context.Context,
	in *describeTrustedAdvisorCheckRefreshStatusesInput,
) (*describeTrustedAdvisorCheckRefreshStatusesOutput, error) {
	if err := validateCheckIDs(in.CheckIDs); err != nil {
		return nil, err
	}
	statuses := h.Backend.DescribeTrustedAdvisorCheckRefreshStatuses(in.CheckIDs)

	views := make([]trustedAdvisorCheckRefreshStatusView, 0, len(statuses))
	for _, s := range statuses {
		views = append(views, trustedAdvisorCheckRefreshStatusView{
			CheckID: s.CheckID, Status: s.Status, MillisUntilNextRefreshable: s.MillisUntilNextRefreshable,
		})
	}

	return &describeTrustedAdvisorCheckRefreshStatusesOutput{Statuses: views}, nil
}

type describeTrustedAdvisorCheckResultInput struct {
	CheckID  string `json:"checkId"`
	Language string `json:"language"`
}

type trustedAdvisorResourceDetailView struct {
	ResourceID   string   `json:"resourceId"`
	Status       string   `json:"status"`
	Region       string   `json:"region"`
	Metadata     []string `json:"metadata"`
	IsSuppressed bool     `json:"isSuppressed"`
}

type trustedAdvisorResourcesSummaryView struct {
	ResourcesFlagged    int64 `json:"resourcesFlagged"`
	ResourcesIgnored    int64 `json:"resourcesIgnored"`
	ResourcesProcessed  int64 `json:"resourcesProcessed"`
	ResourcesSuppressed int64 `json:"resourcesSuppressed"`
}

type trustedAdvisorCheckResultView struct {
	CategorySpecificSummary *TrustedAdvisorCategorySpecificSummary `json:"categorySpecificSummary"`
	CheckID                 string                                 `json:"checkId"`
	Status                  string                                 `json:"status"`
	Timestamp               string                                 `json:"timestamp"`
	FlaggedResources        []trustedAdvisorResourceDetailView     `json:"flaggedResources"`
	ResourcesSummary        trustedAdvisorResourcesSummaryView     `json:"resourcesSummary"`
}

type describeTrustedAdvisorCheckResultOutput struct {
	Result trustedAdvisorCheckResultView `json:"result"`
}

func (h *Handler) handleDescribeTrustedAdvisorCheckResult(
	_ context.Context,
	in *describeTrustedAdvisorCheckResultInput,
) (*describeTrustedAdvisorCheckResultOutput, error) {
	if in.CheckID == "" {
		return nil, fmt.Errorf("%w: checkId is required", ErrValidation)
	}
	if !validCheckID(in.CheckID) {
		return nil, fmt.Errorf("%w: invalid checkId", ErrValidation)
	}

	result := h.Backend.DescribeTrustedAdvisorCheckResult(in.CheckID, in.Language)

	flagged := make([]trustedAdvisorResourceDetailView, 0, len(result.FlaggedResources))
	for _, r := range result.FlaggedResources {
		flagged = append(flagged, trustedAdvisorResourceDetailView(r))
	}

	return &describeTrustedAdvisorCheckResultOutput{
		Result: trustedAdvisorCheckResultView{
			CheckID:          result.CheckID,
			Status:           result.Status,
			Timestamp:        result.Timestamp,
			FlaggedResources: flagged,
			ResourcesSummary: trustedAdvisorResourcesSummaryView{
				ResourcesFlagged:    result.ResourcesSummary.ResourcesFlagged,
				ResourcesIgnored:    result.ResourcesSummary.ResourcesIgnored,
				ResourcesProcessed:  result.ResourcesSummary.ResourcesProcessed,
				ResourcesSuppressed: result.ResourcesSummary.ResourcesSuppressed,
			},
			CategorySpecificSummary: result.CategorySpecificSummary,
		},
	}, nil
}

type describeTrustedAdvisorCheckSummariesInput struct {
	CheckIDs []string `json:"checkIds"`
}

type trustedAdvisorCheckSummaryView struct {
	CategorySpecificSummary *TrustedAdvisorCategorySpecificSummary `json:"categorySpecificSummary"`
	CheckID                 string                                 `json:"checkId"`
	Status                  string                                 `json:"status"`
	Timestamp               string                                 `json:"timestamp"`
	ResourcesSummary        trustedAdvisorResourcesSummaryView     `json:"resourcesSummary"`
	HasFlaggedResources     bool                                   `json:"hasFlaggedResources"`
}

type describeTrustedAdvisorCheckSummariesOutput struct {
	Summaries []trustedAdvisorCheckSummaryView `json:"summaries"`
}

func (h *Handler) handleDescribeTrustedAdvisorCheckSummaries(
	_ context.Context,
	in *describeTrustedAdvisorCheckSummariesInput,
) (*describeTrustedAdvisorCheckSummariesOutput, error) {
	if err := validateCheckIDs(in.CheckIDs); err != nil {
		return nil, err
	}
	summaries := h.Backend.DescribeTrustedAdvisorCheckSummaries(in.CheckIDs)

	views := make([]trustedAdvisorCheckSummaryView, 0, len(summaries))
	for _, s := range summaries {
		views = append(views, trustedAdvisorCheckSummaryView{
			CheckID:             s.CheckID,
			Status:              s.Status,
			Timestamp:           s.Timestamp,
			HasFlaggedResources: s.HasFlaggedResources,
			ResourcesSummary: trustedAdvisorResourcesSummaryView{
				ResourcesFlagged:    s.ResourcesSummary.ResourcesFlagged,
				ResourcesIgnored:    s.ResourcesSummary.ResourcesIgnored,
				ResourcesProcessed:  s.ResourcesSummary.ResourcesProcessed,
				ResourcesSuppressed: s.ResourcesSummary.ResourcesSuppressed,
			},
			CategorySpecificSummary: s.CategorySpecificSummary,
		})
	}

	return &describeTrustedAdvisorCheckSummariesOutput{Summaries: views}, nil
}

type refreshTrustedAdvisorCheckInput struct {
	CheckID string `json:"checkId"`
}

type refreshTrustedAdvisorCheckOutput struct {
	Status trustedAdvisorCheckRefreshStatusView `json:"status"`
}

func (h *Handler) handleRefreshTrustedAdvisorCheck(
	_ context.Context,
	in *refreshTrustedAdvisorCheckInput,
) (*refreshTrustedAdvisorCheckOutput, error) {
	if in.CheckID == "" {
		return nil, fmt.Errorf("%w: checkId is required", ErrValidation)
	}
	if !validCheckID(in.CheckID) {
		return nil, fmt.Errorf("%w: invalid checkId", ErrValidation)
	}

	status, err := h.Backend.RefreshTrustedAdvisorCheck(in.CheckID)
	if err != nil {
		return nil, err
	}

	return &refreshTrustedAdvisorCheckOutput{
		Status: trustedAdvisorCheckRefreshStatusView{
			CheckID:                    status.CheckID,
			Status:                     status.Status,
			MillisUntilNextRefreshable: status.MillisUntilNextRefreshable,
		},
	}, nil
}
