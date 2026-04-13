package support

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const supportTargetPrefix = "AWSSupport_20130415."

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the Echo HTTP handler for AWS Support operations.
type Handler struct {
	Backend StorageBackend
}

// NewHandler creates a new Support handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Support" }

// GetSupportedOperations returns the list of supported Support operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"AddAttachmentsToSet",
		"AddCommunicationToCase",
		"CreateCase",
		"DescribeAttachment",
		"DescribeCases",
		"DescribeCommunications",
		"DescribeCreateCaseOptions",
		"DescribeServices",
		"DescribeSeverityLevels",
		"DescribeSupportedLanguages",
		"DescribeTrustedAdvisorCheckRefreshStatuses",
		"DescribeTrustedAdvisorCheckResult",
		"DescribeTrustedAdvisorCheckSummaries",
		"DescribeTrustedAdvisorChecks",
		"RefreshTrustedAdvisorCheck",
		"ResolveCase",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "support" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this Support instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a function that matches Support requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), supportTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the Support action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, supportTargetPrefix)
	if action == "" || action == target {
		return "Unknown"
	}

	return action
}

type extractSupportResourceInput struct {
	CaseID  string `json:"caseId"`
	Subject string `json:"subject"`
}

// ExtractResource extracts the case ID from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req extractSupportResourceInput
	_ = json.Unmarshal(body, &req)

	if req.CaseID != "" {
		return req.CaseID
	}

	return req.Subject
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"Support", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) dispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateCase":                   service.WrapOp(h.handleCreateCase),
		"DescribeCases":                service.WrapOp(h.handleDescribeCases),
		"ResolveCase":                  service.WrapOp(h.handleResolveCase),
		"AddCommunicationToCase":       service.WrapOp(h.handleAddCommunicationToCase),
		"DescribeCommunications":       service.WrapOp(h.handleDescribeCommunications),
		"DescribeTrustedAdvisorChecks": service.WrapOp(h.handleDescribeTrustedAdvisorChecks),
		"AddAttachmentsToSet":          service.WrapOp(h.handleAddAttachmentsToSet),
		"DescribeAttachment":           service.WrapOp(h.handleDescribeAttachment),
		"DescribeCreateCaseOptions":    service.WrapOp(h.handleDescribeCreateCaseOptions),
		"DescribeServices":             service.WrapOp(h.handleDescribeServices),
		"DescribeSeverityLevels":       service.WrapOp(h.handleDescribeSeverityLevels),
		"DescribeSupportedLanguages":   service.WrapOp(h.handleDescribeSupportedLanguages),
		"DescribeTrustedAdvisorCheckRefreshStatuses": service.WrapOp(
			h.handleDescribeTrustedAdvisorCheckRefreshStatuses,
		),
		"DescribeTrustedAdvisorCheckResult":    service.WrapOp(h.handleDescribeTrustedAdvisorCheckResult),
		"DescribeTrustedAdvisorCheckSummaries": service.WrapOp(h.handleDescribeTrustedAdvisorCheckSummaries),
		"RefreshTrustedAdvisorCheck":           service.WrapOp(h.handleRefreshTrustedAdvisorCheck),
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.dispatchTable()[action]
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
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrAttachmentNotFound):
		return c.JSON(http.StatusNotFound, map[string]string{"message": err.Error()})
	case errors.Is(err, ErrAlreadyResolved), errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}
}

type caseView struct {
	CaseID       string `json:"caseId"`
	Subject      string `json:"subject"`
	Status       string `json:"status"`
	ServiceCode  string `json:"serviceCode"`
	CategoryCode string `json:"categoryCode"`
	SeverityCode string `json:"severityCode"`
}

type createCaseOutput struct {
	CaseID string `json:"caseId"`
}

type describeCasesOutput struct {
	Cases []caseView `json:"cases"`
}

type resolveCaseOutput struct {
	InitialCaseStatus string `json:"initialCaseStatus"`
	FinalCaseStatus   string `json:"finalCaseStatus"`
}

type handleCreateCaseInput struct {
	Subject           string `json:"subject"`
	ServiceCode       string `json:"serviceCode"`
	CategoryCode      string `json:"categoryCode"`
	SeverityCode      string `json:"severityCode"`
	CommunicationBody string `json:"communicationBody"`
}

func (h *Handler) handleCreateCase(_ context.Context, in *handleCreateCaseInput) (*createCaseOutput, error) {
	if in.Subject == "" {
		return nil, fmt.Errorf("%w: subject is required", errInvalidRequest)
	}

	c2, err := h.Backend.CreateCase(
		in.Subject,
		in.ServiceCode,
		in.CategoryCode,
		in.SeverityCode,
		in.CommunicationBody,
	)
	if err != nil {
		return nil, err
	}

	return &createCaseOutput{CaseID: c2.CaseID}, nil
}

type handleDescribeCasesInput struct {
	CaseIDList []string `json:"caseIdList"`
}

func (h *Handler) handleDescribeCases(_ context.Context, in *handleDescribeCasesInput) (*describeCasesOutput, error) {
	cases := h.Backend.DescribeCases(in.CaseIDList)

	views := make([]caseView, 0, len(cases))
	for _, cs := range cases {
		views = append(views, caseView{
			CaseID:       cs.CaseID,
			Subject:      cs.Subject,
			Status:       cs.Status,
			ServiceCode:  cs.ServiceCode,
			CategoryCode: cs.CategoryCode,
			SeverityCode: cs.SeverityCode,
		})
	}

	return &describeCasesOutput{Cases: views}, nil
}

type handleResolveCaseInput struct {
	CaseID string `json:"caseId"`
}

func (h *Handler) handleResolveCase(_ context.Context, in *handleResolveCaseInput) (*resolveCaseOutput, error) {
	cs, err := h.Backend.ResolveCase(in.CaseID)
	if err != nil {
		return nil, err
	}

	return &resolveCaseOutput{
		InitialCaseStatus: "opened",
		FinalCaseStatus:   cs.Status,
	}, nil
}

type addCommunicationToCaseInput struct {
	CaseID            string `json:"caseId"`
	CommunicationBody string `json:"communicationBody"`
}

type addCommunicationToCaseOutput struct {
	Result bool `json:"result"`
}

func (h *Handler) handleAddCommunicationToCase(
	_ context.Context,
	in *addCommunicationToCaseInput,
) (*addCommunicationToCaseOutput, error) {
	if in.CaseID == "" {
		return nil, fmt.Errorf("%w: caseId is required", errInvalidRequest)
	}

	if err := h.Backend.AddCommunicationToCase(in.CaseID, in.CommunicationBody); err != nil {
		return nil, err
	}

	return &addCommunicationToCaseOutput{Result: true}, nil
}

type describeCommunicationsInput struct {
	CaseID string `json:"caseId"`
}

type communicationView struct {
	CaseID      string `json:"caseId"`
	Body        string `json:"body"`
	SubmittedBy string `json:"submittedBy"`
}

type describeCommunicationsOutput struct {
	NextToken      string              `json:"nextToken,omitempty"`
	Communications []communicationView `json:"communications"`
}

func (h *Handler) handleDescribeCommunications(
	_ context.Context,
	in *describeCommunicationsInput,
) (*describeCommunicationsOutput, error) {
	if in.CaseID == "" {
		return nil, fmt.Errorf("%w: caseId is required", errInvalidRequest)
	}

	comms, err := h.Backend.DescribeCommunications(in.CaseID)
	if err != nil {
		return nil, err
	}

	views := make([]communicationView, 0, len(comms))
	for _, c := range comms {
		views = append(views, communicationView{
			CaseID:      c.CaseID,
			Body:        c.Body,
			SubmittedBy: c.SubmittedBy,
		})
	}

	return &describeCommunicationsOutput{Communications: views}, nil
}

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
	_ *describeTrustedAdvisorChecksInput,
) (*describeTrustedAdvisorChecksOutput, error) {
	checks := h.Backend.DescribeTrustedAdvisorChecks()

	views := make([]trustedAdvisorCheckView, 0, len(checks))
	for _, c := range checks {
		views = append(views, trustedAdvisorCheckView(c))
	}

	return &describeTrustedAdvisorChecksOutput{Checks: views}, nil
}

type addAttachmentsToSetInput struct {
	AttachmentSetID string `json:"attachmentSetId,omitempty"`
}

type addAttachmentsToSetOutput struct {
	AttachmentSetID string `json:"attachmentSetId"`
	ExpiryTime      string `json:"expiryTime"`
}

func (h *Handler) handleAddAttachmentsToSet(
	_ context.Context,
	in *addAttachmentsToSetInput,
) (*addAttachmentsToSetOutput, error) {
	setID, expiry, err := h.Backend.AddAttachmentsToSet(in.AttachmentSetID)
	if err != nil {
		return nil, err
	}

	return &addAttachmentsToSetOutput{
		AttachmentSetID: setID,
		ExpiryTime:      expiry.UTC().Format("2006-01-02T15:04:05.000Z"),
	}, nil
}

type describeAttachmentInput struct {
	AttachmentID string `json:"attachmentId"`
}

type attachmentView struct {
	FileName string `json:"fileName"`
	Data     []byte `json:"data"`
}

type describeAttachmentOutput struct {
	Attachment attachmentView `json:"attachment"`
}

func (h *Handler) handleDescribeAttachment(
	_ context.Context,
	in *describeAttachmentInput,
) (*describeAttachmentOutput, error) {
	if in.AttachmentID == "" {
		return nil, fmt.Errorf("%w: attachmentId is required", errInvalidRequest)
	}

	a, err := h.Backend.DescribeAttachment(in.AttachmentID)
	if err != nil {
		return nil, err
	}

	return &describeAttachmentOutput{
		Attachment: attachmentView{
			FileName: a.FileName,
			Data:     a.Data,
		},
	}, nil
}

type describeCreateCaseOptionsInput struct {
	IssueType    string `json:"issueType"`
	ServiceCode  string `json:"serviceCode"`
	CategoryCode string `json:"categoryCode"`
	Language     string `json:"language"`
}

type communicationTypeOptionsView struct {
	Type                string          `json:"type"`
	SupportedHours      []SupportedHour `json:"supportedHours"`
	DatesWithoutSupport []DateInterval  `json:"datesWithoutSupport"`
}

type describeCreateCaseOptionsOutput struct {
	LanguageAvailability string                         `json:"languageAvailability"`
	CommunicationTypes   []communicationTypeOptionsView `json:"communicationTypes"`
}

func (h *Handler) handleDescribeCreateCaseOptions(
	_ context.Context,
	in *describeCreateCaseOptionsInput,
) (*describeCreateCaseOptionsOutput, error) {
	result := h.Backend.DescribeCreateCaseOptions(in.IssueType, in.ServiceCode, in.CategoryCode, in.Language)

	views := make([]communicationTypeOptionsView, 0, len(result.CommunicationTypes))
	for _, ct := range result.CommunicationTypes {
		views = append(views, communicationTypeOptionsView(ct))
	}

	return &describeCreateCaseOptionsOutput{
		CommunicationTypes:   views,
		LanguageAvailability: result.LanguageAvailability,
	}, nil
}

type describeServicesInput struct {
	Language        string   `json:"language"`
	ServiceCodeList []string `json:"serviceCodeList"`
}

type serviceCategoryView struct {
	Code string `json:"code"`
	Name string `json:"name"`
}

type serviceView struct {
	Code       string                `json:"code"`
	Name       string                `json:"name"`
	Categories []serviceCategoryView `json:"categories"`
}

type describeServicesOutput struct {
	Services []serviceView `json:"services"`
}

func (h *Handler) handleDescribeServices(
	_ context.Context,
	in *describeServicesInput,
) (*describeServicesOutput, error) {
	services := h.Backend.DescribeServices(in.ServiceCodeList)

	views := make([]serviceView, 0, len(services))
	for _, svc := range services {
		cats := make([]serviceCategoryView, 0, len(svc.Categories))
		for _, c := range svc.Categories {
			cats = append(cats, serviceCategoryView(c))
		}

		views = append(views, serviceView{
			Code:       svc.Code,
			Name:       svc.Name,
			Categories: cats,
		})
	}

	return &describeServicesOutput{Services: views}, nil
}

type describeSeverityLevelsInput struct {
	Language string `json:"language"`
}

type severityLevelView struct {
	Code string `json:"code"`
	Name string `json:"name"`
}

type describeSeverityLevelsOutput struct {
	SeverityLevels []severityLevelView `json:"severityLevels"`
}

func (h *Handler) handleDescribeSeverityLevels(
	_ context.Context,
	in *describeSeverityLevelsInput,
) (*describeSeverityLevelsOutput, error) {
	levels := h.Backend.DescribeSeverityLevels(in.Language)

	views := make([]severityLevelView, 0, len(levels))
	for _, l := range levels {
		views = append(views, severityLevelView(l))
	}

	return &describeSeverityLevelsOutput{SeverityLevels: views}, nil
}

type describeSupportedLanguagesInput struct {
	IssueType     string `json:"issueType"`
	ServiceCode   string `json:"serviceCode"`
	SeverityLevel string `json:"severityLevel"`
}

type supportedLanguageView struct {
	Code     string `json:"code"`
	Display  string `json:"display"`
	Language string `json:"language"`
}

type describeSupportedLanguagesOutput struct {
	SupportedLanguages []supportedLanguageView `json:"supportedLanguages"`
}

func (h *Handler) handleDescribeSupportedLanguages(
	_ context.Context,
	in *describeSupportedLanguagesInput,
) (*describeSupportedLanguagesOutput, error) {
	langs := h.Backend.DescribeSupportedLanguages(in.IssueType, in.ServiceCode, in.SeverityLevel)

	views := make([]supportedLanguageView, 0, len(langs))
	for _, l := range langs {
		views = append(views, supportedLanguageView(l))
	}

	return &describeSupportedLanguagesOutput{SupportedLanguages: views}, nil
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
	statuses := h.Backend.DescribeTrustedAdvisorCheckRefreshStatuses(in.CheckIDs)

	views := make([]trustedAdvisorCheckRefreshStatusView, 0, len(statuses))
	for _, s := range statuses {
		views = append(views, trustedAdvisorCheckRefreshStatusView(s))
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
	CheckID          string                             `json:"checkId"`
	Status           string                             `json:"status"`
	Timestamp        string                             `json:"timestamp"`
	FlaggedResources []trustedAdvisorResourceDetailView `json:"flaggedResources"`
	ResourcesSummary trustedAdvisorResourcesSummaryView `json:"resourcesSummary"`
}

type describeTrustedAdvisorCheckResultOutput struct {
	Result trustedAdvisorCheckResultView `json:"result"`
}

func (h *Handler) handleDescribeTrustedAdvisorCheckResult(
	_ context.Context,
	in *describeTrustedAdvisorCheckResultInput,
) (*describeTrustedAdvisorCheckResultOutput, error) {
	if in.CheckID == "" {
		return nil, fmt.Errorf("%w: checkId is required", errInvalidRequest)
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
		},
	}, nil
}

type describeTrustedAdvisorCheckSummariesInput struct {
	CheckIDs []string `json:"checkIds"`
}

type trustedAdvisorCheckSummaryView struct {
	CheckID             string                             `json:"checkId"`
	Status              string                             `json:"status"`
	Timestamp           string                             `json:"timestamp"`
	HasFlaggedResources bool                               `json:"hasFlaggedResources"`
	ResourcesSummary    trustedAdvisorResourcesSummaryView `json:"resourcesSummary"`
}

type describeTrustedAdvisorCheckSummariesOutput struct {
	Summaries []trustedAdvisorCheckSummaryView `json:"summaries"`
}

func (h *Handler) handleDescribeTrustedAdvisorCheckSummaries(
	_ context.Context,
	in *describeTrustedAdvisorCheckSummariesInput,
) (*describeTrustedAdvisorCheckSummariesOutput, error) {
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
		return nil, fmt.Errorf("%w: checkId is required", errInvalidRequest)
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
