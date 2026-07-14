package support

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	keyMessageField = "message"
)

const supportTargetPrefix = "AWSSupport_20130415."

var errUnknownAction = errors.New("unknown action")

const defaultJanitorInterval = 10 * time.Minute

// Handler is the Echo HTTP handler for AWS Support operations.
type Handler struct {
	Backend       StorageBackend
	ops           map[string]service.JSONOpFunc
	janitorCancel context.CancelFunc
	janitorDone   chan struct{}
}

// NewHandler creates a new Support handler.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// StartWorker starts the background janitor for attachment sets.
func (h *Handler) StartWorker(ctx context.Context) error {
	if mem, ok := h.Backend.(*InMemoryBackend); ok {
		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		h.janitorCancel = cancel
		h.janitorDone = done

		go func() {
			defer close(done)
			// Run janitor every 10 minutes.
			mem.RunJanitor(runCtx, defaultJanitorInterval)
		}()
	}

	return nil
}

// Shutdown stops the background janitor.
func (h *Handler) Shutdown(ctx context.Context) {
	if h.janitorCancel != nil {
		h.janitorCancel()
	}

	if h.janitorDone != nil {
		select {
		case <-h.janitorDone:
		case <-ctx.Done():
		}
	}
}

var (
	_ service.BackgroundWorker = (*Handler)(nil)
	_ service.Shutdowner       = (*Handler)(nil)
)

// Reset clears the handler's backend state.
func (h *Handler) Reset() { h.Backend.Reset() }

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
func (h *Handler) ChaosRegions() []string { return []string{"us-east-1"} }

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
	CaseID       string `json:"caseId"`
	CheckID      string `json:"checkId"`
	AttachmentID string `json:"attachmentId"`
	Subject      string `json:"subject"`
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
	if req.CheckID != "" {
		return req.CheckID
	}
	if req.AttachmentID != "" {
		return req.AttachmentID
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

// buildOps constructs the dispatch map once at handler creation time.
func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateCase":             service.WrapOp(h.handleCreateCase),
		"DescribeCases":          service.WrapOp(h.handleDescribeCases),
		"ResolveCase":            service.WrapOp(h.handleResolveCase),
		"AddCommunicationToCase": service.WrapOp(h.handleAddCommunicationToCase),
		"DescribeCommunications": service.WrapOp(h.handleDescribeCommunications),
		"DescribeTrustedAdvisorChecks": service.WrapOp(
			h.handleDescribeTrustedAdvisorChecks,
		),
		"AddAttachmentsToSet":       service.WrapOp(h.handleAddAttachmentsToSet),
		"DescribeAttachment":        service.WrapOp(h.handleDescribeAttachment),
		"DescribeCreateCaseOptions": service.WrapOp(h.handleDescribeCreateCaseOptions),
		"DescribeServices":          service.WrapOp(h.handleDescribeServices),
		"DescribeSeverityLevels":    service.WrapOp(h.handleDescribeSeverityLevels),
		"DescribeSupportedLanguages": service.WrapOp(
			h.handleDescribeSupportedLanguages,
		),
		"DescribeTrustedAdvisorCheckRefreshStatuses": service.WrapOp(
			h.handleDescribeTrustedAdvisorCheckRefreshStatuses,
		),
		"DescribeTrustedAdvisorCheckResult": service.WrapOp(
			h.handleDescribeTrustedAdvisorCheckResult,
		),
		"DescribeTrustedAdvisorCheckSummaries": service.WrapOp(
			h.handleDescribeTrustedAdvisorCheckSummaries,
		),
		"RefreshTrustedAdvisorCheck": service.WrapOp(h.handleRefreshTrustedAdvisorCheck),
	}
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
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrAttachmentNotFound), errors.Is(err, ErrAttachmentSetNotFound):
		return c.JSON(http.StatusNotFound, map[string]string{keyMessageField: err.Error()})
	case errors.Is(err, ErrValidation), errors.Is(err, ErrAttachmentSetExpired),
		errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{keyMessageField: err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{keyMessageField: err.Error()})
	}
}

type caseView struct {
	RecentCommunications *recentCaseCommunicationsView `json:"recentCommunications,omitempty"`
	CreatedTime          string                        `json:"timeCreated"`
	CaseID               string                        `json:"caseId"`
	DisplayID            string                        `json:"displayId"`
	Subject              string                        `json:"subject"`
	Status               string                        `json:"status"`
	ServiceCode          string                        `json:"serviceCode"`
	CategoryCode         string                        `json:"categoryCode"`
	SeverityCode         string                        `json:"severityCode"`
	Language             string                        `json:"language"`
	SubmittedBy          string                        `json:"submittedBy"`
	CCEmails             []string                      `json:"ccEmailAddresses"`
}

type createCaseOutput struct {
	CaseID string `json:"caseId"`
}

type describeCasesOutput struct {
	NextToken string     `json:"nextToken,omitempty"`
	Cases     []caseView `json:"cases"`
}

type resolveCaseOutput struct {
	InitialCaseStatus string `json:"initialCaseStatus"`
	FinalCaseStatus   string `json:"finalCaseStatus"`
}

type handleCreateCaseInput struct {
	Subject           string   `json:"subject"`
	ServiceCode       string   `json:"serviceCode"`
	CategoryCode      string   `json:"categoryCode"`
	SeverityCode      string   `json:"severityCode"`
	CommunicationBody string   `json:"communicationBody"`
	AttachmentSetID   string   `json:"attachmentSetId,omitempty"`
	Language          string   `json:"language,omitempty"`
	IssueType         string   `json:"issueType,omitempty"`
	CCEmails          []string `json:"ccEmailAddresses,omitempty"`
}

func (h *Handler) handleCreateCase(_ context.Context, in *handleCreateCaseInput) (*createCaseOutput, error) {
	options := CreateCaseOptions{
		Subject: in.Subject, ServiceCode: in.ServiceCode, CategoryCode: in.CategoryCode,
		SeverityCode: in.SeverityCode, CommunicationBody: in.CommunicationBody,
		AttachmentSetID: in.AttachmentSetID, Language: in.Language, IssueType: in.IssueType,
		CCEmails: in.CCEmails,
	}
	if err := validateCreateCase(options); err != nil {
		return nil, err
	}
	c2, err := h.Backend.CreateCaseWithOptions(options)
	if err != nil {
		return nil, err
	}

	return &createCaseOutput{CaseID: c2.CaseID}, nil
}

type handleDescribeCasesInput struct {
	IncludeCommunications *bool    `json:"includeCommunications,omitempty"`
	Language              string   `json:"language"`
	NextToken             string   `json:"nextToken,omitempty"`
	DisplayID             string   `json:"displayId,omitempty"`
	AfterTime             string   `json:"afterTime,omitempty"`
	BeforeTime            string   `json:"beforeTime,omitempty"`
	CaseIDList            []string `json:"caseIdList"`
	MaxResults            int      `json:"maxResults,omitempty"`
	IncludeResolvedCases  bool     `json:"includeResolvedCases"`
}

func (h *Handler) handleDescribeCases(
	_ context.Context,
	in *handleDescribeCasesInput,
) (*describeCasesOutput, error) {
	if err := validatePageSize(in.MaxResults); err != nil {
		return nil, err
	}
	afterValue, err := parseFilterTime(in.AfterTime)
	if err != nil {
		return nil, err
	}
	beforeValue, err := parseFilterTime(in.BeforeTime)
	if err != nil {
		return nil, err
	}
	includeComms := in.IncludeCommunications == nil || *in.IncludeCommunications
	after := nonZeroTimePointer(afterValue)
	before := nonZeroTimePointer(beforeValue)
	cases, token, err := h.Backend.DescribeCasesWithOptions(DescribeCasesOptions{
		CaseIDs: in.CaseIDList, DisplayID: in.DisplayID, Language: in.Language,
		IncludeResolvedCases: in.IncludeResolvedCases, IncludeCommunications: includeComms,
		AfterTime: after, BeforeTime: before, MaxResults: in.MaxResults, NextToken: in.NextToken,
	})
	if err != nil {
		return nil, err
	}

	views := make([]caseView, 0, len(cases))
	for _, cs := range cases {
		v := caseView{
			CaseID:       cs.CaseID,
			DisplayID:    cs.DisplayID,
			Subject:      cs.Subject,
			Status:       cs.Status,
			ServiceCode:  cs.ServiceCode,
			CategoryCode: cs.CategoryCode,
			SeverityCode: cs.SeverityCode,
			Language:     cs.Language,
			SubmittedBy:  cs.SubmittedBy,
			CCEmails:     cs.CCEmails,
			CreatedTime:  cs.CreatedTime.UTC().Format(time.RFC3339),
		}
		if includeComms {
			comms, nextToken := h.Backend.RecentCommunications(cs.CaseID)
			v.RecentCommunications = &recentCaseCommunicationsView{
				Communications: communicationViews(comms),
				NextToken:      nextToken,
			}
		}
		views = append(views, v)
	}

	return &describeCasesOutput{Cases: views, NextToken: token}, nil
}

type handleResolveCaseInput struct {
	CaseID string `json:"caseId"`
}

func (h *Handler) handleResolveCase(_ context.Context, in *handleResolveCaseInput) (*resolveCaseOutput, error) {
	if in.CaseID == "" {
		return nil, fmt.Errorf("%w: caseId is required", ErrValidation)
	}
	initialStatus, cs, err := h.Backend.ResolveCaseWithStatus(in.CaseID)
	if err != nil {
		return nil, err
	}

	return &resolveCaseOutput{
		InitialCaseStatus: initialStatus,
		FinalCaseStatus:   cs.Status,
	}, nil
}

type addCommunicationToCaseInput struct {
	CaseID            string   `json:"caseId"`
	CommunicationBody string   `json:"communicationBody"`
	AttachmentSetID   string   `json:"attachmentSetId,omitempty"`
	CCEmails          []string `json:"ccEmailAddresses,omitempty"`
}

type addCommunicationToCaseOutput struct {
	Result bool `json:"result"`
}

func (h *Handler) handleAddCommunicationToCase(
	_ context.Context,
	in *addCommunicationToCaseInput,
) (*addCommunicationToCaseOutput, error) {
	options := AddCommunicationOptions{
		CaseID: in.CaseID, CommunicationBody: in.CommunicationBody,
		AttachmentSetID: in.AttachmentSetID, CCEmails: in.CCEmails,
	}
	if err := validateCommunication(options); err != nil {
		return nil, err
	}
	if err := h.Backend.AddCommunicationWithOptions(options); err != nil {
		return nil, err
	}

	return &addCommunicationToCaseOutput{Result: true}, nil
}

type describeCommunicationsInput struct {
	CaseID     string `json:"caseId"`
	NextToken  string `json:"nextToken,omitempty"`
	AfterTime  string `json:"afterTime,omitempty"`
	BeforeTime string `json:"beforeTime,omitempty"`
	MaxResults int    `json:"maxResults,omitempty"`
}

type communicationView struct {
	CaseID        string          `json:"caseId"`
	Body          string          `json:"body"`
	SubmittedBy   string          `json:"submittedBy"`
	TimeCreated   string          `json:"timeCreated"`
	AttachmentSet []AttachmentRef `json:"attachmentSet,omitempty"`
}

type recentCaseCommunicationsView struct {
	NextToken      string              `json:"nextToken,omitempty"`
	Communications []communicationView `json:"communications"`
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
		return nil, fmt.Errorf("%w: caseId is required", ErrValidation)
	}
	if err := validatePageSize(in.MaxResults); err != nil {
		return nil, err
	}
	afterValue, err := parseFilterTime(in.AfterTime)
	if err != nil {
		return nil, err
	}
	beforeValue, err := parseFilterTime(in.BeforeTime)
	if err != nil {
		return nil, err
	}
	after := nonZeroTimePointer(afterValue)
	before := nonZeroTimePointer(beforeValue)
	comms, token, err := h.Backend.DescribeCommunicationsWithOptions(DescribeCommunicationsOptions{
		CaseID: in.CaseID, AfterTime: after, BeforeTime: before,
		MaxResults: in.MaxResults, NextToken: in.NextToken,
	})
	if err != nil {
		return nil, err
	}

	return &describeCommunicationsOutput{Communications: communicationViews(comms), NextToken: token}, nil
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

type addAttachmentsToSetInput struct {
	AttachmentSetID string       `json:"attachmentSetId,omitempty"`
	Attachments     []Attachment `json:"attachments"`
}

type addAttachmentsToSetOutput struct {
	AttachmentSetID string `json:"attachmentSetId"`
	ExpiryTime      string `json:"expiryTime"`
}

func (h *Handler) handleAddAttachmentsToSet(
	_ context.Context,
	in *addAttachmentsToSetInput,
) (*addAttachmentsToSetOutput, error) {
	setID, expiry, err := h.Backend.AddAttachmentsToSetWithAttachments(in.AttachmentSetID, in.Attachments)
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
		return nil, fmt.Errorf("%w: attachmentId is required", ErrValidation)
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
	if !validIssueType(in.IssueType) || in.ServiceCode == "" || in.CategoryCode == "" || !validLanguage(in.Language) {
		return nil, fmt.Errorf("%w: issueType, serviceCode, categoryCode, and language are required", ErrValidation)
	}
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
	if in.Language != "" && !validLanguage(in.Language) {
		return nil, fmt.Errorf("%w: invalid language", ErrValidation)
	}
	services := h.Backend.DescribeServices(in.ServiceCodeList, in.Language)

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
	if in.Language != "" && !validLanguage(in.Language) {
		return nil, fmt.Errorf("%w: invalid language", ErrValidation)
	}
	levels := h.Backend.DescribeSeverityLevels(in.Language)

	views := make([]severityLevelView, 0, len(levels))
	for _, l := range levels {
		views = append(views, severityLevelView(l))
	}

	return &describeSeverityLevelsOutput{SeverityLevels: views}, nil
}

type describeSupportedLanguagesInput struct {
	IssueType    string `json:"issueType"`
	ServiceCode  string `json:"serviceCode"`
	CategoryCode string `json:"categoryCode"`
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
	if !validIssueType(in.IssueType) || in.ServiceCode == "" || in.CategoryCode == "" {
		return nil, fmt.Errorf("%w: issueType, serviceCode, and categoryCode are required", ErrValidation)
	}
	langs := h.Backend.DescribeSupportedLanguages(in.IssueType, in.ServiceCode, in.CategoryCode)

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

func parseFilterTime(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: invalid timestamp", ErrValidation)
	}

	return parsed, nil
}

func nonZeroTimePointer(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}

	return &value
}

func communicationViews(comms []Communication) []communicationView {
	views := make([]communicationView, 0, len(comms))
	for _, comm := range comms {
		views = append(views, communicationView{
			CaseID: comm.CaseID, Body: comm.Body, SubmittedBy: comm.SubmittedBy,
			TimeCreated: comm.TimeCreated.UTC().Format(time.RFC3339), AttachmentSet: comm.AttachmentSet,
		})
	}

	return views
}
