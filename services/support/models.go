package support

import "time"

const (
	// caseStatusOpened is the initial status for a new support case.
	caseStatusOpened = "opened"
	// caseStatusResolved is the status for a resolved support case.
	caseStatusResolved = "resolved"

	// japaneseGeneralGuidance is the shared Japanese localization for the
	// "general guidance" concept, used by both severity levels (as the "low"
	// severity name) and the static services catalog (as a category name).
	japaneseGeneralGuidance = "一般的なガイダンス"
)

// Case represents an AWS Support case.
type Case struct {
	CreatedTime  time.Time  `json:"createdTime"`
	ResolvedTime *time.Time `json:"resolvedTime,omitempty"`
	ServiceCode  string     `json:"serviceCode"`
	DisplayID    string     `json:"displayId"`
	Subject      string     `json:"subject"`
	Status       string     `json:"status"`
	CaseID       string     `json:"caseID"`
	CategoryCode string     `json:"categoryCode"`
	SeverityCode string     `json:"severityCode"`
	Language     string     `json:"language"`
	IssueType    string     `json:"issueType"`
	SubmittedBy  string     `json:"submittedBy"`
	Body         string     `json:"body"`
	CCEmails     []string   `json:"ccEmailAddresses"`
}

// AttachmentRef identifies an attachment included in a communication.
type AttachmentRef struct {
	AttachmentID string `json:"attachmentId"`
	FileName     string `json:"fileName"`
}

// Communication represents a message added to a support case.
type Communication struct {
	TimeCreated     time.Time       `json:"timeCreated"`
	SubmittedBy     string          `json:"submittedBy"`
	Body            string          `json:"body"`
	CaseID          string          `json:"caseId"`
	AttachmentSetID string          `json:"attachmentSetId,omitempty"`
	AttachmentSet   []AttachmentRef `json:"attachmentSet,omitempty"`
	CCEmails        []string        `json:"ccEmailAddresses,omitempty"`
}

// TrustedAdvisorCheck represents a Trusted Advisor check.
type TrustedAdvisorCheck struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Category    string   `json:"category"`
	Metadata    []string `json:"metadata"`
}

// Attachment represents a file attachment for a support case communication.
type Attachment struct {
	AttachmentID string `json:"attachmentId"`
	FileName     string `json:"fileName"`
	Data         []byte `json:"data"`
}

// AttachmentSet holds staged attachments until a case communication consumes them.
type AttachmentSet struct {
	// ID is the attachment set ID this value is keyed by in the
	// attachmentSets Table (see store_setup.go). It is tagged json:"-"
	// because attachmentSets is a "dirty" table -- persistence.go instead
	// round-trips it through a dedicated attachmentSetSnapshot DTO that
	// carries the ID as a real JSON field, so it survives the round trip
	// despite being excluded here. It must never change after the set is
	// created (store.Table's keyFn purity requirement).
	ID            string    `json:"-"`
	Expiry        time.Time `json:"expiry"`
	AttachmentIDs []string  `json:"attachmentIds"`
}

// ServiceCategory represents a category within an AWS service.
type ServiceCategory struct {
	Code string `json:"code"`
	Name string `json:"name"`
}

// Service represents an AWS service for the support case.
type Service struct {
	Code       string            `json:"code"`
	Name       string            `json:"name"`
	Categories []ServiceCategory `json:"categories"`
}

// SeverityLevel represents a support case severity level.
type SeverityLevel struct {
	Code string `json:"code"`
	Name string `json:"name"`
}

// SupportedLanguage represents a language supported for a support case.
type SupportedLanguage struct {
	Code     string `json:"code"`
	Display  string `json:"display"`
	Language string `json:"language"`
}

// TrustedAdvisorCheckRefreshStatus represents the refresh status for a Trusted Advisor check.
type TrustedAdvisorCheckRefreshStatus struct {
	RefreshTime                time.Time `json:"refreshTime,omitzero"`
	CheckID                    string    `json:"checkId"`
	Status                     string    `json:"status"`
	MillisUntilNextRefreshable int64     `json:"millisUntilNextRefreshable"`
	PollCount                  int       `json:"pollCount,omitempty"`
}

// TrustedAdvisorResourcesSummary holds counts of resources examined by a Trusted Advisor check.
type TrustedAdvisorResourcesSummary struct {
	ResourcesFlagged    int64 `json:"resourcesFlagged"`
	ResourcesIgnored    int64 `json:"resourcesIgnored"`
	ResourcesProcessed  int64 `json:"resourcesProcessed"`
	ResourcesSuppressed int64 `json:"resourcesSuppressed"`
}

// TrustedAdvisorResourceDetail represents a single resource flagged by a Trusted Advisor check.
type TrustedAdvisorResourceDetail struct {
	ResourceID   string   `json:"resourceId"`
	Status       string   `json:"status"`
	Region       string   `json:"region"`
	Metadata     []string `json:"metadata"`
	IsSuppressed bool     `json:"isSuppressed"`
}

// TrustedAdvisorCheckResult represents the full result of a Trusted Advisor check.
type TrustedAdvisorCheckResult struct {
	CategorySpecificSummary *TrustedAdvisorCategorySpecificSummary `json:"categorySpecificSummary"`
	CheckID                 string                                 `json:"checkId"`
	Status                  string                                 `json:"status"`
	Timestamp               string                                 `json:"timestamp"`
	FlaggedResources        []TrustedAdvisorResourceDetail         `json:"flaggedResources"`
	ResourcesSummary        TrustedAdvisorResourcesSummary         `json:"resourcesSummary"`
}

// TrustedAdvisorCategorySpecificSummary provides cost optimization estimates.
type TrustedAdvisorCategorySpecificSummary struct {
	CostOptimizing TrustedAdvisorCostOptimizingSummary `json:"costOptimizing"`
}

// TrustedAdvisorCostOptimizingSummary estimates possible monthly savings.
type TrustedAdvisorCostOptimizingSummary struct {
	EstimatedMonthlySavings        float64 `json:"estimatedMonthlySavings"`
	EstimatedPercentMonthlySavings float64 `json:"estimatedPercentMonthlySavings"`
}

// TrustedAdvisorCheckSummary represents a summary of a Trusted Advisor check result.
type TrustedAdvisorCheckSummary struct {
	CategorySpecificSummary *TrustedAdvisorCategorySpecificSummary `json:"categorySpecificSummary"`
	CheckID                 string                                 `json:"checkId"`
	Status                  string                                 `json:"status"`
	Timestamp               string                                 `json:"timestamp"`
	ResourcesSummary        TrustedAdvisorResourcesSummary         `json:"resourcesSummary"`
	HasFlaggedResources     bool                                   `json:"hasFlaggedResources"`
}

// SupportedHour represents a time range when support is available.
type SupportedHour struct {
	StartTime string `json:"startTime"`
	EndTime   string `json:"endTime"`
}

// DateInterval represents a start/end date range.
type DateInterval struct {
	StartDateTime string `json:"startDateTime"`
	EndDateTime   string `json:"endDateTime"`
}

// CommunicationTypeOptions represents communication options for a case type.
type CommunicationTypeOptions struct {
	Type                string          `json:"type"`
	SupportedHours      []SupportedHour `json:"supportedHours"`
	DatesWithoutSupport []DateInterval  `json:"datesWithoutSupport"`
}

// DescribeCreateCaseOptionsResult is the result from DescribeCreateCaseOptions.
type DescribeCreateCaseOptionsResult struct {
	LanguageAvailability string                     `json:"languageAvailability"`
	CommunicationTypes   []CommunicationTypeOptions `json:"communicationTypes"`
}

// CreateCaseOptions contains API-visible case creation data.
type CreateCaseOptions struct {
	Subject           string
	ServiceCode       string
	CategoryCode      string
	SeverityCode      string
	CommunicationBody string
	AttachmentSetID   string
	Language          string
	IssueType         string
	CCEmails          []string
}

// AddCommunicationOptions contains API-visible communication input data.
type AddCommunicationOptions struct {
	CaseID            string
	CommunicationBody string
	AttachmentSetID   string
	CCEmails          []string
}

// DescribeCasesOptions contains supported case query and pagination filters.
type DescribeCasesOptions struct {
	AfterTime             *time.Time
	BeforeTime            *time.Time
	DisplayID             string
	Language              string
	NextToken             string
	CaseIDs               []string
	MaxResults            int
	IncludeResolvedCases  bool
	IncludeCommunications bool
}

// DescribeCommunicationsOptions contains communication filters and pagination data.
type DescribeCommunicationsOptions struct {
	AfterTime  *time.Time
	BeforeTime *time.Time
	CaseID     string
	NextToken  string
	MaxResults int
}

// pageTokenJSON is the decoded shape of an opaque pagination token.
type pageTokenJSON struct {
	Offset int `json:"o"`
}
