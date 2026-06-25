package support

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const (
	keyStatusField             = "Status"
	categoryPerformanceLow     = "performance"
	categoryGeneralGuidanceLow = "general-guidance"
)

const (
	fieldRegion             = "Region"
	categorySecurity        = "security"
	categoryPerformance     = "Performance"
	categoryGeneralGuidance = "General Guidance"
	categoryCostOptimizing  = "cost_optimizing"
)

const (
	// caseStatusOpened is the initial status for a new support case.
	caseStatusOpened = "opened"
	// caseStatusResolved is the status for a resolved support case.
	caseStatusResolved = "resolved"

	checkRefreshStatusNone       = "none"
	checkRefreshStatusEnqueued   = "enqueued"
	checkRefreshStatusProcessing = "processing"
	checkRefreshStatusSuccess    = "success"

	severityLow             = "low"
	severityNormal          = "normal"
	severityHigh            = "high"
	severityUrgent          = "urgent"
	severityCritical        = "critical"
	japaneseGeneralGuidance = "一般的なガイダンス"

	// defaultResourcesProcessed is the default count of processed resources for static check results.
	defaultResourcesProcessed = int64(10)
	// refreshMillisDefault is the default milliseconds until next refreshable after a refresh is enqueued.
	refreshMillisDefault = int64(3_600_000)

	// maxAttachmentSets caps the number of in-flight staged attachment sets to prevent unbounded growth.
	maxAttachmentSets = 1000
	// maxCheckRefreshStatuses caps the number of tracked refresh statuses.
	maxCheckRefreshStatuses = 1000
)

var (
	// ErrNotFound is returned when a support case is not found.
	ErrNotFound = awserr.New("CaseIdNotFound", awserr.ErrNotFound)
	// ErrAlreadyResolved is returned when trying to resolve an already-resolved case.
	ErrAlreadyResolved = errors.New("CaseAlreadyResolved")
	// ErrAttachmentNotFound is returned when an attachment is not found.
	ErrAttachmentNotFound = awserr.New("AttachmentIdNotFound", awserr.ErrNotFound)
	// ErrValidation is returned when required input fields are missing or invalid.
	ErrValidation = awserr.New("ValidationError", awserr.ErrInvalidParameter)
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

// trustedAdvisorChecks returns a static list of mock Trusted Advisor checks.
func trustedAdvisorChecks() []TrustedAdvisorCheck {
	return []TrustedAdvisorCheck{
		{
			ID:          "Pfx0RwqBli",
			Name:        "Service Limits",
			Description: "Checks for service usage that is more than 80% of the service limit.",
			Category:    "service_limits",
			Metadata: []string{
				fieldRegion,
				"Service",
				"Limit Name",
				"Limit Amount",
				"Current Usage",
				keyStatusField,
			},
		},
		{
			ID:   "DAvU99Dc4C",
			Name: "Low Utilization Amazon EC2 Instances",
			Description: "Checks the Amazon Elastic Compute Cloud (Amazon EC2) instances that were " +
				"running at any time during the last 14 days.",
			Category: "cost_optimizing",
			Metadata: []string{
				"Region/AZ",
				"Instance ID",
				"Instance Name",
				"Instance Type",
				"Estimated Monthly Savings",
			},
		},
		{
			ID:   "N430c450f2",
			Name: "Unassociated Elastic IP Addresses",
			Description: "Checks for Elastic IP addresses (EIPs) that are not associated with a running " +
				"Amazon Elastic Compute Cloud (Amazon EC2) instance.",
			Category: "cost_optimizing",
			Metadata: []string{fieldRegion, "IP Address"},
		},
		{
			ID:          "hjLMh88uM8",
			Name:        "MFA on Root Account",
			Description: "Checks the root account and warns if multi-factor authentication (MFA) is not enabled.",
			Category:    categorySecurity,
			Metadata:    []string{keyStatusField},
		},
		{
			ID:          "H7IgqkgtmV",
			Name:        "IAM Use",
			Description: "Checks for your use of AWS Identity and Access Management (IAM).",
			Category:    categorySecurity,
			Metadata:    []string{keyStatusField},
		},
		{
			ID:          "1iG5NDGVre",
			Name:        "Amazon S3 Bucket Permissions",
			Description: "Checks buckets in Amazon Simple Storage Service (Amazon S3) that have open access permissions.",
			Category:    categorySecurity,
			Metadata: []string{
				fieldRegion,
				"Bucket Name",
				"ACL Allows List",
				"ACL Allows Upload/Delete",
				"Policy Allows Access",
			},
		},
		{
			ID:          "R365s2Qddf",
			Name:        "Amazon RDS Multi-AZ",
			Description: "Checks for Amazon RDS DB instances that are deployed in a single Availability Zone.",
			Category:    "fault_tolerance",
			Metadata:    []string{fieldRegion, "DB Instance", "Multi-AZ"},
		},
		{
			ID:          "xSqX82fQu",
			Name:        "Amazon EC2 Availability Zone Balance",
			Description: "Checks the distribution of Amazon EC2 instances across Availability Zones in a region.",
			Category:    "fault_tolerance",
			Metadata:    []string{fieldRegion, "Availability Zone", "Instance Count"},
		},
	}
}

// InMemoryBackend is the in-memory store for Support cases.
type InMemoryBackend struct {
	cases                map[string]*Case
	communications       map[string][]Communication                   // caseID -> communications
	attachmentSets       map[string]*AttachmentSet                    // attachmentSetID -> staged attachments
	attachments          map[string]*Attachment                       // attachmentID -> Attachment
	checkRefreshStatuses map[string]*TrustedAdvisorCheckRefreshStatus // checkID -> status
	checkResults         map[string]*TrustedAdvisorCheckResult        // checkID -> result
	nextDisplayID        uint64
	mu                   sync.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		cases:                make(map[string]*Case),
		communications:       make(map[string][]Communication),
		attachmentSets:       make(map[string]*AttachmentSet),
		attachments:          make(map[string]*Attachment),
		checkRefreshStatuses: make(map[string]*TrustedAdvisorCheckRefreshStatus),
		checkResults:         make(map[string]*TrustedAdvisorCheckResult),
	}
}

// Reset clears all backend state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.cases = make(map[string]*Case)
	b.communications = make(map[string][]Communication)
	b.attachmentSets = make(map[string]*AttachmentSet)
	b.attachments = make(map[string]*Attachment)
	b.checkRefreshStatuses = make(map[string]*TrustedAdvisorCheckRefreshStatus)
	b.checkResults = make(map[string]*TrustedAdvisorCheckResult)
	b.nextDisplayID = 0
}

// CreateCase creates a new support case.
func (b *InMemoryBackend) CreateCase(subject, serviceCode, categoryCode, severityCode, body string) (*Case, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	caseID := "case-" + uuid.New().String()[:8]
	c := &Case{
		CaseID:       caseID,
		Subject:      subject,
		Status:       caseStatusOpened,
		ServiceCode:  serviceCode,
		CategoryCode: categoryCode,
		SeverityCode: severityCode,
		Body:         body,
		CreatedTime:  time.Now(),
	}
	b.cases[caseID] = c

	cp := *c

	return &cp, nil
}

// DescribeCases returns all support cases, optionally filtered by caseIds.
// When includeResolvedCases is false, resolved cases are excluded.
//
// Fast path: when caseIDs are supplied, look them up directly in the
// case-ID-keyed map instead of scanning every case in the backend.
func (b *InMemoryBackend) DescribeCases(caseIDs []string, includeResolvedCases bool) []Case {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if len(caseIDs) > 0 {
		out := make([]Case, 0, len(caseIDs))

		for _, id := range caseIDs {
			c, ok := b.cases[id]
			if !ok {
				continue
			}

			if !includeResolvedCases && c.Status == caseStatusResolved {
				continue
			}

			out = append(out, *c)
		}

		return out
	}

	out := make([]Case, 0, len(b.cases))

	for _, c := range b.cases {
		if !includeResolvedCases && c.Status == caseStatusResolved {
			continue
		}

		out = append(out, *c)
	}

	return out
}

// ResolveCase resolves a support case by caseId.
func (b *InMemoryBackend) ResolveCase(caseID string) (*Case, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	c, ok := b.cases[caseID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, caseID)
	}

	if c.Status == caseStatusResolved {
		return nil, fmt.Errorf("%w: %s", ErrAlreadyResolved, caseID)
	}

	now := time.Now()
	c.Status = caseStatusResolved
	c.ResolvedTime = &now

	cp := *c

	return &cp, nil
}

// AddCommunicationToCase adds a communication to an existing support case.
func (b *InMemoryBackend) AddCommunicationToCase(caseID, body, attachmentSetID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if body == "" {
		return fmt.Errorf("%w: communicationBody is required", ErrValidation)
	}

	if _, ok := b.cases[caseID]; !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, caseID)
	}

	comm := Communication{
		CaseID:          caseID,
		Body:            body,
		SubmittedBy:     "customer",
		TimeCreated:     time.Now(),
		AttachmentSetID: attachmentSetID,
	}

	b.communications[caseID] = append(b.communications[caseID], comm)

	return nil
}

// DescribeCommunications returns communications for the given case.
func (b *InMemoryBackend) DescribeCommunications(caseID string) ([]Communication, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.cases[caseID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, caseID)
	}

	comms := b.communications[caseID]
	out := make([]Communication, len(comms))
	copy(out, comms)

	return out, nil
}

// DescribeTrustedAdvisorChecks returns the static list of Trusted Advisor checks.
func (b *InMemoryBackend) DescribeTrustedAdvisorChecks() []TrustedAdvisorCheck {
	return trustedAdvisorChecks()
}

// AddAttachmentsToSet creates a new attachment set and returns its ID.
func (b *InMemoryBackend) AddAttachmentsToSet(attachmentSetID string) (string, time.Time, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if attachmentSetID == "" {
		attachmentSetID = uuid.New().String()
	}

	expiry := time.Now().Add(time.Hour)
	b.attachmentSets[attachmentSetID] = &AttachmentSet{Expiry: expiry}

	return attachmentSetID, expiry, nil
}

// DescribeAttachment returns the attachment with the given ID.
func (b *InMemoryBackend) DescribeAttachment(attachmentID string) (*Attachment, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	a, ok := b.attachments[attachmentID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrAttachmentNotFound, attachmentID)
	}

	cp := *a

	return &cp, nil
}

// AddAttachmentInternal seeds an attachment directly into the backend (for testing).
func (b *InMemoryBackend) AddAttachmentInternal(a *Attachment) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cp := *a
	if a.Data != nil {
		cp.Data = make([]byte, len(a.Data))
		copy(cp.Data, a.Data)
	}

	b.attachments[a.AttachmentID] = &cp
}

// AddCaseInternal seeds a case directly into the backend (for testing).
func (b *InMemoryBackend) AddCaseInternal(c *Case) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cp := *c
	b.cases[c.CaseID] = &cp
}

// DescribeCreateCaseOptions returns available case creation options.
// Chat support hours differ by locale: Japanese support runs 07:00–21:00 JST,
// all other locales use 06:00–22:00 UTC.
func (b *InMemoryBackend) DescribeCreateCaseOptions(_, _, _, language string) *DescribeCreateCaseOptionsResult {
	chatStart, chatEnd := "06:00", "22:00"
	if language == "ja" {
		chatStart, chatEnd = "07:00", "21:00"
	}

	return &DescribeCreateCaseOptionsResult{
		LanguageAvailability: "available",
		CommunicationTypes: []CommunicationTypeOptions{
			{
				Type: "web",
				SupportedHours: []SupportedHour{
					{StartTime: "00:00", EndTime: "24:00"},
				},
				DatesWithoutSupport: []DateInterval{},
			},
			{
				Type: "chat",
				SupportedHours: []SupportedHour{
					{StartTime: chatStart, EndTime: chatEnd},
				},
				DatesWithoutSupport: []DateInterval{},
			},
		},
	}
}

// DescribeServices returns AWS services, optionally filtered by service codes.
func (b *InMemoryBackend) DescribeServices(serviceCodeList []string, language string) []Service {
	all := staticServices(language)
	if len(serviceCodeList) == 0 {
		return all
	}

	filter := make(map[string]bool, len(serviceCodeList))
	for _, c := range serviceCodeList {
		filter[c] = true
	}

	out := make([]Service, 0, len(all))
	for _, svc := range all {
		if filter[svc.Code] {
			out = append(out, svc)
		}
	}

	return out
}

// DescribeSeverityLevels returns the available severity levels.
func (b *InMemoryBackend) DescribeSeverityLevels(language string) []SeverityLevel {
	if language == "ja" {
		return []SeverityLevel{
			{Code: severityLow, Name: japaneseGeneralGuidance},
			{Code: severityNormal, Name: "システム障害"},
			{Code: severityHigh, Name: "本番システム障害"},
			{Code: severityUrgent, Name: "本番システム停止"},
			{Code: severityCritical, Name: "ビジネスクリティカルシステム停止"},
		}
	}

	return []SeverityLevel{
		{Code: severityLow, Name: "General guidance"},
		{Code: severityNormal, Name: "System impaired"},
		{Code: severityHigh, Name: "Production system impaired"},
		{Code: severityUrgent, Name: "Production system down"},
		{Code: severityCritical, Name: "Business-critical system down"},
	}
}

// DescribeSupportedLanguages returns languages supported for the given parameters.
// The account-and-billing issue type is handled exclusively in English.
func (b *InMemoryBackend) DescribeSupportedLanguages(issueType, _, _ string) []SupportedLanguage {
	all := []SupportedLanguage{
		{Code: "en", Display: "ENGLISH", Language: "English"},
		{Code: "zh", Display: "CHINESE", Language: "Chinese"},
		{Code: "ja", Display: "JAPANESE", Language: "Japanese"},
		{Code: "ko", Display: "KOREAN", Language: "Korean"},
	}

	if issueType == "account-and-billing" {
		return all[:1]
	}

	return all
}

// DescribeTrustedAdvisorCheckRefreshStatuses returns refresh statuses for the given check IDs.
// Uses RLock for the common read-only case; upgrades to a write lock only when state advancement
// is required (simulates the async progression: enqueued → processing → success).
func (b *InMemoryBackend) DescribeTrustedAdvisorCheckRefreshStatuses(
	checkIDs []string,
) []TrustedAdvisorCheckRefreshStatus {
	b.mu.RLock()
	needsWrite := false

	for _, id := range checkIDs {
		if s, ok := b.checkRefreshStatuses[id]; ok && s.Status != checkRefreshStatusSuccess {
			needsWrite = true

			break
		}
	}

	if !needsWrite {
		out := make([]TrustedAdvisorCheckRefreshStatus, 0, len(checkIDs))
		for _, id := range checkIDs {
			if s, ok := b.checkRefreshStatuses[id]; ok {
				out = append(out, *s)
			} else {
				out = append(out, TrustedAdvisorCheckRefreshStatus{
					CheckID:                    id,
					Status:                     checkRefreshStatusNone,
					MillisUntilNextRefreshable: 0,
				})
			}
		}
		b.mu.RUnlock()

		return out
	}

	b.mu.RUnlock()
	b.mu.Lock()
	defer b.mu.Unlock()

	out := make([]TrustedAdvisorCheckRefreshStatus, 0, len(checkIDs))
	for _, id := range checkIDs {
		if s, ok := b.checkRefreshStatuses[id]; ok {
			switch s.PollCount {
			case 0:
				s.PollCount++
			case 1:
				s.Status = checkRefreshStatusProcessing
				s.PollCount++
			default:
				s.Status = checkRefreshStatusSuccess
				s.MillisUntilNextRefreshable = 0
			}
			out = append(out, *s)
		} else {
			out = append(out, TrustedAdvisorCheckRefreshStatus{
				CheckID:                    id,
				Status:                     checkRefreshStatusNone,
				MillisUntilNextRefreshable: 0,
			})
		}
	}

	return out
}

// DescribeTrustedAdvisorCheckResult returns the result for the given Trusted Advisor check.
// Stored results (written by RefreshTrustedAdvisorCheck) take precedence over the static default.
func (b *InMemoryBackend) DescribeTrustedAdvisorCheckResult(checkID, _ string) *TrustedAdvisorCheckResult {
	b.mu.RLock()
	if stored, ok := b.checkResults[checkID]; ok {
		cp := *stored
		b.mu.RUnlock()

		return &cp
	}
	b.mu.RUnlock()

	// Default: cost_optimizing checks show "warning" to indicate potential savings;
	// all other categories show "ok".
	status := "ok"
	for _, c := range trustedAdvisorChecks() {
		if c.ID == checkID && c.Category == categoryCostOptimizing {
			status = "warning"

			break
		}
	}

	return &TrustedAdvisorCheckResult{
		CheckID:          checkID,
		Status:           status,
		Timestamp:        time.Now().UTC().Format(time.RFC3339),
		FlaggedResources: []TrustedAdvisorResourceDetail{},
		ResourcesSummary: TrustedAdvisorResourcesSummary{
			ResourcesProcessed:  defaultResourcesProcessed,
			ResourcesFlagged:    0,
			ResourcesIgnored:    0,
			ResourcesSuppressed: 0,
		},
		CategorySpecificSummary: &TrustedAdvisorCategorySpecificSummary{},
	}
}

// DescribeTrustedAdvisorCheckSummaries returns summaries for the given check IDs,
// derived from stored check results where available.
func (b *InMemoryBackend) DescribeTrustedAdvisorCheckSummaries(checkIDs []string) []TrustedAdvisorCheckSummary {
	b.mu.RLock()
	defer b.mu.RUnlock()

	ts := time.Now().UTC().Format(time.RFC3339)
	out := make([]TrustedAdvisorCheckSummary, 0, len(checkIDs))

	for _, id := range checkIDs {
		if stored, ok := b.checkResults[id]; ok {
			out = append(out, TrustedAdvisorCheckSummary{
				CheckID:                 id,
				Status:                  stored.Status,
				Timestamp:               stored.Timestamp,
				ResourcesSummary:        stored.ResourcesSummary,
				HasFlaggedResources:     stored.ResourcesSummary.ResourcesFlagged > 0,
				CategorySpecificSummary: stored.CategorySpecificSummary,
			})
		} else {
			out = append(out, TrustedAdvisorCheckSummary{
				CheckID:             id,
				Status:              "ok",
				Timestamp:           ts,
				HasFlaggedResources: false,
				ResourcesSummary: TrustedAdvisorResourcesSummary{
					ResourcesProcessed:  defaultResourcesProcessed,
					ResourcesFlagged:    0,
					ResourcesIgnored:    0,
					ResourcesSuppressed: 0,
				},
				CategorySpecificSummary: &TrustedAdvisorCategorySpecificSummary{},
			})
		}
	}

	return out
}

// RefreshTrustedAdvisorCheck enqueues a refresh for the given Trusted Advisor check.
// Evicts a random entry when the status map is at capacity to prevent unbounded growth.
func (b *InMemoryBackend) RefreshTrustedAdvisorCheck(checkID string) (*TrustedAdvisorCheckRefreshStatus, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if existing, ok := b.checkRefreshStatuses[checkID]; ok && time.Since(existing.RefreshTime) < time.Hour {
		cp := *existing

		return &cp, nil
	}

	if len(b.checkRefreshStatuses) >= maxCheckRefreshStatuses {
		for k := range b.checkRefreshStatuses {
			delete(b.checkRefreshStatuses, k)

			break
		}
	}

	status := &TrustedAdvisorCheckRefreshStatus{
		CheckID:                    checkID,
		Status:                     checkRefreshStatusEnqueued,
		MillisUntilNextRefreshable: refreshMillisDefault,
		RefreshTime:                time.Now(),
	}
	b.checkRefreshStatuses[checkID] = status

	cp := *status

	return &cp, nil
}

// staticServices returns a small static list of common AWS services.
func staticServices(language string) []Service {
	all := []Service{
		{
			Code: "amazon-s3",
			Name: "Amazon Simple Storage Service (Amazon S3)",
			Categories: []ServiceCategory{
				{Code: "data-management", Name: "Data Management"},
				{Code: categoryPerformanceLow, Name: categoryPerformance},
				{Code: categoryGeneralGuidanceLow, Name: categoryGeneralGuidance},
			},
		},
		{
			Code: "amazon-ec2",
			Name: "Amazon Elastic Compute Cloud (Amazon EC2)",
			Categories: []ServiceCategory{
				{Code: "instance-issue", Name: "Instance Issue"},
				{Code: categoryPerformanceLow, Name: categoryPerformance},
				{Code: categoryGeneralGuidanceLow, Name: categoryGeneralGuidance},
			},
		},
		{
			Code: "amazon-dynamodb",
			Name: "Amazon DynamoDB",
			Categories: []ServiceCategory{
				{Code: categoryGeneralGuidanceLow, Name: categoryGeneralGuidance},
				{Code: categoryPerformanceLow, Name: categoryPerformance},
			},
		},
		{
			Code: "amazon-rds",
			Name: "Amazon Relational Database Service (Amazon RDS)",
			Categories: []ServiceCategory{
				{Code: categoryGeneralGuidanceLow, Name: categoryGeneralGuidance},
				{Code: "connectivity", Name: "Connectivity"},
			},
		},
		{
			Code: "amazon-cloudfront",
			Name: "Amazon CloudFront",
			Categories: []ServiceCategory{
				{Code: categoryGeneralGuidanceLow, Name: categoryGeneralGuidance},
				{Code: categoryPerformanceLow, Name: categoryPerformance},
			},
		},
	}
	if language == "ja" {
		all[0].Name = "Amazon Simple Storage Service (Amazon S3)"
		all[0].Categories[2].Name = japaneseGeneralGuidance
		all[1].Categories[2].Name = japaneseGeneralGuidance
	}

	return all
}
