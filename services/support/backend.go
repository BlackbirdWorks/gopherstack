package support

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	checkRefreshStatusNone       = "none"
	checkRefreshStatusEnqueued   = "enqueued"
	checkRefreshStatusProcessing = "processing"
	checkRefreshStatusSuccess    = "success"

	// defaultResourcesProcessed is the default count of processed resources for static check results.
	defaultResourcesProcessed = int64(10)
	// refreshMillisDefault is the default milliseconds until next refreshable after a refresh is enqueued.
	refreshMillisDefault = int64(3_600_000)
)

var (
	// ErrNotFound is returned when a support case is not found.
	ErrNotFound = awserr.New("CaseIdNotFound", awserr.ErrNotFound)
	// ErrAlreadyResolved is returned when trying to resolve an already-resolved case.
	ErrAlreadyResolved = errors.New("CaseAlreadyResolved")
	// ErrAttachmentNotFound is returned when an attachment is not found.
	ErrAttachmentNotFound = awserr.New("AttachmentIdNotFound", awserr.ErrNotFound)
)

// Case represents an AWS Support case.
type Case struct {
	CreatedTime  time.Time  `json:"createdTime"`
	ResolvedTime *time.Time `json:"resolvedTime,omitempty"`
	CaseID       string     `json:"caseID"`
	Subject      string     `json:"subject"`
	Status       string     `json:"status"`
	ServiceCode  string     `json:"serviceCode"`
	CategoryCode string     `json:"categoryCode"`
	SeverityCode string     `json:"severityCode"`
	Body         string     `json:"body"`
}

// Communication represents a message added to a support case.
type Communication struct {
	SubmittedBy string    `json:"submittedBy"`
	TimeCreated time.Time `json:"timeCreated"`
	Body        string    `json:"body"`
	CaseID      string    `json:"caseId"`
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
	CheckID                    string `json:"checkId"`
	Status                     string `json:"status"`
	MillisUntilNextRefreshable int64  `json:"millisUntilNextRefreshable"`
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
	CheckID          string                         `json:"checkId"`
	Status           string                         `json:"status"`
	Timestamp        string                         `json:"timestamp"`
	FlaggedResources []TrustedAdvisorResourceDetail `json:"flaggedResources"`
	ResourcesSummary TrustedAdvisorResourcesSummary `json:"resourcesSummary"`
}

// TrustedAdvisorCheckSummary represents a summary of a Trusted Advisor check result.
type TrustedAdvisorCheckSummary struct {
	CheckID             string                         `json:"checkId"`
	Status              string                         `json:"status"`
	Timestamp           string                         `json:"timestamp"`
	HasFlaggedResources bool                           `json:"hasFlaggedResources"`
	ResourcesSummary    TrustedAdvisorResourcesSummary `json:"resourcesSummary"`
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
			Metadata:    []string{"Region", "Service", "Limit Name", "Limit Amount", "Current Usage", "Status"},
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
			Metadata: []string{"Region", "IP Address"},
		},
	}
}

// InMemoryBackend is the in-memory store for Support cases.
type InMemoryBackend struct {
	cases                map[string]*Case
	communications       map[string][]Communication                   // caseID -> communications
	attachmentSets       map[string]time.Time                         // attachmentSetID -> expiryTime
	attachments          map[string]*Attachment                       // attachmentID -> Attachment
	checkRefreshStatuses map[string]*TrustedAdvisorCheckRefreshStatus // checkID -> status
	mu                   *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		cases:                make(map[string]*Case),
		communications:       make(map[string][]Communication),
		attachmentSets:       make(map[string]time.Time),
		attachments:          make(map[string]*Attachment),
		checkRefreshStatuses: make(map[string]*TrustedAdvisorCheckRefreshStatus),
		mu:                   lockmetrics.New("support"),
	}
}

// CreateCase creates a new support case.
func (b *InMemoryBackend) CreateCase(subject, serviceCode, categoryCode, severityCode, body string) (*Case, error) {
	b.mu.Lock("CreateCase")
	defer b.mu.Unlock()

	caseID := "case-" + uuid.New().String()[:8]
	c := &Case{
		CaseID:       caseID,
		Subject:      subject,
		Status:       "opened",
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
func (b *InMemoryBackend) DescribeCases(caseIDs []string) []Case {
	b.mu.RLock("DescribeCases")
	defer b.mu.RUnlock()

	out := make([]Case, 0, len(b.cases))
	if len(caseIDs) == 0 {
		for _, c := range b.cases {
			out = append(out, *c)
		}

		return out
	}

	idSet := make(map[string]bool, len(caseIDs))
	for _, id := range caseIDs {
		idSet[id] = true
	}

	for _, c := range b.cases {
		if idSet[c.CaseID] {
			out = append(out, *c)
		}
	}

	return out
}

// ResolveCase resolves a support case by caseId.
func (b *InMemoryBackend) ResolveCase(caseID string) (*Case, error) {
	b.mu.Lock("ResolveCase")
	defer b.mu.Unlock()

	c, ok := b.cases[caseID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, caseID)
	}

	if c.Status == "resolved" {
		return nil, fmt.Errorf("%w: %s", ErrAlreadyResolved, caseID)
	}

	now := time.Now()
	c.Status = "resolved"
	c.ResolvedTime = &now

	cp := *c

	return &cp, nil
}

// AddCommunicationToCase adds a communication to an existing support case.
func (b *InMemoryBackend) AddCommunicationToCase(caseID, body string) error {
	b.mu.Lock("AddCommunicationToCase")
	defer b.mu.Unlock()

	if _, ok := b.cases[caseID]; !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, caseID)
	}

	comm := Communication{
		CaseID:      caseID,
		Body:        body,
		SubmittedBy: "customer",
		TimeCreated: time.Now(),
	}

	b.communications[caseID] = append(b.communications[caseID], comm)

	return nil
}

// DescribeCommunications returns communications for the given case.
func (b *InMemoryBackend) DescribeCommunications(caseID string) ([]Communication, error) {
	b.mu.RLock("DescribeCommunications")
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
	b.mu.Lock("AddAttachmentsToSet")
	defer b.mu.Unlock()

	if attachmentSetID == "" {
		attachmentSetID = uuid.New().String()
	}

	expiry := time.Now().Add(time.Hour)
	b.attachmentSets[attachmentSetID] = expiry

	return attachmentSetID, expiry, nil
}

// DescribeAttachment returns the attachment with the given ID.
func (b *InMemoryBackend) DescribeAttachment(attachmentID string) (*Attachment, error) {
	b.mu.RLock("DescribeAttachment")
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
	b.mu.Lock("AddAttachmentInternal")
	defer b.mu.Unlock()

	cp := *a
	b.attachments[a.AttachmentID] = &cp
}

// DescribeCreateCaseOptions returns available case creation options.
func (b *InMemoryBackend) DescribeCreateCaseOptions(_, _, _, _ string) *DescribeCreateCaseOptionsResult {
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
					{StartTime: "06:00", EndTime: "22:00"},
				},
				DatesWithoutSupport: []DateInterval{},
			},
		},
	}
}

// DescribeServices returns AWS services, optionally filtered by service codes.
func (b *InMemoryBackend) DescribeServices(serviceCodeList []string) []Service {
	all := staticServices()
	if len(serviceCodeList) == 0 {
		return all
	}

	filter := make(map[string]bool, len(serviceCodeList))
	for _, c := range serviceCodeList {
		filter[c] = true
	}

	out := make([]Service, 0)
	for _, svc := range all {
		if filter[svc.Code] {
			out = append(out, svc)
		}
	}

	return out
}

// DescribeSeverityLevels returns the available severity levels.
func (b *InMemoryBackend) DescribeSeverityLevels(_ string) []SeverityLevel {
	return []SeverityLevel{
		{Code: "low", Name: "General guidance"},
		{Code: "normal", Name: "System impaired"},
		{Code: "high", Name: "Production system impaired"},
		{Code: "urgent", Name: "Production system down"},
		{Code: "critical", Name: "Business-critical system down"},
	}
}

// DescribeSupportedLanguages returns languages supported for the given parameters.
func (b *InMemoryBackend) DescribeSupportedLanguages(_, _, _ string) []SupportedLanguage {
	return []SupportedLanguage{
		{Code: "en", Display: "ENGLISH", Language: "English"},
		{Code: "zh", Display: "CHINESE", Language: "Chinese"},
		{Code: "ja", Display: "JAPANESE", Language: "Japanese"},
		{Code: "ko", Display: "KOREAN", Language: "Korean"},
	}
}

// DescribeTrustedAdvisorCheckRefreshStatuses returns refresh statuses for the given check IDs.
func (b *InMemoryBackend) DescribeTrustedAdvisorCheckRefreshStatuses(
	checkIDs []string,
) []TrustedAdvisorCheckRefreshStatus {
	b.mu.RLock("DescribeTrustedAdvisorCheckRefreshStatuses")
	defer b.mu.RUnlock()

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

	return out
}

// DescribeTrustedAdvisorCheckResult returns the result for the given Trusted Advisor check.
func (b *InMemoryBackend) DescribeTrustedAdvisorCheckResult(checkID, _ string) *TrustedAdvisorCheckResult {
	return &TrustedAdvisorCheckResult{
		CheckID:          checkID,
		Status:           "ok",
		Timestamp:        time.Now().UTC().Format(time.RFC3339),
		FlaggedResources: []TrustedAdvisorResourceDetail{},
		ResourcesSummary: TrustedAdvisorResourcesSummary{
			ResourcesProcessed:  defaultResourcesProcessed,
			ResourcesFlagged:    0,
			ResourcesIgnored:    0,
			ResourcesSuppressed: 0,
		},
	}
}

// DescribeTrustedAdvisorCheckSummaries returns summaries for the given check IDs.
func (b *InMemoryBackend) DescribeTrustedAdvisorCheckSummaries(checkIDs []string) []TrustedAdvisorCheckSummary {
	out := make([]TrustedAdvisorCheckSummary, 0, len(checkIDs))
	ts := time.Now().UTC().Format(time.RFC3339)

	for _, id := range checkIDs {
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
		})
	}

	return out
}

// RefreshTrustedAdvisorCheck enqueues a refresh for the given Trusted Advisor check.
func (b *InMemoryBackend) RefreshTrustedAdvisorCheck(checkID string) (*TrustedAdvisorCheckRefreshStatus, error) {
	b.mu.Lock("RefreshTrustedAdvisorCheck")
	defer b.mu.Unlock()

	status := &TrustedAdvisorCheckRefreshStatus{
		CheckID:                    checkID,
		Status:                     checkRefreshStatusEnqueued,
		MillisUntilNextRefreshable: refreshMillisDefault,
	}
	b.checkRefreshStatuses[checkID] = status

	cp := *status

	return &cp, nil
}

// staticServices returns a small static list of common AWS services.
func staticServices() []Service {
	return []Service{
		{
			Code: "amazon-s3",
			Name: "Amazon Simple Storage Service (Amazon S3)",
			Categories: []ServiceCategory{
				{Code: "data-management", Name: "Data Management"},
				{Code: "performance", Name: "Performance"},
				{Code: "general-guidance", Name: "General Guidance"},
			},
		},
		{
			Code: "amazon-ec2",
			Name: "Amazon Elastic Compute Cloud (Amazon EC2)",
			Categories: []ServiceCategory{
				{Code: "instance-issue", Name: "Instance Issue"},
				{Code: "performance", Name: "Performance"},
				{Code: "general-guidance", Name: "General Guidance"},
			},
		},
		{
			Code: "amazon-dynamodb",
			Name: "Amazon DynamoDB",
			Categories: []ServiceCategory{
				{Code: "general-guidance", Name: "General Guidance"},
				{Code: "performance", Name: "Performance"},
			},
		},
		{
			Code: "amazon-rds",
			Name: "Amazon Relational Database Service (Amazon RDS)",
			Categories: []ServiceCategory{
				{Code: "general-guidance", Name: "General Guidance"},
				{Code: "connectivity", Name: "Connectivity"},
			},
		},
		{
			Code: "amazon-cloudfront",
			Name: "Amazon CloudFront",
			Categories: []ServiceCategory{
				{Code: "general-guidance", Name: "General Guidance"},
				{Code: "performance", Name: "Performance"},
			},
		},
	}
}
