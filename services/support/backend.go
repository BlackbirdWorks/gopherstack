package support

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrNotFound is returned when a support case is not found.
	ErrNotFound = awserr.New("CaseIdNotFound", awserr.ErrNotFound)
	// ErrAlreadyResolved is returned when trying to resolve an already-resolved case.
	ErrAlreadyResolved = errors.New("CaseAlreadyResolved")
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
	cases          map[string]*Case
	communications map[string][]Communication // caseID -> communications
	attachmentSets map[string]time.Time       // attachmentSetID -> expiryTime
	mu             *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		cases:          make(map[string]*Case),
		communications: make(map[string][]Communication),
		attachmentSets: make(map[string]time.Time),
		mu:             lockmetrics.New("support"),
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
