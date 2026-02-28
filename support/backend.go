package support

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

var (
	// ErrNotFound is returned when a support case is not found.
	ErrNotFound = errors.New("CaseIdNotFound")
	// ErrAlreadyResolved is returned when trying to resolve an already-resolved case.
	ErrAlreadyResolved = errors.New("CaseAlreadyResolved")
)

// Case represents an AWS Support case.
type Case struct {
	CreatedTime  time.Time
	ResolvedTime *time.Time
	CaseID       string
	Subject      string
	Status       string
	ServiceCode  string
	CategoryCode string
	SeverityCode string
	Body         string
}

// InMemoryBackend is the in-memory store for Support cases.
type InMemoryBackend struct {
	cases map[string]*Case
	mu    sync.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		cases: make(map[string]*Case),
	}
}

// CreateCase creates a new support case.
func (b *InMemoryBackend) CreateCase(subject, serviceCode, categoryCode, severityCode, body string) (*Case, error) {
	b.mu.Lock()
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
	b.mu.RLock()
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
	b.mu.Lock()
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
