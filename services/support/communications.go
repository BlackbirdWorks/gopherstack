package support

import (
	"fmt"
	"sort"
	"time"
)

const (
	maxCCEmailAddresses      = 10
	maxCommunicationBodySize = 8000
	recentCommunicationsSize = 5
)

// AddCommunicationToCase adds a communication to an existing support case.
func (b *InMemoryBackend) AddCommunicationToCase(caseID, body, attachmentSetID string) error {
	b.mu.Lock("AddCommunicationToCase")
	defer b.mu.Unlock()

	if body == "" {
		return fmt.Errorf("%w: communicationBody is required", ErrValidation)
	}

	if !b.cases.Has(caseID) {
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
	b.mu.RLock("DescribeCommunications")
	defer b.mu.RUnlock()

	if !b.cases.Has(caseID) {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, caseID)
	}

	comms := b.communications[caseID]
	out := make([]Communication, len(comms))
	copy(out, comms)

	return out, nil
}

// RecentCommunications returns up to five newest communications for DescribeCases.
func (b *InMemoryBackend) RecentCommunications(caseID string) ([]Communication, string) {
	b.mu.RLock("RecentCommunications")
	defer b.mu.RUnlock()

	comms := cloneCommunications(b.communications[caseID])
	sortCommunications(comms)
	page, token, _ := paginate(comms, 0, recentCommunicationsSize)

	return page, token
}

// AddCommunicationWithOptions stores a customer response and reopens a resolved case.
func (b *InMemoryBackend) AddCommunicationWithOptions(in AddCommunicationOptions) error {
	b.mu.Lock("AddCommunicationWithOptions")
	defer b.mu.Unlock()

	cs, ok := b.cases.Get(in.CaseID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, in.CaseID)
	}
	attachments, err := b.consumeAttachmentSetLocked(in.AttachmentSetID)
	if err != nil {
		return err
	}
	if cs.Status == caseStatusResolved {
		cs.Status = "reopened"
		cs.ResolvedTime = nil
	}
	b.communications[in.CaseID] = append(
		b.communications[in.CaseID],
		newCommunication(in.CaseID, in.CommunicationBody, in.CCEmails, attachments, time.Now()),
	)

	return nil
}

// DescribeCommunicationsWithOptions returns ordered communications matching API filters.
func (b *InMemoryBackend) DescribeCommunicationsWithOptions(
	in DescribeCommunicationsOptions,
) ([]Communication, string, error) {
	b.mu.RLock("DescribeCommunicationsWithOptions")
	defer b.mu.RUnlock()

	if !b.cases.Has(in.CaseID) {
		return nil, "", fmt.Errorf("%w: %s", ErrNotFound, in.CaseID)
	}
	comms := make([]Communication, 0, len(b.communications[in.CaseID]))
	for _, comm := range b.communications[in.CaseID] {
		if in.AfterTime != nil && !comm.TimeCreated.After(*in.AfterTime) {
			continue
		}
		if in.BeforeTime != nil && !comm.TimeCreated.Before(*in.BeforeTime) {
			continue
		}
		comms = append(comms, cloneCommunication(comm))
	}
	sortCommunications(comms)
	start, err := pageOffset(in.NextToken)
	if err != nil {
		return nil, "", err
	}

	return paginate(comms, start, pageLimit(in.MaxResults))
}

func validateCommunication(in AddCommunicationOptions) error {
	switch {
	case in.CaseID == "":
		return fmt.Errorf("%w: caseId is required", ErrValidation)
	case in.CommunicationBody == "":
		return fmt.Errorf("%w: communicationBody is required", ErrValidation)
	case len(in.CommunicationBody) > maxCommunicationBodySize:
		return fmt.Errorf("%w: communicationBody exceeds %d characters", ErrValidation, maxCommunicationBodySize)
	case len(in.CCEmails) > maxCCEmailAddresses:
		return fmt.Errorf("%w: too many ccEmailAddresses", ErrValidation)
	}

	return nil
}

func newCommunication(
	caseID string,
	body string,
	ccEmails []string,
	attachments []AttachmentRef,
	now time.Time,
) Communication {
	return Communication{
		CaseID:        caseID,
		Body:          body,
		SubmittedBy:   defaultSubmittedBy,
		TimeCreated:   now,
		CCEmails:      append([]string(nil), ccEmails...),
		AttachmentSet: append([]AttachmentRef(nil), attachments...),
	}
}

func cloneCommunication(comm Communication) Communication {
	comm.CCEmails = append([]string(nil), comm.CCEmails...)
	comm.AttachmentSet = append([]AttachmentRef(nil), comm.AttachmentSet...)

	return comm
}

func cloneCommunications(comms []Communication) []Communication {
	out := make([]Communication, 0, len(comms))
	for _, comm := range comms {
		out = append(out, cloneCommunication(comm))
	}

	return out
}

func sortCommunications(comms []Communication) {
	sort.Slice(comms, func(i, j int) bool { return comms[i].TimeCreated.After(comms[j].TimeCreated) })
}
