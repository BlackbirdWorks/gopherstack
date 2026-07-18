package support

import (
	"context"
	"fmt"
	"time"
)

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
