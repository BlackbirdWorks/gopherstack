package datasync

import (
	"context"
	"fmt"
)

// --- Agent operations ---

type createAgentInput struct {
	ActivationKey string     `json:"ActivationKey"`
	AgentName     string     `json:"AgentName"`
	Tags          []tagInput `json:"Tags"`
}

type createAgentOutput struct {
	AgentArn string `json:"AgentArn"`
}

func (h *Handler) handleCreateAgent(_ context.Context, in *createAgentInput) (*createAgentOutput, error) {
	if in.ActivationKey == "" {
		return nil, fmt.Errorf("%w: ActivationKey is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)

	a, err := h.Backend.CreateAgent(in.AgentName, in.ActivationKey, tags)
	if err != nil {
		return nil, err
	}

	return &createAgentOutput{AgentArn: a.AgentArn}, nil
}

type describeAgentInput struct {
	AgentArn string `json:"AgentArn"`
}

type describeAgentOutput struct {
	AgentArn     string `json:"AgentArn"`
	Name         string `json:"Name"`
	Status       string `json:"Status"`
	EndpointType string `json:"EndpointType"`
	CreationTime int64  `json:"CreationTime"`
}

func (h *Handler) handleDescribeAgent(_ context.Context, in *describeAgentInput) (*describeAgentOutput, error) {
	if in.AgentArn == "" {
		return nil, fmt.Errorf("%w: AgentArn is required", errInvalidRequest)
	}

	a, err := h.Backend.DescribeAgent(in.AgentArn)
	if err != nil {
		return nil, err
	}

	return &describeAgentOutput{
		AgentArn:     a.AgentArn,
		Name:         a.Name,
		Status:       a.Status,
		EndpointType: a.EndpointType,
		CreationTime: a.CreationTime.Unix(),
	}, nil
}

type updateAgentInput struct {
	AgentArn string `json:"AgentArn"`
	Name     string `json:"Name"`
}

type updateAgentOutput struct{}

func (h *Handler) handleUpdateAgent(_ context.Context, in *updateAgentInput) (*updateAgentOutput, error) {
	if in.AgentArn == "" {
		return nil, fmt.Errorf("%w: AgentArn is required", errInvalidRequest)
	}

	if err := h.Backend.UpdateAgent(in.AgentArn, in.Name); err != nil {
		return nil, err
	}

	return &updateAgentOutput{}, nil
}

type deleteAgentInput struct {
	AgentArn string `json:"AgentArn"`
}

type deleteAgentOutput struct{}

func (h *Handler) handleDeleteAgent(_ context.Context, in *deleteAgentInput) (*deleteAgentOutput, error) {
	if in.AgentArn == "" {
		return nil, fmt.Errorf("%w: AgentArn is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteAgent(in.AgentArn); err != nil {
		return nil, err
	}

	return &deleteAgentOutput{}, nil
}

type listAgentsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type agentListEntryOutput struct {
	AgentArn string `json:"AgentArn"`
	Name     string `json:"Name"`
	Status   string `json:"Status"`
}

type listAgentsOutput struct {
	NextToken string                 `json:"NextToken,omitempty"`
	Agents    []agentListEntryOutput `json:"Agents"`
}

func (h *Handler) handleListAgents(_ context.Context, in *listAgentsInput) (*listAgentsOutput, error) {
	agents, nextToken, err := h.Backend.ListAgents(in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}

	out := make([]agentListEntryOutput, 0, len(agents))
	for _, a := range agents {
		out = append(out, agentListEntryOutput{
			AgentArn: a.AgentArn,
			Name:     a.Name,
			Status:   a.Status,
		})
	}

	return &listAgentsOutput{Agents: out, NextToken: nextToken}, nil
}
