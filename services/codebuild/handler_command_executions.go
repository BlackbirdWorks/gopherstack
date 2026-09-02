package codebuild

import (
	"context"
	"fmt"
)

type batchGetCommandExecutionsInput struct {
	SandboxID           string   `json:"sandboxId"`
	CommandExecutionIDs []string `json:"commandExecutionIds"`
}

type batchGetCommandExecutionsOutput struct {
	CommandExecutions         []*CommandExecution `json:"commandExecutions"`
	CommandExecutionsNotFound []string            `json:"commandExecutionsNotFound"`
}

func (h *Handler) handleBatchGetCommandExecutions(
	_ context.Context,
	in *batchGetCommandExecutionsInput,
) (*batchGetCommandExecutionsOutput, error) {
	if in.SandboxID == "" {
		return nil, fmt.Errorf("%w: sandboxId is required", errInvalidRequest)
	}

	found, notFound := h.Backend.BatchGetCommandExecutions(in.SandboxID, in.CommandExecutionIDs)

	return &batchGetCommandExecutionsOutput{
		CommandExecutions:         found,
		CommandExecutionsNotFound: notFound,
	}, nil
}

type startCommandExecutionInput struct {
	SandboxID string `json:"sandboxId"`
	Command   string `json:"command"`
	Type      string `json:"type"`
}

type startCommandExecutionOutput struct {
	CommandExecution *CommandExecution `json:"commandExecution"`
}

func (h *Handler) handleStartCommandExecution(
	_ context.Context,
	in *startCommandExecutionInput,
) (*startCommandExecutionOutput, error) {
	if in.SandboxID == "" {
		return nil, fmt.Errorf("%w: sandboxId is required", errInvalidRequest)
	}

	ce, err := h.Backend.StartCommandExecution(in.SandboxID, in.Command, in.Type)
	if err != nil {
		return nil, err
	}

	return &startCommandExecutionOutput{CommandExecution: ce}, nil
}

type listCommandExecutionsForSandboxInput struct {
	SandboxID  string `json:"sandboxId"`
	NextToken  string `json:"nextToken"`
	SortOrder  string `json:"sortOrder"`
	MaxResults int32  `json:"maxResults"`
}

type listCommandExecutionsForSandboxOutput struct {
	NextToken         string              `json:"nextToken,omitempty"`
	CommandExecutions []*CommandExecution `json:"commandExecutions"`
}

func (h *Handler) handleListCommandExecutionsForSandbox(
	_ context.Context,
	in *listCommandExecutionsForSandboxInput,
) (*listCommandExecutionsForSandboxOutput, error) {
	if in.SandboxID == "" {
		return nil, fmt.Errorf("%w: sandboxId is required", errInvalidRequest)
	}

	ces, err := h.Backend.ListCommandExecutionsForSandbox(in.SandboxID)
	if err != nil {
		return nil, err
	}

	pg, err := paginateCommandExecutions(ces, in.NextToken, in.SortOrder, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listCommandExecutionsForSandboxOutput{CommandExecutions: pg.Data, NextToken: pg.Next}, nil
}
