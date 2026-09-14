package codebuild

import (
	"context"
	"fmt"
)

type batchGetSandboxesInput struct {
	IDs []string `json:"ids"`
}

type batchGetSandboxesOutput struct {
	Sandboxes         []*Sandbox `json:"sandboxes"`
	SandboxesNotFound []string   `json:"sandboxesNotFound"`
}

func (h *Handler) handleBatchGetSandboxes(
	_ context.Context,
	in *batchGetSandboxesInput,
) (*batchGetSandboxesOutput, error) {
	found, notFound := h.Backend.BatchGetSandboxes(in.IDs)

	return &batchGetSandboxesOutput{
		Sandboxes:         found,
		SandboxesNotFound: notFound,
	}, nil
}

type listSandboxesInput struct {
	NextToken  string `json:"nextToken"`
	SortOrder  string `json:"sortOrder"`
	MaxResults int32  `json:"maxResults"`
}

type listSandboxesOutput struct {
	NextToken string   `json:"nextToken,omitempty"`
	IDs       []string `json:"ids"`
}

func (h *Handler) handleListSandboxes(_ context.Context, in *listSandboxesInput) (*listSandboxesOutput, error) {
	ids := h.Backend.ListSandboxes()

	pg, err := paginateIDs(ids, in.NextToken, in.SortOrder, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listSandboxesOutput{IDs: pg.Data, NextToken: pg.Next}, nil
}

type listSandboxesForProjectInput struct {
	ProjectName string `json:"projectName"`
	NextToken   string `json:"nextToken"`
	SortOrder   string `json:"sortOrder"`
	MaxResults  int32  `json:"maxResults"`
}

type listSandboxesForProjectOutput struct {
	NextToken string   `json:"nextToken,omitempty"`
	IDs       []string `json:"ids"`
}

func (h *Handler) handleListSandboxesForProject(
	_ context.Context,
	in *listSandboxesForProjectInput,
) (*listSandboxesForProjectOutput, error) {
	if in.ProjectName == "" {
		return nil, fmt.Errorf("%w: projectName is required", errInvalidRequest)
	}

	ids, err := h.Backend.ListSandboxesForProject(in.ProjectName)
	if err != nil {
		return nil, err
	}

	pg, err := paginateIDs(ids, in.NextToken, in.SortOrder, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listSandboxesForProjectOutput{IDs: pg.Data, NextToken: pg.Next}, nil
}

type startSandboxInput struct {
	ProjectName string `json:"projectName"`
}

type startSandboxOutput struct {
	Sandbox *Sandbox `json:"sandbox"`
}

func (h *Handler) handleStartSandbox(_ context.Context, in *startSandboxInput) (*startSandboxOutput, error) {
	if in.ProjectName == "" {
		return nil, fmt.Errorf("%w: projectName is required", errInvalidRequest)
	}

	sb, err := h.Backend.StartSandbox(in.ProjectName)
	if err != nil {
		return nil, err
	}

	return &startSandboxOutput{Sandbox: sb}, nil
}

type startSandboxConnectionInput struct {
	SandboxID string `json:"sandboxId"`
}

type startSandboxConnectionOutput struct {
	SsmSession *SSMSession `json:"ssmSession"`
}

// handleStartSandboxConnection previously returned a fabricated
// {"endpoint": "wss://..."} shape, a field the real StartSandboxConnectionOutput
// does not have at all (real member: ssmSession, an SSMSession object --
// aws-sdk-go-v2/service/codebuild@v1.72.4 api_op_StartSandboxConnection.go:38-41,
// deserializers.go:16919). A real client's out.SsmSession was always nil
// regardless of what gopherstack sent. Fixed to emit the documented shape;
// no real Session Manager streaming is simulated, so the values are
// synthesized placeholders (see SSMSession's doc comment).
func (h *Handler) handleStartSandboxConnection(
	_ context.Context,
	in *startSandboxConnectionInput,
) (*startSandboxConnectionOutput, error) {
	if in.SandboxID == "" {
		return nil, fmt.Errorf("%w: sandboxId is required", errInvalidRequest)
	}

	return &startSandboxConnectionOutput{SsmSession: &SSMSession{
		SessionID:  "session-" + in.SandboxID,
		StreamURL:  "wss://localhost:9999/" + in.SandboxID,
		TokenValue: "token-" + in.SandboxID,
	}}, nil
}

type stopSandboxInput struct {
	ID string `json:"id"`
}

type stopSandboxOutput struct {
	Sandbox *Sandbox `json:"sandbox"`
}

func (h *Handler) handleStopSandbox(_ context.Context, in *stopSandboxInput) (*stopSandboxOutput, error) {
	if in.ID == "" {
		return nil, fmt.Errorf("%w: id is required", errInvalidRequest)
	}

	sb, err := h.Backend.StopSandbox(in.ID)
	if err != nil {
		return nil, err
	}

	return &stopSandboxOutput{Sandbox: sb}, nil
}
