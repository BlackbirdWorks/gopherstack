package emr

import (
	"context"
)

// --- StartNotebookExecution ---

type startNotebookExecutionInput struct {
	EditorID              string `json:"EditorId,omitempty"`
	NotebookExecutionName string `json:"NotebookExecutionName,omitempty"`
	NotebookParams        string `json:"NotebookParams,omitempty"`
	// ExecutionEngine is the real StartNotebookExecutionInput member name
	// (types.ExecutionEngineConfig) -- it was previously declared here with
	// the JSON tag "ExecutionEngineConfig" (the Go *type* name, not the
	// wire field name), so a real client's top-level ExecutionEngine field
	// was silently dropped by this handler's json.Unmarshal and
	// NotebookExecution.ExecutionEngineId came back empty no matter what
	// cluster the caller named.
	ExecutionEngine struct {
		ID string `json:"Id,omitempty"`
	} `json:"ExecutionEngine"`
	Tags []Tag `json:"Tags,omitempty"`
}

type startNotebookExecutionOutput struct {
	NotebookExecutionID string `json:"NotebookExecutionId"`
}

func (h *Handler) handleStartNotebookExecution(
	ctx context.Context,
	in *startNotebookExecutionInput,
) (*startNotebookExecutionOutput, error) {
	ne, err := h.Backend.StartNotebookExecution(ctx, in.EditorID,
		in.NotebookExecutionName,
		in.NotebookParams,
		in.ExecutionEngine.ID,
		in.Tags,
	)
	if err != nil {
		return nil, err
	}

	return &startNotebookExecutionOutput{NotebookExecutionID: ne.NotebookExecutionID}, nil
}

// --- StopNotebookExecution ---

type stopNotebookExecutionInput struct {
	NotebookExecutionID string `json:"NotebookExecutionId"`
}

type stopNotebookExecutionOutput struct{}

func (h *Handler) handleStopNotebookExecution(
	ctx context.Context,
	in *stopNotebookExecutionInput,
) (*stopNotebookExecutionOutput, error) {
	if err := h.Backend.StopNotebookExecution(ctx, in.NotebookExecutionID); err != nil {
		return nil, err
	}

	return &stopNotebookExecutionOutput{}, nil
}

// --- DescribeNotebookExecution ---

type describeNotebookExecutionInput struct {
	NotebookExecutionID string `json:"NotebookExecutionId"`
}

type describeNotebookExecutionOutput struct {
	NotebookExecution *NotebookExecution `json:"NotebookExecution"`
}

func (h *Handler) handleDescribeNotebookExecution(
	ctx context.Context,
	in *describeNotebookExecutionInput,
) (*describeNotebookExecutionOutput, error) {
	ne, err := h.Backend.DescribeNotebookExecution(ctx, in.NotebookExecutionID)
	if err != nil {
		return nil, err
	}

	return &describeNotebookExecutionOutput{NotebookExecution: ne}, nil
}

// --- ListNotebookExecutions ---

type listNotebookExecutionsInput struct {
	EditorID string `json:"EditorId,omitempty"`
	Status   string `json:"Status,omitempty"`
	Marker   string `json:"Marker,omitempty"`
}

type listNotebookExecutionsOutput struct {
	Marker             string                     `json:"Marker,omitempty"`
	NotebookExecutions []NotebookExecutionSummary `json:"NotebookExecutions"`
}

func (h *Handler) handleListNotebookExecutions(
	ctx context.Context,
	in *listNotebookExecutionsInput,
) (*listNotebookExecutionsOutput, error) {
	list, marker := h.Backend.ListNotebookExecutions(ctx, ListNotebookExecutionsParams{
		EditorID: in.EditorID,
		Status:   in.Status,
		Marker:   in.Marker,
	})

	summaries := make([]NotebookExecutionSummary, 0, len(list))
	for _, ne := range list {
		summaries = append(summaries, newNotebookExecutionSummary(ne))
	}

	return &listNotebookExecutionsOutput{NotebookExecutions: summaries, Marker: marker}, nil
}
