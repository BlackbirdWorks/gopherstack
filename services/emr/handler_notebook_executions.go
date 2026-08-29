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

// notebookExecutionEngineWire is the real DescribeNotebookExecutionOutput
// nested shape (types.ExecutionEngineConfig) -- see NotebookExecution's own
// doc comment (models.go) for why this can't just be
// NotebookExecution.ExecutionEngineID emitted flat. Type/ExecutionRoleArn/
// MasterInstanceSecurityGroupId are real, non-required ExecutionEngineConfig
// members this backend doesn't track (StartNotebookExecution only stores an
// editor ID) -- left unset/omitted rather than fabricated.
type notebookExecutionEngineWire struct {
	ID string `json:"Id,omitempty"`
}

// notebookExecutionDetailWire is the real
// DescribeNotebookExecutionOutput.NotebookExecution shape.
type notebookExecutionDetailWire struct {
	NotebookExecutionID   string                       `json:"NotebookExecutionId"`
	EditorID              string                       `json:"EditorId,omitempty"`
	NotebookExecutionName string                       `json:"NotebookExecutionName,omitempty"`
	NotebookParams        string                       `json:"NotebookParams,omitempty"`
	ExecutionEngine       *notebookExecutionEngineWire `json:"ExecutionEngine,omitempty"`
	Status                string                       `json:"Status"`
	Tags                  []Tag                        `json:"Tags"`
	StartTime             float64                      `json:"StartTime,omitempty"`
	EndTime               float64                      `json:"EndTime,omitempty"`
}

// newNotebookExecutionDetail projects a NotebookExecution into
// DescribeNotebookExecution's real per-op response shape.
func newNotebookExecutionDetail(ne *NotebookExecution) *notebookExecutionDetailWire {
	var engine *notebookExecutionEngineWire
	if ne.ExecutionEngineID != "" {
		engine = &notebookExecutionEngineWire{ID: ne.ExecutionEngineID}
	}

	return &notebookExecutionDetailWire{
		NotebookExecutionID:   ne.NotebookExecutionID,
		EditorID:              ne.EditorID,
		NotebookExecutionName: ne.NotebookExecutionName,
		NotebookParams:        ne.NotebookParams,
		ExecutionEngine:       engine,
		Status:                ne.Status,
		Tags:                  ne.Tags,
		StartTime:             ne.StartTime,
		EndTime:               ne.EndTime,
	}
}

type describeNotebookExecutionOutput struct {
	NotebookExecution *notebookExecutionDetailWire `json:"NotebookExecution"`
}

func (h *Handler) handleDescribeNotebookExecution(
	ctx context.Context,
	in *describeNotebookExecutionInput,
) (*describeNotebookExecutionOutput, error) {
	ne, err := h.Backend.DescribeNotebookExecution(ctx, in.NotebookExecutionID)
	if err != nil {
		return nil, err
	}

	return &describeNotebookExecutionOutput{NotebookExecution: newNotebookExecutionDetail(ne)}, nil
}

// --- ListNotebookExecutions ---

type listNotebookExecutionsInput struct {
	From              *float64 `json:"From"`
	To                *float64 `json:"To"`
	EditorID          string   `json:"EditorId,omitempty"`
	ExecutionEngineID string   `json:"ExecutionEngineId,omitempty"`
	Status            string   `json:"Status,omitempty"`
	Marker            string   `json:"Marker,omitempty"`
}

type listNotebookExecutionsOutput struct {
	Marker             string                     `json:"Marker,omitempty"`
	NotebookExecutions []NotebookExecutionSummary `json:"NotebookExecutions"`
}

func (h *Handler) handleListNotebookExecutions(
	ctx context.Context,
	in *listNotebookExecutionsInput,
) (*listNotebookExecutionsOutput, error) {
	params := ListNotebookExecutionsParams{
		EditorID:          in.EditorID,
		ExecutionEngineID: in.ExecutionEngineID,
		Status:            in.Status,
		Marker:            in.Marker,
	}

	if in.From != nil {
		t := epochSecondsToTime(*in.From)
		params.From = &t
	}

	if in.To != nil {
		t := epochSecondsToTime(*in.To)
		params.To = &t
	}

	list, marker := h.Backend.ListNotebookExecutions(ctx, params)

	summaries := make([]NotebookExecutionSummary, 0, len(list))
	for _, ne := range list {
		summaries = append(summaries, newNotebookExecutionSummary(ne))
	}

	return &listNotebookExecutionsOutput{NotebookExecutions: summaries, Marker: marker}, nil
}
