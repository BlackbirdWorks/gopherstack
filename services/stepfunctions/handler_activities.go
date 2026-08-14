package stepfunctions

import (
	"context"
	"encoding/json"
)

type createActivityInput struct {
	EncryptionConfiguration *EncryptionConfiguration `json:"encryptionConfiguration,omitempty"`
	Name                    string                   `json:"name"`
	Tags                    []sfnTagEntry            `json:"tags,omitempty"`
}

type createActivityOutput struct {
	ActivityArn  string  `json:"activityArn"`
	CreationDate float64 `json:"creationDate"`
}

type deleteActivityInput struct {
	ActivityArn string `json:"activityArn"`
}

type deleteActivityOutput struct{}

type describeActivityInput struct {
	ActivityArn string `json:"activityArn"`
}

type listActivitiesInput struct {
	NextToken  string `json:"nextToken"`
	MaxResults int    `json:"maxResults"`
}

type listActivitiesOutput struct {
	NextToken  string             `json:"nextToken,omitempty"`
	Activities []activityListItem `json:"activities"`
}

// activityListItem mirrors AWS's ActivityListItem, which -- unlike the full
// Activity shape DescribeActivity returns -- has no encryptionConfiguration
// (types.go, sfn@v1.45.4).
type activityListItem struct {
	ActivityArn  string  `json:"activityArn"`
	Name         string  `json:"name"`
	CreationDate float64 `json:"creationDate"`
}

func newActivityListItem(a *Activity) activityListItem {
	return activityListItem{
		ActivityArn:  a.ActivityArn,
		CreationDate: a.CreationDate,
		Name:         a.Name,
	}
}

type getActivityTaskInput struct {
	ActivityArn string `json:"activityArn"`
	WorkerName  string `json:"workerName"`
}

type getActivityTaskOutput struct {
	TaskToken string `json:"taskToken"`
	Input     string `json:"input"`
}

type sendTaskSuccessInput struct {
	TaskToken string `json:"taskToken"`
	Output    string `json:"output"`
}

type sendTaskSuccessOutput struct{}

type sendTaskFailureInput struct {
	TaskToken string `json:"taskToken"`
	Error     string `json:"error"`
	Cause     string `json:"cause"`
}

type sendTaskFailureOutput struct{}

type sendTaskHeartbeatInput struct {
	TaskToken string `json:"taskToken"`
}

type sendTaskHeartbeatOutput struct{}

// ── Version / Alias request/response types ────────────────────────────────────

// activityActions returns handler functions for activity-related operations.
// CreateActivity and ListActivities are handled in dispatch (ctx-aware).
func (h *Handler) activityActions() map[string]actionFn {
	return map[string]actionFn{
		"DeleteActivity":    h.handleDeleteActivity,
		"DescribeActivity":  h.handleDescribeActivity,
		"SendTaskSuccess":   h.handleSendTaskSuccess,
		"SendTaskFailure":   h.handleSendTaskFailure,
		"SendTaskHeartbeat": h.handleSendTaskHeartbeat,
	}
}

func (h *Handler) handleCreateActivity(ctx context.Context, b []byte) (any, error) {
	var input createActivityInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	a, err := h.Backend.CreateActivity(ctx, input.Name)
	if err != nil {
		return nil, err
	}

	if input.EncryptionConfiguration != nil {
		if cfgErr := h.Backend.SetActivityEncryptionConfiguration(
			a.ActivityArn, input.EncryptionConfiguration,
		); cfgErr != nil {
			return nil, cfgErr
		}
	}

	// Apply inline tags when provided, matching CreateStateMachine's pattern.
	if len(input.Tags) > 0 {
		kv := make(map[string]string, len(input.Tags))
		for _, t := range input.Tags {
			kv[t.Key] = t.Value
		}
		h.setTags(a.ActivityArn, kv)
	}

	return &createActivityOutput{ActivityArn: a.ActivityArn, CreationDate: a.CreationDate}, nil
}

func (h *Handler) handleDeleteActivity(b []byte) (any, error) {
	var input deleteActivityInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteActivity(input.ActivityArn); err != nil {
		return nil, err
	}

	// Clean up tags for the deleted activity, matching DeleteStateMachine's
	// handler (handler_state_machines.go) -- otherwise h.tags accumulates a
	// permanent tombstone entry per deleted activity ARN.
	h.tagsMu.Lock("DeleteActivity")
	if t, ok := h.tags[input.ActivityArn]; ok {
		t.Close()
		delete(h.tags, input.ActivityArn)
	}
	h.tagsMu.Unlock()

	return &deleteActivityOutput{}, nil
}

func (h *Handler) handleDescribeActivity(b []byte) (any, error) {
	var input describeActivityInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	return h.Backend.DescribeActivity(input.ActivityArn)
}

func (h *Handler) handleListActivities(ctx context.Context, b []byte) (any, error) {
	var input listActivitiesInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	acts, next, err := h.Backend.ListActivities(ctx, input.NextToken, input.MaxResults)
	if err != nil {
		return nil, err
	}

	items := make([]activityListItem, len(acts))
	for i := range acts {
		items[i] = newActivityListItem(&acts[i])
	}

	return &listActivitiesOutput{Activities: items, NextToken: next}, nil
}

func (h *Handler) handleSendTaskSuccess(b []byte) (any, error) {
	var input sendTaskSuccessInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	if err := h.Backend.SendTaskSuccess(input.TaskToken, input.Output); err != nil {
		return nil, err
	}

	return &sendTaskSuccessOutput{}, nil
}

func (h *Handler) handleSendTaskFailure(b []byte) (any, error) {
	var input sendTaskFailureInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	if err := h.Backend.SendTaskFailure(input.TaskToken, input.Error, input.Cause); err != nil {
		return nil, err
	}

	return &sendTaskFailureOutput{}, nil
}

func (h *Handler) handleSendTaskHeartbeat(b []byte) (any, error) {
	var input sendTaskHeartbeatInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	if err := h.Backend.SendTaskHeartbeat(input.TaskToken); err != nil {
		return nil, err
	}

	return &sendTaskHeartbeatOutput{}, nil
}

// handleGetActivityTask handles GetActivityTask with context support for long-polling.
func (h *Handler) handleGetActivityTask(ctx context.Context, body []byte) ([]byte, error) {
	var input getActivityTaskInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	task, err := h.Backend.GetActivityTask(ctx, input.ActivityArn, input.WorkerName)
	if err != nil {
		return nil, err
	}

	if task == nil {
		return json.Marshal(&getActivityTaskOutput{})
	}

	return json.Marshal(&getActivityTaskOutput{
		TaskToken: task.TaskToken,
		Input:     task.Input,
	})
}
