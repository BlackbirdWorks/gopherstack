package glue

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// MLTaskType is the category of an ML transform task run.
type MLTaskType string

const (
	mlTaskTypeEvaluation            MLTaskType = "EVALUATION"
	mlTaskTypeLabelingSetGeneration MLTaskType = "LABELING_SET_GENERATION"
	mlTaskTypeExportLabels          MLTaskType = "EXPORT_LABELS"
	mlTaskTypeImportLabels          MLTaskType = "IMPORT_LABELS"
)

// MLTaskRun represents a single ML transform task run.
type MLTaskRun struct {
	Properties    map[string]string `json:"Properties,omitempty"`
	TransformID   string            `json:"TransformId"`
	TaskRunID     string            `json:"TaskRunId"`
	TaskType      string            `json:"TaskType"`
	Status        string            `json:"Status"`
	ErrorString   string            `json:"ErrorString,omitempty"`
	LogGroupName  string            `json:"LogGroupName,omitempty"`
	StartedOn     float64           `json:"StartedOn,omitempty"`
	CompletedOn   float64           `json:"CompletedOn,omitempty"`
	ExecutionTime int               `json:"ExecutionTime,omitempty"`
}

// ErrMLTaskRunNotFound is returned when an ML task run does not exist.
var ErrMLTaskRunNotFound = fmt.Errorf("ML task run not found: %w", ErrNotFound)

func mlTaskRunKey(transformID, taskRunID string) string {
	return transformID + "|" + taskRunID
}

func (b *InMemoryBackend) startMLTaskRunLocked(transformID string, taskType MLTaskType) (*MLTaskRun, error) {
	if !b.mlTransforms.Has(transformID) {
		return nil, fmt.Errorf("ML transform %q not found: %w", transformID, ErrNotFound)
	}

	taskRunID := "run-" + uuid.NewString()[:8]
	run := &MLTaskRun{
		TransformID: transformID,
		TaskRunID:   taskRunID,
		TaskType:    string(taskType),
		Status:      stateRunning,
		StartedOn:   float64(time.Now().Unix()),
	}
	b.mlTaskRuns.Put(run)
	cp := *run

	return &cp, nil
}

// StartMLEvaluationTaskRun starts an ML transform evaluation task run.
func (b *InMemoryBackend) StartMLEvaluationTaskRun(transformID string) (*MLTaskRun, error) {
	b.mu.Lock("StartMLEvaluationTaskRun")
	defer b.mu.Unlock()

	return b.startMLTaskRunLocked(transformID, mlTaskTypeEvaluation)
}

// StartMLLabelingSetGenerationTaskRun starts an ML transform labeling-set generation task run.
func (b *InMemoryBackend) StartMLLabelingSetGenerationTaskRun(transformID string) (*MLTaskRun, error) {
	b.mu.Lock("StartMLLabelingSetGenerationTaskRun")
	defer b.mu.Unlock()

	return b.startMLTaskRunLocked(transformID, mlTaskTypeLabelingSetGeneration)
}

// StartExportLabelsTaskRun starts an ML transform export-labels task run.
func (b *InMemoryBackend) StartExportLabelsTaskRun(transformID, _ string) (*MLTaskRun, error) {
	b.mu.Lock("StartExportLabelsTaskRun")
	defer b.mu.Unlock()

	return b.startMLTaskRunLocked(transformID, mlTaskTypeExportLabels)
}

// StartImportLabelsTaskRun starts an ML transform import-labels task run.
func (b *InMemoryBackend) StartImportLabelsTaskRun(transformID, _ string) (*MLTaskRun, error) {
	b.mu.Lock("StartImportLabelsTaskRun")
	defer b.mu.Unlock()

	return b.startMLTaskRunLocked(transformID, mlTaskTypeImportLabels)
}

// GetMLTaskRun retrieves a single ML task run by transform ID and task run ID.
func (b *InMemoryBackend) GetMLTaskRun(transformID, taskRunID string) (*MLTaskRun, error) {
	b.mu.RLock("GetMLTaskRun")
	defer b.mu.RUnlock()

	run, ok := b.mlTaskRuns.Get(mlTaskRunKey(transformID, taskRunID))
	if !ok {
		return nil, ErrMLTaskRunNotFound
	}

	cp := *run

	return &cp, nil
}

// GetMLTaskRuns returns all task runs for a given ML transform, newest first.
func (b *InMemoryBackend) GetMLTaskRuns(transformID string) ([]*MLTaskRun, error) {
	b.mu.RLock("GetMLTaskRuns")
	defer b.mu.RUnlock()

	if !b.mlTransforms.Has(transformID) {
		return nil, fmt.Errorf("ML transform %q not found: %w", transformID, ErrNotFound)
	}

	prefix := transformID + "|"
	out := make([]*MLTaskRun, 0)

	for _, r := range b.mlTaskRuns.Snapshot() {
		if k := mlTaskRunEntryKeyFn(r); strings.HasPrefix(k, prefix) {
			cp := *r
			out = append(out, &cp)
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].StartedOn > out[j].StartedOn })

	return out, nil
}

// CancelMLTaskRun transitions an ML task run to STOPPED status.
func (b *InMemoryBackend) CancelMLTaskRun(transformID, taskRunID string) error {
	b.mu.Lock("CancelMLTaskRun")
	defer b.mu.Unlock()

	key := mlTaskRunKey(transformID, taskRunID)

	run, ok := b.mlTaskRuns.Get(key)
	if !ok {
		return ErrMLTaskRunNotFound
	}

	run.Status = stateStopped
	run.CompletedOn = float64(time.Now().Unix())

	return nil
}

// ListDataQualityEvaluationRuns returns all DataQuality ruleset evaluation runs.
func (b *InMemoryBackend) ListDataQualityEvaluationRuns() []*DataQualityEvaluationRun {
	b.mu.RLock("ListDataQualityEvaluationRuns")
	defer b.mu.RUnlock()

	src := b.dataQualityEvalRuns.All()
	out := make([]*DataQualityEvaluationRun, 0, len(src))
	for _, r := range src {
		cp := *r
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].StartedOn < out[j].StartedOn })

	return out
}

// ListDataQualityResults returns all DataQuality results.
func (b *InMemoryBackend) ListDataQualityResults() []*DataQualityResult {
	b.mu.RLock("ListDataQualityResults")
	defer b.mu.RUnlock()

	src := b.dataQualityResult.All()
	out := make([]*DataQualityResult, 0, len(src))
	for _, r := range src {
		cp := *r
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ResultID < out[j].ResultID })

	return out
}
