package glue

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const mlTaskTypeEvaluation MLTaskType = "EVALUATION"

const mlTaskTypeLabelingSetGeneration MLTaskType = "LABELING_SET_GENERATION"

const mlTaskTypeExportLabels MLTaskType = "EXPORT_LABELS"

const mlTaskTypeImportLabels MLTaskType = "IMPORT_LABELS"

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

func cloneMLTransform(m *MLTransform) *MLTransform {
	cp := *m
	cp.InputRecordTables = make([]GlueTable, len(m.InputRecordTables))
	copy(cp.InputRecordTables, m.InputRecordTables)

	return &cp
}

func (b *InMemoryBackend) mlTransformARN(id string) string {
	return arn.Build("glue", b.region, b.accountID, "mlTransform/"+id)
}

func (b *InMemoryBackend) CreateMLTransform(
	name, description, role string,
	tables []GlueTable,
	params MLTransformParameter,
	tags map[string]string,
) (*MLTransform, error) {
	b.mu.Lock("CreateMLTransform")
	defer b.mu.Unlock()

	id := "transform-" + uuid.NewString()[:8]
	m := &MLTransform{
		TransformID:       id,
		Name:              name,
		Description:       description,
		Role:              role,
		InputRecordTables: tables,
		Parameters:        params,
		Status:            "NOT_READY",
		CreatedOn:         float64(time.Now().Unix()),
		LastModifiedOn:    float64(time.Now().Unix()),
	}
	b.mlTransforms.Put(m)
	if len(tags) > 0 {
		_ = b.tagResource(b.mlTransformARN(id), tags)
	}

	return cloneMLTransform(m), nil
}

func (b *InMemoryBackend) GetMLTransform(id string) (*MLTransform, error) {
	b.mu.RLock("GetMLTransform")
	defer b.mu.RUnlock()

	m, ok := b.mlTransforms.Get(id)
	if !ok {
		return nil, fmt.Errorf("ML transform %q not found: %w", id, ErrNotFound)
	}

	return cloneMLTransform(m), nil
}

func (b *InMemoryBackend) GetMLTransforms() []*MLTransform {
	b.mu.RLock("GetMLTransforms")
	defer b.mu.RUnlock()

	src := b.mlTransforms.All()
	out := make([]*MLTransform, 0, len(src))
	for _, m := range src {
		out = append(out, cloneMLTransform(m))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

func (b *InMemoryBackend) UpdateMLTransform(id string, update MLTransform) error {
	b.mu.Lock("UpdateMLTransform")
	defer b.mu.Unlock()

	existing, ok := b.mlTransforms.Get(id)
	if !ok {
		return fmt.Errorf("ML transform %q not found: %w", id, ErrNotFound)
	}
	update.TransformID = id
	update.CreatedOn = existing.CreatedOn
	update.LastModifiedOn = float64(time.Now().Unix())
	b.mlTransforms.Put(&update)

	return nil
}

func (b *InMemoryBackend) DeleteMLTransform(id string) error {
	b.mu.Lock("DeleteMLTransform")
	defer b.mu.Unlock()

	if !b.mlTransforms.Has(id) {
		return fmt.Errorf("ML transform %q not found: %w", id, ErrNotFound)
	}
	b.mlTransforms.Delete(id)

	return nil
}
