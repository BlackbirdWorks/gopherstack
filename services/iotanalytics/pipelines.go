package iotanalytics

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/google/uuid"
)

// clonePipelineActivities deep-copies a slice of PipelineActivity.
func clonePipelineActivities(activities []PipelineActivity) []PipelineActivity {
	if activities == nil {
		return nil
	}

	cp := make([]PipelineActivity, len(activities))
	copy(cp, activities)

	return cp
}

// clonePipeline returns a deep copy of p.
func clonePipeline(p *Pipeline) *Pipeline {
	cp := *p
	cp.Tags = make(map[string]string, len(p.Tags))
	maps.Copy(cp.Tags, p.Tags)
	cp.Activities = clonePipelineActivities(p.Activities)

	if len(p.Reprocessings) > 0 {
		cp.Reprocessings = make(map[string]*PipelineReprocessing, len(p.Reprocessings))
		for k, v := range p.Reprocessings {
			rpCp := *v
			cp.Reprocessings[k] = &rpCp
		}
	} else {
		cp.Reprocessings = make(map[string]*PipelineReprocessing)
	}

	return &cp
}

// reprocessingSummariesSorted returns reprocessing summaries sorted by creation time ascending.
func reprocessingSummariesSorted(reprocessings map[string]*PipelineReprocessing) []pipelineReprocessingSummary {
	if len(reprocessings) == 0 {
		return nil
	}

	summaries := make([]pipelineReprocessingSummary, 0, len(reprocessings))

	for _, rp := range reprocessings {
		summaries = append(summaries, pipelineReprocessingSummary{
			ID:           rp.ID,
			Status:       rp.Status,
			CreationTime: rp.CreationTime,
			StartTime:    rp.StartTime,
			EndTime:      rp.EndTime,
		})
	}

	slices.SortFunc(summaries, func(a, b pipelineReprocessingSummary) int {
		return cmp.Compare(a.CreationTime, b.CreationTime)
	})

	return summaries
}

// CreatePipeline creates a new IoT Analytics pipeline.
func (b *InMemoryBackend) CreatePipeline(
	ctx context.Context,
	name string,
	tags map[string]string,
	activities []PipelineActivity,
) (*Pipeline, error) {
	if err := validateResourceName(name); err != nil {
		return nil, err
	}

	b.mu.Lock("CreatePipeline")
	defer b.mu.Unlock()

	if b.pipelines.Has(name) {
		return nil, ErrAlreadyExists
	}

	now := epochSeconds(time.Now())
	arn := resourceARN(ctx, "pipeline", name)
	p := &Pipeline{
		Name:          name,
		ARN:           arn,
		CreationTime:  now,
		LastUpdate:    now,
		Tags:          make(map[string]string),
		Reprocessings: make(map[string]*PipelineReprocessing),
		Activities:    clonePipelineActivities(activities),
	}
	maps.Copy(p.Tags, tags)
	b.pipelines.Put(p)
	b.tags[arn] = make(map[string]string)
	maps.Copy(b.tags[arn], tags)

	return clonePipeline(p), nil
}

// DescribePipeline returns pipeline metadata.
func (b *InMemoryBackend) DescribePipeline(name string) (*Pipeline, error) {
	b.mu.RLock("DescribePipeline")
	defer b.mu.RUnlock()

	p, ok := b.pipelines.Get(name)
	if !ok {
		return nil, ErrPipelineNotFound
	}

	return clonePipeline(p), nil
}

// UpdatePipeline updates a pipeline's activities and last update time.
func (b *InMemoryBackend) UpdatePipeline(name string, activities []PipelineActivity) error {
	b.mu.Lock("UpdatePipeline")
	defer b.mu.Unlock()

	p, ok := b.pipelines.Get(name)
	if !ok {
		return ErrPipelineNotFound
	}

	p.LastUpdate = epochSeconds(time.Now())

	if activities != nil {
		p.Activities = clonePipelineActivities(activities)
	}

	return nil
}

// DeletePipeline deletes a pipeline.
func (b *InMemoryBackend) DeletePipeline(name string) error {
	b.mu.Lock("DeletePipeline")
	defer b.mu.Unlock()

	p, ok := b.pipelines.Get(name)
	if !ok {
		return ErrPipelineNotFound
	}

	delete(b.tags, p.ARN)
	b.pipelines.Delete(name)

	return nil
}

// ListPipelines returns all pipelines sorted by name.
func (b *InMemoryBackend) ListPipelines() []*Pipeline {
	b.mu.RLock("ListPipelines")
	defer b.mu.RUnlock()

	items := b.pipelines.Snapshot()
	result := make([]*Pipeline, 0, len(items))

	for _, p := range items {
		result = append(result, clonePipeline(p))
	}

	return result
}

// AddPipelineInternal seeds a pipeline by name (test helper).
func (b *InMemoryBackend) AddPipelineInternal(name string) *Pipeline {
	p, _ := b.CreatePipeline(b.svcCtx, name, nil, nil)

	return p
}

// StartPipelineReprocessing creates a new reprocessing job for a pipeline.
// Optional startTime and endTime define the message window to reprocess.
func (b *InMemoryBackend) StartPipelineReprocessing(pipelineName string, startTime, endTime *float64) (string, error) {
	if startTime != nil && endTime != nil && *startTime >= *endTime {
		return "", fmt.Errorf("%w: startTime must be before endTime", ErrValidation)
	}

	b.mu.Lock("StartPipelineReprocessing")
	defer b.mu.Unlock()

	p, ok := b.pipelines.Get(pipelineName)
	if !ok {
		return "", ErrPipelineNotFound
	}

	if len(p.Reprocessings) >= maxPipelineReprocessings {
		return "", fmt.Errorf("%w: pipeline reprocessing limit (%d) exceeded", ErrValidation, maxPipelineReprocessings)
	}

	id := uuid.NewString()
	now := epochSeconds(time.Now())

	rp := &PipelineReprocessing{
		ID:           id,
		Status:       "RUNNING",
		CreationTime: now,
	}

	if startTime != nil {
		rp.StartTime = *startTime
	}

	if endTime != nil {
		rp.EndTime = *endTime
	}

	if p.Reprocessings == nil {
		p.Reprocessings = make(map[string]*PipelineReprocessing)
	}

	p.Reprocessings[id] = rp

	return id, nil
}

// CancelPipelineReprocessing cancels a running pipeline reprocessing job.
func (b *InMemoryBackend) CancelPipelineReprocessing(pipelineName, reprocessingID string) error {
	b.mu.Lock("CancelPipelineReprocessing")
	defer b.mu.Unlock()

	p, ok := b.pipelines.Get(pipelineName)
	if !ok {
		return ErrPipelineNotFound
	}

	rp, ok := p.Reprocessings[reprocessingID]
	if !ok {
		return ErrReprocessingNotFound
	}

	if rp.Status == "CANCELLED" {
		return fmt.Errorf("%w: reprocessing job is already cancelled", ErrValidation)
	}

	rp.Status = "CANCELLED"
	rp.EndTime = epochSeconds(time.Now())

	return nil
}

// RunPipelineActivity runs payloads through a single pipeline activity and returns the
// results, matching AWS RunPipelineActivity semantics per activity type:
//
//   - addAttributes/removeAttributes/selectAttributes: pure JSON-object transforms,
//     applied to every payload that parses as a JSON object (see applyAddAttributes et al.).
//   - filter: evaluates the SQL-like filter expression (see pipeline_expr.go) against each
//     payload and returns only the payloads that match -- non-matching or unparsable
//     payloads are dropped from the pipeline, exactly as a real filter activity would.
//   - math: evaluates the math expression and stores the result under Attribute.
//   - channel/datastore: legitimately pass-through in real AWS too (they are the pipeline's
//     source/sink activities, not transforms).
//   - lambda/deviceRegistryEnrich/deviceShadowEnrich: real AWS invokes Lambda / looks up IoT
//     Device Registry or Device Shadow data for these. This backend has no cross-service
//     wiring to reach the lambda/iot backends from iotanalytics's Provider.Init (no shared
//     backend registry is threaded through, unlike e.g. cloudformation's ResourceCreator),
//     so these remain pass-through -- a documented gap, not a silent stub (see PARITY.md).
//
// A payload that fails to parse as JSON, or a message missing a referenced attribute, is a
// soft per-message failure: it is left unchanged (addAttributes/removeAttributes/
// selectAttributes/math) or dropped (filter), matching a single bad message failing its own
// pipeline activity step rather than the entire batch call.
func (b *InMemoryBackend) RunPipelineActivity(activity PipelineActivity, payloads [][]byte) ([][]byte, error) {
	switch {
	case activity.AddAttributes != nil:
		return applyAddAttributes(activity.AddAttributes, payloads), nil
	case activity.RemoveAttributes != nil:
		return applyRemoveAttributes(activity.RemoveAttributes, payloads), nil
	case activity.SelectAttributes != nil:
		return applySelectAttributes(activity.SelectAttributes, payloads), nil
	case activity.Filter != nil:
		return applyFilter(activity.Filter, payloads), nil
	case activity.Math != nil:
		return applyMath(activity.Math, payloads), nil
	default:
		result := make([][]byte, len(payloads))
		copy(result, payloads)

		return result, nil
	}
}

// decodeMessage attempts to unmarshal a message payload as a JSON object.
func decodeMessage(payload []byte) (map[string]any, bool) {
	var m map[string]any
	if err := json.Unmarshal(payload, &m); err != nil {
		return nil, false
	}

	return m, true
}

// encodeMessage re-marshals a mutated message, falling back to the original payload if
// marshaling somehow fails (m came from a successful Unmarshal, so this is unreachable in
// practice, but encodeMessage must never panic or drop data on error).
func encodeMessage(m map[string]any, fallback []byte) []byte {
	b, err := json.Marshal(m)
	if err != nil {
		return fallback
	}

	return b
}

// applyAddAttributes merges a.Attributes into every payload that parses as a JSON object.
func applyAddAttributes(a *PipelineAddAttributesActivity, payloads [][]byte) [][]byte {
	out := make([][]byte, len(payloads))

	for i, p := range payloads {
		msg, ok := decodeMessage(p)
		if !ok {
			out[i] = p

			continue
		}

		for k, v := range a.Attributes {
			msg[k] = v
		}

		out[i] = encodeMessage(msg, p)
	}

	return out
}

// applyRemoveAttributes deletes a.Attributes keys from every payload that parses as a JSON object.
func applyRemoveAttributes(a *PipelineRemoveAttributesActivity, payloads [][]byte) [][]byte {
	out := make([][]byte, len(payloads))

	for i, p := range payloads {
		msg, ok := decodeMessage(p)
		if !ok {
			out[i] = p

			continue
		}

		for _, k := range a.Attributes {
			delete(msg, k)
		}

		out[i] = encodeMessage(msg, p)
	}

	return out
}

// applySelectAttributes keeps only a.Attributes keys in every payload that parses as a JSON object.
func applySelectAttributes(a *PipelineSelectAttributesActivity, payloads [][]byte) [][]byte {
	out := make([][]byte, len(payloads))

	for i, p := range payloads {
		msg, ok := decodeMessage(p)
		if !ok {
			out[i] = p

			continue
		}

		selected := make(map[string]any, len(a.Attributes))

		for _, k := range a.Attributes {
			if v, exists := msg[k]; exists {
				selected[k] = v
			}
		}

		out[i] = encodeMessage(selected, p)
	}

	return out
}

// applyFilter evaluates f.Filter against every payload and returns only the payloads for
// which it evaluates true. A payload that isn't a JSON object, or whose evaluation fails
// (e.g. references a missing attribute or compares mismatched types), is dropped -- an
// unevaluable filter cannot be said to have matched. A malformed filter expression matches
// nothing.
func applyFilter(f *PipelineFilterActivity, payloads [][]byte) [][]byte {
	node, err := parseExpr(f.Filter)
	if err != nil {
		return [][]byte{}
	}

	out := make([][]byte, 0, len(payloads))

	for _, p := range payloads {
		msg, ok := decodeMessage(p)
		if !ok {
			continue
		}

		v, evalErr := node.eval(msg)
		if evalErr != nil {
			continue
		}

		if matched, isBool := v.(bool); isBool && matched {
			out = append(out, p)
		}
	}

	return out
}

// applyMath evaluates m.Math against every payload and stores the numeric result under
// m.Attribute. A payload that isn't a JSON object, a malformed expression, or an expression
// that fails to evaluate to a number leaves that payload unchanged.
func applyMath(m *PipelineMathActivity, payloads [][]byte) [][]byte {
	out := make([][]byte, len(payloads))

	node, parseErr := parseExpr(m.Math)

	for i, p := range payloads {
		out[i] = p

		if parseErr != nil {
			continue
		}

		msg, ok := decodeMessage(p)
		if !ok {
			continue
		}

		v, evalErr := node.eval(msg)
		if evalErr != nil {
			continue
		}

		f, numOK := toFloat(v)
		if !numOK {
			continue
		}

		msg[m.Attribute] = f
		out[i] = encodeMessage(msg, p)
	}

	return out
}
