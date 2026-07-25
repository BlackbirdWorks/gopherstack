package glue

import (
	"fmt"
	"maps"
	mrand "math/rand/v2"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// dataQualityRulesetARN returns the ARN for a data quality ruleset.
func (b *InMemoryBackend) dataQualityRulesetARN(name string) string {
	return arn.Build("glue", b.region, b.accountID, "dataQualityRuleset/"+name)
}

// DataQualityRulesetOptions carries the optional CreateDataQualityRuleset
// settings beyond Name/Ruleset/Tags.
type DataQualityRulesetOptions struct {
	TargetTable                      *DataQualityTargetTable
	Description                      string
	DataQualitySecurityConfiguration string
}

// CreateDataQualityRuleset creates a new data quality ruleset.
func (b *InMemoryBackend) CreateDataQualityRuleset(
	name, ruleset string,
	tags map[string]string,
) (*DataQualityRuleset, error) {
	return b.CreateDataQualityRulesetWithOptions(name, ruleset, tags, DataQualityRulesetOptions{})
}

// CreateDataQualityRulesetWithOptions is CreateDataQualityRuleset plus the
// optional creation-time settings CreateDataQualityRulesetInput also
// supports (Description/DataQualitySecurityConfiguration/TargetTable).
func (b *InMemoryBackend) CreateDataQualityRulesetWithOptions(
	name, ruleset string,
	tags map[string]string,
	opts DataQualityRulesetOptions,
) (*DataQualityRuleset, error) {
	b.mu.Lock("CreateDataQualityRuleset")
	defer b.mu.Unlock()

	if name == "" || ruleset == "" {
		return nil, ErrValidation
	}

	if b.dataQualityRulesets.Has(name) {
		return nil, ErrAlreadyExists
	}

	now := float64(time.Now().Unix())
	r := &DataQualityRuleset{
		Name:                             name,
		Ruleset:                          ruleset,
		Tags:                             maps.Clone(tags),
		ARN:                              b.dataQualityRulesetARN(name),
		Description:                      opts.Description,
		DataQualitySecurityConfiguration: opts.DataQualitySecurityConfiguration,
		TargetTable:                      opts.TargetTable,
		CreatedOn:                        now,
		LastModifiedOn:                   now,
	}
	b.dataQualityRulesets.Put(r)

	return r, nil
}

// GetDataQualityRuleset retrieves a data quality ruleset by name.
func (b *InMemoryBackend) GetDataQualityRuleset(name string) (*DataQualityRuleset, error) {
	b.mu.RLock("GetDataQualityRuleset")
	defer b.mu.RUnlock()

	r, ok := b.dataQualityRulesets.Get(name)
	if !ok {
		return nil, ErrNotFound
	}

	cp := *r
	cp.Tags = maps.Clone(r.Tags)

	return &cp, nil
}

// DeleteDataQualityRuleset removes a data quality ruleset by name.
func (b *InMemoryBackend) DeleteDataQualityRuleset(name string) error {
	b.mu.Lock("DeleteDataQualityRuleset")
	defer b.mu.Unlock()

	if !b.dataQualityRulesets.Has(name) {
		return ErrNotFound
	}
	b.dataQualityRulesets.Delete(name)

	return nil
}

// UpdateDataQualityRuleset updates the ruleset expression for a named ruleset.
func (b *InMemoryBackend) UpdateDataQualityRuleset(name, ruleset, description string) error {
	b.mu.Lock("UpdateDataQualityRuleset")
	defer b.mu.Unlock()

	r, ok := b.dataQualityRulesets.Get(name)
	if !ok {
		return ErrNotFound
	}

	if ruleset != "" {
		r.Ruleset = ruleset
	}

	if description != "" {
		r.Description = description
	}

	r.LastModifiedOn = float64(time.Now().Unix())

	return nil
}

// ListDataQualityRulesets returns all rulesets sorted by name.
func (b *InMemoryBackend) ListDataQualityRulesets() []*DataQualityRuleset {
	b.mu.RLock("ListDataQualityRulesets")
	defer b.mu.RUnlock()

	src := b.dataQualityRulesets.Snapshot()
	out := make([]*DataQualityRuleset, 0, len(src))
	for _, r := range src {
		cp := *r
		cp.Tags = maps.Clone(r.Tags)
		out = append(out, &cp)
	}

	return out
}

// StartDataQualityRulesetEvaluationRun validates the rulesets exist and creates a run.
func (b *InMemoryBackend) StartDataQualityRulesetEvaluationRun(
	rulesetNames []string,
) (*DataQualityEvaluationRun, error) {
	b.mu.Lock("StartDataQualityRulesetEvaluationRun")
	defer b.mu.Unlock()

	for _, name := range rulesetNames {
		if !b.dataQualityRulesets.Has(name) {
			return nil, ErrNotFound
		}
	}

	run := &DataQualityEvaluationRun{
		RunID: fmt.Sprintf(
			"dqer_%d_%04d",
			time.Now().UnixNano(),
			mrand.IntN(10000), //nolint:gosec,mnd // non-security mock run ID
		),
		RulesetNames: append([]string(nil), rulesetNames...),
		Status:       stateRunning,
		StartedOn:    float64(time.Now().Unix()),
	}
	b.dataQualityEvalRuns.Put(run)

	return run, nil
}

// GetDataQualityRulesetEvaluationRun retrieves an evaluation run by ID.
func (b *InMemoryBackend) GetDataQualityRulesetEvaluationRun(
	runID string,
) (*DataQualityEvaluationRun, error) {
	b.mu.RLock("GetDataQualityRulesetEvaluationRun")
	defer b.mu.RUnlock()

	run, ok := b.dataQualityEvalRuns.Get(runID)
	if !ok {
		return nil, ErrNotFound
	}

	cp := *run
	cp.RulesetNames = append([]string(nil), run.RulesetNames...)

	return &cp, nil
}

// CancelDataQualityRulesetEvaluationRun cancels an active evaluation run.
func (b *InMemoryBackend) CancelDataQualityRulesetEvaluationRun(runID string) error {
	b.mu.Lock("CancelDataQualityRulesetEvaluationRun")
	defer b.mu.Unlock()

	run, ok := b.dataQualityEvalRuns.Get(runID)
	if !ok {
		return ErrNotFound
	}
	if run.Status != stateRunning {
		return ErrValidation
	}
	run.Status = "CANCELLED"

	return nil
}

// AddDataQualityRulesetInternal adds a data quality ruleset without validation.
func (b *InMemoryBackend) AddDataQualityRulesetInternal(r *DataQualityRuleset) {
	b.mu.Lock("AddDataQualityRulesetInternal")
	defer b.mu.Unlock()

	cp := *r
	cp.Tags = maps.Clone(r.Tags)
	b.dataQualityRulesets.Put(&cp)
}

var ErrDQRecommendationRunNotFound = fmt.Errorf("data quality recommendation run not found: %w", ErrNotFound)

// StartDataQualityRuleRecommendationRun creates a recommendation run.
func (b *InMemoryBackend) StartDataQualityRuleRecommendationRun(s3Path string) (*DQRuleRecommendationRun, error) {
	b.mu.Lock("StartDataQualityRuleRecommendationRun")
	defer b.mu.Unlock()

	runID := "dqrec-" + uuid.NewString()[:8]
	run := &DQRuleRecommendationRun{
		RecommendationRunID: runID,
		DataSourceS3Path:    s3Path,
		Status:              stateRunning,
		StartedOn:           float64(time.Now().Unix()),
	}
	b.dqRecommendationRuns.Put(run)
	cp := *run

	return &cp, nil
}

// GetDataQualityRuleRecommendationRun returns a recommendation run.
func (b *InMemoryBackend) GetDataQualityRuleRecommendationRun(runID string) (*DQRuleRecommendationRun, error) {
	b.mu.RLock("GetDataQualityRuleRecommendationRun")
	defer b.mu.RUnlock()

	run, ok := b.dqRecommendationRuns.Get(runID)
	if !ok {
		return nil, ErrDQRecommendationRunNotFound
	}

	cp := *run

	return &cp, nil
}

// CancelDataQualityRuleRecommendationRun marks a recommendation run as cancelled.
func (b *InMemoryBackend) CancelDataQualityRuleRecommendationRun(runID string) error {
	b.mu.Lock("CancelDataQualityRuleRecommendationRun")
	defer b.mu.Unlock()

	run, ok := b.dqRecommendationRuns.Get(runID)
	if !ok {
		return ErrDQRecommendationRunNotFound
	}

	run.Status = "CANCELLED"

	return nil
}

// ListDataQualityRuleRecommendationRuns returns all recommendation runs.
func (b *InMemoryBackend) ListDataQualityRuleRecommendationRuns() []*DQRuleRecommendationRun {
	b.mu.RLock("ListDataQualityRuleRecommendationRuns")
	defer b.mu.RUnlock()

	src := b.dqRecommendationRuns.All()
	runs := make([]*DQRuleRecommendationRun, 0, len(src))
	for _, r := range src {
		cp := *r
		runs = append(runs, &cp)
	}

	sort.Slice(runs, func(i, k int) bool {
		return runs[i].StartedOn < runs[k].StartedOn
	})

	return runs
}
