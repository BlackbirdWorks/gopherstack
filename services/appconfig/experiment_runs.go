// Package appconfig -- this file implements the experiment RUN half of the
// A/B-testing family (experiment_definitions.go implements the definition
// half). Unlike deployments.go's growth-curve state machine, an experiment
// run has no duration to progress through automatically: real AWS AppConfig
// keeps a run RUNNING, serving its configured exposure percentage, until a
// caller explicitly stops it (or updates it). So there is no background
// reconciler goroutine here -- StartExperimentRun/StopExperimentRun/
// UpdateExperimentRun perform their entire state transition synchronously,
// and ListExperimentRunEvents returns exactly what those calls recorded as
// they happened.
//
// This backend does not model the underlying "real" deployment AWS creates
// to actually serve treatment variations to production traffic, nor an
// analytics engine that would compute variant metrics/statistical
// significance from live exposure -- see ExperimentRunResult's doc comment
// in models.go and the results_verdict discussion in PARITY.md for what
// this means for what StopExperimentRun's Result field can and cannot
// contain.
package appconfig

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"time"
)

// ExperimentRun.Status values, matching real AWS AppConfig's
// ExperimentRunStatus enum.
const (
	experimentRunStatusRunning = "RUNNING"
	experimentRunStatusDone    = "DONE"
)

// ExperimentRunEvent.EventType values, matching real AWS AppConfig's
// ExperimentRunEventType enum.
const (
	experimentRunEventRunStarted       = "RUN_STARTED"
	experimentRunEventExposureUpdated  = "EXPOSURE_UPDATED"
	experimentRunEventOverridesUpdated = "OVERRIDES_UPDATED"
	experimentRunEventRunStopped       = "RUN_STOPPED"
)

const (
	// minExposurePercentage/maxExposurePercentage bound
	// StartExperimentRun/UpdateExperimentRun's ExposurePercentage, matching
	// the real SDK's "the percentage of the target audience to expose".
	minExposurePercentage = 0.0
	maxExposurePercentage = 100.0
)

// appendExperimentRunEventLocked prepends ev to the run's recorded event
// history, matching the most-recent-first ordering this package's
// DeploymentEvent family already uses (appendDeploymentEvent in
// deployments.go). Must be called under lock.
func (b *InMemoryBackend) appendExperimentRunEventLocked(defID string, run int32, ev ExperimentRunEvent) {
	key := experimentRunKey(defID, run)
	b.experimentRunEvents[key] = append([]ExperimentRunEvent{ev}, b.experimentRunEvents[key]...)
}

// experimentDefinitionSnapshotFrom captures def's fields as they stand right
// now, for ExperimentRun.ExperimentDefinitionSnapshot -- see that field's
// doc comment in models.go.
func experimentDefinitionSnapshotFrom(def *ExperimentDefinition) *ExperimentDefinitionSnapshot {
	return &ExperimentDefinitionSnapshot{
		ApplicationID:          def.ApplicationID,
		AudienceDescription:    def.AudienceDescription,
		AudienceRule:           def.AudienceRule,
		ConfigurationProfileID: def.ConfigurationProfileID,
		Control:                def.Control,
		EnvironmentID:          def.EnvironmentID,
		FlagKey:                def.FlagKey,
		Hypothesis:             def.Hypothesis,
		ID:                     def.ID,
		LaunchCriteria:         def.LaunchCriteria,
		Name:                   def.Name,
		Treatments:             slices.Clone(def.Treatments),
	}
}

// StartExperimentRun starts a new run of an experiment definition. See the
// StorageBackend interface doc comment for parameter semantics.
func (b *InMemoryBackend) StartExperimentRun(
	applicationIdentifier, experimentDefinitionIdentifier, description string,
	exposurePercentage *float32,
	treatmentOverrides map[string]string,
	tags map[string]string,
) (*ExperimentRun, error) {
	b.mu.Lock("StartExperimentRun")
	defer b.mu.Unlock()

	appID, err := b.resolveAppID(applicationIdentifier)
	if err != nil {
		return nil, err
	}

	defID, err := b.resolveExperimentDefinitionID(appID, experimentDefinitionIdentifier)
	if err != nil {
		return nil, err
	}

	def, _ := b.experimentDefinitions.Get(defID)
	if def.Status == experimentDefinitionStatusArchived {
		return nil, fmt.Errorf(
			"%w: cannot start a run for archived experiment definition %s",
			ErrBadRequest, experimentDefinitionIdentifier,
		)
	}

	if b.experimentDefinitionHasActiveRunLocked(defID) {
		return nil, fmt.Errorf(
			"%w: experiment definition %s already has a running experiment run",
			ErrConflict, experimentDefinitionIdentifier,
		)
	}

	// ExposurePercentage's default is not documented by the SDK (it is an
	// optional *float32, not "This member is required"); 0 is chosen
	// because the SDK's own doc comment calls out "Set to 0 to validate
	// the experiment before exposing production users" -- the safest
	// reading absent a confirmed default. See PARITY.md.
	exposure := float32(minExposurePercentage)
	if exposurePercentage != nil {
		exposure = *exposurePercentage
	}

	if exposure < minExposurePercentage || exposure > maxExposurePercentage {
		return nil, fmt.Errorf(
			"%w: ExposurePercentage must be between 0 and 100", ErrBadRequest,
		)
	}

	b.experimentRunCounters[defID]++
	run := b.experimentRunCounters[defID]

	now := time.Now()
	er := &ExperimentRun{
		ApplicationID:                appID,
		ExperimentDefinitionID:       defID,
		Description:                  description,
		Status:                       experimentRunStatusRunning,
		ExperimentDefinitionSnapshot: experimentDefinitionSnapshotFrom(def),
		ExposurePercentage:           exposure,
		Run:                          run,
		StartedAt:                    now,
		UpdatedAt:                    now,
	}

	if treatmentOverrides != nil {
		er.TreatmentOverrides = TreatmentOverrides{Inline: maps.Clone(treatmentOverrides)}
	}

	b.appendExperimentRunEventLocked(defID, run, ExperimentRunEvent{
		EventType:          experimentRunEventRunStarted,
		Description:        "Experiment run started",
		TriggeredBy:        triggeredByUser,
		OccurredAt:         now,
		ExposurePercentage: exposure,
		TreatmentOverrides: er.TreatmentOverrides,
	})

	b.experimentRuns.Put(er)

	updatedDef := *def
	updatedDef.Status = experimentDefinitionStatusActive
	b.experimentDefinitions.Put(&updatedDef)

	if len(tags) > 0 {
		b.tags[b.experimentRunARN(appID, defID, run)] = maps.Clone(tags)
	}

	cp := *er

	return &cp, nil
}

// resolveExperimentRunLocked resolves applicationIdentifier/
// experimentDefinitionIdentifier and looks up the run at the given run
// number, returning the resolved definition ID and the run. Must be called
// under lock.
func (b *InMemoryBackend) resolveExperimentRunLocked(
	applicationIdentifier, experimentDefinitionIdentifier string, run int32,
) (string, *ExperimentRun, error) {
	appID, err := b.resolveAppID(applicationIdentifier)
	if err != nil {
		return "", nil, err
	}

	defID, err := b.resolveExperimentDefinitionID(appID, experimentDefinitionIdentifier)
	if err != nil {
		return "", nil, err
	}

	r, ok := b.experimentRuns.Get(experimentRunKey(defID, run))
	if !ok {
		return "", nil, fmt.Errorf("%w: experiment run %d", ErrExperimentRunNotFound, run)
	}

	return defID, r, nil
}

// GetExperimentRun retrieves an experiment run by application, experiment
// definition identifier, and run number.
func (b *InMemoryBackend) GetExperimentRun(
	applicationIdentifier, experimentDefinitionIdentifier string, run int32,
) (*ExperimentRun, error) {
	b.mu.RLock("GetExperimentRun")
	defer b.mu.RUnlock()

	_, r, err := b.resolveExperimentRunLocked(applicationIdentifier, experimentDefinitionIdentifier, run)
	if err != nil {
		return nil, err
	}

	cp := *r

	return &cp, nil
}

// ListExperimentRuns returns paginated runs for an experiment definition,
// optionally filtered by status.
func (b *InMemoryBackend) ListExperimentRuns(
	applicationIdentifier, experimentDefinitionIdentifier, status, nextToken string,
	maxResults int,
) ([]ExperimentRun, string, error) {
	b.mu.RLock("ListExperimentRuns")
	defer b.mu.RUnlock()

	appID, err := b.resolveAppID(applicationIdentifier)
	if err != nil {
		return nil, "", err
	}

	defID, err := b.resolveExperimentDefinitionID(appID, experimentDefinitionIdentifier)
	if err != nil {
		return nil, "", err
	}

	runs := b.experimentRunsByDef.Get(defID)
	out := make([]ExperimentRun, 0, len(runs))

	for _, r := range runs {
		if status != "" && r.Status != status {
			continue
		}

		out = append(out, *r)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Run < out[j].Run })

	page, token := appConfigPaginate(out, nextToken, b.paginationSecret, maxResults)

	return page, token, nil
}

// experimentRunToSummary builds the types.ExperimentRunSummary shape -- see
// its doc comment in models.go.
func experimentRunToSummary(r ExperimentRun) ExperimentRunSummary {
	return ExperimentRunSummary{
		StartedAt:              r.StartedAt,
		EndedAt:                r.EndedAt,
		UpdatedAt:              r.UpdatedAt,
		ExperimentDefinitionID: r.ExperimentDefinitionID,
		Description:            r.Description,
		Status:                 r.Status,
		Run:                    r.Run,
	}
}

// UpdateExperimentRun updates a RUNNING experiment run. See the
// StorageBackend interface doc comment for semantics.
func (b *InMemoryBackend) UpdateExperimentRun(
	applicationIdentifier, experimentDefinitionIdentifier string,
	run int32,
	description *string,
	exposurePercentage *float32,
	treatmentOverrides *TreatmentOverrides,
) (*ExperimentRun, error) {
	b.mu.Lock("UpdateExperimentRun")
	defer b.mu.Unlock()

	defID, existing, err := b.resolveExperimentRunLocked(
		applicationIdentifier, experimentDefinitionIdentifier, run,
	)
	if err != nil {
		return nil, err
	}

	if existing.Status != experimentRunStatusRunning {
		return nil, fmt.Errorf(
			"%w: cannot update experiment run %d in state %s", ErrBadRequest, run, existing.Status,
		)
	}

	updated := *existing
	if description != nil {
		updated.Description = *description
	}

	now := time.Now()

	exposureChanged, err := applyExperimentRunExposureUpdate(&updated, existing, exposurePercentage)
	if err != nil {
		return nil, err
	}

	overridesChanged := false
	if treatmentOverrides != nil {
		updated.TreatmentOverrides = TreatmentOverrides{Inline: maps.Clone(treatmentOverrides.Inline)}
		overridesChanged = !maps.Equal(updated.TreatmentOverrides.Inline, existing.TreatmentOverrides.Inline)
	}

	if exposureChanged {
		b.appendExperimentRunEventLocked(defID, run, ExperimentRunEvent{
			EventType:          experimentRunEventExposureUpdated,
			Description:        "Exposure percentage updated",
			TriggeredBy:        triggeredByUser,
			OccurredAt:         now,
			ExposurePercentage: updated.ExposurePercentage,
			TreatmentOverrides: updated.TreatmentOverrides,
		})
	}

	if overridesChanged {
		b.appendExperimentRunEventLocked(defID, run, ExperimentRunEvent{
			EventType:          experimentRunEventOverridesUpdated,
			Description:        "Treatment overrides updated",
			TriggeredBy:        triggeredByUser,
			OccurredAt:         now,
			ExposurePercentage: updated.ExposurePercentage,
			TreatmentOverrides: updated.TreatmentOverrides,
		})
	}

	updated.UpdatedAt = now
	b.experimentRuns.Put(&updated)
	cp := updated

	return &cp, nil
}

// applyExperimentRunExposureUpdate applies an optional exposure-percentage
// change to updated, enforcing real AWS's "can only be increased, not
// decreased" rule, and reports whether the value actually changed.
func applyExperimentRunExposureUpdate(
	updated, existing *ExperimentRun, exposurePercentage *float32,
) (bool, error) {
	if exposurePercentage == nil {
		return false, nil
	}

	next := *exposurePercentage
	if next < minExposurePercentage || next > maxExposurePercentage {
		return false, fmt.Errorf("%w: ExposurePercentage must be between 0 and 100", ErrBadRequest)
	}

	if next < existing.ExposurePercentage {
		return false, fmt.Errorf(
			"%w: ExposurePercentage can only be increased, not decreased", ErrBadRequest,
		)
	}

	updated.ExposurePercentage = next

	return next != existing.ExposurePercentage, nil
}

// StopExperimentRun stops a RUNNING experiment run, moving it to DONE.
func (b *InMemoryBackend) StopExperimentRun(
	applicationIdentifier, experimentDefinitionIdentifier string,
	run int32,
	result *ExperimentRunResult,
) (*ExperimentRun, error) {
	b.mu.Lock("StopExperimentRun")
	defer b.mu.Unlock()

	defID, existing, err := b.resolveExperimentRunLocked(
		applicationIdentifier, experimentDefinitionIdentifier, run,
	)
	if err != nil {
		return nil, err
	}

	if existing.Status != experimentRunStatusRunning {
		return nil, fmt.Errorf(
			"%w: cannot stop experiment run %d in state %s", ErrBadRequest, run, existing.Status,
		)
	}

	now := time.Now()
	updated := *existing
	updated.Status = experimentRunStatusDone
	updated.EndedAt = now
	updated.UpdatedAt = now

	if result != nil {
		updated.Result = result
	}

	b.appendExperimentRunEventLocked(defID, run, ExperimentRunEvent{
		EventType:          experimentRunEventRunStopped,
		Description:        "Experiment run stopped",
		TriggeredBy:        triggeredByUser,
		OccurredAt:         now,
		ExposurePercentage: existing.ExposurePercentage,
		TreatmentOverrides: existing.TreatmentOverrides,
	})

	b.experimentRuns.Put(&updated)

	// No other run for this definition can be RUNNING (StartExperimentRun
	// enforces at most one), so the definition returns to IDLE.
	if def, ok := b.experimentDefinitions.Get(defID); ok && def.Status == experimentDefinitionStatusActive {
		updatedDef := *def
		updatedDef.Status = experimentDefinitionStatusIdle
		b.experimentDefinitions.Put(&updatedDef)
	}

	cp := updated

	return &cp, nil
}

// ListExperimentRunEvents returns the events this backend actually recorded
// during the run's lifecycle, most-recent-first.
func (b *InMemoryBackend) ListExperimentRunEvents(
	applicationIdentifier, experimentDefinitionIdentifier string,
	run int32,
	nextToken string,
	maxResults int,
) ([]ExperimentRunEvent, string, error) {
	b.mu.RLock("ListExperimentRunEvents")
	defer b.mu.RUnlock()

	defID, _, err := b.resolveExperimentRunLocked(applicationIdentifier, experimentDefinitionIdentifier, run)
	if err != nil {
		return nil, "", err
	}

	events := slices.Clone(b.experimentRunEvents[experimentRunKey(defID, run)])

	page, token := appConfigPaginate(events, nextToken, b.paginationSecret, maxResults)

	return page, token, nil
}
