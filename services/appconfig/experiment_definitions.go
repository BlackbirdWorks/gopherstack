package appconfig

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"strconv"
	"time"
)

// featureFlagsProfileType is the ConfigurationProfile.Type real AWS AppConfig
// requires an experiment's ConfigurationProfileIdentifier to reference (an
// experiment evaluates a feature flag stored in a feature-flag profile, not
// a freeform configuration). This backend enforces the check only when Type
// was explicitly set to something else on the profile -- an empty Type
// (this backend's CreateConfigurationProfile treats Type as optional, and a
// large share of this package's existing test fixtures never set it) is
// treated as "unspecified," not "wrong," so pre-existing freeform-profile
// fixtures are not retroactively broken by this addition.
const featureFlagsProfileType = "AWS.AppConfig.FeatureFlags"

// ExperimentDefinition.Status values, matching real AWS AppConfig's
// ExperimentDefinitionStatus enum.
const (
	experimentDefinitionStatusIdle     = "IDLE"
	experimentDefinitionStatusActive   = "ACTIVE"
	experimentDefinitionStatusArchived = "ARCHIVED"
)

// DeleteExperimentDefinition's delete_type query param values, matching
// real AWS AppConfig's DeleteType enum.
const (
	deleteTypeArchive = "ARCHIVE"
	deleteTypeDestroy = "DESTROY"
)

// controlTreatmentKey and treatmentKeyPrefix are the server-generated
// Treatment.Key values this backend assigns -- see the Treatment doc
// comment in models.go for why real AWS assigns keys itself (TreatmentInput
// carries none) and why this specific scheme was chosen.
const (
	controlTreatmentKey = "Control"
	treatmentKeyPrefix  = "Treatment"
)

// assignTreatmentKeys returns a copy of treatments with Key set to
// "Treatment1".."TreatmentN", 1-indexed in the given order.
func assignTreatmentKeys(treatments []Treatment) []Treatment {
	out := make([]Treatment, len(treatments))

	for i, t := range treatments {
		t.Key = treatmentKeyPrefix + strconv.Itoa(i+1)
		out[i] = t
	}

	return out
}

// CreateExperimentDefinition creates a new experiment definition. See the
// StorageBackend interface doc comment for parameter semantics.
func (b *InMemoryBackend) CreateExperimentDefinition(
	applicationIdentifier, name, environmentIdentifier, configurationProfileIdentifier, flagKey,
	audienceRule, audienceDescription, hypothesis, launchCriteria string,
	control *Treatment,
	treatments []Treatment,
	tags map[string]string,
) (*ExperimentDefinition, error) {
	b.mu.Lock("CreateExperimentDefinition")
	defer b.mu.Unlock()

	if err := validateCreateExperimentDefinitionInput(name, audienceRule, flagKey, control, treatments); err != nil {
		return nil, err
	}

	appID, envID, profileID, err := b.resolveExperimentScopeLocked(
		applicationIdentifier, environmentIdentifier, configurationProfileIdentifier,
	)
	if err != nil {
		return nil, err
	}

	profile, _ := b.configProfiles.Get(profileID)
	if profile.Type != "" && profile.Type != featureFlagsProfileType {
		return nil, fmt.Errorf(
			"%w: configuration profile %s is not a %s profile",
			ErrBadRequest, configurationProfileIdentifier, featureFlagsProfileType,
		)
	}

	// "The key of the existing feature flag to use with the experiment"
	// (CreateExperimentDefinitionInput.FlagKey's doc comment) -- "existing"
	// means this should be checked against the profile's actual flag
	// content, not merely non-empty. Only enforced when the profile has
	// parseable AWS.AppConfig.FeatureFlags content to check against (see
	// latestFeatureFlagKeysLocked): a profile with no uploaded version yet,
	// or non-feature-flag content, stays permissive -- same "unspecified,
	// not wrong" treatment as the Type check above, so pre-existing
	// freeform/content-less test fixtures are not retroactively broken.
	if flagKeys, ok := b.latestFeatureFlagKeysLocked(appID, profileID); ok {
		if _, exists := flagKeys[flagKey]; !exists {
			return nil, fmt.Errorf(
				"%w: flag key %q does not exist in configuration profile %s",
				ErrBadRequest, flagKey, configurationProfileIdentifier,
			)
		}
	}

	if len(b.experimentDefinitionsByAppName.Get(appNameKey(appID, name))) > 0 {
		return nil, fmt.Errorf(
			"%w: experiment definition with name %q already exists in application %s",
			ErrConflict, name, appID,
		)
	}

	now := time.Now()
	controlCopy := *control
	controlCopy.Key = controlTreatmentKey

	def := &ExperimentDefinition{
		ID:                     newResourceID(),
		ApplicationID:          appID,
		Name:                   name,
		ConfigurationProfileID: profileID,
		EnvironmentID:          envID,
		FlagKey:                flagKey,
		AudienceDescription:    audienceDescription,
		AudienceRule:           audienceRule,
		Hypothesis:             hypothesis,
		LaunchCriteria:         launchCriteria,
		Status:                 experimentDefinitionStatusIdle,
		Control:                controlCopy,
		Treatments:             assignTreatmentKeys(treatments),
		CreatedAt:              now,
		UpdatedAt:              now,
	}
	b.experimentDefinitions.Put(def)

	if len(tags) > 0 {
		// Applied directly rather than via TagResource (which takes its
		// own lock and would deadlock re-entered here) -- see
		// tags_handling in the campaign return receipt for why this
		// avoids the bd gopherstack-lcan inline-Tags-dropped bug the six
		// pre-existing Create* handlers still have.
		b.tags[b.experimentDefinitionARN(appID, def.ID)] = maps.Clone(tags)
	}

	cp := *def

	return &cp, nil
}

// validateCreateExperimentDefinitionInput checks the request fields real
// CreateExperimentDefinitionInput marks "This member is required" -- a real
// SDK client validates these locally before ever sending the request, but a
// non-SDK caller (or a raw HTTP request) is not guaranteed to, so this
// backend validates independently, matching every other Create* method in
// this package.
func validateCreateExperimentDefinitionInput(
	name, audienceRule, flagKey string,
	control *Treatment,
	treatments []Treatment,
) error {
	if name == "" {
		return fmt.Errorf("%w: Name is required", ErrBadRequest)
	}

	if audienceRule == "" {
		return fmt.Errorf("%w: AudienceRule is required", ErrBadRequest)
	}

	if flagKey == "" {
		return fmt.Errorf("%w: FlagKey is required", ErrBadRequest)
	}

	if control == nil || control.FlagValue == nil {
		return fmt.Errorf("%w: Control with a FlagValue is required", ErrBadRequest)
	}

	if len(treatments) == 0 {
		return fmt.Errorf("%w: at least one Treatment is required", ErrBadRequest)
	}

	for i, t := range treatments {
		if t.FlagValue == nil {
			return fmt.Errorf("%w: Treatments[%d] is missing a FlagValue", ErrBadRequest, i)
		}
	}

	return nil
}

// resolveExperimentScopeLocked resolves an application/environment/
// configuration-profile identifier triple (each accepted by ID or name),
// validating every reference against real backend state rather than
// accepting any string -- reusing the same resolveAppID/resolveEnvID/
// resolveProfileID helpers GetConfiguration already relies on (see
// configuration.go). Must be called under lock.
func (b *InMemoryBackend) resolveExperimentScopeLocked(
	applicationIdentifier, environmentIdentifier, configurationProfileIdentifier string,
) (string, string, string, error) {
	appID, err := b.resolveAppID(applicationIdentifier)
	if err != nil {
		return "", "", "", err
	}

	envID, err := b.resolveEnvID(appID, environmentIdentifier)
	if err != nil {
		return "", "", "", err
	}

	profileID, err := b.resolveProfileID(appID, configurationProfileIdentifier)
	if err != nil {
		return "", "", "", err
	}

	return appID, envID, profileID, nil
}

// resolveExperimentDefinitionID finds an experiment definition ID by ID or
// name within an application. Must be called under lock.
func (b *InMemoryBackend) resolveExperimentDefinitionID(appID, identifier string) (string, error) {
	if def, ok := b.experimentDefinitions.Get(identifier); ok && def.ApplicationID == appID {
		return identifier, nil
	}

	if matches := b.experimentDefinitionsByAppName.Get(appNameKey(appID, identifier)); len(matches) > 0 {
		return matches[0].ID, nil
	}

	return "", fmt.Errorf("%w: experiment definition %s", ErrExperimentDefinitionNotFound, identifier)
}

// GetExperimentDefinition retrieves an experiment definition by application
// and experiment definition identifier (each accepted by ID or name).
func (b *InMemoryBackend) GetExperimentDefinition(
	applicationIdentifier, experimentDefinitionIdentifier string,
) (*ExperimentDefinition, error) {
	b.mu.RLock("GetExperimentDefinition")
	defer b.mu.RUnlock()

	appID, err := b.resolveAppID(applicationIdentifier)
	if err != nil {
		return nil, err
	}

	defID, err := b.resolveExperimentDefinitionID(appID, experimentDefinitionIdentifier)
	if err != nil {
		return nil, err
	}

	def, _ := b.experimentDefinitions.Get(defID)
	cp := *def

	return &cp, nil
}

// ListExperimentDefinitions returns experiment definitions across the
// account, optionally filtered by application/configuration-profile/
// environment identifier and status. See the StorageBackend interface doc
// comment for the identifier-filter resolution rules.
func (b *InMemoryBackend) ListExperimentDefinitions(
	applicationIdentifier, configurationProfileIdentifier, environmentIdentifier, status, nextToken string,
	maxResults int,
) ([]ExperimentDefinition, string) {
	b.mu.RLock("ListExperimentDefinitions")
	defer b.mu.RUnlock()

	filter, ok := b.buildExperimentDefinitionFilterLocked(
		applicationIdentifier, configurationProfileIdentifier, environmentIdentifier, status,
	)
	if !ok {
		return nil, ""
	}

	out := make([]ExperimentDefinition, 0, b.experimentDefinitions.Len())

	for _, def := range b.experimentDefinitions.All() {
		if filter(def) {
			out = append(out, *def)
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	page, token := appConfigPaginate(out, nextToken, b.paginationSecret, maxResults)

	return page, token
}

// experimentDefinitionToSummary builds the types.ExperimentDefinitionSummary
// shape -- see its doc comment in models.go.
func experimentDefinitionToSummary(d ExperimentDefinition) ExperimentDefinitionSummary {
	return ExperimentDefinitionSummary{
		CreatedAt:              d.CreatedAt,
		UpdatedAt:              d.UpdatedAt,
		ApplicationID:          d.ApplicationID,
		ID:                     d.ID,
		Name:                   d.Name,
		ConfigurationProfileID: d.ConfigurationProfileID,
		EnvironmentID:          d.EnvironmentID,
		FlagKey:                d.FlagKey,
		Hypothesis:             d.Hypothesis,
		Status:                 d.Status,
	}
}

// buildExperimentDefinitionFilterLocked builds a predicate matching
// ListExperimentDefinitions's optional identifier/status filters. Returns
// ok=false when an identifier filter was supplied but could not be
// resolved to anything -- callers should treat that as "no matches"
// (an empty result), not an error, matching normal AWS list-filter
// semantics for a filter value that names nothing. Must be called under
// lock.
func (b *InMemoryBackend) buildExperimentDefinitionFilterLocked(
	applicationIdentifier, configurationProfileIdentifier, environmentIdentifier, status string,
) (func(*ExperimentDefinition) bool, bool) {
	appID, appFilterActive, ok := b.resolveExperimentDefinitionFilterAppLocked(applicationIdentifier)
	if !ok {
		return nil, false
	}

	profileID, ok := b.resolveExperimentDefinitionFilterProfileLocked(
		appID, appFilterActive, configurationProfileIdentifier,
	)
	if !ok {
		return nil, false
	}

	envID, ok := b.resolveExperimentDefinitionFilterEnvLocked(appID, appFilterActive, environmentIdentifier)
	if !ok {
		return nil, false
	}

	return experimentDefinitionFilterPredicate(appID, profileID, envID, status, appFilterActive), true
}

// resolveExperimentDefinitionFilterAppLocked resolves
// ListExperimentDefinitions's optional ApplicationIdentifier filter. ok is
// false when a non-empty identifier was supplied but does not resolve to
// any application -- callers must treat that as "no matches," not an
// error. Must be called under lock.
func (b *InMemoryBackend) resolveExperimentDefinitionFilterAppLocked(
	applicationIdentifier string,
) (string, bool, bool) {
	if applicationIdentifier == "" {
		return "", false, true
	}

	resolved, err := b.resolveAppID(applicationIdentifier)
	if err != nil {
		return "", true, false
	}

	return resolved, true, true
}

// resolveExperimentDefinitionFilterProfileLocked resolves
// ListExperimentDefinitions's optional ConfigurationProfileIdentifier
// filter. A configuration-profile identifier can only be resolved by name
// within an application scope; without one (appFilterActive is false) this
// backend falls back to matching the literal value against the ID field
// only -- a documented, narrower gap (see PARITY.md) rather than a
// fabricated cross-application name resolution. Must be called under lock.
func (b *InMemoryBackend) resolveExperimentDefinitionFilterProfileLocked(
	appID string, appFilterActive bool, configurationProfileIdentifier string,
) (string, bool) {
	if !appFilterActive || configurationProfileIdentifier == "" {
		return configurationProfileIdentifier, true
	}

	resolved, err := b.resolveProfileID(appID, configurationProfileIdentifier)
	if err != nil {
		return "", false
	}

	return resolved, true
}

// resolveExperimentDefinitionFilterEnvLocked resolves
// ListExperimentDefinitions's optional EnvironmentIdentifier filter, with
// the same app-scope caveat as
// resolveExperimentDefinitionFilterProfileLocked. Must be called under lock.
func (b *InMemoryBackend) resolveExperimentDefinitionFilterEnvLocked(
	appID string, appFilterActive bool, environmentIdentifier string,
) (string, bool) {
	if !appFilterActive || environmentIdentifier == "" {
		return environmentIdentifier, true
	}

	resolved, err := b.resolveEnvID(appID, environmentIdentifier)
	if err != nil {
		return "", false
	}

	return resolved, true
}

// experimentDefinitionFilterPredicate builds the actual per-definition
// match predicate from already-resolved filter values.
func experimentDefinitionFilterPredicate(
	appID, profileID, envID, status string, appFilterActive bool,
) func(*ExperimentDefinition) bool {
	return func(def *ExperimentDefinition) bool {
		if appFilterActive && def.ApplicationID != appID {
			return false
		}

		if profileID != "" && def.ConfigurationProfileID != profileID {
			return false
		}

		if envID != "" && def.EnvironmentID != envID {
			return false
		}

		return status == "" || def.Status == status
	}
}

// experimentDefinitionHasActiveRunLocked reports whether any RUNNING run
// exists for the given experiment definition. Must be called under lock.
func (b *InMemoryBackend) experimentDefinitionHasActiveRunLocked(defID string) bool {
	for _, r := range b.experimentRunsByDef.Get(defID) {
		if r.Status == experimentRunStatusRunning {
			return true
		}
	}

	return false
}

// UpdateExperimentDefinition updates an experiment definition. See the
// StorageBackend interface doc comment for nil-means-unchanged semantics.
func (b *InMemoryBackend) UpdateExperimentDefinition(
	applicationIdentifier, experimentDefinitionIdentifier string,
	audienceDescription, audienceRule *string,
	control *Treatment,
	hypothesis, launchCriteria *string,
	treatments *[]Treatment,
) (*ExperimentDefinition, error) {
	b.mu.Lock("UpdateExperimentDefinition")
	defer b.mu.Unlock()

	appID, err := b.resolveAppID(applicationIdentifier)
	if err != nil {
		return nil, err
	}

	defID, err := b.resolveExperimentDefinitionID(appID, experimentDefinitionIdentifier)
	if err != nil {
		return nil, err
	}

	if b.experimentDefinitionHasActiveRunLocked(defID) {
		return nil, fmt.Errorf(
			"%w: cannot update experiment definition %s while a run is active",
			ErrConflict, experimentDefinitionIdentifier,
		)
	}

	existing, _ := b.experimentDefinitions.Get(defID)
	updated := *existing

	if audienceDescription != nil {
		updated.AudienceDescription = *audienceDescription
	}

	if audienceRule != nil {
		updated.AudienceRule = *audienceRule
	}

	if control != nil && control.FlagValue != nil {
		cp := *control
		cp.Key = controlTreatmentKey
		updated.Control = cp
	}

	if hypothesis != nil {
		updated.Hypothesis = *hypothesis
	}

	if launchCriteria != nil {
		updated.LaunchCriteria = *launchCriteria
	}

	if treatments != nil {
		updated.Treatments = assignTreatmentKeys(*treatments)
	}

	updated.UpdatedAt = time.Now()
	b.experimentDefinitions.Put(&updated)
	cp := updated

	return &cp, nil
}

// destroyExperimentDefinitionLocked permanently removes def, every run
// scoped to it, their recorded events, and every tag (definition and
// per-run). Must be called under lock.
func (b *InMemoryBackend) destroyExperimentDefinitionLocked(def *ExperimentDefinition) {
	delete(b.tags, b.experimentDefinitionARN(def.ApplicationID, def.ID))

	for _, r := range slices.Clone(b.experimentRunsByDef.Get(def.ID)) {
		delete(b.tags, b.experimentRunARN(def.ApplicationID, def.ID, r.Run))
		delete(b.experimentRunEvents, experimentRunKey(def.ID, r.Run))
		b.experimentRuns.Delete(experimentRunKey(def.ID, r.Run))
	}

	delete(b.experimentRunCounters, def.ID)
	b.experimentDefinitions.Delete(def.ID)
}

// deleteExperimentDefinitionsForAppLocked destroys every ExperimentDefinition
// scoped to applicationID (and its runs/events/tags) -- called from
// DeleteApplication so no ghost experiment rows survive the parent
// application's deletion, the same cascade-cleanup precedent already set
// for environments/configProfiles/hostedConfigVersions/deployments/
// ExtensionAssociations. Must be called under lock.
func (b *InMemoryBackend) deleteExperimentDefinitionsForAppLocked(applicationID string) {
	for _, def := range slices.Clone(b.experimentDefinitionsByApp.Get(applicationID)) {
		b.destroyExperimentDefinitionLocked(def)
	}
}

// DeleteExperimentDefinition archives or permanently destroys an experiment
// definition. See the StorageBackend interface doc comment for deleteType
// semantics, including the ARCHIVE default this backend applies when
// deleteType is empty: the real SDK's DeleteType doc text describes
// ARCHIVE as "hide but preserve" and DESTROY as the explicit opt-in to
// permanent removal, so an omitted deleteType is treated as the
// non-destructive choice rather than assuming irreversible deletion was
// intended (unverified against real AWS -- the SDK does not document a
// default -- and called out here plus in PARITY.md as an assumption, not a
// confirmed wire fact).
func (b *InMemoryBackend) DeleteExperimentDefinition(
	applicationIdentifier, experimentDefinitionIdentifier, deleteType string,
) error {
	b.mu.Lock("DeleteExperimentDefinition")
	defer b.mu.Unlock()

	appID, err := b.resolveAppID(applicationIdentifier)
	if err != nil {
		return err
	}

	defID, err := b.resolveExperimentDefinitionID(appID, experimentDefinitionIdentifier)
	if err != nil {
		return err
	}

	def, _ := b.experimentDefinitions.Get(defID)

	effectiveType := deleteType
	if effectiveType == "" {
		effectiveType = deleteTypeArchive
	}

	switch effectiveType {
	case deleteTypeArchive:
		updated := *def
		updated.Status = experimentDefinitionStatusArchived
		updated.UpdatedAt = time.Now()
		b.experimentDefinitions.Put(&updated)
	case deleteTypeDestroy:
		b.destroyExperimentDefinitionLocked(def)
	default:
		return fmt.Errorf("%w: invalid delete_type %q", ErrBadRequest, deleteType)
	}

	return nil
}
