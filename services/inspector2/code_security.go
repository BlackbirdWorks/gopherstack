package inspector2

import (
	"fmt"
	"regexp"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// codeSecurityNameMinLen/codeSecurityNameMaxLen enforce the real, documented
// length constraint shared by CreateCodeSecurityIntegrationInput.name and
// CreateCodeSecurityScanConfigurationInput.name (confirmed via the AWS API
// Reference -- the Go SDK module's doc comments carry no length/pattern
// prose for either field, unlike CreateFilterInput.name, so the API
// Reference is the only source for these constraints): "Minimum length of
// 1. Maximum length of 60." Both share the identical pattern too, so one
// validator covers both ops.
const (
	codeSecurityNameMinLen = 1
	codeSecurityNameMaxLen = 60

	codeScanStatusInProgress = "IN_PROGRESS"
	codeScanStatusSuccessful = "SUCCESSFUL"
)

// onceCodeSecurityNamePattern lazily compiles the real, documented
// CreateCodeSecurityIntegrationInput.name / CreateCodeSecurityScanConfigurationInput.name
// pattern (AWS API Reference: `[a-zA-Z0-9-_$:.]*`), exactly once.
//
//nolint:gochecknoglobals // read-only package-level regexp, built once via sync.OnceValue
var onceCodeSecurityNamePattern = sync.OnceValue(func() *regexp.Regexp {
	return regexp.MustCompile(`^[a-zA-Z0-9\-_$:.]*$`)
})

// validateCodeSecurityName enforces the real name constraint shared by
// CreateCodeSecurityIntegration and CreateCodeSecurityScanConfiguration: 1-60
// characters, alphanumeric plus dash/underscore/dollar-sign/colon/dot. Real
// AWS returns ValidationException for violations; this backend previously
// accepted any non-empty string.
func validateCodeSecurityName(name string) error {
	if len(name) < codeSecurityNameMinLen || len(name) > codeSecurityNameMaxLen {
		return fmt.Errorf(
			"%w: name must be between %d and %d characters, got %d",
			ErrValidation, codeSecurityNameMinLen, codeSecurityNameMaxLen, len(name),
		)
	}

	if !onceCodeSecurityNamePattern().MatchString(name) {
		return fmt.Errorf(
			"%w: name must contain only alphanumeric characters, dashes, underscores, dollar signs, colons, and dots",
			ErrValidation,
		)
	}

	return nil
}

func (b *InMemoryBackend) buildCodeSecurityIntegrationARN() string {
	return arn.Build(inspector2Service, b.region, b.accountID, "integration/code-security/"+uuid.New().String())
}

func (b *InMemoryBackend) buildCodeSecurityScanConfigARN() string {
	return arn.Build(inspector2Service, b.region, b.accountID, "code-security-scan-configuration/"+uuid.New().String())
}

// CreateCodeSecurityIntegration creates a new code security integration.
func (b *InMemoryBackend) CreateCodeSecurityIntegration(
	name, integType string,
	tags map[string]string,
	details map[string]any,
) (*CodeSecurityIntegration, error) {
	b.mu.Lock("CreateCodeSecurityIntegration")
	defer b.mu.Unlock()

	if err := validateCodeSecurityName(name); err != nil {
		return nil, err
	}

	if err := validateTags(tags); err != nil {
		return nil, err
	}

	integARN := b.buildCodeSecurityIntegrationARN()
	now := time.Now().UTC()
	integ := &CodeSecurityIntegration{
		IntegrationArn: integARN,
		Name:           name,
		Type:           integType,
		Status:         statusActive,
		Tags:           tags,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	_ = details
	b.codeSecurityIntegrations.Put(integ)

	return integ, nil
}

// DeleteCodeSecurityIntegration deletes a code security integration.
func (b *InMemoryBackend) DeleteCodeSecurityIntegration(integrationARN string) error {
	b.mu.Lock("DeleteCodeSecurityIntegration")
	defer b.mu.Unlock()

	if !b.codeSecurityIntegrations.Delete(integrationARN) {
		return ErrCodeSecurityIntegrationNotFound
	}

	return nil
}

// GetCodeSecurityIntegration returns a code security integration.
func (b *InMemoryBackend) GetCodeSecurityIntegration(integrationARN string) (*CodeSecurityIntegration, error) {
	b.mu.RLock("GetCodeSecurityIntegration")
	defer b.mu.RUnlock()

	integ, ok := b.codeSecurityIntegrations.Get(integrationARN)
	if !ok {
		return nil, ErrCodeSecurityIntegrationNotFound
	}

	cp := *integ

	return &cp, nil
}

// UpdateCodeSecurityIntegration updates a code security integration.
func (b *InMemoryBackend) UpdateCodeSecurityIntegration(
	integrationARN string,
	details map[string]any,
) (*CodeSecurityIntegration, error) {
	b.mu.Lock("UpdateCodeSecurityIntegration")
	defer b.mu.Unlock()

	integ, ok := b.codeSecurityIntegrations.Get(integrationARN)
	if !ok {
		return nil, ErrCodeSecurityIntegrationNotFound
	}

	integ.UpdatedAt = time.Now().UTC()
	_ = details

	cp := *integ

	return &cp, nil
}

// ListCodeSecurityIntegrations returns all code security integrations.
func (b *InMemoryBackend) ListCodeSecurityIntegrations() ([]*CodeSecurityIntegration, error) {
	b.mu.RLock("ListCodeSecurityIntegrations")
	defer b.mu.RUnlock()

	result := make([]*CodeSecurityIntegration, 0, b.codeSecurityIntegrations.Len())

	for _, integ := range b.codeSecurityIntegrations.Snapshot() {
		cp := *integ
		result = append(result, &cp)
	}

	return result, nil
}

// isValidCodeSecurityLevel reports whether level is one of the
// ConfigurationLevel enum values accepted by the real
// CreateCodeSecurityScanConfigurationInput.level member.
func isValidCodeSecurityLevel(level string) bool {
	return slices.Contains([]string{"ORGANIZATION", "ACCOUNT"}, level)
}

// validateCodeSecurityRuleSetCategories enforces the real API's required,
// enum-constrained configuration.ruleSetCategories member (confirmed via
// types.CodeSecurityScanConfiguration's "This member is required" doc comment
// -- required on both Create and Update since both share the same shape).
func validateCodeSecurityRuleSetCategories(categories []string) error {
	if len(categories) == 0 {
		return fmt.Errorf("%w: configuration.ruleSetCategories is required", ErrValidation)
	}

	valid := []string{"SAST", "IAC", "SCA"}

	for _, cat := range categories {
		if !slices.Contains(valid, cat) {
			return fmt.Errorf("%w: configuration.ruleSetCategories: invalid value %q", ErrValidation, cat)
		}
	}

	return nil
}

// validCodeSecurityProjectSelectionScope is the sole real ProjectSelectionScope
// enum value (types.ProjectSelectionScopeAll, inspector2 SDK enums.go) -- the
// real type has exactly one legal value, not the ALL/SPECIFIC pair a prior
// audit guessed at.
const validCodeSecurityProjectSelectionScope = "ALL"

// validateCodeSecurityScopeSettings enforces ScopeSettings.projectSelectionScope's
// real enum constraint when the field is present. Real AWS returns
// ValidationException for a violation; this backend previously accepted any value.
func validateCodeSecurityScopeSettings(scopeSettings map[string]any) error {
	raw, ok := scopeSettings["projectSelectionScope"]
	if !ok {
		return nil
	}

	scope, isStr := raw.(string)
	if !isStr || scope != validCodeSecurityProjectSelectionScope {
		return fmt.Errorf("%w: scopeSettings.projectSelectionScope: invalid value %v", ErrValidation, raw)
	}

	return nil
}

// validateCodeSecurityPeriodicScanConfig enforces PeriodicScanConfiguration.frequency's
// real enum constraint when the field is present (frequency itself is optional).
func validateCodeSecurityPeriodicScanConfig(cfg map[string]any) error {
	raw, ok := cfg["frequency"]
	if !ok {
		return nil
	}

	validFrequencies := [...]string{"WEEKLY", "MONTHLY", "NEVER"}

	freq, isStr := raw.(string)
	if !isStr || !slices.Contains(validFrequencies[:], freq) {
		return fmt.Errorf(
			"%w: configuration.periodicScanConfiguration.frequency: invalid value %v", ErrValidation, raw,
		)
	}

	return nil
}

// validateCodeSecurityContinuousIntegrationScanConfig enforces
// ContinuousIntegrationScanConfiguration.supportedEvents' real required,
// enum-constrained shape (confirmed via types.ContinuousIntegrationScanConfiguration's
// "This member is required" doc comment) when the config itself is present --
// the config as a whole remains optional at the CodeSecurityScanConfiguration level.
func validateCodeSecurityContinuousIntegrationScanConfig(cfg map[string]any) error {
	raw, ok := cfg["supportedEvents"]
	if !ok {
		return fmt.Errorf(
			"%w: configuration.continuousIntegrationScanConfiguration.supportedEvents is required", ErrValidation,
		)
	}

	events, isSlice := raw.([]any)
	if !isSlice || len(events) == 0 {
		return fmt.Errorf(
			"%w: configuration.continuousIntegrationScanConfiguration.supportedEvents is required", ErrValidation,
		)
	}

	validEvents := [...]string{"PULL_REQUEST", "PUSH"}

	for _, e := range events {
		event, isStr := e.(string)
		if !isStr || !slices.Contains(validEvents[:], event) {
			return fmt.Errorf(
				"%w: configuration.continuousIntegrationScanConfiguration.supportedEvents: invalid value %v",
				ErrValidation, e,
			)
		}
	}

	return nil
}

// CreateCodeSecurityScanConfiguration creates a code security scan configuration.
func (b *InMemoryBackend) CreateCodeSecurityScanConfiguration(
	name, level string,
	ruleSetCategories []string,
	continuousIntegrationScanConfig map[string]any,
	periodicConfig map[string]any,
	scopeSettings map[string]any,
	tags map[string]string,
) (*CodeSecurityScanConfiguration, error) {
	b.mu.Lock("CreateCodeSecurityScanConfiguration")
	defer b.mu.Unlock()

	if err := validateCodeSecurityName(name); err != nil {
		return nil, err
	}

	if level == "" {
		return nil, fmt.Errorf("%w: level is required", ErrValidation)
	}

	if !isValidCodeSecurityLevel(level) {
		return nil, fmt.Errorf("%w: level: invalid value %q", ErrValidation, level)
	}

	if err := validateCodeSecurityRuleSetCategories(ruleSetCategories); err != nil {
		return nil, err
	}

	if scopeSettings != nil {
		if err := validateCodeSecurityScopeSettings(scopeSettings); err != nil {
			return nil, err
		}
	}

	if periodicConfig != nil {
		if err := validateCodeSecurityPeriodicScanConfig(periodicConfig); err != nil {
			return nil, err
		}
	}

	if continuousIntegrationScanConfig != nil {
		if err := validateCodeSecurityContinuousIntegrationScanConfig(continuousIntegrationScanConfig); err != nil {
			return nil, err
		}
	}

	if err := validateTags(tags); err != nil {
		return nil, err
	}

	cfgARN := b.buildCodeSecurityScanConfigARN()
	now := time.Now().UTC()
	cfg := &CodeSecurityScanConfiguration{
		Arn:                             cfgARN,
		Name:                            name,
		Level:                           level,
		RuleSetCategories:               ruleSetCategories,
		ContinuousIntegrationScanConfig: continuousIntegrationScanConfig,
		PeriodicScanConfig:              periodicConfig,
		ScopeSettings:                   scopeSettings,
		Tags:                            tags,
		CreatedAt:                       now,
		UpdatedAt:                       now,
	}
	b.codeSecurityScanConfigs.Put(cfg)

	return cfg, nil
}

// DeleteCodeSecurityScanConfiguration deletes a code security scan configuration.
func (b *InMemoryBackend) DeleteCodeSecurityScanConfiguration(scanConfigARN string) error {
	b.mu.Lock("DeleteCodeSecurityScanConfiguration")
	defer b.mu.Unlock()

	if !b.codeSecurityScanConfigs.Delete(scanConfigARN) {
		return ErrCodeSecurityScanConfigNotFound
	}

	// slices.Clone is required here: the index's returned slice mutates in
	// place as each Delete below removes an entry from it.
	for _, assoc := range slices.Clone(b.scanConfigAssociationsByConfig.Get(scanConfigARN)) {
		b.scanConfigAssociations.Delete(scanConfigAssociationKeyFn(assoc))
	}

	return nil
}

// GetCodeSecurityScanConfiguration returns a code security scan configuration.
func (b *InMemoryBackend) GetCodeSecurityScanConfiguration(
	scanConfigARN string,
) (*CodeSecurityScanConfiguration, error) {
	b.mu.RLock("GetCodeSecurityScanConfiguration")
	defer b.mu.RUnlock()

	cfg, ok := b.codeSecurityScanConfigs.Get(scanConfigARN)
	if !ok {
		return nil, ErrCodeSecurityScanConfigNotFound
	}

	cp := *cfg

	return &cp, nil
}

// UpdateCodeSecurityScanConfiguration updates a code security scan
// configuration. Real UpdateCodeSecurityScanConfigurationInput only carries
// "configuration" (ruleSetCategories/periodicScanConfiguration/
// continuousIntegrationScanConfiguration) and "scanConfigurationArn" -- level,
// scopeSettings, and name are set at creation and are not update targets.
func (b *InMemoryBackend) UpdateCodeSecurityScanConfiguration(
	scanConfigARN string,
	ruleSetCategories []string,
	continuousIntegrationScanConfig map[string]any,
	periodicConfig map[string]any,
) (*CodeSecurityScanConfiguration, error) {
	b.mu.Lock("UpdateCodeSecurityScanConfiguration")
	defer b.mu.Unlock()

	cfg, ok := b.codeSecurityScanConfigs.Get(scanConfigARN)
	if !ok {
		return nil, ErrCodeSecurityScanConfigNotFound
	}

	if err := validateCodeSecurityRuleSetCategories(ruleSetCategories); err != nil {
		return nil, err
	}

	if periodicConfig != nil {
		if err := validateCodeSecurityPeriodicScanConfig(periodicConfig); err != nil {
			return nil, err
		}
	}

	if continuousIntegrationScanConfig != nil {
		if err := validateCodeSecurityContinuousIntegrationScanConfig(continuousIntegrationScanConfig); err != nil {
			return nil, err
		}
	}

	cfg.RuleSetCategories = ruleSetCategories
	cfg.ContinuousIntegrationScanConfig = continuousIntegrationScanConfig
	cfg.PeriodicScanConfig = periodicConfig
	cfg.UpdatedAt = time.Now().UTC()
	cp := *cfg

	return &cp, nil
}

// ListCodeSecurityScanConfigurations returns all code security scan configurations.
func (b *InMemoryBackend) ListCodeSecurityScanConfigurations() ([]*CodeSecurityScanConfiguration, error) {
	b.mu.RLock("ListCodeSecurityScanConfigurations")
	defer b.mu.RUnlock()

	result := make([]*CodeSecurityScanConfiguration, 0, b.codeSecurityScanConfigs.Len())

	for _, cfg := range b.codeSecurityScanConfigs.Snapshot() {
		cp := *cfg
		result = append(result, &cp)
	}

	return result, nil
}

// BatchAssociateCodeSecurityScanConfiguration associates scan configs with resources.
func (b *InMemoryBackend) BatchAssociateCodeSecurityScanConfiguration(
	scanConfigARN string,
	resources []string,
) ([]map[string]any, error) {
	b.mu.Lock("BatchAssociateCodeSecurityScanConfiguration")
	defer b.mu.Unlock()

	if !b.codeSecurityScanConfigs.Has(scanConfigARN) {
		return nil, ErrCodeSecurityScanConfigNotFound
	}

	for _, resource := range resources {
		b.scanConfigAssociations.Put(&CodeSecurityScanConfigurationAssociation{
			ScanConfigurationArn: scanConfigARN,
			Resource:             resource,
			Status:               "ASSOCIATED",
		})
	}

	return []map[string]any{}, nil
}

// BatchDisassociateCodeSecurityScanConfiguration removes scan config associations.
func (b *InMemoryBackend) BatchDisassociateCodeSecurityScanConfiguration(
	scanConfigARN string,
	resources []string,
) ([]map[string]any, error) {
	b.mu.Lock("BatchDisassociateCodeSecurityScanConfiguration")
	defer b.mu.Unlock()

	if !b.codeSecurityScanConfigs.Has(scanConfigARN) {
		return nil, ErrCodeSecurityScanConfigNotFound
	}

	for _, resource := range resources {
		b.scanConfigAssociations.Delete(scanConfigARN + "/" + resource)
	}

	return []map[string]any{}, nil
}

// ListCodeSecurityScanConfigurationAssociations returns associations for a scan config.
func (b *InMemoryBackend) ListCodeSecurityScanConfigurationAssociations(
	scanConfigARN string,
) ([]*CodeSecurityScanConfigurationAssociation, error) {
	b.mu.RLock("ListCodeSecurityScanConfigurationAssociations")
	defer b.mu.RUnlock()

	assocs := b.scanConfigAssociationsByConfig.Get(scanConfigARN)
	result := make([]*CodeSecurityScanConfigurationAssociation, 0, len(assocs))

	for _, assoc := range assocs {
		cp := *assoc
		result = append(result, &cp)
	}

	return result, nil
}

// StartCodeSecurityScan starts a code security scan.
func (b *InMemoryBackend) StartCodeSecurityScan(resourceID string) (map[string]any, error) {
	b.mu.Lock("StartCodeSecurityScan")
	defer b.mu.Unlock()

	scanID := uuid.New().String()
	scan := map[string]any{
		"scanId":      scanID,
		keyResourceID: resourceID,
		keyStatus:     codeScanStatusInProgress,
	}
	b.codeSecurityScans[scanID] = scan

	// Real StartCodeSecurityScanOutput carries both scanId and status
	// (awsRestjson1_deserializeOpDocumentStartCodeSecurityScanOutput in the
	// pinned inspector2 SDK's deserializers.go) -- omitting status left a
	// real client's Status field always empty.
	return map[string]any{"scanId": scanID, keyStatus: codeScanStatusInProgress}, nil
}

// GetCodeSecurityScan returns status of a code security scan, advancing
// IN_PROGRESS->SUCCESSFUL on first poll. StartCodeSecurityScan stamped
// keyStatus IN_PROGRESS and nothing else in this backend ever wrote to it
// again -- a client polling GetCodeSecurityScan for readiness never saw a
// terminal status. Inspector2 ships no generated waiter for this op, but
// that only means a caller must hand-roll its own poll loop; it does not
// make an unadvancing status correct. Mirrors the reap-on-read pattern
// services/omics uses for its own Get*-advances-Creating resources.
func (b *InMemoryBackend) GetCodeSecurityScan(scanID string) (map[string]any, error) {
	b.mu.Lock("GetCodeSecurityScan")
	defer b.mu.Unlock()

	scan, ok := b.codeSecurityScans[scanID]
	if !ok {
		return nil, fmt.Errorf("%w: scanId %q not found", ErrReportNotFound, scanID)
	}

	if scan[keyStatus] == codeScanStatusInProgress {
		scan[keyStatus] = codeScanStatusSuccessful
	}

	return scan, nil
}
