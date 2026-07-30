package inspector2

import (
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

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

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
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

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
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
		"scanId":     scanID,
		"resourceId": resourceID,
		keyStatus:    "IN_PROGRESS",
	}
	b.codeSecurityScans[scanID] = scan

	return map[string]any{"scanId": scanID}, nil
}

// GetCodeSecurityScan returns status of a code security scan.
func (b *InMemoryBackend) GetCodeSecurityScan(scanID string) (map[string]any, error) {
	b.mu.RLock("GetCodeSecurityScan")
	defer b.mu.RUnlock()

	scan, ok := b.codeSecurityScans[scanID]
	if !ok {
		return nil, fmt.Errorf("%w: scanId %q not found", ErrReportNotFound, scanID)
	}

	return scan, nil
}
