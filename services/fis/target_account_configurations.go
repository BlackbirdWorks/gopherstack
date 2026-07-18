package fis

import (
	"fmt"
	"slices"
	"strings"
)

// ----------------------------------------
// Target Account Configuration operations
// ----------------------------------------

// CreateTargetAccountConfiguration creates or replaces a target account configuration for the given template.
func (b *InMemoryBackend) CreateTargetAccountConfiguration(
	templateID, accountID, roleArn, description string,
) (*TargetAccountConfiguration, error) {
	if strings.TrimSpace(roleArn) == "" {
		return nil, fmt.Errorf("%w: roleArn is required", ErrValidation)
	}

	b.mu.Lock("CreateTargetAccountConfiguration")
	defer b.mu.Unlock()

	if !b.templates.Has(templateID) {
		return nil, fmt.Errorf("%w: %s", ErrTemplateNotFound, templateID)
	}

	cfg := &TargetAccountConfiguration{
		ExperimentTemplateID: templateID,
		AccountID:            accountID,
		RoleArn:              roleArn,
		Description:          description,
	}

	b.targetAccountConfigs.Put(cfg)

	cp := *cfg

	return &cp, nil
}

// DeleteTargetAccountConfiguration deletes a target account configuration.
func (b *InMemoryBackend) DeleteTargetAccountConfiguration(
	templateID, accountID string,
) (*TargetAccountConfiguration, error) {
	b.mu.Lock("DeleteTargetAccountConfiguration")
	defer b.mu.Unlock()

	key := targetAccountConfigKey(templateID, accountID)

	cfg, ok := b.targetAccountConfigs.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: template=%s account=%s", ErrTargetAccountConfigNotFound, templateID, accountID)
	}

	cp := *cfg

	b.targetAccountConfigs.Delete(key)

	return &cp, nil
}

// GetTargetAccountConfiguration returns a single target account configuration by template ID and account ID.
func (b *InMemoryBackend) GetTargetAccountConfiguration(
	templateID, accountID string,
) (*TargetAccountConfiguration, error) {
	b.mu.RLock("GetTargetAccountConfiguration")
	defer b.mu.RUnlock()

	cfg, ok := b.targetAccountConfigs.Get(targetAccountConfigKey(templateID, accountID))
	if !ok {
		return nil, fmt.Errorf("%w: template=%s account=%s", ErrTargetAccountConfigNotFound, templateID, accountID)
	}

	cp := *cfg

	return &cp, nil
}

// UpdateTargetAccountConfiguration updates an existing target account configuration.
// Only non-nil pointer fields are applied.
func (b *InMemoryBackend) UpdateTargetAccountConfiguration(
	templateID, accountID string,
	roleArn, description *string,
) (*TargetAccountConfiguration, error) {
	b.mu.Lock("UpdateTargetAccountConfiguration")
	defer b.mu.Unlock()

	cfg, ok := b.targetAccountConfigs.Get(targetAccountConfigKey(templateID, accountID))
	if !ok {
		return nil, fmt.Errorf("%w: template=%s account=%s", ErrTargetAccountConfigNotFound, templateID, accountID)
	}

	if roleArn != nil {
		cfg.RoleArn = *roleArn
	}

	if description != nil {
		cfg.Description = *description
	}

	cp := *cfg

	return &cp, nil
}

// ListTargetAccountConfigurations returns all target account configurations for a template, sorted by account ID.
func (b *InMemoryBackend) ListTargetAccountConfigurations(templateID string) ([]*TargetAccountConfiguration, error) {
	b.mu.RLock("ListTargetAccountConfigurations")
	defer b.mu.RUnlock()

	if !b.templates.Has(templateID) {
		return nil, fmt.Errorf("%w: %s", ErrTemplateNotFound, templateID)
	}

	cfgs := b.targetAccountConfigsByTemplate.Get(templateID)
	result := make([]*TargetAccountConfiguration, 0, len(cfgs))

	for _, cfg := range cfgs {
		cp := *cfg
		result = append(result, &cp)
	}

	slices.SortFunc(
		result,
		func(a, b *TargetAccountConfiguration) int { return strings.Compare(a.AccountID, b.AccountID) },
	)

	return result, nil
}

// GetExperimentTargetAccountConfiguration returns the target account configuration for a running experiment.
// It resolves the configuration from the experiment's source template.
func (b *InMemoryBackend) GetExperimentTargetAccountConfiguration(
	experimentID, accountID string,
) (*ExperimentTargetAccountConfiguration, error) {
	b.mu.RLock("GetExperimentTargetAccountConfiguration")
	defer b.mu.RUnlock()

	exp, ok := b.experiments.Get(experimentID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrExperimentNotFound, experimentID)
	}

	cfg, ok := b.targetAccountConfigs.Get(targetAccountConfigKey(exp.ExperimentTemplateID, accountID))
	if !ok {
		return nil, fmt.Errorf("%w: experiment=%s account=%s", ErrTargetAccountConfigNotFound, experimentID, accountID)
	}

	return &ExperimentTargetAccountConfiguration{
		ExperimentID: experimentID,
		AccountID:    cfg.AccountID,
		Description:  cfg.Description,
		RoleArn:      cfg.RoleArn,
	}, nil
}

// ListExperimentTargetAccountConfigurations lists all target account configurations for a running experiment,
// sorted by account ID. It resolves configurations from the experiment's source template.
func (b *InMemoryBackend) ListExperimentTargetAccountConfigurations(
	experimentID string,
) ([]*ExperimentTargetAccountConfiguration, error) {
	b.mu.RLock("ListExperimentTargetAccountConfigurations")
	defer b.mu.RUnlock()

	exp, ok := b.experiments.Get(experimentID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrExperimentNotFound, experimentID)
	}

	cfgs := b.targetAccountConfigsByTemplate.Get(exp.ExperimentTemplateID)
	result := make([]*ExperimentTargetAccountConfiguration, 0, len(cfgs))

	for _, cfg := range cfgs {
		result = append(result, &ExperimentTargetAccountConfiguration{
			ExperimentID: experimentID,
			AccountID:    cfg.AccountID,
			Description:  cfg.Description,
			RoleArn:      cfg.RoleArn,
		})
	}

	slices.SortFunc(result, func(a, b *ExperimentTargetAccountConfiguration) int {
		return strings.Compare(a.AccountID, b.AccountID)
	})

	return result, nil
}
