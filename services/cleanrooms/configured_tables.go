package cleanrooms

import (
	"maps"
	"slices"
	"sort"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) configuredTableARN(id string) string {
	return arn.Build("cleanrooms", b.region, b.accountID, "configuredtable/"+id)
}

func (b *InMemoryBackend) CreateConfiguredTable(
	name, description string,
	tableReference map[string]any,
	allowedColumns []string,
	analysisMethod string,
	tags map[string]string,
	settings ...ConfiguredTableSettings,
) (*ConfiguredTable, error) {
	b.mu.Lock("CreateConfiguredTable")
	defer b.mu.Unlock()
	if name == "" {
		return nil, ErrValidation
	}
	var selected []string
	if len(settings) > 0 {
		selected = settings[0].SelectedAnalysisMethods
	}
	if err := validateSelectedMethods(selected); err != nil {
		return nil, err
	}
	id := uuid.NewString()
	ts := b.now()
	if allowedColumns == nil {
		// AllowedColumns/AnalysisRuleTypes are required on the wire; a nil Go
		// slice marshals as JSON null, indistinguishable from an absent key to
		// a real client's deserializer -- must be non-nil so it marshals as []
		// (gopherstack-r80d).
		allowedColumns = []string{}
	}
	ct := &ConfiguredTable{
		ConfiguredTableIdentifier: id,
		Arn:                       b.configuredTableARN(id),
		Name:                      name,
		Description:               description,
		TableReference:            tableReference,
		AllowedColumns:            allowedColumns,
		AnalysisRuleTypes:         []string{},
		AnalysisMethod:            analysisMethod,
		SelectedAnalysisMethods:   slices.Clone(selected),
		CreateTime:                ts,
		UpdateTime:                ts,
		Tags:                      tags,
		ID:                        id,
	}
	b.configuredTables.Put(ct)
	if len(tags) > 0 {
		b.tagsByArn[ct.Arn] = maps.Clone(tags)
	}

	return cloneConfiguredTable(ct), nil
}

func (b *InMemoryBackend) GetConfiguredTable(id string) (*ConfiguredTable, error) {
	b.mu.RLock("GetConfiguredTable")
	defer b.mu.RUnlock()
	ct, ok := b.configuredTables.Get(id)
	if !ok {
		return nil, ErrNotFound
	}

	return cloneConfiguredTable(ct), nil
}

func (b *InMemoryBackend) ListConfiguredTables(
	maxResults, nextToken string,
) ([]*ConfiguredTableSummary, string) {
	b.mu.RLock("ListConfiguredTables")
	defer b.mu.RUnlock()
	all := b.configuredTables.All()
	items := make([]*ConfiguredTableSummary, 0, len(all))
	for _, ct := range all {
		items = append(items, &ConfiguredTableSummary{
			ConfiguredTableIdentifier: ct.ConfiguredTableIdentifier,
			Arn:                       ct.Arn,
			Name:                      ct.Name,
			AnalysisMethod:            ct.AnalysisMethod,
			AnalysisRuleTypes:         slices.Clone(ct.AnalysisRuleTypes),
			SelectedAnalysisMethods:   slices.Clone(ct.SelectedAnalysisMethods),
			CreateTime:                ct.CreateTime,
			UpdateTime:                ct.UpdateTime,
			ID:                        ct.ID,
		})
	}
	sort.Slice(
		items,
		func(i, j int) bool { return items[i].ID < items[j].ID },
	)
	page, next := paginate(items, maxResults, nextToken)

	return page, next
}

func (b *InMemoryBackend) UpdateConfiguredTable(
	id, name, description string,
	settings ...ConfiguredTableSettings,
) (*ConfiguredTable, error) {
	b.mu.Lock("UpdateConfiguredTable")
	defer b.mu.Unlock()
	var selected []string
	if len(settings) > 0 {
		selected = settings[0].SelectedAnalysisMethods
	}
	if err := validateSelectedMethods(selected); err != nil {
		return nil, err
	}
	ct, ok := b.configuredTables.Get(id)
	if !ok {
		return nil, ErrNotFound
	}
	if len(selected) > 0 {
		ct.SelectedAnalysisMethods = slices.Clone(selected)
	}
	if name != "" {
		ct.Name = name
	}
	if description != "" {
		ct.Description = description
	}
	ct.UpdateTime = b.now()

	return cloneConfiguredTable(ct), nil
}

func (b *InMemoryBackend) DeleteConfiguredTable(id string) error {
	b.mu.Lock("DeleteConfiguredTable")
	defer b.mu.Unlock()
	ct, ok := b.configuredTables.Get(id)
	if !ok {
		return ErrNotFound
	}
	delete(b.tagsByArn, ct.Arn)
	b.configuredTables.Delete(id)

	for _, rule := range slices.Clone(b.ctAnalysisRulesByTable.Get(id)) {
		b.ctAnalysisRules.Delete(ctAnalysisRuleKey(rule.ConfiguredTableID, rule.Type))
	}

	return nil
}

func (b *InMemoryBackend) CreateConfiguredTableAnalysisRule(
	configuredTableID, analysisRuleType string,
	policy map[string]any,
) (*ConfiguredTableAnalysisRule, error) {
	b.mu.Lock("CreateConfiguredTableAnalysisRule")
	defer b.mu.Unlock()
	ct, ok := b.configuredTables.Get(configuredTableID)
	if !ok {
		return nil, ErrNotFound
	}
	if b.ctAnalysisRules.Has(ctAnalysisRuleKey(configuredTableID, analysisRuleType)) {
		return nil, ErrAlreadyExists
	}
	ts := b.now()
	rule := &ConfiguredTableAnalysisRule{
		ConfiguredTableIdentifier: configuredTableID,
		ConfiguredTableArn:        ct.Arn,
		Type:                      analysisRuleType,
		Policy:                    policy,
		CreateTime:                ts,
		UpdateTime:                ts,
		ConfiguredTableID:         configuredTableID,
	}
	b.ctAnalysisRules.Put(rule)
	if !contains(ct.AnalysisRuleTypes, analysisRuleType) {
		ct.AnalysisRuleTypes = append(ct.AnalysisRuleTypes, analysisRuleType)
	}
	b.syncTableSchemasLocked(configuredTableID)

	return rule, nil
}

func (b *InMemoryBackend) GetConfiguredTableAnalysisRule(
	configuredTableID, analysisRuleType string,
) (*ConfiguredTableAnalysisRule, error) {
	b.mu.RLock("GetConfiguredTableAnalysisRule")
	defer b.mu.RUnlock()
	rule, ok := b.ctAnalysisRules.Get(ctAnalysisRuleKey(configuredTableID, analysisRuleType))
	if !ok {
		return nil, ErrNotFound
	}

	return rule, nil
}

func (b *InMemoryBackend) UpdateConfiguredTableAnalysisRule(
	configuredTableID, analysisRuleType string,
	policy map[string]any,
) (*ConfiguredTableAnalysisRule, error) {
	b.mu.Lock("UpdateConfiguredTableAnalysisRule")
	defer b.mu.Unlock()
	rule, ok := b.ctAnalysisRules.Get(ctAnalysisRuleKey(configuredTableID, analysisRuleType))
	if !ok {
		return nil, ErrNotFound
	}
	rule.Policy = policy
	rule.UpdateTime = b.now()
	b.syncTableSchemasLocked(configuredTableID)

	return rule, nil
}

func (b *InMemoryBackend) DeleteConfiguredTableAnalysisRule(
	configuredTableID, analysisRuleType string,
) error {
	b.mu.Lock("DeleteConfiguredTableAnalysisRule")
	defer b.mu.Unlock()
	if !b.ctAnalysisRules.Delete(ctAnalysisRuleKey(configuredTableID, analysisRuleType)) {
		return ErrNotFound
	}
	if ct, ctOK := b.configuredTables.Get(configuredTableID); ctOK {
		ct.AnalysisRuleTypes = removeFrom(ct.AnalysisRuleTypes, analysisRuleType)
	}
	b.syncTableSchemasLocked(configuredTableID)

	return nil
}
