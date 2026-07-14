package backup

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// CreateBackupPlan creates a new backup plan.
func (b *InMemoryBackend) CreateBackupPlan(
	planName string,
	rules []Rule,
	advancedSettings []AdvancedBackupSetting,
	kv map[string]string,
) (*Plan, error) {
	b.mu.Lock("CreateBackupPlan")
	defer b.mu.Unlock()

	if b.plans.Has(planName) {
		return nil, fmt.Errorf("%w: plan %s already exists", ErrAlreadyExists, planName)
	}

	id := uuid.NewString()
	planARN := arn.Build("backup", b.region, b.accountID, "backup-plan:"+id)
	t := tags.New("backup.plan." + planName + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}
	p := &Plan{
		BackupPlanName:         planName,
		BackupPlanArn:          planARN,
		BackupPlanID:           id,
		VersionID:              uuid.NewString(),
		Rules:                  rules,
		AdvancedBackupSettings: advancedSettings,
		AccountID:              b.accountID,
		Region:                 b.region,
		CreationTime:           time.Now().UTC(),
		Tags:                   t,
	}
	b.plans.Put(p)
	b.planARNIndex[planARN] = planName
	b.planIDIndex[id] = planName
	cp := *p
	cp.Rules = make([]Rule, len(p.Rules))
	copy(cp.Rules, p.Rules)

	return &cp, nil
}

// GetBackupPlan returns a backup plan by ID or name.
func (b *InMemoryBackend) GetBackupPlan(idOrName string) (*Plan, error) {
	b.mu.RLock("GetBackupPlan")
	defer b.mu.RUnlock()

	// Try by name first.
	if p, ok := b.plans.Get(idOrName); ok {
		cp := *p
		cp.Rules = make([]Rule, len(p.Rules))
		copy(cp.Rules, p.Rules)

		return &cp, nil
	}
	// Try by ID using the O(1) index.
	if name, ok := b.planIDIndex[idOrName]; ok {
		p, _ := b.plans.Get(name)
		cp := *p
		cp.Rules = make([]Rule, len(p.Rules))
		copy(cp.Rules, p.Rules)

		return &cp, nil
	}

	return nil, fmt.Errorf("%w: backup plan %s not found", ErrNotFound, idOrName)
}

// ListBackupPlans returns all backup plans sorted by name.
func (b *InMemoryBackend) ListBackupPlans() []*Plan {
	b.mu.RLock("ListBackupPlans")
	defer b.mu.RUnlock()

	all := b.plans.All()
	list := make([]*Plan, 0, len(all))
	for _, p := range all {
		cp := *p
		cp.Rules = make([]Rule, len(p.Rules))
		copy(cp.Rules, p.Rules)
		list = append(list, &cp)
	}

	slices.SortFunc(list, func(a, b *Plan) int {
		if a.BackupPlanName < b.BackupPlanName {
			return -1
		}
		if a.BackupPlanName > b.BackupPlanName {
			return 1
		}

		return 0
	})

	return list
}

// UpdateBackupPlan updates an existing backup plan.
func (b *InMemoryBackend) UpdateBackupPlan(
	idOrName string,
	rules []Rule,
	advancedSettings []AdvancedBackupSetting,
) (*Plan, error) {
	b.mu.Lock("UpdateBackupPlan")
	defer b.mu.Unlock()

	// Find by name first, then by ID using the O(1) index.
	var found *Plan
	if p, ok := b.plans.Get(idOrName); ok {
		found = p
	} else if idName, ok2 := b.planIDIndex[idOrName]; ok2 {
		found, _ = b.plans.Get(idName)
	}

	if found == nil {
		return nil, fmt.Errorf("%w: backup plan %s not found", ErrNotFound, idOrName)
	}

	found.Rules = rules
	if advancedSettings != nil {
		found.AdvancedBackupSettings = advancedSettings
	}
	found.VersionID = uuid.NewString()
	now := time.Now().UTC()
	found.UpdateTime = &now
	cp := *found
	cp.Rules = make([]Rule, len(found.Rules))
	copy(cp.Rules, found.Rules)

	return &cp, nil
}

// DeleteBackupPlan deletes a backup plan by ID or name.
func (b *InMemoryBackend) DeleteBackupPlan(idOrName string) error {
	b.mu.Lock("DeleteBackupPlan")
	defer b.mu.Unlock()

	// Resolve by name or by ID using the O(1) index.
	var planName string
	if b.plans.Has(idOrName) {
		planName = idOrName
	} else if idName, ok2 := b.planIDIndex[idOrName]; ok2 {
		planName = idName
	} else {
		return fmt.Errorf("%w: backup plan %s not found", ErrNotFound, idOrName)
	}

	p, _ := b.plans.Get(planName)
	delete(b.planARNIndex, p.BackupPlanArn)
	delete(b.planIDIndex, p.BackupPlanID)
	b.plans.Delete(planName)
	p.Tags.Close()

	return nil
}

// ListBackupPlanVersions returns all versions of a backup plan.
func (b *InMemoryBackend) ListBackupPlanVersions(planID string) ([]*Plan, error) {
	b.mu.RLock("ListBackupPlanVersions")
	defer b.mu.RUnlock()

	// Find plan by ID.
	name, ok := b.planIDIndex[planID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errBackupPlanNotFoundB1, planID)
	}
	plan, ok := b.plans.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: %s", errBackupPlanNotFoundB1, name)
	}

	cp := *plan
	// Return single version (versioning not tracked in depth).
	return []*Plan{&cp}, nil
}

// ExportBackupPlanTemplate exports a plan as a template JSON string.
func (b *InMemoryBackend) ExportBackupPlanTemplate(planID string) (string, error) {
	b.mu.RLock("ExportBackupPlanTemplate")
	defer b.mu.RUnlock()

	name, ok := b.planIDIndex[planID]
	if !ok {
		return "", fmt.Errorf("%w: %s", errBackupPlanNotFoundB1, planID)
	}
	plan, ok := b.plans.Get(name)
	if !ok {
		return "", fmt.Errorf("%w: %s", errBackupPlanNotFoundB1, name)
	}

	doc := backupPlanBodyDoc{
		BackupPlanName: plan.BackupPlanName,
		Rules:          rulesToJSON(plan.Rules),
	}
	if len(plan.AdvancedBackupSettings) > 0 {
		doc.AdvancedBackupSettings = advancedSettingsToJSON(plan.AdvancedBackupSettings)
	}

	out, err := json.Marshal(doc)
	if err != nil {
		return "", err
	}

	return string(out), nil
}

// ---- Tiering configurations ----

// validateRules checks that each rule has required fields and that rule names are unique.
func validateRules(rules []Rule) error {
	seen := make(map[string]struct{}, len(rules))
	for i, r := range rules {
		if r.RuleName == "" {
			return fmt.Errorf("%w: rule[%d]: RuleName is required", ErrValidation, i)
		}
		if r.TargetVaultName == "" {
			return fmt.Errorf(
				"%w: rule %q: TargetBackupVaultName is required",
				ErrValidation,
				r.RuleName,
			)
		}
		if _, dup := seen[r.RuleName]; dup {
			return fmt.Errorf("%w: duplicate rule name %q", ErrValidation, r.RuleName)
		}
		seen[r.RuleName] = struct{}{}
		if r.Lifecycle != nil {
			if r.Lifecycle.DeleteAfterDays > 0 && r.Lifecycle.MoveToColdStorageAfterDays > 0 &&
				r.Lifecycle.DeleteAfterDays <= r.Lifecycle.MoveToColdStorageAfterDays {
				return fmt.Errorf(
					"%w: rule %q: DeleteAfterDays must be greater than MoveToColdStorageAfterDays",
					ErrValidation,
					r.RuleName,
				)
			}
		}
	}

	return nil
}

// ---- ListBackupJobs filtering + pagination ----

// ListPlansFilter contains pagination parameters for listing backup plans.
type ListPlansFilter struct {
	NextToken  string
	MaxResults int
}

// ListBackupPlansPaged returns backup plans with pagination.
func (b *InMemoryBackend) ListBackupPlansPaged(f ListPlansFilter) ([]*Plan, string) {
	b.mu.RLock("ListBackupPlansPaged")
	defer b.mu.RUnlock()

	all := b.plans.All()
	list := make([]*Plan, 0, len(all))
	for _, p := range all {
		cp := *p
		cp.Rules = make([]Rule, len(p.Rules))
		copy(cp.Rules, p.Rules)
		list = append(list, &cp)
	}

	slices.SortFunc(list, func(a, b *Plan) int {
		return strings.Compare(a.BackupPlanName, b.BackupPlanName)
	})

	return paginateByID(
		list,
		func(p *Plan) string { return p.BackupPlanName },
		f.MaxResults,
		f.NextToken,
	)
}

// ---- DeleteBackupPlan with selection validation ----

// DeleteBackupPlanChecked deletes a backup plan, returning an error if selections exist.
func (b *InMemoryBackend) DeleteBackupPlanChecked(idOrName string) (*Plan, error) {
	b.mu.Lock("DeleteBackupPlanChecked")
	defer b.mu.Unlock()

	var planName string
	if b.plans.Has(idOrName) {
		planName = idOrName
	} else if name, ok2 := b.planIDIndex[idOrName]; ok2 {
		planName = name
	} else {
		return nil, fmt.Errorf("%w: backup plan %s not found", ErrNotFound, idOrName)
	}

	p, _ := b.plans.Get(planName)

	// AWS requires all selections to be deleted before the plan can be deleted.
	if sels := b.selectionsByPlan.Get(p.BackupPlanID); len(sels) > 0 {
		return nil, fmt.Errorf(
			"%w: backup plan %s has %d active selection(s); delete them first",
			ErrValidation,
			planName,
			len(sels),
		)
	}

	delete(b.planARNIndex, p.BackupPlanArn)
	delete(b.planIDIndex, p.BackupPlanID)
	b.plans.Delete(planName)

	// Cascade-delete any remaining selections for this plan. Clone the
	// index's result first: deleting from the table while ranging over the
	// live index slice would mutate it out from under the loop. The check
	// above already guarantees this is empty in practice; this mirrors the
	// original delete(b.selections, planID) wipe defensively.
	for _, sel := range slices.Clone(b.selectionsByPlan.Get(p.BackupPlanID)) {
		b.selections.Delete(selectionKey(sel.BackupPlanID, sel.SelectionID))
	}

	cp := *p
	p.Tags.Close()

	return &cp, nil
}

// ---- DeleteBackupVault with lock enforcement ----

// CreateBackupPlanValidated creates a backup plan after validating its rules.
func (b *InMemoryBackend) CreateBackupPlanValidated(
	planName string,
	rules []Rule,
	advancedSettings []AdvancedBackupSetting,
	kv map[string]string,
) (*Plan, error) {
	if planName == "" {
		return nil, fmt.Errorf("%w: BackupPlanName is required", ErrValidation)
	}
	if err := validateRules(rules); err != nil {
		return nil, err
	}

	return b.CreateBackupPlan(planName, rules, advancedSettings, kv)
}

// UpdateBackupPlanValidated updates a backup plan after validating rules.
func (b *InMemoryBackend) UpdateBackupPlanValidated(
	idOrName string,
	rules []Rule,
	advancedSettings []AdvancedBackupSetting,
) (*Plan, error) {
	if err := validateRules(rules); err != nil {
		return nil, err
	}

	return b.UpdateBackupPlan(idOrName, rules, advancedSettings)
}

// ---- Generic pagination helper ----
