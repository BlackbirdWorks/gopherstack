package guardduty

import (
	"maps"
	"slices"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// EnableOrganizationAdminAccount designates an account as org admin.
func (b *InMemoryBackend) EnableOrganizationAdminAccount(adminAccountID string) error {
	b.mu.Lock("EnableOrganizationAdminAccount")
	defer b.mu.Unlock()

	b.orgAdminAccounts.Put(&OrgAdminAccount{
		AdminAccountID: adminAccountID,
		AdminStatus:    "ENABLED",
	})

	return nil
}

// DisableOrganizationAdminAccount removes an account as org admin.
func (b *InMemoryBackend) DisableOrganizationAdminAccount(adminAccountID string) error {
	b.mu.Lock("DisableOrganizationAdminAccount")
	defer b.mu.Unlock()

	b.orgAdminAccounts.Delete(adminAccountID)

	return nil
}

// ListOrganizationAdminAccounts returns all org admin accounts.
func (b *InMemoryBackend) ListOrganizationAdminAccounts(
	maxResults int32,
	nextToken string,
) ([]*OrgAdminAccount, string) {
	b.mu.RLock("ListOrganizationAdminAccounts")
	defer b.mu.RUnlock()

	items := b.orgAdminAccounts.Snapshot()
	all := make([]*OrgAdminAccount, 0, len(items))

	for _, a := range items {
		cp := *a
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].AdminAccountID < all[j].AdminAccountID })

	offset, err := decodeToken(nextToken)
	if err != nil {
		return nil, ""
	}

	size := resolvePageSize(int(maxResults))
	page, next := paginate(all, offset, size)

	return page, next
}

// DescribeOrganizationConfiguration returns org config for a detector.
func (b *InMemoryBackend) DescribeOrganizationConfiguration(detectorID string) (*OrgConfig, error) {
	b.mu.RLock("DescribeOrganizationConfiguration")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	cfg, ok := b.orgConfigs.Get(detectorID)
	if !ok {
		return &OrgConfig{DataSources: map[string]any{}, Features: []OrgFeature{}}, nil
	}

	cp := *cfg

	return &cp, nil
}

// UpdateOrganizationConfiguration updates org config for a detector.
func (b *InMemoryBackend) UpdateOrganizationConfiguration(
	detectorID string,
	autoEnable *bool,
	autoEnableOrganizationMembers string,
	features []OrgFeature,
) error {
	b.mu.Lock("UpdateOrganizationConfiguration")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	existing, ok := b.orgConfigs.Get(detectorID)
	if !ok {
		existing = &OrgConfig{DataSources: map[string]any{}}
		existing.detectorID = detectorID
	}

	if err := validateOrgUpdate(autoEnableOrganizationMembers, features); err != nil {
		return err
	}

	if autoEnable != nil {
		existing.AutoEnable = *autoEnable
	}

	if autoEnableOrganizationMembers != "" {
		existing.AutoEnableOrganizationMembers = autoEnableOrganizationMembers
	}

	existing.Features = mergeOrgFeatures(existing.Features, features)

	b.orgConfigs.Put(existing)

	return nil
}

// GetOrganizationStatistics returns org-level statistics.
//
// Real GetOrganizationStatisticsOutput wraps everything under a single
// organizationDetails object (types.OrganizationDetails), which itself
// carries updatedAt (epoch seconds) alongside the nested
// organizationStatistics object -- both were previously missing entirely.
// activeAccountsCount/totalAccountsCount/enabledAccountsCount/
// memberAccountsCount are computed from the members table (the accounts
// actually associated with this account's GuardDuty organization), not
// orgAdminAccounts (a distinct concept: which accounts are *delegated
// administrators*, not which accounts are *members*).
func (b *InMemoryBackend) GetOrganizationStatistics() map[string]any {
	b.mu.RLock("GetOrganizationStatistics")
	defer b.mu.RUnlock()

	var memberCount, enabledCount int

	for _, m := range b.members.All() {
		memberCount++

		if m.RelationshipStatus == "Enabled" { //nolint:goconst // matches members.go's existing RelationshipStatus literals
			enabledCount++
		}
	}

	// +1 for this account itself, which is always "active"/"associated"
	// alongside however many member accounts it has.
	totalAccounts := memberCount + 1
	activeAccounts := enabledCount + 1

	return map[string]any{
		"organizationDetails": map[string]any{
			"updatedAt": awstime.Epoch(time.Now().UTC()),
			"organizationStatistics": map[string]any{
				"activeAccountsCount":   activeAccounts,
				"totalAccountsCount":    totalAccounts,
				"memberAccountsCount":   memberCount,
				keyEnabledAccountsCount: enabledCount,
				"countByFeature":        b.orgFeatureStatisticsLocked(),
			},
		},
	}
}

// featureAccounts tracks the distinct accounts with a feature (and each of its
// additional configurations) ENABLED.
type featureAccounts struct {
	accounts map[string]struct{}
	extras   map[string]map[string]struct{}
}

type orgFeatureTally map[string]*featureAccounts

func (t orgFeatureTally) add(account, name, status string, extras []AdditionalConfig) {
	if status != statusEnabled {
		return
	}

	fa := t[name]
	if fa == nil {
		fa = &featureAccounts{accounts: map[string]struct{}{}, extras: map[string]map[string]struct{}{}}
		t[name] = fa
	}

	fa.accounts[account] = struct{}{}

	for _, e := range extras {
		if e.Status != statusEnabled {
			continue
		}

		if fa.extras[e.Name] == nil {
			fa.extras[e.Name] = map[string]struct{}{}
		}

		fa.extras[e.Name][account] = struct{}{}
	}
}

func (t orgFeatureTally) wire() []map[string]any {
	out := make([]map[string]any, 0, len(t))

	for _, name := range slices.Sorted(maps.Keys(t)) {
		fa := t[name]
		entry := map[string]any{keyName: name, keyEnabledAccountsCount: len(fa.accounts)}

		if len(fa.extras) > 0 {
			extras := make([]map[string]any, 0, len(fa.extras))
			for _, en := range slices.Sorted(maps.Keys(fa.extras)) {
				extras = append(extras, map[string]any{keyName: en, keyEnabledAccountsCount: len(fa.extras[en])})
			}

			entry["additionalConfiguration"] = extras
		}

		out = append(out, entry)
	}

	return out
}

// orgFeatureStatisticsLocked counts, per feature (and additional
// configuration), the distinct accounts with it ENABLED across this account's
// detectors and its members. Caller holds b.mu.
func (b *InMemoryBackend) orgFeatureStatisticsLocked() []map[string]any {
	tally := orgFeatureTally{}

	for _, d := range b.detectors.All() {
		for _, f := range d.Features {
			tally.add(b.accountID, f.Name, f.Status, f.AdditionalConfiguration)
		}
	}

	for _, m := range b.members.All() {
		for _, f := range m.Features {
			extras := make([]AdditionalConfig, 0, len(f.AdditionalConfiguration))
			for _, a := range f.AdditionalConfiguration {
				extras = append(extras, AdditionalConfig{Name: a.Name, Status: a.Status})
			}

			tally.add(m.AccountID, f.Name, f.Status, extras)
		}
	}

	return tally.wire()
}

const keyEnabledAccountsCount = "enabledAccountsCount"
