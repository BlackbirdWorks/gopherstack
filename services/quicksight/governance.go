package quicksight

import (
	"maps"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"
)

// ---- Approval Policies ----
//
// ApprovalPolicy is not scoped by AwsAccountId on the wire (see
// approvalPolicyKey's doc comment in store.go), so every method below takes
// no accountID parameter -- PolicyId alone identifies the resource, matching
// the real API surface (quicksight@v1.129.0 api_op_CreateApprovalPolicy.go
// etc. carry no AwsAccountId member).

type storedApprovalPolicy struct {
	CreatedAt      time.Time    `json:"createdAt"`
	UpdatedAt      time.Time    `json:"updatedAt"`
	PolicyID       string       `json:"policyId"`
	Arn            string       `json:"arn"`
	Name           string       `json:"name"`
	Description    string       `json:"description"`
	Actions        []string     `json:"actions"`
	AssetTypes     []string     `json:"assetTypes"`
	ApprovalGroups []string     `json:"approvalGroups"`
	ApplicableTo   ApplicableTo `json:"applicableTo"`
}

func cloneApplicableTo(a ApplicableTo) ApplicableTo {
	return ApplicableTo{
		Type:      a.Type,
		GroupArns: slices.Clone(a.GroupArns),
	}
}

func (p *storedApprovalPolicy) toApprovalPolicy() *ApprovalPolicy {
	return &ApprovalPolicy{
		CreatedAt:      p.CreatedAt,
		UpdatedAt:      p.UpdatedAt,
		PolicyID:       p.PolicyID,
		Arn:            p.Arn,
		Name:           p.Name,
		Description:    p.Description,
		Actions:        slices.Clone(p.Actions),
		AssetTypes:     slices.Clone(p.AssetTypes),
		ApprovalGroups: slices.Clone(p.ApprovalGroups),
		ApplicableTo:   cloneApplicableTo(p.ApplicableTo),
	}
}

func (b *InMemoryBackend) CreateApprovalPolicy(
	policyID, name, description string,
	actions, assetTypes, approvalGroups []string,
	applicableTo ApplicableTo,
) (*ApprovalPolicy, error) {
	if policyID == "" || name == "" || len(actions) == 0 || len(assetTypes) == 0 ||
		len(approvalGroups) == 0 || applicableTo.Type == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateApprovalPolicy")
	defer b.mu.Unlock()

	key := approvalPolicyKey(policyID)
	if b.approvalPolicies.Has(key) {
		return nil, ErrApprovalPolicyAlreadyExists
	}

	now := time.Now().UTC()
	p := &storedApprovalPolicy{
		CreatedAt:      now,
		UpdatedAt:      now,
		PolicyID:       policyID,
		Arn:            b.buildARN("approval-policy", policyID),
		Name:           name,
		Description:    description,
		Actions:        slices.Clone(actions),
		AssetTypes:     slices.Clone(assetTypes),
		ApprovalGroups: slices.Clone(approvalGroups),
		ApplicableTo:   cloneApplicableTo(applicableTo),
	}
	b.approvalPolicies.Put(p)

	return p.toApprovalPolicy(), nil
}

func (b *InMemoryBackend) DescribeApprovalPolicy(policyID string) (*ApprovalPolicy, error) {
	b.mu.RLock("DescribeApprovalPolicy")
	defer b.mu.RUnlock()

	p, ok := b.approvalPolicies.Get(approvalPolicyKey(policyID))
	if !ok {
		return nil, ErrApprovalPolicyNotFound
	}

	return p.toApprovalPolicy(), nil
}

func (b *InMemoryBackend) UpdateApprovalPolicy(
	policyID, name, description string,
	actions, assetTypes, approvalGroups []string,
	applicableTo *ApplicableTo,
) (*ApprovalPolicy, error) {
	b.mu.Lock("UpdateApprovalPolicy")
	defer b.mu.Unlock()

	p, ok := b.approvalPolicies.Get(approvalPolicyKey(policyID))
	if !ok {
		return nil, ErrApprovalPolicyNotFound
	}

	if name != "" {
		p.Name = name
	}
	if description != "" {
		p.Description = description
	}
	if len(actions) > 0 {
		p.Actions = slices.Clone(actions)
	}
	if len(assetTypes) > 0 {
		p.AssetTypes = slices.Clone(assetTypes)
	}
	if len(approvalGroups) > 0 {
		p.ApprovalGroups = slices.Clone(approvalGroups)
	}
	if applicableTo != nil {
		p.ApplicableTo = cloneApplicableTo(*applicableTo)
	}
	p.UpdatedAt = time.Now().UTC()

	return p.toApprovalPolicy(), nil
}

func (b *InMemoryBackend) DeleteApprovalPolicy(policyID string) error {
	b.mu.Lock("DeleteApprovalPolicy")
	defer b.mu.Unlock()

	if !b.approvalPolicies.Delete(approvalPolicyKey(policyID)) {
		return ErrApprovalPolicyNotFound
	}

	return nil
}

func (b *InMemoryBackend) ListApprovalPolicies(maxResults int32, nextToken string) ([]*ApprovalPolicy, string, error) {
	b.mu.RLock("ListApprovalPolicies")
	defer b.mu.RUnlock()

	all := b.approvalPolicies.All()
	sort.Slice(all, func(i, j int) bool { return all[i].PolicyID < all[j].PolicyID })

	page, next := paginateOffset(all, maxResults, nextToken)

	result := make([]*ApprovalPolicy, 0, len(page))
	for _, p := range page {
		result = append(result, p.toApprovalPolicy())
	}

	return result, next, nil
}

// ---- DLP Settings ----

const (
	dlpSettingStatusActive   = "ACTIVE"
	dlpSettingStatusInactive = "INACTIVE"
)

type storedDlpSetting struct {
	CreatedAt            time.Time      `json:"createdAt"`
	UpdatedAt            time.Time      `json:"updatedAt"`
	ProviderConfig       ProviderConfig `json:"providerConfig"`
	DlpSettingID         string         `json:"dlpSettingId"`
	Arn                  string         `json:"arn"`
	Name                 string         `json:"name"`
	ProviderType         string         `json:"providerType"`
	ProviderOutageAction string         `json:"providerOutageAction"`
	Enabled              bool           `json:"enabled"`
}

// dlpSettingStatus derives DlpSettingDetails/DlpSettingSummary's required
// Status field (ACTIVE/INACTIVE) from Enabled: the SDK documents both as the
// same underlying fact ("whether DLP enforcement is active",
// CreateDlpSettingInput.Enabled's doc comment, vs. DlpSettingDetails.Status's
// "The status of the DLP setting"), so this is an honest derivation, not a
// fabricated field.
func dlpSettingStatus(enabled bool) string {
	if enabled {
		return dlpSettingStatusActive
	}

	return dlpSettingStatusInactive
}

func cloneProviderConfig(pc ProviderConfig) ProviderConfig {
	if pc.MicrosoftPurview == nil {
		return ProviderConfig{}
	}

	return ProviderConfig{
		MicrosoftPurview: &MicrosoftPurviewProviderConfig{
			SecretArn:           pc.MicrosoftPurview.SecretArn,
			UnmappedAction:      pc.MicrosoftPurview.UnmappedAction,
			LabelActionMappings: slices.Clone(pc.MicrosoftPurview.LabelActionMappings),
		},
	}
}

func (d *storedDlpSetting) toDlpSetting() *DlpSetting {
	return &DlpSetting{
		CreatedAt:            d.CreatedAt,
		UpdatedAt:            d.UpdatedAt,
		DlpSettingID:         d.DlpSettingID,
		Arn:                  d.Arn,
		Name:                 d.Name,
		ProviderType:         d.ProviderType,
		ProviderOutageAction: d.ProviderOutageAction,
		ProviderConfig:       cloneProviderConfig(d.ProviderConfig),
		Enabled:              d.Enabled,
	}
}

func (b *InMemoryBackend) CreateDlpSetting(
	accountID, dlpSettingID, name string,
	enabled bool,
	providerType, providerOutageAction string,
	providerConfig ProviderConfig,
	tags map[string]string,
) (*DlpSetting, error) {
	if accountID == "" || dlpSettingID == "" || name == "" || providerType == "" || providerOutageAction == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateDlpSetting")
	defer b.mu.Unlock()

	key := dlpSettingKey(accountID, dlpSettingID)
	if b.dlpSettings.Has(key) {
		return nil, ErrDlpSettingAlreadyExists
	}

	now := time.Now().UTC()
	d := &storedDlpSetting{
		CreatedAt:            now,
		UpdatedAt:            now,
		DlpSettingID:         dlpSettingID,
		Arn:                  b.buildARN("dlp-setting", dlpSettingID),
		Name:                 name,
		ProviderType:         providerType,
		ProviderOutageAction: providerOutageAction,
		ProviderConfig:       cloneProviderConfig(providerConfig),
		Enabled:              enabled,
	}
	b.dlpSettings.Put(d)

	if len(tags) > 0 {
		b.tags[d.Arn] = maps.Clone(tags)
	}

	return d.toDlpSetting(), nil
}

func (b *InMemoryBackend) DescribeDlpSetting(accountID, dlpSettingID string) (*DlpSetting, error) {
	b.mu.RLock("DescribeDlpSetting")
	defer b.mu.RUnlock()

	d, ok := b.dlpSettings.Get(dlpSettingKey(accountID, dlpSettingID))
	if !ok {
		return nil, ErrDlpSettingNotFound
	}

	return d.toDlpSetting(), nil
}

func (b *InMemoryBackend) UpdateDlpSetting(
	accountID, dlpSettingID, name string,
	enabled *bool,
	providerType, providerOutageAction string,
	providerConfig *ProviderConfig,
) (*DlpSetting, error) {
	b.mu.Lock("UpdateDlpSetting")
	defer b.mu.Unlock()

	d, ok := b.dlpSettings.Get(dlpSettingKey(accountID, dlpSettingID))
	if !ok {
		return nil, ErrDlpSettingNotFound
	}

	if name != "" {
		d.Name = name
	}
	if enabled != nil {
		d.Enabled = *enabled
	}
	if providerType != "" {
		d.ProviderType = providerType
	}
	if providerOutageAction != "" {
		d.ProviderOutageAction = providerOutageAction
	}
	if providerConfig != nil {
		d.ProviderConfig = cloneProviderConfig(*providerConfig)
	}
	d.UpdatedAt = time.Now().UTC()

	return d.toDlpSetting(), nil
}

func (b *InMemoryBackend) DeleteDlpSetting(accountID, dlpSettingID string) (string, error) {
	b.mu.Lock("DeleteDlpSetting")
	defer b.mu.Unlock()

	key := dlpSettingKey(accountID, dlpSettingID)
	d, ok := b.dlpSettings.Get(key)
	if !ok {
		return "", ErrDlpSettingNotFound
	}
	arn := d.Arn

	delete(b.tags, arn)
	b.dlpSettings.Delete(key)

	return arn, nil
}

// ListDlpSettings ignores accountID beyond identity: this backend holds
// exactly one (accountID, region) pair (see store_setup.go's "Composite
// keys" doc comment), so every stored DLP setting already belongs to it --
// same rationale as allFoldersLocked's unused accountID parameter
// (folders.go).
func (b *InMemoryBackend) ListDlpSettings(
	_ string,
	maxResults int32,
	nextToken string,
) ([]*DlpSetting, string, error) {
	b.mu.RLock("ListDlpSettings")
	defer b.mu.RUnlock()

	all := b.dlpSettings.All()
	sort.Slice(all, func(i, j int) bool { return all[i].DlpSettingID < all[j].DlpSettingID })

	page, next := paginateOffset(all, maxResults, nextToken)

	result := make([]*DlpSetting, 0, len(page))
	for _, d := range page {
		result = append(result, d.toDlpSetting())
	}

	return result, next, nil
}

// ---- Limits Profiles ----

type storedLimitsProfile struct {
	CreatedAt      time.Time                    `json:"createdAt"`
	UpdatedAt      time.Time                    `json:"updatedAt"`
	ResourceLimits map[string]ProfileLimitValue `json:"resourceLimits"`
	ProfileID      string                       `json:"profileId"`
	Arn            string                       `json:"arn"`
	ClientToken    string                       `json:"clientToken"`
	ProfileName    string                       `json:"profileName"`
	Description    string                       `json:"description"`
}

func (p *storedLimitsProfile) toLimitsProfile(accountID string) *LimitsProfile {
	return &LimitsProfile{
		CreatedAt:      p.CreatedAt,
		UpdatedAt:      p.UpdatedAt,
		ProfileID:      p.ProfileID,
		Arn:            p.Arn,
		AccountID:      accountID,
		ProfileName:    p.ProfileName,
		Description:    p.Description,
		ResourceLimits: maps.Clone(p.ResourceLimits),
	}
}

// CreateLimitsProfile implements CreateLimitsProfileInput.ClientToken's
// idempotency contract ("if this token matches a previous request, the
// service ignores the request, but does not return an error") by returning
// the existing profile for a repeated token instead of minting a second one.
// ProfileId is server-generated (the input has no ProfileId member at all,
// quicksight@v1.129.0 api_op_CreateLimitsProfile.go), so uuid.New is the same
// generation convention used elsewhere in this package (e.g.
// automation.go's StartAutomationJob).
func (b *InMemoryBackend) CreateLimitsProfile(
	accountID, clientToken, profileName, description string,
	resourceLimits map[string]ProfileLimitValue,
) (*LimitsProfile, error) {
	if accountID == "" || clientToken == "" || profileName == "" || len(resourceLimits) == 0 {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateLimitsProfile")
	defer b.mu.Unlock()

	for _, existing := range b.limitsProfiles.All() {
		if existing.ClientToken == clientToken {
			return existing.toLimitsProfile(accountID), nil
		}
	}

	now := time.Now().UTC()
	profileID := uuid.New().String()
	p := &storedLimitsProfile{
		CreatedAt:      now,
		UpdatedAt:      now,
		ProfileID:      profileID,
		Arn:            b.buildARN("limits-profile", profileID),
		ClientToken:    clientToken,
		ProfileName:    profileName,
		Description:    description,
		ResourceLimits: maps.Clone(resourceLimits),
	}
	b.limitsProfiles.Put(p)

	return p.toLimitsProfile(accountID), nil
}

func (b *InMemoryBackend) DescribeLimitsProfile(accountID, profileID string) (*LimitsProfile, error) {
	b.mu.RLock("DescribeLimitsProfile")
	defer b.mu.RUnlock()

	p, ok := b.limitsProfiles.Get(limitsProfileKey(accountID, profileID))
	if !ok {
		return nil, ErrLimitsProfileNotFound
	}

	return p.toLimitsProfile(accountID), nil
}

func (b *InMemoryBackend) UpdateLimitsProfile(
	accountID, profileID, profileName, description string,
	resourceLimits map[string]ProfileLimitValue,
) (*LimitsProfile, error) {
	b.mu.Lock("UpdateLimitsProfile")
	defer b.mu.Unlock()

	p, ok := b.limitsProfiles.Get(limitsProfileKey(accountID, profileID))
	if !ok {
		return nil, ErrLimitsProfileNotFound
	}

	if profileName != "" {
		p.ProfileName = profileName
	}
	if description != "" {
		p.Description = description
	}
	if len(resourceLimits) > 0 {
		p.ResourceLimits = maps.Clone(resourceLimits)
	}
	p.UpdatedAt = time.Now().UTC()

	return p.toLimitsProfile(accountID), nil
}

func (b *InMemoryBackend) DeleteLimitsProfile(accountID, profileID string) (string, error) {
	b.mu.Lock("DeleteLimitsProfile")
	defer b.mu.Unlock()

	key := limitsProfileKey(accountID, profileID)
	p, ok := b.limitsProfiles.Get(key)
	if !ok {
		return "", ErrLimitsProfileNotFound
	}
	arn := p.Arn
	b.limitsProfiles.Delete(key)

	return arn, nil
}

func (b *InMemoryBackend) ListLimitsProfiles(
	accountID, resourceType string,
	maxResults int32,
	nextToken string,
) ([]*LimitsProfile, string, error) {
	b.mu.RLock("ListLimitsProfiles")
	defer b.mu.RUnlock()

	all := b.limitsProfiles.All()
	sort.Slice(all, func(i, j int) bool { return all[i].ProfileID < all[j].ProfileID })

	filtered := all
	if resourceType != "" {
		filtered = make([]*storedLimitsProfile, 0, len(all))
		for _, p := range all {
			if _, ok := p.ResourceLimits[resourceType]; ok {
				filtered = append(filtered, p)
			}
		}
	}

	page, next := paginateOffset(filtered, maxResults, nextToken)

	result := make([]*LimitsProfile, 0, len(page))
	for _, p := range page {
		result = append(result, p.toLimitsProfile(accountID))
	}

	return result, next, nil
}
