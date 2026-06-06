package macie2

import (
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrClassificationJobNotFound is returned when a classification job does not exist.
	ErrClassificationJobNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrMemberNotFound is returned when a member does not exist.
	ErrMemberNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrMemberAlreadyExists is returned when a member already exists.
	ErrMemberAlreadyExists = awserr.New(errConflictException, awserr.ErrConflict)
	// ErrInvitationNotFound is returned when an invitation does not exist.
	ErrInvitationNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrOrgAdminNotFound is returned when an org admin account does not exist.
	ErrOrgAdminNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrClassificationScopeNotFound is returned when a classification scope does not exist.
	ErrClassificationScopeNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrSensitivityTemplateNotFound is returned when a sensitivity inspection template does not exist.
	ErrSensitivityTemplateNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
)

// --- classification jobs ---

// CreateClassificationJob creates a new classification job.
func (b *InMemoryBackend) CreateClassificationJob(
	name, description, jobType, clientToken string,
	s3JobDefinition, scheduleFrequency map[string]any,
	tags map[string]string,
	samplingPercentage int32,
	initialRun bool,
) (string, error) {
	b.mu.Lock("CreateClassificationJob")
	defer b.mu.Unlock()

	id := uuid.New().String()
	now := time.Now().UTC()

	status := "RUNNING"
	if jobType == "SCHEDULED" {
		status = "IDLE"
	}

	pct := samplingPercentage
	if pct == 0 {
		pct = 100
	}

	b.classificationJobs[id] = &ClassificationJob{
		JobID:              id,
		Name:               name,
		Description:        description,
		JobType:            jobType,
		JobStatus:          status,
		CreatedAt:          now,
		S3JobDefinition:    s3JobDefinition,
		ScheduleFrequency:  scheduleFrequency,
		Tags:               maps.Clone(tags),
		SamplingPercentage: pct,
		InitialRun:         initialRun,
		ClientToken:        clientToken,
	}

	return id, nil
}

// DescribeClassificationJob returns a classification job by ID.
func (b *InMemoryBackend) DescribeClassificationJob(jobID string) (*ClassificationJob, error) {
	b.mu.RLock("DescribeClassificationJob")
	defer b.mu.RUnlock()

	job, ok := b.classificationJobs[jobID]
	if !ok {
		return nil, ErrClassificationJobNotFound
	}

	cp := *job
	cp.Tags = maps.Clone(job.Tags)

	return &cp, nil
}

// ListClassificationJobs returns summaries of all classification jobs.
func (b *InMemoryBackend) ListClassificationJobs(
	_ map[string]any, _ int, _ string,
) ([]*ClassificationJobSummary, string, error) {
	b.mu.RLock("ListClassificationJobs")
	defer b.mu.RUnlock()

	result := make([]*ClassificationJobSummary, 0, len(b.classificationJobs))

	for _, job := range b.classificationJobs {
		result = append(result, &ClassificationJobSummary{
			JobID:       job.JobID,
			Name:        job.Name,
			Description: job.Description,
			JobType:     job.JobType,
			JobStatus:   job.JobStatus,
			CreatedAt:   job.CreatedAt,
			LastRunTime: job.LastRunTime,
			Tags:        maps.Clone(job.Tags),
		})
	}

	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })

	return result, "", nil
}

// UpdateClassificationJob updates a job's status.
func (b *InMemoryBackend) UpdateClassificationJob(jobID, status string) error {
	b.mu.Lock("UpdateClassificationJob")
	defer b.mu.Unlock()

	job, ok := b.classificationJobs[jobID]
	if !ok {
		return ErrClassificationJobNotFound
	}

	job.JobStatus = status

	return nil
}

// --- members ---

// CreateMember creates a new member account relationship.
func (b *InMemoryBackend) CreateMember(accountID, email string, tags map[string]string) error {
	b.mu.Lock("CreateMember")
	defer b.mu.Unlock()

	if _, exists := b.members[accountID]; exists {
		return ErrMemberAlreadyExists
	}

	now := time.Now().UTC()
	b.members[accountID] = &Member{
		AccountID:              accountID,
		AdministratorAccountID: b.accountID,
		Email:                  email,
		MasteredBy:             b.accountID,
		RelationshipStatus:     "CREATED",
		InvitedAt:              now,
		UpdatedAt:              now,
		Tags:                   maps.Clone(tags),
	}

	return nil
}

// GetMember returns a member account by account ID.
func (b *InMemoryBackend) GetMember(accountID string) (*Member, error) {
	b.mu.RLock("GetMember")
	defer b.mu.RUnlock()

	m, ok := b.members[accountID]
	if !ok {
		return nil, ErrMemberNotFound
	}

	cp := *m
	cp.Tags = maps.Clone(m.Tags)

	return &cp, nil
}

// DeleteMember removes a member account.
func (b *InMemoryBackend) DeleteMember(accountID string) error {
	b.mu.Lock("DeleteMember")
	defer b.mu.Unlock()

	if _, ok := b.members[accountID]; !ok {
		return ErrMemberNotFound
	}

	delete(b.members, accountID)

	return nil
}

// ListMembers returns all member accounts.
func (b *InMemoryBackend) ListMembers(onlyAssociated bool) ([]*Member, error) {
	b.mu.RLock("ListMembers")
	defer b.mu.RUnlock()

	result := make([]*Member, 0, len(b.members))

	for _, m := range b.members {
		if onlyAssociated && m.RelationshipStatus == "DISASSOCIATED" {
			continue
		}

		cp := *m
		cp.Tags = maps.Clone(m.Tags)
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].AccountID < result[j].AccountID })

	return result, nil
}

// DisassociateMember sets a member's relationship status to DISASSOCIATED.
func (b *InMemoryBackend) DisassociateMember(accountID string) error {
	b.mu.Lock("DisassociateMember")
	defer b.mu.Unlock()

	m, ok := b.members[accountID]
	if !ok {
		return ErrMemberNotFound
	}

	m.RelationshipStatus = "DISASSOCIATED"
	m.UpdatedAt = time.Now().UTC()

	return nil
}

// UpdateMemberSession updates the status of a member's Macie session.
func (b *InMemoryBackend) UpdateMemberSession(accountID, status string) error {
	b.mu.Lock("UpdateMemberSession")
	defer b.mu.Unlock()

	m, ok := b.members[accountID]
	if !ok {
		return ErrMemberNotFound
	}

	if status != "" {
		m.RelationshipStatus = status
	}

	m.UpdatedAt = time.Now().UTC()

	return nil
}

// --- invitations ---

// CreateInvitations creates invitations for the given account IDs.
func (b *InMemoryBackend) CreateInvitations(
	accountIDs []string, _ string, _ bool,
) ([]UnprocessedAccount, error) {
	b.mu.Lock("CreateInvitations")
	defer b.mu.Unlock()

	now := time.Now().UTC()

	for _, accountID := range accountIDs {
		id := uuid.New().String()
		b.invitations[id] = &Invitation{
			AccountID:          accountID,
			InvitationID:       id,
			InvitedAt:          now,
			RelationshipStatus: "INVITED",
		}
	}

	return nil, nil
}

// AcceptInvitation accepts a Macie invitation.
func (b *InMemoryBackend) AcceptInvitation(administratorAccountID, invitationID string) error {
	b.mu.Lock("AcceptInvitation")
	defer b.mu.Unlock()

	b.administrator = &AdministratorAccount{
		AccountID:          administratorAccountID,
		InvitationID:       invitationID,
		InvitedAt:          time.Now().UTC(),
		RelationshipStatus: "ENABLED",
	}

	return nil
}

// DeclineInvitations marks invitations from the given accounts as declined.
func (b *InMemoryBackend) DeclineInvitations(accountIDs []string) ([]UnprocessedAccount, error) {
	b.mu.Lock("DeclineInvitations")
	defer b.mu.Unlock()

	decline := make(map[string]bool, len(accountIDs))
	for _, id := range accountIDs {
		decline[id] = true
	}

	for _, inv := range b.invitations {
		if decline[inv.AccountID] {
			inv.RelationshipStatus = "RESIGNED"
		}
	}

	return nil, nil
}

// DeleteInvitations removes invitations from the given accounts.
func (b *InMemoryBackend) DeleteInvitations(accountIDs []string) ([]UnprocessedAccount, error) {
	b.mu.Lock("DeleteInvitations")
	defer b.mu.Unlock()

	toDelete := make(map[string]bool, len(accountIDs))
	for _, id := range accountIDs {
		toDelete[id] = true
	}

	for id, inv := range b.invitations {
		if toDelete[inv.AccountID] {
			delete(b.invitations, id)
		}
	}

	return nil, nil
}

// GetInvitationsCount returns the number of active invitations.
func (b *InMemoryBackend) GetInvitationsCount() (int64, error) {
	b.mu.RLock("GetInvitationsCount")
	defer b.mu.RUnlock()

	var count int64

	for _, inv := range b.invitations {
		if inv.RelationshipStatus == "INVITED" {
			count++
		}
	}

	return count, nil
}

// ListInvitations returns all active invitations.
func (b *InMemoryBackend) ListInvitations() ([]*Invitation, error) {
	b.mu.RLock("ListInvitations")
	defer b.mu.RUnlock()

	result := make([]*Invitation, 0, len(b.invitations))

	for _, inv := range b.invitations {
		cp := *inv
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].InvitationID < result[j].InvitationID })

	return result, nil
}

// --- administrator / master ---

// GetAdministratorAccount returns the administrator account relationship.
func (b *InMemoryBackend) GetAdministratorAccount() (*AdministratorAccount, error) {
	b.mu.RLock("GetAdministratorAccount")
	defer b.mu.RUnlock()

	if b.administrator == nil {
		return nil, nil
	}

	cp := *b.administrator

	return &cp, nil
}

// GetMasterAccount is the legacy alias for GetAdministratorAccount.
func (b *InMemoryBackend) GetMasterAccount() (*AdministratorAccount, error) {
	return b.GetAdministratorAccount()
}

// DisassociateFromAdministratorAccount removes the administrator relationship.
func (b *InMemoryBackend) DisassociateFromAdministratorAccount() error {
	b.mu.Lock("DisassociateFromAdministratorAccount")
	defer b.mu.Unlock()

	b.administrator = nil

	return nil
}

// DisassociateFromMasterAccount is the legacy alias.
func (b *InMemoryBackend) DisassociateFromMasterAccount() error {
	return b.DisassociateFromAdministratorAccount()
}

// --- org admin accounts ---

// EnableOrganizationAdminAccount designates an account as org admin.
func (b *InMemoryBackend) EnableOrganizationAdminAccount(accountID string) error {
	b.mu.Lock("EnableOrganizationAdminAccount")
	defer b.mu.Unlock()

	b.orgAdminAccounts[accountID] = &OrgAdminAccount{
		AccountID: accountID,
		Status:    "ENABLED",
	}

	return nil
}

// DisableOrganizationAdminAccount removes the org admin designation.
func (b *InMemoryBackend) DisableOrganizationAdminAccount(accountID string) error {
	b.mu.Lock("DisableOrganizationAdminAccount")
	defer b.mu.Unlock()

	if _, ok := b.orgAdminAccounts[accountID]; !ok {
		return ErrOrgAdminNotFound
	}

	delete(b.orgAdminAccounts, accountID)

	return nil
}

// ListOrganizationAdminAccounts returns all org admin accounts.
func (b *InMemoryBackend) ListOrganizationAdminAccounts() ([]*OrgAdminAccount, error) {
	b.mu.RLock("ListOrganizationAdminAccounts")
	defer b.mu.RUnlock()

	result := make([]*OrgAdminAccount, 0, len(b.orgAdminAccounts))

	for _, acct := range b.orgAdminAccounts {
		cp := *acct
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].AccountID < result[j].AccountID })

	return result, nil
}

// --- org configuration ---

// DescribeOrganizationConfiguration returns the org-level Macie configuration.
func (b *InMemoryBackend) DescribeOrganizationConfiguration() (*OrgConfig, error) {
	b.mu.RLock("DescribeOrganizationConfiguration")
	defer b.mu.RUnlock()

	if b.orgConfig == nil {
		return &OrgConfig{}, nil
	}

	cp := *b.orgConfig

	return &cp, nil
}

// UpdateOrganizationConfiguration updates the org-level configuration.
func (b *InMemoryBackend) UpdateOrganizationConfiguration(autoEnable bool) error {
	b.mu.Lock("UpdateOrganizationConfiguration")
	defer b.mu.Unlock()

	if b.orgConfig == nil {
		b.orgConfig = &OrgConfig{}
	}

	b.orgConfig.AutoEnable = autoEnable

	return nil
}

// --- automated discovery ---

// GetAutomatedDiscoveryConfiguration returns the automated discovery config.
func (b *InMemoryBackend) GetAutomatedDiscoveryConfiguration() (*AutoDiscoveryConfig, error) {
	b.mu.RLock("GetAutomatedDiscoveryConfiguration")
	defer b.mu.RUnlock()

	if b.autoDiscoveryConfig == nil {
		return &AutoDiscoveryConfig{Status: "DISABLED"}, nil
	}

	cp := *b.autoDiscoveryConfig

	return &cp, nil
}

// UpdateAutomatedDiscoveryConfiguration updates the automated discovery config.
func (b *InMemoryBackend) UpdateAutomatedDiscoveryConfiguration(autoEnableMembers, status string) error {
	b.mu.Lock("UpdateAutomatedDiscoveryConfiguration")
	defer b.mu.Unlock()

	if b.autoDiscoveryConfig == nil {
		b.autoDiscoveryConfig = &AutoDiscoveryConfig{}
	}

	if autoEnableMembers != "" {
		b.autoDiscoveryConfig.AutoEnableOrganizationMembers = autoEnableMembers
	}

	if status != "" {
		b.autoDiscoveryConfig.Status = status
	}

	return nil
}

// ListAutomatedDiscoveryAccounts returns accounts with automated discovery status.
func (b *InMemoryBackend) ListAutomatedDiscoveryAccounts() ([]*AutoDiscoveryAccount, error) {
	b.mu.RLock("ListAutomatedDiscoveryAccounts")
	defer b.mu.RUnlock()

	result := make([]*AutoDiscoveryAccount, 0, len(b.autoDiscoveryAccounts))

	for _, acct := range b.autoDiscoveryAccounts {
		cp := *acct
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].AccountID < result[j].AccountID })

	return result, nil
}

// BatchUpdateAutomatedDiscoveryAccounts updates automated discovery status for multiple accounts.
func (b *InMemoryBackend) BatchUpdateAutomatedDiscoveryAccounts(updates []AutoDiscoveryAccountUpdate) error {
	b.mu.Lock("BatchUpdateAutomatedDiscoveryAccounts")
	defer b.mu.Unlock()

	for _, u := range updates {
		if acct, ok := b.autoDiscoveryAccounts[u.AccountID]; ok {
			acct.Status = u.Status
		} else {
			b.autoDiscoveryAccounts[u.AccountID] = &AutoDiscoveryAccount{
				AccountID: u.AccountID,
				Status:    u.Status,
			}
		}
	}

	return nil
}

// --- buckets ---

// DescribeBuckets returns S3 bucket metadata (always empty — no real S3 scanning).
func (b *InMemoryBackend) DescribeBuckets(_ map[string]any) ([]map[string]any, error) {
	return []map[string]any{}, nil
}

// GetBucketStatistics returns aggregate S3 statistics (all zeros).
func (b *InMemoryBackend) GetBucketStatistics(_ string) (map[string]any, error) {
	return map[string]any{
		"bucketCount":                              int64(0),
		"bucketCountByEffectivePermission":         map[string]any{},
		"bucketCountByEncryptionType":              map[string]any{},
		"bucketCountByObjectEncryptionRequirement": map[string]any{},
		"bucketCountBySharedAccessType":            map[string]any{},
		"classifiableBucketCount":                  int64(0),
		"classifiableSizeInBytes":                  int64(0),
		"unclassifiableObjectCount":                map[string]any{},
		"unclassifiableObjectSizeInBytes":          map[string]any{},
	}, nil
}

// --- batch custom data identifiers ---

// BatchGetCustomDataIdentifiers returns full details for the given IDs.
func (b *InMemoryBackend) BatchGetCustomDataIdentifiers(ids []string) ([]*CustomDataIdentifier, error) {
	b.mu.RLock("BatchGetCustomDataIdentifiers")
	defer b.mu.RUnlock()

	result := make([]*CustomDataIdentifier, 0, len(ids))

	for _, id := range ids {
		cdi, ok := b.customDataIDs[id]
		if !ok || cdi.Deleted {
			continue
		}

		cp := cdi.CustomDataIdentifier
		cp.Tags = maps.Clone(cdi.Tags)
		result = append(result, &cp)
	}

	return result, nil
}

// --- classification export configuration ---

// GetClassificationExportConfiguration returns the export config.
func (b *InMemoryBackend) GetClassificationExportConfiguration() (*ClassificationExportConfig, error) {
	b.mu.RLock("GetClassificationExportConfiguration")
	defer b.mu.RUnlock()

	if b.classExportConfig == nil {
		return &ClassificationExportConfig{}, nil
	}

	cp := *b.classExportConfig

	return &cp, nil
}

// PutClassificationExportConfiguration stores the export config.
func (b *InMemoryBackend) PutClassificationExportConfiguration(cfg *ClassificationExportConfig) error {
	b.mu.Lock("PutClassificationExportConfiguration")
	defer b.mu.Unlock()

	if cfg == nil {
		b.classExportConfig = nil

		return nil
	}

	cp := *cfg
	b.classExportConfig = &cp

	return nil
}

// --- classification scopes ---

const defaultScopeName = "Default classification scope"

func (b *InMemoryBackend) ensureDefaultScope() {
	if len(b.classScopes) > 0 {
		return
	}

	id := uuid.New().String()
	now := time.Now().UTC()
	b.classScopes[id] = &ClassificationScope{
		ID:        id,
		Name:      defaultScopeName,
		S3:        &ClassificationScopeS3{},
		CreatedAt: now,
		UpdatedAt: now,
	}
}

// GetClassificationScope returns a classification scope by ID.
func (b *InMemoryBackend) GetClassificationScope(scopeID string) (*ClassificationScope, error) {
	b.mu.Lock("GetClassificationScope")
	defer b.mu.Unlock()

	b.ensureDefaultScope()

	scope, ok := b.classScopes[scopeID]
	if !ok {
		return nil, ErrClassificationScopeNotFound
	}

	cp := *scope

	return &cp, nil
}

// ListClassificationScopes returns all classification scopes.
func (b *InMemoryBackend) ListClassificationScopes() ([]*ClassificationScopeSummary, error) {
	b.mu.Lock("ListClassificationScopes")
	defer b.mu.Unlock()

	b.ensureDefaultScope()

	result := make([]*ClassificationScopeSummary, 0, len(b.classScopes))

	for _, s := range b.classScopes {
		result = append(result, &ClassificationScopeSummary{ID: s.ID, Name: s.Name})
	}

	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })

	return result, nil
}

// UpdateClassificationScope updates a scope's S3 settings.
func (b *InMemoryBackend) UpdateClassificationScope(scopeID string, s3 *ClassificationScopeS3) error {
	b.mu.Lock("UpdateClassificationScope")
	defer b.mu.Unlock()

	scope, ok := b.classScopes[scopeID]
	if !ok {
		return ErrClassificationScopeNotFound
	}

	if s3 != nil {
		scope.S3 = s3
	}

	scope.UpdatedAt = time.Now().UTC()

	return nil
}

// --- findings publication configuration ---

// GetFindingsPublicationConfiguration returns the findings publication config.
func (b *InMemoryBackend) GetFindingsPublicationConfiguration() (*FindingsPublicationConfig, error) {
	b.mu.RLock("GetFindingsPublicationConfiguration")
	defer b.mu.RUnlock()

	if b.findingsPubConfig == nil {
		return &FindingsPublicationConfig{}, nil
	}

	cp := *b.findingsPubConfig

	return &cp, nil
}

// PutFindingsPublicationConfiguration stores the findings publication config.
func (b *InMemoryBackend) PutFindingsPublicationConfiguration(cfg *FindingsPublicationConfig) error {
	b.mu.Lock("PutFindingsPublicationConfiguration")
	defer b.mu.Unlock()

	if cfg == nil {
		b.findingsPubConfig = nil

		return nil
	}

	cp := *cfg
	b.findingsPubConfig = &cp

	return nil
}

// --- resource profiles ---

// GetResourceProfile returns a bucket's sensitivity profile.
func (b *InMemoryBackend) GetResourceProfile(resourceARN string) (*ResourceProfile, error) {
	b.mu.RLock("GetResourceProfile")
	defer b.mu.RUnlock()

	if p, ok := b.resourceProfiles[resourceARN]; ok {
		cp := *p

		return &cp, nil
	}

	return &ResourceProfile{
		ResourceArn:      resourceARN,
		SensitivityScore: 0,
		Statistics:       &ResourceStatistics{},
	}, nil
}

// UpdateResourceProfile sets a bucket's sensitivity score override.
func (b *InMemoryBackend) UpdateResourceProfile(resourceARN string, sensitivityScore int32) error {
	b.mu.Lock("UpdateResourceProfile")
	defer b.mu.Unlock()

	if p, ok := b.resourceProfiles[resourceARN]; ok {
		p.SensitivityScore = sensitivityScore
		p.SensitivityScoreOverride = true
	} else {
		b.resourceProfiles[resourceARN] = &ResourceProfile{
			ResourceArn:              resourceARN,
			SensitivityScore:         sensitivityScore,
			SensitivityScoreOverride: true,
			Statistics:               &ResourceStatistics{},
		}
	}

	return nil
}

// ListResourceProfileArtifacts returns classified artifacts for a bucket (always empty).
func (b *InMemoryBackend) ListResourceProfileArtifacts(_ string) ([]ResourceProfileArtifact, error) {
	return []ResourceProfileArtifact{}, nil
}

// ListResourceProfileDetections returns data identifier detections for a bucket.
func (b *InMemoryBackend) ListResourceProfileDetections(resourceARN string) ([]ResourceProfileDetection, error) {
	b.mu.RLock("ListResourceProfileDetections")
	defer b.mu.RUnlock()

	detections := b.resourceDetections[resourceARN]
	if len(detections) == 0 {
		return []ResourceProfileDetection{}, nil
	}

	cp := make([]ResourceProfileDetection, len(detections))
	copy(cp, detections)

	return cp, nil
}

// UpdateResourceProfileDetections updates suppression status of detections.
func (b *InMemoryBackend) UpdateResourceProfileDetections(
	resourceARN string, suppressDataIdentifiers []map[string]any,
) error {
	b.mu.Lock("UpdateResourceProfileDetections")
	defer b.mu.Unlock()

	suppress := make(map[string]bool, len(suppressDataIdentifiers))
	for _, s := range suppressDataIdentifiers {
		if id, ok := s["id"].(string); ok {
			suppress[id] = true
		}
	}

	for i := range b.resourceDetections[resourceARN] {
		d := &b.resourceDetections[resourceARN][i]
		if suppress[d.ID] {
			d.Suppressed = true
		}
	}

	return nil
}

// --- reveal configuration ---

// GetRevealConfiguration returns the sensitive data reveal configuration.
func (b *InMemoryBackend) GetRevealConfiguration() (*RevealConfiguration, error) {
	b.mu.RLock("GetRevealConfiguration")
	defer b.mu.RUnlock()

	if b.revealConfig == nil {
		return &RevealConfiguration{Status: "DISABLED"}, nil
	}

	cp := *b.revealConfig

	return &cp, nil
}

// UpdateRevealConfiguration stores the reveal configuration.
func (b *InMemoryBackend) UpdateRevealConfiguration(kmsKeyID, status string) error {
	b.mu.Lock("UpdateRevealConfiguration")
	defer b.mu.Unlock()

	b.revealConfig = &RevealConfiguration{
		KmsKeyID: kmsKeyID,
		Status:   status,
	}

	return nil
}

// --- sensitive data occurrences ---

// GetSensitiveDataOccurrences returns redacted occurrences for a finding.
func (b *InMemoryBackend) GetSensitiveDataOccurrences(findingID string) (map[string]any, error) {
	b.mu.RLock("GetSensitiveDataOccurrences")
	defer b.mu.RUnlock()

	if _, ok := b.findings[findingID]; !ok {
		return nil, ErrFindingNotFound
	}

	return map[string]any{"sensitiveDataOccurrences": map[string]any{}}, nil
}

// GetSensitiveDataOccurrencesAvailability reports reveal availability for a finding.
func (b *InMemoryBackend) GetSensitiveDataOccurrencesAvailability(findingID string) (string, error) {
	b.mu.RLock("GetSensitiveDataOccurrencesAvailability")
	defer b.mu.RUnlock()

	if _, ok := b.findings[findingID]; !ok {
		return "", ErrFindingNotFound
	}

	return "AVAILABLE", nil
}

// --- sensitivity inspection templates ---

const defaultTemplateName = "Default sensitivity inspection template"

func (b *InMemoryBackend) ensureDefaultTemplate() {
	if len(b.sensitivityTemplates) > 0 {
		return
	}

	id := uuid.New().String()
	b.sensitivityTemplates[id] = &SensitivityInspectionTemplate{
		ID:   id,
		Name: defaultTemplateName,
	}
}

// GetSensitivityInspectionTemplate returns a sensitivity inspection template by ID.
func (b *InMemoryBackend) GetSensitivityInspectionTemplate(templateID string) (*SensitivityInspectionTemplate, error) {
	b.mu.Lock("GetSensitivityInspectionTemplate")
	defer b.mu.Unlock()

	b.ensureDefaultTemplate()

	tmpl, ok := b.sensitivityTemplates[templateID]
	if !ok {
		return nil, ErrSensitivityTemplateNotFound
	}

	cp := *tmpl

	return &cp, nil
}

// ListSensitivityInspectionTemplates returns all templates.
func (b *InMemoryBackend) ListSensitivityInspectionTemplates() ([]*SensitivityInspectionTemplateSummary, error) {
	b.mu.Lock("ListSensitivityInspectionTemplates")
	defer b.mu.Unlock()

	b.ensureDefaultTemplate()

	result := make([]*SensitivityInspectionTemplateSummary, 0, len(b.sensitivityTemplates))

	for _, t := range b.sensitivityTemplates {
		result = append(result, &SensitivityInspectionTemplateSummary{ID: t.ID, Name: t.Name})
	}

	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })

	return result, nil
}

// UpdateSensitivityInspectionTemplate updates a template.
func (b *InMemoryBackend) UpdateSensitivityInspectionTemplate(
	templateID, name, description string,
	excludes, includes map[string]any,
) error {
	b.mu.Lock("UpdateSensitivityInspectionTemplate")
	defer b.mu.Unlock()

	tmpl, ok := b.sensitivityTemplates[templateID]
	if !ok {
		return ErrSensitivityTemplateNotFound
	}

	if name != "" {
		tmpl.Name = name
	}

	tmpl.Description = description
	tmpl.Excludes = excludes
	tmpl.Includes = includes

	return nil
}

// --- usage ---

// GetUsageStatistics returns usage statistics (empty — no billing data in emulator).
func (b *InMemoryBackend) GetUsageStatistics(
	_ []map[string]any, _ int, _ string, _ string,
) ([]UsageRecord, string, error) {
	return []UsageRecord{}, "", nil
}

// GetUsageTotals returns aggregated usage totals (all zero).
func (b *InMemoryBackend) GetUsageTotals(_ string) ([]UsageTotal, error) {
	return []UsageTotal{
		{Currency: "USD", EstimatedCost: "0", Type: "DATA_INVENTORY_EVALUATION"},
		{Currency: "USD", EstimatedCost: "0", Type: "SENSITIVE_DATA_DISCOVERY"},
		{Currency: "USD", EstimatedCost: "0", Type: "AUTOMATED_SENSITIVE_DATA_DISCOVERY"},
	}, nil
}

// --- managed data identifiers ---

// managedDataIdentifiers is the static list of built-in Macie data identifiers.
var managedDataIdentifiers = []ManagedDataIdentifier{
	{Category: "CREDENTIALS", ID: "AWS_CREDENTIALS"},
	{Category: "CREDENTIALS", ID: "PRIVATE_KEY"},
	{Category: "CREDENTIALS", ID: "AWS_SECRET_ACCESS_KEY"},
	{Category: "FINANCIAL_INFORMATION", ID: "CREDIT_CARD_NUMBER"},
	{Category: "FINANCIAL_INFORMATION", ID: "BANK_ACCOUNT_NUMBER_US"},
	{Category: "PERSONAL_INFORMATION", ID: "EMAIL_ADDRESS"},
	{Category: "PERSONAL_INFORMATION", ID: "PHONE_NUMBER_US"},
	{Category: "PERSONAL_INFORMATION", ID: "NAME"},
	{Category: "PERSONAL_INFORMATION", ID: "US_SOCIAL_SECURITY_NUMBER"},
	{Category: "PERSONAL_INFORMATION", ID: "US_DRIVERS_LICENSE"},
	{Category: "PERSONAL_INFORMATION", ID: "US_PASSPORT_NUMBER"},
	{Category: "PERSONAL_INFORMATION", ID: "DATE_OF_BIRTH"},
	{Category: "PERSONAL_INFORMATION", ID: "ADDRESS"},
}

// ListManagedDataIdentifiers returns the built-in Macie data identifiers.
func (b *InMemoryBackend) ListManagedDataIdentifiers() ([]ManagedDataIdentifier, error) {
	result := make([]ManagedDataIdentifier, len(managedDataIdentifiers))
	copy(result, managedDataIdentifiers)

	return result, nil
}

// --- search resources ---

// SearchResources searches S3 resources (always returns empty — no real S3 scanning).
func (b *InMemoryBackend) SearchResources(_ map[string]any, _ int, _ string) ([]map[string]any, string, error) {
	return []map[string]any{}, "", nil
}
