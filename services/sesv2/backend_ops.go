package sesv2

import (
	"fmt"
	"maps"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// ---- account ----

// AccountDetails stores account-level SESv2 settings.
type AccountDetails struct {
	MailType        string `json:"mailType"`
	WebsiteURL      string `json:"websiteURL"`
	ContactLanguage string `json:"contactLanguage"`
	UseCaseName     string `json:"useCaseName"`
	SendingEnabled  bool   `json:"sendingEnabled"`
}

// SuppressedDestination stores a suppressed email address.
type SuppressedDestination struct {
	LastUpdateTime time.Time `json:"lastUpdateTime"`
	EmailAddress   string    `json:"emailAddress"`
	Reason         string    `json:"reason"`
}

// ImportJob stores an import job.
type ImportJob struct {
	CreatedAt time.Time `json:"createdAt"`
	JobID     string    `json:"jobId"`
	JobStatus string    `json:"jobStatus"`
}

// GetAccount returns the account details.
func (b *InMemoryBackend) GetAccount() (*AccountDetails, error) {
	b.mu.RLock("GetAccount")
	defer b.mu.RUnlock()

	if b.accountDetails == nil {
		return &AccountDetails{SendingEnabled: true}, nil
	}

	cp := *b.accountDetails

	return &cp, nil
}

// PutAccountDetails stores account details.
func (b *InMemoryBackend) PutAccountDetails(details *AccountDetails) error {
	b.mu.Lock("PutAccountDetails")
	defer b.mu.Unlock()

	cp := *details
	b.accountDetails = &cp

	return nil
}

// PutAccountSendingAttributes sets the sending enabled flag.
func (b *InMemoryBackend) PutAccountSendingAttributes(sendingEnabled bool) error {
	b.mu.Lock("PutAccountSendingAttributes")
	defer b.mu.Unlock()

	if b.accountDetails == nil {
		b.accountDetails = &AccountDetails{}
	}

	b.accountDetails.SendingEnabled = sendingEnabled

	return nil
}

// PutAccountSuppressionAttributes is a no-op stub.
func (b *InMemoryBackend) PutAccountSuppressionAttributes() error {
	return nil
}

// PutAccountVdmAttributes is a no-op stub.
func (b *InMemoryBackend) PutAccountVdmAttributes() error {
	return nil
}

// PutAccountDedicatedIPWarmupAttributes is a no-op stub.
func (b *InMemoryBackend) PutAccountDedicatedIPWarmupAttributes() error {
	return nil
}

// GetBlacklistReports returns empty blacklist reports.
func (b *InMemoryBackend) GetBlacklistReports() (map[string][]string, error) {
	return map[string][]string{}, nil
}

// ---- suppressed destinations ----

// PutSuppressedDestination adds or updates a suppressed destination.
func (b *InMemoryBackend) PutSuppressedDestination(email, reason string) error {
	b.mu.Lock("PutSuppressedDestination")
	defer b.mu.Unlock()

	b.suppressedDestinations[email] = &SuppressedDestination{
		EmailAddress:   email,
		Reason:         reason,
		LastUpdateTime: time.Now(),
	}

	return nil
}

// GetSuppressedDestination retrieves a suppressed destination.
func (b *InMemoryBackend) GetSuppressedDestination(email string) (*SuppressedDestination, error) {
	b.mu.RLock("GetSuppressedDestination")
	defer b.mu.RUnlock()

	dest, ok := b.suppressedDestinations[email]
	if !ok {
		return nil, fmt.Errorf("%w: suppressed destination %s not found", ErrNotFound, email)
	}

	cp := *dest

	return &cp, nil
}

// DeleteSuppressedDestination removes a suppressed destination.
func (b *InMemoryBackend) DeleteSuppressedDestination(email string) error {
	b.mu.Lock("DeleteSuppressedDestination")
	defer b.mu.Unlock()

	if _, ok := b.suppressedDestinations[email]; !ok {
		return fmt.Errorf("%w: suppressed destination %s not found", ErrNotFound, email)
	}

	delete(b.suppressedDestinations, email)

	return nil
}

// ListSuppressedDestinations lists all suppressed destinations.
func (b *InMemoryBackend) ListSuppressedDestinations(
	nextToken string,
	pageSize int,
) page.Page[*SuppressedDestination] {
	b.mu.RLock("ListSuppressedDestinations")
	defer b.mu.RUnlock()

	keys := collections.SortedKeys(b.suppressedDestinations)

	items := make([]*SuppressedDestination, 0, len(keys))
	for _, k := range keys {
		cp := *b.suppressedDestinations[k]
		items = append(items, &cp)
	}

	return page.New(items, nextToken, pageSize, sesv2DefaultMaxItems)
}

// ---- contact list / contact ----

// GetContactList retrieves a contact list.
func (b *InMemoryBackend) GetContactList(name string) (*ContactList, error) {
	b.mu.RLock("GetContactList")
	defer b.mu.RUnlock()

	cl, ok := b.contactLists[name]
	if !ok {
		return nil, fmt.Errorf("%w: contact list %s not found", ErrNotFound, name)
	}

	cp := *cl

	return &cp, nil
}

// DeleteContactList removes a contact list and all its contacts.
func (b *InMemoryBackend) DeleteContactList(name string) error {
	b.mu.Lock("DeleteContactList")
	defer b.mu.Unlock()

	if _, ok := b.contactLists[name]; !ok {
		return fmt.Errorf("%w: contact list %s not found", ErrNotFound, name)
	}

	delete(b.contactLists, name)
	delete(b.contacts, name)

	return nil
}

// UpdateContactList updates a contact list description.
func (b *InMemoryBackend) UpdateContactList(name, description string) error {
	b.mu.Lock("UpdateContactList")
	defer b.mu.Unlock()

	cl, ok := b.contactLists[name]
	if !ok {
		return fmt.Errorf("%w: contact list %s not found", ErrNotFound, name)
	}

	cl.Description = description
	cl.LastUpdatedAt = time.Now()

	return nil
}

// ListContactLists returns all contact lists.
func (b *InMemoryBackend) ListContactLists(nextToken string, pageSize int) page.Page[*ContactList] {
	b.mu.RLock("ListContactLists")
	defer b.mu.RUnlock()

	keys := collections.SortedKeys(b.contactLists)

	items := make([]*ContactList, 0, len(keys))
	for _, k := range keys {
		cp := *b.contactLists[k]
		items = append(items, &cp)
	}

	return page.New(items, nextToken, pageSize, sesv2DefaultMaxItems)
}

// GetContact retrieves a contact from a contact list.
func (b *InMemoryBackend) GetContact(contactListName, emailAddress string) (*Contact, error) {
	b.mu.RLock("GetContact")
	defer b.mu.RUnlock()

	listContacts, ok := b.contacts[contactListName]
	if !ok {
		return nil, fmt.Errorf("%w: contact list %s not found", ErrNotFound, contactListName)
	}

	c, ok := listContacts[emailAddress]
	if !ok {
		return nil, fmt.Errorf(
			"%w: contact %s not found in list %s",
			ErrNotFound,
			emailAddress,
			contactListName,
		)
	}

	cp := *c

	return &cp, nil
}

// DeleteContact removes a contact from a contact list.
func (b *InMemoryBackend) DeleteContact(contactListName, emailAddress string) error {
	b.mu.Lock("DeleteContact")
	defer b.mu.Unlock()

	listContacts, ok := b.contacts[contactListName]
	if !ok {
		return fmt.Errorf("%w: contact list %s not found", ErrNotFound, contactListName)
	}

	if _, exists := listContacts[emailAddress]; !exists {
		return fmt.Errorf(
			"%w: contact %s not found in list %s",
			ErrNotFound,
			emailAddress,
			contactListName,
		)
	}

	delete(listContacts, emailAddress)

	return nil
}

// UpdateContact updates a contact's topic preferences.
func (b *InMemoryBackend) UpdateContact(
	contactListName, emailAddress string,
	topicPreferences []TopicPreference,
) error {
	b.mu.Lock("UpdateContact")
	defer b.mu.Unlock()

	listContacts, ok := b.contacts[contactListName]
	if !ok {
		return fmt.Errorf("%w: contact list %s not found", ErrNotFound, contactListName)
	}

	c, ok := listContacts[emailAddress]
	if !ok {
		return fmt.Errorf(
			"%w: contact %s not found in list %s",
			ErrNotFound,
			emailAddress,
			contactListName,
		)
	}

	prefs := make([]TopicPreference, len(topicPreferences))
	copy(prefs, topicPreferences)
	c.TopicPreferences = prefs
	c.LastUpdatedAt = time.Now()

	return nil
}

// ListContacts returns all contacts in a contact list.
func (b *InMemoryBackend) ListContacts(
	contactListName, nextToken string,
	pageSize int,
) (page.Page[*Contact], error) {
	b.mu.RLock("ListContacts")
	defer b.mu.RUnlock()

	listContacts, ok := b.contacts[contactListName]
	if !ok {
		return page.Page[*Contact]{}, fmt.Errorf(
			"%w: contact list %s not found",
			ErrNotFound,
			contactListName,
		)
	}

	keys := collections.SortedKeys(listContacts)

	items := make([]*Contact, 0, len(keys))
	for _, k := range keys {
		cp := *listContacts[k]
		items = append(items, &cp)
	}

	return page.New(items, nextToken, pageSize, sesv2DefaultMaxItems), nil
}

// ---- custom verification templates ----

// GetCustomVerificationEmailTemplate retrieves a custom verification template.
func (b *InMemoryBackend) GetCustomVerificationEmailTemplate(
	name string,
) (*CustomVerificationEmailTemplate, error) {
	b.mu.RLock("GetCustomVerificationEmailTemplate")
	defer b.mu.RUnlock()

	t, ok := b.customVerificationTemplates[name]
	if !ok {
		return nil, fmt.Errorf("%w: custom verification template %s not found", ErrNotFound, name)
	}

	cp := *t

	return &cp, nil
}

// DeleteCustomVerificationEmailTemplate removes a custom verification template.
func (b *InMemoryBackend) DeleteCustomVerificationEmailTemplate(name string) error {
	b.mu.Lock("DeleteCustomVerificationEmailTemplate")
	defer b.mu.Unlock()

	if _, ok := b.customVerificationTemplates[name]; !ok {
		return fmt.Errorf("%w: custom verification template %s not found", ErrNotFound, name)
	}

	delete(b.customVerificationTemplates, name)

	return nil
}

// UpdateCustomVerificationEmailTemplate updates a custom verification template.
func (b *InMemoryBackend) UpdateCustomVerificationEmailTemplate(
	in *CustomVerificationEmailTemplate,
) error {
	b.mu.Lock("UpdateCustomVerificationEmailTemplate")
	defer b.mu.Unlock()

	if _, ok := b.customVerificationTemplates[in.TemplateName]; !ok {
		return fmt.Errorf(
			"%w: custom verification template %s not found",
			ErrNotFound,
			in.TemplateName,
		)
	}

	cp := *in
	b.customVerificationTemplates[in.TemplateName] = &cp

	return nil
}

// ListCustomVerificationEmailTemplates returns all custom verification templates.
func (b *InMemoryBackend) ListCustomVerificationEmailTemplates(
	nextToken string,
	pageSize int,
) page.Page[*CustomVerificationEmailTemplate] {
	b.mu.RLock("ListCustomVerificationEmailTemplates")
	defer b.mu.RUnlock()

	keys := collections.SortedKeys(b.customVerificationTemplates)

	items := make([]*CustomVerificationEmailTemplate, 0, len(keys))
	for _, k := range keys {
		cp := *b.customVerificationTemplates[k]
		items = append(items, &cp)
	}

	return page.New(items, nextToken, pageSize, sesv2DefaultMaxItems)
}

// ---- dedicated IP pools ----

// GetDedicatedIPPool retrieves a dedicated IP pool.
func (b *InMemoryBackend) GetDedicatedIPPool(poolName string) (*DedicatedIPPool, error) {
	b.mu.RLock("GetDedicatedIPPool")
	defer b.mu.RUnlock()

	pool, ok := b.dedicatedIPPools[poolName]
	if !ok {
		return nil, fmt.Errorf("%w: dedicated IP pool %s not found", ErrNotFound, poolName)
	}

	cp := *pool

	return &cp, nil
}

// DeleteDedicatedIPPool removes a dedicated IP pool.
func (b *InMemoryBackend) DeleteDedicatedIPPool(poolName string) error {
	b.mu.Lock("DeleteDedicatedIPPool")
	defer b.mu.Unlock()

	if _, ok := b.dedicatedIPPools[poolName]; !ok {
		return fmt.Errorf("%w: dedicated IP pool %s not found", ErrNotFound, poolName)
	}

	delete(b.dedicatedIPPools, poolName)

	return nil
}

// ListDedicatedIPPools returns all dedicated IP pool names.
func (b *InMemoryBackend) ListDedicatedIPPools(nextToken string, pageSize int) page.Page[string] {
	b.mu.RLock("ListDedicatedIPPools")
	defer b.mu.RUnlock()

	keys := collections.SortedKeys(b.dedicatedIPPools)

	return page.New(keys, nextToken, pageSize, sesv2DefaultMaxItems)
}

// GetDedicatedIP returns an empty dedicated IP (stub).
func (b *InMemoryBackend) GetDedicatedIP(_ string) (map[string]any, error) {
	return map[string]any{
		"WarmupPercentage": warmupPercentComplete,
		"WarmupStatus":     warmupDone,
	}, nil
}

// GetDedicatedIps returns empty dedicated IPs (stub).
func (b *InMemoryBackend) GetDedicatedIps() []map[string]any {
	return []map[string]any{}
}

// PutDedicatedIPInPool is a no-op stub.
func (b *InMemoryBackend) PutDedicatedIPInPool(_, _ string) error {
	return nil
}

// PutDedicatedIPPoolScalingAttributes updates a pool's scaling mode.
func (b *InMemoryBackend) PutDedicatedIPPoolScalingAttributes(poolName, scalingMode string) error {
	b.mu.Lock("PutDedicatedIPPoolScalingAttributes")
	defer b.mu.Unlock()

	pool, ok := b.dedicatedIPPools[poolName]
	if !ok {
		return fmt.Errorf("%w: dedicated IP pool %s not found", ErrNotFound, poolName)
	}

	pool.ScalingMode = scalingMode

	return nil
}

// PutDedicatedIPWarmupAttributes is a no-op stub.
func (b *InMemoryBackend) PutDedicatedIPWarmupAttributes(_, _ string) error {
	return nil
}

// ---- deliverability ----

// GetDeliverabilityDashboardOptions returns stub options.
func (b *InMemoryBackend) GetDeliverabilityDashboardOptions() (map[string]any, error) {
	return map[string]any{"DashboardEnabled": false}, nil
}

// PutDeliverabilityDashboardOption is a no-op stub.
func (b *InMemoryBackend) PutDeliverabilityDashboardOption() error {
	return nil
}

// GetDeliverabilityTestReport retrieves a test report.
func (b *InMemoryBackend) GetDeliverabilityTestReport(
	reportID string,
) (*DeliverabilityTestReport, error) {
	b.mu.RLock("GetDeliverabilityTestReport")
	defer b.mu.RUnlock()

	r, ok := b.deliverabilityTestReports[reportID]
	if !ok {
		return nil, fmt.Errorf("%w: deliverability test report %s not found", ErrNotFound, reportID)
	}

	cp := *r

	return &cp, nil
}

// ListDeliverabilityTestReports lists all test reports.
func (b *InMemoryBackend) ListDeliverabilityTestReports(
	nextToken string,
	pageSize int,
) page.Page[*DeliverabilityTestReport] {
	b.mu.RLock("ListDeliverabilityTestReports")
	defer b.mu.RUnlock()

	keys := collections.SortedKeys(b.deliverabilityTestReports)

	items := make([]*DeliverabilityTestReport, 0, len(keys))
	for _, k := range keys {
		cp := *b.deliverabilityTestReports[k]
		items = append(items, &cp)
	}

	return page.New(items, nextToken, pageSize, sesv2DefaultMaxItems)
}

// GetDomainDeliverabilityCampaign returns an empty stub.
func (b *InMemoryBackend) GetDomainDeliverabilityCampaign(_, _ string) (map[string]any, error) {
	return map[string]any{}, nil
}

// GetDomainStatisticsReport returns a stub report.
func (b *InMemoryBackend) GetDomainStatisticsReport(_, _, _ string) (map[string]any, error) {
	return map[string]any{}, nil
}

// ListDomainDeliverabilityCampaigns returns empty list.
func (b *InMemoryBackend) ListDomainDeliverabilityCampaigns(
	_, _, _, _ string,
) ([]map[string]any, string, error) {
	return []map[string]any{}, "", nil
}

// GetEmailAddressInsights returns a stub.
func (b *InMemoryBackend) GetEmailAddressInsights(_ string) (map[string]any, error) {
	return map[string]any{}, nil
}

// GetMessageInsights returns a stub.
func (b *InMemoryBackend) GetMessageInsights(_ string) (map[string]any, error) {
	return map[string]any{}, nil
}

// ListRecommendations returns empty list.
func (b *InMemoryBackend) ListRecommendations(_ string, _ int) ([]map[string]any, string, error) {
	return []map[string]any{}, "", nil
}

// ---- email templates ----

// GetEmailTemplate retrieves a template.
func (b *InMemoryBackend) GetEmailTemplate(name string) (*EmailTemplate, error) {
	b.mu.RLock("GetEmailTemplate")
	defer b.mu.RUnlock()

	t, ok := b.emailTemplates[name]
	if !ok {
		return nil, fmt.Errorf("%w: email template %s not found", ErrNotFound, name)
	}

	cp := *t

	return &cp, nil
}

// DeleteEmailTemplate removes a template.
func (b *InMemoryBackend) DeleteEmailTemplate(name string) error {
	b.mu.Lock("DeleteEmailTemplate")
	defer b.mu.Unlock()

	if _, ok := b.emailTemplates[name]; !ok {
		return fmt.Errorf("%w: email template %s not found", ErrNotFound, name)
	}

	delete(b.emailTemplates, name)

	return nil
}

// UpdateEmailTemplate updates a template.
func (b *InMemoryBackend) UpdateEmailTemplate(name string, content *EmailTemplateContent) error {
	b.mu.Lock("UpdateEmailTemplate")
	defer b.mu.Unlock()

	t, ok := b.emailTemplates[name]
	if !ok {
		return fmt.Errorf("%w: email template %s not found", ErrNotFound, name)
	}

	if content != nil {
		cp := *content
		t.TemplateContent = &cp
	}

	return nil
}

// ListEmailTemplates returns all email templates.
func (b *InMemoryBackend) ListEmailTemplates(
	nextToken string,
	pageSize int,
) page.Page[*EmailTemplate] {
	b.mu.RLock("ListEmailTemplates")
	defer b.mu.RUnlock()

	keys := collections.SortedKeys(b.emailTemplates)

	items := make([]*EmailTemplate, 0, len(keys))
	for _, k := range keys {
		cp := *b.emailTemplates[k]
		items = append(items, &cp)
	}

	return page.New(items, nextToken, pageSize, sesv2DefaultMaxItems)
}

// TestRenderEmailTemplate renders a template with merge data (stub).
func (b *InMemoryBackend) TestRenderEmailTemplate(name, templateData string) (string, error) {
	b.mu.RLock("TestRenderEmailTemplate")
	defer b.mu.RUnlock()

	t, ok := b.emailTemplates[name]
	if !ok {
		return "", fmt.Errorf("%w: email template %s not found", ErrNotFound, name)
	}

	_ = templateData

	rendered := ""
	if t.TemplateContent != nil {
		rendered = t.TemplateContent.HTML
		if rendered == "" {
			rendered = t.TemplateContent.Text
		}
	}

	return rendered, nil
}

// ---- export / import jobs ----

// CreateExportJob creates a new export job.
func (b *InMemoryBackend) CreateExportJob(dataSource string) (*ExportJob, error) {
	jobID := uuid.New().String()

	job := &ExportJob{
		JobID:     jobID,
		JobStatus: "CREATED",
		CreatedAt: time.Now(),
	}

	_ = dataSource

	b.mu.Lock("CreateExportJob")
	b.exportJobs[jobID] = job
	b.mu.Unlock()

	cp := *job

	return &cp, nil
}

// GetExportJob retrieves an export job.
func (b *InMemoryBackend) GetExportJob(jobID string) (*ExportJob, error) {
	b.mu.RLock("GetExportJob")
	defer b.mu.RUnlock()

	job, ok := b.exportJobs[jobID]
	if !ok {
		return nil, fmt.Errorf("%w: export job %s not found", ErrNotFound, jobID)
	}

	cp := *job

	return &cp, nil
}

// ListExportJobs returns all export jobs.
func (b *InMemoryBackend) ListExportJobs(nextToken string, pageSize int) page.Page[*ExportJob] {
	b.mu.RLock("ListExportJobs")
	defer b.mu.RUnlock()

	keys := collections.SortedKeys(b.exportJobs)

	items := make([]*ExportJob, 0, len(keys))
	for _, k := range keys {
		cp := *b.exportJobs[k]
		items = append(items, &cp)
	}

	return page.New(items, nextToken, pageSize, sesv2DefaultMaxItems)
}

// CreateImportJob creates an import job.
func (b *InMemoryBackend) CreateImportJob(dataSource string) (*ImportJob, error) {
	jobID := uuid.New().String()

	job := &ImportJob{
		JobID:     jobID,
		JobStatus: "CREATED",
		CreatedAt: time.Now(),
	}

	_ = dataSource

	b.mu.Lock("CreateImportJob")
	b.importJobs[jobID] = job
	b.mu.Unlock()

	cp := *job

	return &cp, nil
}

// GetImportJob retrieves an import job.
func (b *InMemoryBackend) GetImportJob(jobID string) (*ImportJob, error) {
	b.mu.RLock("GetImportJob")
	defer b.mu.RUnlock()

	job, ok := b.importJobs[jobID]
	if !ok {
		return nil, fmt.Errorf("%w: import job %s not found", ErrNotFound, jobID)
	}

	cp := *job

	return &cp, nil
}

// ListImportJobs returns all import jobs.
func (b *InMemoryBackend) ListImportJobs(nextToken string, pageSize int) page.Page[*ImportJob] {
	b.mu.RLock("ListImportJobs")
	defer b.mu.RUnlock()

	keys := collections.SortedKeys(b.importJobs)

	items := make([]*ImportJob, 0, len(keys))
	for _, k := range keys {
		cp := *b.importJobs[k]
		items = append(items, &cp)
	}

	return page.New(items, nextToken, pageSize, sesv2DefaultMaxItems)
}

// ---- email identity policies ----

// GetEmailIdentityPolicies returns policies for an identity.
func (b *InMemoryBackend) GetEmailIdentityPolicies(identity string) (map[string]string, error) {
	b.mu.RLock("GetEmailIdentityPolicies")
	defer b.mu.RUnlock()

	if _, ok := b.identities[identity]; !ok {
		return nil, fmt.Errorf("%w: identity %s not found", ErrNotFound, identity)
	}

	policies := b.emailIdentityPolicies[identity]
	out := make(map[string]string, len(policies))
	maps.Copy(out, policies)

	return out, nil
}

// DeleteEmailIdentityPolicy deletes a policy from an identity.
func (b *InMemoryBackend) DeleteEmailIdentityPolicy(identity, policyName string) error {
	b.mu.Lock("DeleteEmailIdentityPolicy")
	defer b.mu.Unlock()

	policies, ok := b.emailIdentityPolicies[identity]
	if !ok {
		return fmt.Errorf("%w: identity %s not found", ErrNotFound, identity)
	}

	if _, exists := policies[policyName]; !exists {
		return fmt.Errorf(
			"%w: policy %s not found for identity %s",
			ErrNotFound,
			policyName,
			identity,
		)
	}

	delete(policies, policyName)

	return nil
}

// UpdateEmailIdentityPolicy updates a policy for an identity.
func (b *InMemoryBackend) UpdateEmailIdentityPolicy(identity, policyName, policy string) error {
	b.mu.Lock("UpdateEmailIdentityPolicy")
	defer b.mu.Unlock()

	if _, ok := b.identities[identity]; !ok {
		return fmt.Errorf("%w: identity %s not found", ErrNotFound, identity)
	}

	if _, ok := b.emailIdentityPolicies[identity]; !ok {
		b.emailIdentityPolicies[identity] = make(map[string]string)
	}

	b.emailIdentityPolicies[identity][policyName] = policy

	return nil
}

// ---- configuration set operations ----

// GetConfigurationSetEventDestinations retrieves event destinations for a config set.
func (b *InMemoryBackend) GetConfigurationSetEventDestinations(
	configSetName string,
) ([]*EventDestination, error) {
	b.mu.RLock("GetConfigurationSetEventDestinations")
	defer b.mu.RUnlock()

	if _, ok := b.configurationSets[configSetName]; !ok {
		return nil, fmt.Errorf("%w: configuration set %s not found", ErrNotFound, configSetName)
	}

	dests := b.eventDestinations[configSetName]
	out := make([]*EventDestination, 0, len(dests))

	for _, d := range dests {
		cp := *d
		out = append(out, &cp)
	}

	return out, nil
}

// DeleteConfigurationSetEventDestination removes an event destination.
func (b *InMemoryBackend) DeleteConfigurationSetEventDestination(
	configSetName, destName string,
) error {
	b.mu.Lock("DeleteConfigurationSetEventDestination")
	defer b.mu.Unlock()

	dests, ok := b.eventDestinations[configSetName]
	if !ok {
		return fmt.Errorf("%w: configuration set %s not found", ErrNotFound, configSetName)
	}

	if _, exists := dests[destName]; !exists {
		return fmt.Errorf(
			"%w: event destination %s not found in %s",
			ErrNotFound,
			destName,
			configSetName,
		)
	}

	delete(dests, destName)

	return nil
}

// UpdateConfigurationSetEventDestination updates an event destination.
func (b *InMemoryBackend) UpdateConfigurationSetEventDestination(
	configSetName, destName string,
	enabled bool,
	matchingEventTypes []string,
) error {
	b.mu.Lock("UpdateConfigurationSetEventDestination")
	defer b.mu.Unlock()

	dests, ok := b.eventDestinations[configSetName]
	if !ok {
		return fmt.Errorf("%w: configuration set %s not found", ErrNotFound, configSetName)
	}

	dest, ok := dests[destName]
	if !ok {
		return fmt.Errorf(
			"%w: event destination %s not found in %s",
			ErrNotFound,
			destName,
			configSetName,
		)
	}

	dest.Enabled = enabled

	types := make([]string, len(matchingEventTypes))
	copy(types, matchingEventTypes)
	dest.MatchingEventTypes = types

	return nil
}

// PutConfigurationSetArchivingOptions validates the config set exists (archiving not modelled).
func (b *InMemoryBackend) PutConfigurationSetArchivingOptions(name string) error {
	b.mu.RLock("PutConfigurationSetArchivingOptions")
	defer b.mu.RUnlock()

	if _, ok := b.configurationSets[name]; !ok {
		return fmt.Errorf("%w: configuration set %s not found", ErrNotFound, name)
	}

	return nil
}

// PutConfigurationSetDeliveryOptions stores the TLS policy and sending pool name.
func (b *InMemoryBackend) PutConfigurationSetDeliveryOptions(
	name, tlsPolicy, sendingPoolName string,
) error {
	b.mu.Lock("PutConfigurationSetDeliveryOptions")
	defer b.mu.Unlock()

	cs, ok := b.configurationSets[name]
	if !ok {
		return fmt.Errorf("%w: configuration set %s not found", ErrNotFound, name)
	}

	cs.DeliveryTLSPolicy = tlsPolicy
	cs.DeliverySendingPoolName = sendingPoolName

	return nil
}

// PutConfigurationSetReputationOptions stores the reputation metrics flag.
func (b *InMemoryBackend) PutConfigurationSetReputationOptions(
	name string,
	metricsEnabled bool,
) error {
	b.mu.Lock("PutConfigurationSetReputationOptions")
	defer b.mu.Unlock()

	cs, ok := b.configurationSets[name]
	if !ok {
		return fmt.Errorf("%w: configuration set %s not found", ErrNotFound, name)
	}

	cs.ReputationMetricsEnabled = metricsEnabled

	return nil
}

// PutConfigurationSetSendingOptions enables or disables sending for the config set.
func (b *InMemoryBackend) PutConfigurationSetSendingOptions(
	name string,
	sendingEnabled bool,
) error {
	b.mu.Lock("PutConfigurationSetSendingOptions")
	defer b.mu.Unlock()

	cs, ok := b.configurationSets[name]
	if !ok {
		return fmt.Errorf("%w: configuration set %s not found", ErrNotFound, name)
	}

	cs.SendingEnabled = sendingEnabled

	return nil
}

// PutConfigurationSetSuppressionOptions stores the suppression reason list.
func (b *InMemoryBackend) PutConfigurationSetSuppressionOptions(
	name string,
	suppressedReasons []string,
) error {
	b.mu.Lock("PutConfigurationSetSuppressionOptions")
	defer b.mu.Unlock()

	cs, ok := b.configurationSets[name]
	if !ok {
		return fmt.Errorf("%w: configuration set %s not found", ErrNotFound, name)
	}

	reasons := make([]string, len(suppressedReasons))
	copy(reasons, suppressedReasons)
	cs.SuppressionReasons = reasons

	return nil
}

// PutConfigurationSetTrackingOptions stores the custom redirect domain and HTTPS policy.
func (b *InMemoryBackend) PutConfigurationSetTrackingOptions(
	name, customRedirectDomain, httpsPolicy string,
) error {
	b.mu.Lock("PutConfigurationSetTrackingOptions")
	defer b.mu.Unlock()

	cs, ok := b.configurationSets[name]
	if !ok {
		return fmt.Errorf("%w: configuration set %s not found", ErrNotFound, name)
	}

	cs.TrackingCustomRedirectDomain = customRedirectDomain
	cs.TrackingHTTPSPolicy = httpsPolicy

	return nil
}

// PutConfigurationSetVdmOptions validates the config set exists (VDM not modelled).
func (b *InMemoryBackend) PutConfigurationSetVdmOptions(name string) error {
	b.mu.RLock("PutConfigurationSetVdmOptions")
	defer b.mu.RUnlock()

	if _, ok := b.configurationSets[name]; !ok {
		return fmt.Errorf("%w: configuration set %s not found", ErrNotFound, name)
	}

	return nil
}

// ---- email identity attributes ----

// PutEmailIdentityConfigurationSetAttributes associates a configuration set with the identity.
func (b *InMemoryBackend) PutEmailIdentityConfigurationSetAttributes(
	identity, configSetName string,
) error {
	b.mu.Lock("PutEmailIdentityConfigurationSetAttributes")
	defer b.mu.Unlock()

	ei, ok := b.identities[identity]
	if !ok {
		return fmt.Errorf("%w: identity %s not found", ErrNotFound, identity)
	}

	ei.ConfigurationSetName = configSetName

	return nil
}

// PutEmailIdentityDkimAttributes enables or disables DKIM signing for the identity.
func (b *InMemoryBackend) PutEmailIdentityDkimAttributes(
	identity string,
	signingEnabled bool,
) error {
	b.mu.Lock("PutEmailIdentityDkimAttributes")
	defer b.mu.Unlock()

	ei, ok := b.identities[identity]
	if !ok {
		return fmt.Errorf("%w: identity %s not found", ErrNotFound, identity)
	}

	ei.DkimSigningEnabled = signingEnabled

	return nil
}

// PutEmailIdentityDkimSigningAttributes validates the identity exists (BYODKIM not modelled).
func (b *InMemoryBackend) PutEmailIdentityDkimSigningAttributes(identity string) error {
	b.mu.RLock("PutEmailIdentityDkimSigningAttributes")
	defer b.mu.RUnlock()

	if _, ok := b.identities[identity]; !ok {
		return fmt.Errorf("%w: identity %s not found", ErrNotFound, identity)
	}

	return nil
}

// PutEmailIdentityFeedbackAttributes sets the feedback forwarding flag for the identity.
func (b *InMemoryBackend) PutEmailIdentityFeedbackAttributes(
	identity string,
	emailForwardingEnabled bool,
) error {
	b.mu.Lock("PutEmailIdentityFeedbackAttributes")
	defer b.mu.Unlock()

	ei, ok := b.identities[identity]
	if !ok {
		return fmt.Errorf("%w: identity %s not found", ErrNotFound, identity)
	}

	ei.FeedbackForwarding = emailForwardingEnabled

	return nil
}

// PutEmailIdentityMailFromAttributes stores the custom MAIL FROM domain and failure behaviour.
func (b *InMemoryBackend) PutEmailIdentityMailFromAttributes(
	identity, mailFromDomain, behaviorOnMxFailure string,
) error {
	b.mu.Lock("PutEmailIdentityMailFromAttributes")
	defer b.mu.Unlock()

	ei, ok := b.identities[identity]
	if !ok {
		return fmt.Errorf("%w: identity %s not found", ErrNotFound, identity)
	}

	ei.MailFromDomain = mailFromDomain

	if mailFromDomain == "" {
		ei.MailFromDomainStatus = ""
		ei.BehaviorOnMxFailure = ""
	} else {
		ei.MailFromDomainStatus = mailFromStatusPending
		if behaviorOnMxFailure != "" {
			ei.BehaviorOnMxFailure = behaviorOnMxFailure
		} else {
			ei.BehaviorOnMxFailure = behaviorOnMxFailureUseDefault
		}
	}

	return nil
}

// ---- email sending ----

// SendBulkEmail sends bulk emails (stub — records sent emails).
func (b *InMemoryBackend) SendBulkEmail(
	fromEmailAddress string,
	bulkEmailEntries []map[string]any,
) ([]map[string]any, error) {
	results := make([]map[string]any, 0, len(bulkEmailEntries))

	for range bulkEmailEntries {
		msgID := "sesv2-bulk-" + uuid.New().String()
		_, _ = b.SendEmail(fromEmailAddress, []string{}, "", "", "")
		results = append(results, map[string]any{
			"MessageId": msgID,
			keyStatus:   keyStatusSuccess,
		})
	}

	return results, nil
}

// SendCustomVerificationEmail sends a custom verification email (stub).
func (b *InMemoryBackend) SendCustomVerificationEmail(
	emailAddress, templateName string,
) (string, error) {
	b.mu.RLock("SendCustomVerificationEmail")

	_, ok := b.customVerificationTemplates[templateName]
	b.mu.RUnlock()

	if !ok {
		return "", fmt.Errorf(
			"%w: custom verification template %s not found",
			ErrNotFound,
			templateName,
		)
	}

	msgID := "sesv2-cvr-" + uuid.New().String()
	_, _ = b.SendEmail("noreply@example.com", []string{emailAddress}, "Verify your email", "", "")

	return msgID, nil
}

// ---- multi-region endpoints (stubs) ----

// CreateMultiRegionEndpoint creates a multi-region endpoint (stub).
func (b *InMemoryBackend) CreateMultiRegionEndpoint(endpointName string) (string, error) {
	_ = endpointName

	return "CREATING", nil
}

// GetMultiRegionEndpoint returns a stub.
func (b *InMemoryBackend) GetMultiRegionEndpoint(_ string) (map[string]any, error) {
	return map[string]any{keyStatus: "READY"}, nil
}

// DeleteMultiRegionEndpoint is a no-op stub.
func (b *InMemoryBackend) DeleteMultiRegionEndpoint(_ string) error {
	return nil
}

// ListMultiRegionEndpoints returns empty list.
func (b *InMemoryBackend) ListMultiRegionEndpoints(
	_ string,
	_ int,
) ([]map[string]any, string, error) {
	return []map[string]any{}, "", nil
}

// ---- tenants (stubs) ----

// CreateTenant creates a tenant (stub).
func (b *InMemoryBackend) CreateTenant(_ string) (map[string]any, error) {
	return map[string]any{}, nil
}

// GetTenant returns a stub.
func (b *InMemoryBackend) GetTenant(_ string) (map[string]any, error) {
	return map[string]any{}, nil
}

// DeleteTenant is a no-op stub.
func (b *InMemoryBackend) DeleteTenant(_ string) error {
	return nil
}

// ListTenants returns empty list.
func (b *InMemoryBackend) ListTenants(_ string, _ int) ([]map[string]any, string, error) {
	return []map[string]any{}, "", nil
}

// CreateTenantResourceAssociation is a no-op stub.
func (b *InMemoryBackend) CreateTenantResourceAssociation(_, _ string) error {
	return nil
}

// DeleteTenantResourceAssociation is a no-op stub.
func (b *InMemoryBackend) DeleteTenantResourceAssociation(_, _ string) error {
	return nil
}

// ListResourceTenants returns empty list.
func (b *InMemoryBackend) ListResourceTenants(_ string, _ int) ([]map[string]any, string, error) {
	return []map[string]any{}, "", nil
}

// ListTenantResources returns empty list.
func (b *InMemoryBackend) ListTenantResources(_ string, _ int) ([]map[string]any, string, error) {
	return []map[string]any{}, "", nil
}

// ---- reputation entities (stubs) ----

// GetReputationEntity returns a stub.
func (b *InMemoryBackend) GetReputationEntity(_ string) (map[string]any, error) {
	return map[string]any{}, nil
}

// ListReputationEntities returns empty list.
func (b *InMemoryBackend) ListReputationEntities(
	_ string,
	_ int,
) ([]map[string]any, string, error) {
	return []map[string]any{}, "", nil
}

// UpdateReputationEntityCustomerManagedStatus is a no-op stub.
func (b *InMemoryBackend) UpdateReputationEntityCustomerManagedStatus(_ string) error {
	return nil
}

// UpdateReputationEntityPolicy is a no-op stub.
func (b *InMemoryBackend) UpdateReputationEntityPolicy(_, _ string) error {
	return nil
}
