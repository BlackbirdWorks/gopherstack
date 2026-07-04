package sesv2

import (
	"encoding/json"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// ---- account ----

// AccountDetails stores account-level SESv2 settings.
type AccountDetails struct {
	VdmAttributes         map[string]any `json:"vdmAttributes,omitempty"`
	MailType              string         `json:"mailType"`
	WebsiteURL            string         `json:"websiteURL"`
	ContactLanguage       string         `json:"contactLanguage"`
	UseCaseName           string         `json:"useCaseName"`
	SuppressionAttributes []string       `json:"suppressionAttributes,omitempty"`
	SendingEnabled        bool           `json:"sendingEnabled"`
	AutoWarmupEnabled     bool           `json:"autoWarmupEnabled,omitempty"`
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

func (b *InMemoryBackend) PutAccountSuppressionAttributes(suppressedReasons []string) error {
	b.mu.Lock("PutAccountSuppressionAttributes")
	defer b.mu.Unlock()

	if b.accountDetails == nil {
		b.accountDetails = &AccountDetails{}
	}

	b.accountDetails.SuppressionAttributes = suppressedReasons

	return nil
}

func (b *InMemoryBackend) PutAccountVdmAttributes(vdmAttributes map[string]any) error {
	b.mu.Lock("PutAccountVdmAttributes")
	defer b.mu.Unlock()

	if b.accountDetails == nil {
		b.accountDetails = &AccountDetails{}
	}

	b.accountDetails.VdmAttributes = vdmAttributes

	return nil
}

func (b *InMemoryBackend) PutAccountDedicatedIPWarmupAttributes(autoWarmupEnabled bool) error {
	b.mu.Lock("PutAccountDedicatedIPWarmupAttributes")
	defer b.mu.Unlock()

	if b.accountDetails == nil {
		b.accountDetails = &AccountDetails{}
	}

	b.accountDetails.AutoWarmupEnabled = autoWarmupEnabled

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

// dedicatedIPToMap renders a dedicated IP as the AWS-shaped response map.
func dedicatedIPToMap(ip *DedicatedIP) map[string]any {
	return map[string]any{
		"Ip":               ip.IP,
		"PoolName":         ip.PoolName,
		"WarmupPercentage": ip.WarmupPercentage,
		"WarmupStatus":     ip.WarmupStatus,
	}
}

// GetDedicatedIP returns the stored dedicated IP attributes. IPs that have never
// been assigned to a pool or warmed up are reported as fully warmed up, matching
// the prior stub behaviour for IPs SES manages implicitly.
func (b *InMemoryBackend) GetDedicatedIP(ip string) (map[string]any, error) {
	b.mu.RLock("GetDedicatedIP")
	defer b.mu.RUnlock()

	if d, ok := b.dedicatedIPs[ip]; ok {
		return dedicatedIPToMap(d), nil
	}

	return map[string]any{
		"Ip":               ip,
		"WarmupPercentage": warmupPercentComplete,
		"WarmupStatus":     warmupDone,
	}, nil
}

// GetDedicatedIps returns all tracked dedicated IPs.
func (b *InMemoryBackend) GetDedicatedIps() []map[string]any {
	b.mu.RLock("GetDedicatedIps")
	defer b.mu.RUnlock()

	keys := collections.SortedKeys(b.dedicatedIPs)

	out := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		out = append(out, dedicatedIPToMap(b.dedicatedIPs[k]))
	}

	return out
}

// dedicatedIPLocked returns the tracked dedicated IP, creating a default entry if
// it does not yet exist. Callers must hold the write lock.
func (b *InMemoryBackend) dedicatedIPLocked(ip string) *DedicatedIP {
	d, ok := b.dedicatedIPs[ip]
	if !ok {
		d = &DedicatedIP{
			IP:               ip,
			WarmupPercentage: warmupPercentComplete,
			WarmupStatus:     warmupDone,
		}
		b.dedicatedIPs[ip] = d
	}

	return d
}

// PutDedicatedIPInPool moves a dedicated IP into the requested pool. The
// destination pool must exist.
func (b *InMemoryBackend) PutDedicatedIPInPool(ip, poolName string) error {
	b.mu.Lock("PutDedicatedIPInPool")
	defer b.mu.Unlock()

	if _, ok := b.dedicatedIPPools[poolName]; !ok {
		return fmt.Errorf("%w: dedicated IP pool %s not found", ErrNotFound, poolName)
	}

	b.dedicatedIPLocked(ip).PoolName = poolName

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

// PutDedicatedIPWarmupAttributes records the warmup percentage for a dedicated IP
// and derives the warmup status from it.
func (b *InMemoryBackend) PutDedicatedIPWarmupAttributes(ip string, warmupPercentage int) error {
	b.mu.Lock("PutDedicatedIPWarmupAttributes")
	defer b.mu.Unlock()

	d := b.dedicatedIPLocked(ip)
	d.WarmupPercentage = warmupPercentage

	if warmupPercentage >= warmupPercentComplete {
		d.WarmupStatus = warmupDone
	} else {
		d.WarmupStatus = warmupInProgress
	}

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

func (b *InMemoryBackend) GetDomainDeliverabilityCampaign(domain, campaignID string) (map[string]any, error) {
	now := float64(time.Now().Unix())

	return map[string]any{
		"CampaignId":        campaignID,
		"FromAddress":       "sender@" + domain,
		"Subject":           "",
		"FirstSeenDateTime": now,
		"LastSeenDateTime":  now,
		"InboxCount":        float64(0),
		"SpamCount":         float64(0),
		"ReadRate":          float64(0),
		"DeleteRate":        float64(0),
		"ReadDeleteRate":    float64(0),
		"ProjectedVolume":   float64(0),
		"Esps":              []any{},
		"SendingIps":        []any{},
	}, nil
}

func (b *InMemoryBackend) GetDomainStatisticsReport(domain, startDate, endDate string) (map[string]any, error) {
	_ = startDate
	_ = endDate

	return map[string]any{
		"Domain": domain,
		"OverallVolume": map[string]any{
			"VolumeStatistics": map[string]any{
				"InboxRawCount":  float64(0),
				"SpamRawCount":   float64(0),
				"ProjectedInbox": float64(0),
				"ProjectedSpam":  float64(0),
			},
			"ReadRatePercent":     float64(0),
			"DomainIspPlacements": []any{},
		},
		"DailyVolumes": []any{},
	}, nil
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

// TestRenderEmailTemplate renders a template with merge data.
func (b *InMemoryBackend) TestRenderEmailTemplate(name, templateData string) (string, error) {
	b.mu.RLock("TestRenderEmailTemplate")
	defer b.mu.RUnlock()

	t, ok := b.emailTemplates[name]
	if !ok {
		return "", fmt.Errorf("%w: email template %s not found", ErrNotFound, name)
	}

	vars := map[string]string{}
	if strings.TrimSpace(templateData) != "" {
		raw := map[string]any{}
		if err := json.Unmarshal([]byte(templateData), &raw); err != nil {
			return "", fmt.Errorf("%w: TemplateData must be valid JSON", ErrInvalidInput)
		}
		for k, v := range raw {
			vars[k] = fmt.Sprintf("%v", v)
		}
	}

	renderVars := func(s string) string {
		for k, v := range vars {
			s = strings.ReplaceAll(s, "{{"+k+"}}", v)
		}

		return s
	}

	subject, html, text := "", "", ""
	if t.TemplateContent != nil {
		subject = renderVars(t.TemplateContent.Subject)
		html = renderVars(t.TemplateContent.HTML)
		text = renderVars(t.TemplateContent.Text)
	}

	return strings.Join([]string{subject, html, text}, "\n---\n"), nil
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

// PutConfigurationSetArchivingOptions stores the archive ARN on the config set.
func (b *InMemoryBackend) PutConfigurationSetArchivingOptions(name, archiveARN string) error {
	b.mu.Lock("PutConfigurationSetArchivingOptions")
	defer b.mu.Unlock()

	cs, ok := b.configurationSets[name]
	if !ok {
		return fmt.Errorf("%w: configuration set %s not found", ErrNotFound, name)
	}

	cs.ArchivingOptions = &ArchivingOptions{ArchiveARN: archiveARN}

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

// PutConfigurationSetVdmOptions stores the VDM options on the config set.
func (b *InMemoryBackend) PutConfigurationSetVdmOptions(
	name string,
	dashboardOptions, guardianOptions map[string]any,
) error {
	b.mu.Lock("PutConfigurationSetVdmOptions")
	defer b.mu.Unlock()

	cs, ok := b.configurationSets[name]
	if !ok {
		return fmt.Errorf("%w: configuration set %s not found", ErrNotFound, name)
	}

	cs.VdmOptions = &VdmOptions{
		DashboardOptions: dashboardOptions,
		GuardianOptions:  guardianOptions,
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

// SendBulkEmail sends bulk emails — records sent emails with actual recipients.
func (b *InMemoryBackend) SendBulkEmail(
	fromEmailAddress string,
	bulkEmailEntries []map[string]any,
) ([]map[string]any, error) {
	results := make([]map[string]any, 0, len(bulkEmailEntries))

	for _, entry := range bulkEmailEntries {
		var toAddresses []string
		if dest, destOK := entry["Destination"].(map[string]any); destOK {
			if raw, rawOK := dest["ToAddresses"].([]any); rawOK {
				for _, v := range raw {
					if s, strOK := v.(string); strOK {
						toAddresses = append(toAddresses, s)
					}
				}
			}
		}

		msgID, _ := b.SendEmail(fromEmailAddress, toAddresses, "", "", "")
		if msgID == "" {
			msgID = "sesv2-bulk-" + uuid.New().String()
		}

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

	email := Email{
		MessageID: msgID,
		From:      "noreply@example.com",
		To:        []string{emailAddress},
		Subject:   "Verify your email",
		Timestamp: time.Now(),
	}

	b.mu.Lock("SendCustomVerificationEmail-record")
	b.emails = append(b.emails, email)
	if len(b.emails) >= emailCompactionHighWater {
		trimmed := make([]Email, maxRetainedEmails, emailCompactionHighWater)
		copy(trimmed, b.emails[len(b.emails)-maxRetainedEmails:])
		b.emails = trimmed
	}
	b.mu.Unlock()

	return msgID, nil
}

// ---- multi-region endpoints ----

func (b *InMemoryBackend) CreateMultiRegionEndpoint(endpointName string) (string, error) {
	b.mu.Lock("CreateMultiRegionEndpoint")
	defer b.mu.Unlock()

	b.multiRegionEndpoints[endpointName] = map[string]any{
		"EndpointName": endpointName,
		keyStatus:      "READY",
	}

	return "READY", nil
}

func (b *InMemoryBackend) GetMultiRegionEndpoint(endpointName string) (map[string]any, error) {
	b.mu.RLock("GetMultiRegionEndpoint")
	defer b.mu.RUnlock()

	ep, ok := b.multiRegionEndpoints[endpointName]
	if !ok {
		return nil, fmt.Errorf("%w: MultiRegionEndpoint %s not found", ErrNotFound, endpointName)
	}

	out := make(map[string]any, len(ep))
	maps.Copy(out, ep)

	return out, nil
}

func (b *InMemoryBackend) DeleteMultiRegionEndpoint(endpointName string) error {
	b.mu.Lock("DeleteMultiRegionEndpoint")
	defer b.mu.Unlock()

	delete(b.multiRegionEndpoints, endpointName)

	return nil
}

func (b *InMemoryBackend) ListMultiRegionEndpoints(
	nextToken string,
	pageSize int,
) ([]map[string]any, string, error) {
	b.mu.RLock("ListMultiRegionEndpoints")

	all := make([]map[string]any, 0, len(b.multiRegionEndpoints))
	for _, ep := range b.multiRegionEndpoints {
		cp := make(map[string]any, len(ep))
		maps.Copy(cp, ep)
		all = append(all, cp)
	}

	b.mu.RUnlock()

	return paginateMaps(all, nextToken, pageSize, "EndpointName")
}

const keyTenantName = "TenantName"

// paginateMaps applies simple nextToken/pageSize pagination to a slice of
// maps, using keyName as the cursor field. Returns the page, next token, and nil error.
func paginateMaps(
	all []map[string]any,
	nextToken string,
	pageSize int,
	keyName string,
) ([]map[string]any, string, error) {
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 100
	}

	start := 0
	if nextToken != "" {
		for i, item := range all {
			if item[keyName] == nextToken {
				start = i

				break
			}
		}
	}

	end := start + pageSize

	var next string
	if end < len(all) {
		v, ok := all[end][keyName].(string)
		if ok {
			next = v
		}
	} else {
		end = len(all)
	}

	return all[start:end], next, nil
}

// ---- tenants ----

func (b *InMemoryBackend) CreateTenant(tenantName string) (map[string]any, error) {
	b.mu.Lock("CreateTenant")
	defer b.mu.Unlock()

	b.tenants[tenantName] = map[string]any{keyTenantName: tenantName, keyStatus: "ACTIVE"}

	return map[string]any{keyTenantName: tenantName}, nil
}

func (b *InMemoryBackend) GetTenant(tenantName string) (map[string]any, error) {
	b.mu.RLock("GetTenant")
	defer b.mu.RUnlock()

	t, ok := b.tenants[tenantName]
	if !ok {
		return nil, fmt.Errorf("%w: Tenant %s not found", ErrNotFound, tenantName)
	}

	out := make(map[string]any, len(t))
	maps.Copy(out, t)

	return out, nil
}

func (b *InMemoryBackend) DeleteTenant(tenantName string) error {
	b.mu.Lock("DeleteTenant")
	defer b.mu.Unlock()

	delete(b.tenants, tenantName)

	return nil
}

func (b *InMemoryBackend) ListTenants(nextToken string, pageSize int) ([]map[string]any, string, error) {
	b.mu.RLock("ListTenants")

	all := make([]map[string]any, 0, len(b.tenants))
	for _, t := range b.tenants {
		cp := make(map[string]any, len(t))
		maps.Copy(cp, t)
		all = append(all, cp)
	}

	b.mu.RUnlock()

	return paginateMaps(all, nextToken, pageSize, keyTenantName)
}

func (b *InMemoryBackend) CreateTenantResourceAssociation(tenantName, resourceArn string) error {
	b.mu.Lock("CreateTenantResourceAssociation")
	defer b.mu.Unlock()

	b.tenantResources[tenantName] = append(b.tenantResources[tenantName], resourceArn)
	b.resourceTenants[resourceArn] = append(b.resourceTenants[resourceArn], tenantName)

	return nil
}

func (b *InMemoryBackend) DeleteTenantResourceAssociation(tenantName, resourceArn string) error {
	b.mu.Lock("DeleteTenantResourceAssociation")
	defer b.mu.Unlock()

	b.tenantResources[tenantName] = removeString(b.tenantResources[tenantName], resourceArn)
	b.resourceTenants[resourceArn] = removeString(b.resourceTenants[resourceArn], tenantName)

	return nil
}

func (b *InMemoryBackend) ListResourceTenants(
	resourceArn string,
	_ int,
) ([]map[string]any, string, error) {
	b.mu.RLock("ListResourceTenants")
	names := b.resourceTenants[resourceArn]
	b.mu.RUnlock()

	out := make([]map[string]any, 0, len(names))
	for _, n := range names {
		out = append(out, map[string]any{keyTenantName: n})
	}

	return out, "", nil
}

func (b *InMemoryBackend) ListTenantResources(
	tenantName string,
	_ int,
) ([]map[string]any, string, error) {
	b.mu.RLock("ListTenantResources")
	arns := b.tenantResources[tenantName]
	b.mu.RUnlock()

	out := make([]map[string]any, 0, len(arns))
	for _, a := range arns {
		out = append(out, map[string]any{"ResourceArn": a})
	}

	return out, "", nil
}

func removeString(s []string, v string) []string {
	out := s[:0]
	for _, x := range s {
		if x != v {
			out = append(out, x)
		}
	}

	return out
}

// ---- reputation entities ----

// reputationEntityToMap renders a reputation entity as the AWS-shaped response map.
func reputationEntityToMap(e *ReputationEntity) map[string]any {
	m := map[string]any{
		"ReputationEntityReference": e.EntityRef,
	}

	if e.EntityType != "" {
		m["ReputationEntityType"] = e.EntityType
	}

	if e.CustomerManagedStatus != "" {
		m["CustomerManagedStatus"] = map[string]any{keyStatus: e.CustomerManagedStatus}
	}

	if e.ReputationPolicy != "" {
		m["ReputationManagementPolicy"] = e.ReputationPolicy
	}

	return m
}

// reputationEntityLocked returns the tracked reputation entity, creating an entry
// if it does not yet exist. Callers must hold the write lock.
func (b *InMemoryBackend) reputationEntityLocked(entityID string) *ReputationEntity {
	e, ok := b.reputationEntities[entityID]
	if !ok {
		e = &ReputationEntity{EntityRef: entityID}
		b.reputationEntities[entityID] = e
	}

	return e
}

// GetReputationEntity returns the stored reputation entity attributes. Entities
// in SES exist implicitly for every configuration set and identity, so an entity
// that has never been updated is reported with its reference and no overrides
// rather than as not-found.
func (b *InMemoryBackend) GetReputationEntity(entityID string) (map[string]any, error) {
	b.mu.RLock("GetReputationEntity")
	defer b.mu.RUnlock()

	if e, ok := b.reputationEntities[entityID]; ok {
		return reputationEntityToMap(e), nil
	}

	return reputationEntityToMap(&ReputationEntity{EntityRef: entityID}), nil
}

// ListReputationEntities returns all tracked reputation entities.
func (b *InMemoryBackend) ListReputationEntities(
	_ string,
	_ int,
) ([]map[string]any, string, error) {
	b.mu.RLock("ListReputationEntities")
	defer b.mu.RUnlock()

	keys := collections.SortedKeys(b.reputationEntities)

	out := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		out = append(out, reputationEntityToMap(b.reputationEntities[k]))
	}

	return out, "", nil
}

// UpdateReputationEntityCustomerManagedStatus stores the customer-managed status.
func (b *InMemoryBackend) UpdateReputationEntityCustomerManagedStatus(
	entityID, status string,
) error {
	b.mu.Lock("UpdateReputationEntityCustomerManagedStatus")
	defer b.mu.Unlock()

	b.reputationEntityLocked(entityID).CustomerManagedStatus = status

	return nil
}

// UpdateReputationEntityPolicy stores the reputation management policy.
func (b *InMemoryBackend) UpdateReputationEntityPolicy(entityID, policy string) error {
	b.mu.Lock("UpdateReputationEntityPolicy")
	defer b.mu.Unlock()

	b.reputationEntityLocked(entityID).ReputationPolicy = policy

	return nil
}
