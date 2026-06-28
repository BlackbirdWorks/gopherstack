package sesv2

import (
	"errors"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// Errors returned by the SES v2 backend.
var (
	ErrNotFound      = errors.New("NotFoundException")
	ErrAlreadyExists = errors.New("AlreadyExistsException")
	ErrInvalidInput  = errors.New("BadRequestException")
)

// Aliases for backward compatibility within the package.
var (
	ErrIdentityNotFound       = ErrNotFound
	ErrIdentityAlreadyExists  = ErrAlreadyExists
	ErrConfigSetNotFound      = ErrNotFound
	ErrConfigSetAlreadyExists = ErrAlreadyExists
	ErrInvalidParameter       = ErrInvalidInput
)

const (
	scalingModeStandard                = "STANDARD"
	scalingModeManaged                 = "MANAGED"
	deliverabilityTestStatusInProgress = "IN_PROGRESS"
	exportJobStatusCancelled           = "CANCELLED"

	// identity / DKIM defaults.
	dkimStatusSuccess             = "SUCCESS"
	verificationStatusSuccess     = "SUCCESS"
	mailFromStatusPending         = "PENDING"
	behaviorOnMxFailureUseDefault = "USE_DEFAULT_VALUE"
)

// maxRetainedEmails is the maximum number of sent emails retained in memory.
// When the cap is exceeded the oldest entries are dropped FIFO so a long-running
// instance cannot leak memory through repeated SendEmail calls.
const maxRetainedEmails = 10000

// emailCompactionHighWater is the slice length that triggers compaction.
// Compacting only when the slice has grown to twice the cap keeps
// trimming amortized O(1) per SendEmail.
const emailCompactionHighWater = maxRetainedEmails + maxRetainedEmails

// EmailIdentity represents a verified email address or domain identity.
type EmailIdentity struct {
	Tags                 map[string]string `json:"tags,omitempty"`
	Identity             string            `json:"identity"`
	IdentityType         string            `json:"identityType"`
	ConfigurationSetName string            `json:"configurationSetName,omitempty"`
	MailFromDomain       string            `json:"mailFromDomain,omitempty"`
	MailFromDomainStatus string            `json:"mailFromDomainStatus,omitempty"`
	BehaviorOnMxFailure  string            `json:"behaviorOnMxFailure,omitempty"`
	VerificationStatus   string            `json:"verificationStatus"`
	DkimStatus           string            `json:"dkimStatus"`
	DkimTokens           []string          `json:"dkimTokens,omitempty"`
	VerifiedForSending   bool              `json:"verifiedForSending"`
	FeedbackForwarding   bool              `json:"feedbackForwarding"`
	DkimSigningEnabled   bool              `json:"dkimSigningEnabled"`
}

// ConfigurationSet represents a SES v2 configuration set.
type ConfigurationSet struct {
	CreatedAt                    time.Time         `json:"createdAt"`
	Name                         string            `json:"name"`
	Tags                         map[string]string `json:"tags,omitempty"`
	TrackingCustomRedirectDomain string            `json:"trackingCustomRedirectDomain,omitempty"`
	TrackingHTTPSPolicy          string            `json:"trackingHttpsPolicy,omitempty"`
	DeliveryTLSPolicy            string            `json:"deliveryTlsPolicy,omitempty"`
	DeliverySendingPoolName      string            `json:"deliverySendingPoolName,omitempty"`
	SuppressionReasons           []string          `json:"suppressionReasons,omitempty"`
	SendingEnabled               bool              `json:"sendingEnabled"`
	ReputationMetricsEnabled     bool              `json:"reputationMetricsEnabled"`
}

// Email captures a sent email for local inspection.
type Email struct {
	Timestamp time.Time `json:"timestamp"`
	From      string    `json:"from"`
	Subject   string    `json:"subject"`
	BodyHTML  string    `json:"bodyHTML"`
	BodyText  string    `json:"bodyText"`
	MessageID string    `json:"messageID"`
	To        []string  `json:"to"`
}

// EventDestination represents an event destination for a configuration set.
type EventDestination struct {
	CreatedAt            time.Time `json:"createdAt"`
	Name                 string    `json:"name"`
	ConfigurationSetName string    `json:"configurationSetName"`
	MatchingEventTypes   []string  `json:"matchingEventTypes"`
	Enabled              bool      `json:"enabled"`
}

// ContactList represents a SES v2 contact list.
type ContactList struct {
	CreatedAt     time.Time         `json:"createdAt"`
	LastUpdatedAt time.Time         `json:"lastUpdatedAt"`
	Tags          map[string]string `json:"tags"`
	Name          string            `json:"name"`
	Description   string            `json:"description"`
}

// TopicPreference represents a contact topic preference.
type TopicPreference struct {
	TopicName          string `json:"topicName"`
	SubscriptionStatus string `json:"subscriptionStatus"`
}

// Contact represents a contact in a contact list.
type Contact struct {
	CreatedAt        time.Time         `json:"createdAt"`
	LastUpdatedAt    time.Time         `json:"lastUpdatedAt"`
	EmailAddress     string            `json:"emailAddress"`
	ContactListName  string            `json:"contactListName"`
	TopicPreferences []TopicPreference `json:"topicPreferences"`
	UnsubscribeAll   bool              `json:"unsubscribeAll"`
}

// CustomVerificationEmailTemplate represents a custom verification email template.
type CustomVerificationEmailTemplate struct {
	TemplateName          string `json:"templateName"`
	FromEmailAddress      string `json:"fromEmailAddress"`
	TemplateSubject       string `json:"templateSubject"`
	TemplateContent       string `json:"templateContent"`
	SuccessRedirectionURL string `json:"successRedirectionURL"`
	FailureRedirectionURL string `json:"failureRedirectionURL"`
}

// DedicatedIPPool represents a SES v2 dedicated IP pool.
type DedicatedIPPool struct {
	PoolName    string `json:"poolName"`
	ScalingMode string `json:"scalingMode"`
}

// DeliverabilityTestReport represents a deliverability test report.
type DeliverabilityTestReport struct {
	CreateDate               time.Time `json:"createDate"`
	ReportID                 string    `json:"reportId"`
	ReportName               string    `json:"reportName"`
	FromEmailAddress         string    `json:"fromEmailAddress"`
	DeliverabilityTestStatus string    `json:"deliverabilityTestStatus"`
}

// EmailTemplateContent holds the content for an email template.
type EmailTemplateContent struct {
	Subject string `json:"subject"`
	Text    string `json:"text"`
	HTML    string `json:"html"`
}

// EmailTemplate represents a SES v2 email template.
type EmailTemplate struct {
	CreatedAt       time.Time             `json:"createdAt"`
	TemplateContent *EmailTemplateContent `json:"templateContent"`
	TemplateName    string                `json:"templateName"`
}

// ExportJob represents a SES v2 export job (minimal model for CancelExportJob).
type ExportJob struct {
	CreatedAt time.Time `json:"createdAt"`
	JobID     string    `json:"jobId"`
	JobStatus string    `json:"jobStatus"`
}

// MetricDataQuery represents a single query for BatchGetMetricData.
type MetricDataQuery struct {
	ID        string `json:"id"`
	Namespace string `json:"namespace"`
	Metric    string `json:"metric"`
}

// MetricDataResult is the result for a single metric query.
type MetricDataResult struct {
	ID         string      `json:"id"`
	Timestamps []time.Time `json:"timestamps"`
	Values     []float64   `json:"values"`
}

// InMemoryBackend is an in-memory store for SES v2 email identities, emails, and configuration sets.
type InMemoryBackend struct {
	identities                  map[string]*EmailIdentity
	configurationSets           map[string]*ConfigurationSet
	eventDestinations           map[string]map[string]*EventDestination
	contactLists                map[string]*ContactList
	contacts                    map[string]map[string]*Contact
	customVerificationTemplates map[string]*CustomVerificationEmailTemplate
	dedicatedIPPools            map[string]*DedicatedIPPool
	deliverabilityTestReports   map[string]*DeliverabilityTestReport
	emailTemplates              map[string]*EmailTemplate
	exportJobs                  map[string]*ExportJob
	importJobs                  map[string]*ImportJob
	suppressedDestinations      map[string]*SuppressedDestination
	emailIdentityPolicies       map[string]map[string]string
	resourceTags                map[string]map[string]string
	multiRegionEndpoints        map[string]map[string]any
	tenants                     map[string]map[string]any
	tenantResources             map[string][]string
	resourceTenants             map[string][]string
	accountDetails              *AccountDetails
	mu                          *lockmetrics.RWMutex
	region                      string
	accountID                   string
	emails                      []Email
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		identities:                  make(map[string]*EmailIdentity),
		configurationSets:           make(map[string]*ConfigurationSet),
		eventDestinations:           make(map[string]map[string]*EventDestination),
		contactLists:                make(map[string]*ContactList),
		contacts:                    make(map[string]map[string]*Contact),
		customVerificationTemplates: make(map[string]*CustomVerificationEmailTemplate),
		dedicatedIPPools:            make(map[string]*DedicatedIPPool),
		deliverabilityTestReports:   make(map[string]*DeliverabilityTestReport),
		emailTemplates:              make(map[string]*EmailTemplate),
		exportJobs:                  make(map[string]*ExportJob),
		emailIdentityPolicies:       make(map[string]map[string]string),
		importJobs:                  make(map[string]*ImportJob),
		suppressedDestinations:      make(map[string]*SuppressedDestination),
		resourceTags:                make(map[string]map[string]string),
		multiRegionEndpoints:        make(map[string]map[string]any),
		tenants:                     make(map[string]map[string]any),
		tenantResources:             make(map[string][]string),
		resourceTenants:             make(map[string][]string),
		mu:                          lockmetrics.New("sesv2"),
		region:                      config.DefaultRegion,
		accountID:                   config.DefaultAccountID,
	}
}

// NewInMemoryBackendWithConfig creates a new InMemoryBackend with the given config.
func NewInMemoryBackendWithConfig(cfg *config.GlobalConfig) *InMemoryBackend {
	b := NewInMemoryBackend()
	if cfg.GetRegion() != "" {
		b.region = cfg.GetRegion()
	}

	if cfg.GetAccountID() != "" {
		b.accountID = cfg.GetAccountID()
	}

	return b
}

// Region returns the backend's AWS region.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the backend's AWS account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Reset closes the mutex and re-initialises all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Close()
	b.mu = lockmetrics.New("sesv2")
	b.identities = make(map[string]*EmailIdentity)
	b.configurationSets = make(map[string]*ConfigurationSet)
	b.eventDestinations = make(map[string]map[string]*EventDestination)
	b.contactLists = make(map[string]*ContactList)
	b.contacts = make(map[string]map[string]*Contact)
	b.customVerificationTemplates = make(map[string]*CustomVerificationEmailTemplate)
	b.dedicatedIPPools = make(map[string]*DedicatedIPPool)
	b.deliverabilityTestReports = make(map[string]*DeliverabilityTestReport)
	b.emailTemplates = make(map[string]*EmailTemplate)
	b.exportJobs = make(map[string]*ExportJob)
	b.importJobs = make(map[string]*ImportJob)
	b.suppressedDestinations = make(map[string]*SuppressedDestination)
	b.emailIdentityPolicies = make(map[string]map[string]string)
	b.resourceTags = make(map[string]map[string]string)
	b.multiRegionEndpoints = make(map[string]map[string]any)
	b.tenants = make(map[string]map[string]any)
	b.tenantResources = make(map[string][]string)
	b.resourceTenants = make(map[string][]string)
	b.accountDetails = nil
	b.emails = nil
}

// identityType determines the identity type from the identity string.
func identityType(identity string) string {
	if strings.Contains(identity, "@") {
		return "EMAIL_ADDRESS"
	}

	return "DOMAIN"
}

// CreateEmailIdentity creates a new email identity and marks it as verified.
func (b *InMemoryBackend) CreateEmailIdentity(
	identity, configurationSetName string,
	tags map[string]string,
) (*EmailIdentity, error) {
	if strings.TrimSpace(identity) == "" {
		return nil, fmt.Errorf("%w: EmailIdentity is required", ErrInvalidInput)
	}

	b.mu.Lock("CreateEmailIdentity")
	defer b.mu.Unlock()

	if _, exists := b.identities[identity]; exists {
		return nil, fmt.Errorf("%w: identity %s already exists", ErrAlreadyExists, identity)
	}

	idType := identityType(identity)

	ei := &EmailIdentity{
		Identity:             identity,
		IdentityType:         idType,
		VerifiedForSending:   true,
		FeedbackForwarding:   true,
		DkimSigningEnabled:   true,
		DkimStatus:           dkimStatusSuccess,
		VerificationStatus:   verificationStatusSuccess,
		ConfigurationSetName: configurationSetName,
		Tags:                 tags,
	}

	if idType == "DOMAIN" {
		ei.DkimTokens = generateDkimTokens()
	}

	b.identities[identity] = ei

	cp := *ei

	return &cp, nil
}

const (
	dkimTokenCount  = 3
	dkimTokenLength = 24
)

// generateDkimTokens generates three DKIM CNAME selector tokens.
func generateDkimTokens() []string {
	tokens := make([]string, dkimTokenCount)
	for i := range tokens {
		id := uuid.New().String()
		tokens[i] = strings.ReplaceAll(id, "-", "")[:dkimTokenLength]
	}

	return tokens
}

// GetEmailIdentity returns the email identity with the given name.
func (b *InMemoryBackend) GetEmailIdentity(identity string) (*EmailIdentity, error) {
	b.mu.RLock("GetEmailIdentity")
	defer b.mu.RUnlock()

	ei, ok := b.identities[identity]
	if !ok {
		return nil, fmt.Errorf("%w: identity %s not found", ErrNotFound, identity)
	}

	cp := *ei

	return &cp, nil
}

// ListEmailIdentities returns a paginated, sorted list of email identities.
func (b *InMemoryBackend) ListEmailIdentities(
	nextToken string,
	pageSize int,
) page.Page[*EmailIdentity] {
	b.mu.RLock("ListEmailIdentities")

	out := make([]*EmailIdentity, 0, len(b.identities))
	for _, ei := range b.identities {
		cp := *ei
		out = append(out, &cp)
	}

	b.mu.RUnlock()

	sort.Slice(out, func(i, j int) bool {
		return out[i].Identity < out[j].Identity
	})

	return page.New(out, nextToken, pageSize, sesv2DefaultMaxItems)
}

// DeleteEmailIdentity removes an email identity.
func (b *InMemoryBackend) DeleteEmailIdentity(identity string) error {
	b.mu.Lock("DeleteEmailIdentity")
	defer b.mu.Unlock()

	if _, ok := b.identities[identity]; !ok {
		return fmt.Errorf("%w: identity %s not found", ErrNotFound, identity)
	}

	delete(b.identities, identity)

	return nil
}

// CreateConfigurationSet creates a new configuration set.
func (b *InMemoryBackend) CreateConfigurationSet(name string) (*ConfigurationSet, error) {
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("%w: ConfigurationSetName is required", ErrInvalidInput)
	}

	b.mu.Lock("CreateConfigurationSet")
	defer b.mu.Unlock()

	if _, exists := b.configurationSets[name]; exists {
		return nil, fmt.Errorf("%w: configuration set %s already exists", ErrAlreadyExists, name)
	}

	cs := &ConfigurationSet{
		Name:           name,
		CreatedAt:      time.Now(),
		SendingEnabled: true,
	}
	b.configurationSets[name] = cs

	cp := *cs

	return &cp, nil
}

// GetConfigurationSet returns the configuration set with the given name.
func (b *InMemoryBackend) GetConfigurationSet(name string) (*ConfigurationSet, error) {
	b.mu.RLock("GetConfigurationSet")
	defer b.mu.RUnlock()

	cs, ok := b.configurationSets[name]
	if !ok {
		return nil, fmt.Errorf("%w: configuration set %s not found", ErrNotFound, name)
	}

	cp := *cs

	return &cp, nil
}

// ListConfigurationSets returns a paginated, sorted list of configuration sets.
func (b *InMemoryBackend) ListConfigurationSets(
	nextToken string,
	pageSize int,
) page.Page[*ConfigurationSet] {
	b.mu.RLock("ListConfigurationSets")
	defer b.mu.RUnlock()

	out := make([]*ConfigurationSet, 0, len(b.configurationSets))
	for _, cs := range b.configurationSets {
		cp := *cs
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})

	return page.New(out, nextToken, pageSize, sesv2DefaultMaxItems)
}

// DeleteConfigurationSet removes a configuration set and its event destinations.
func (b *InMemoryBackend) DeleteConfigurationSet(name string) error {
	b.mu.Lock("DeleteConfigurationSet")
	defer b.mu.Unlock()

	if _, ok := b.configurationSets[name]; !ok {
		return fmt.Errorf("%w: configuration set %s not found", ErrNotFound, name)
	}

	delete(b.configurationSets, name)
	delete(b.eventDestinations, name)

	return nil
}

// SendEmail captures an outbound email and returns a message ID.
func (b *InMemoryBackend) SendEmail(
	from string,
	to []string,
	subject, bodyHTML, bodyText string,
) (string, error) {
	if from == "" {
		return "", fmt.Errorf("%w: FromEmailAddress is required", ErrInvalidInput)
	}

	msgID := "sesv2-" + uuid.New().String()

	email := Email{
		MessageID: msgID,
		From:      from,
		To:        to,
		Subject:   subject,
		BodyHTML:  bodyHTML,
		BodyText:  bodyText,
		Timestamp: time.Now(),
	}

	b.mu.Lock("SendEmail")
	if err := b.checkFromIdentityLocked(from); err != nil {
		b.mu.Unlock()

		return "", err
	}
	b.emails = append(b.emails, email)
	// Compact only when the slice has grown to twice the cap so trimming is
	// amortized O(1) per send rather than O(maxRetainedEmails) on every send
	// past the cap. The dropped prefix becomes unreachable once the slice
	// header advances and is collected on the next reslice/grow.
	if len(b.emails) >= emailCompactionHighWater {
		// Reslice into a fresh backing array so the dropped tail can be GC'd
		// immediately rather than held by the original (now larger) backing
		// array.
		trimmed := make([]Email, maxRetainedEmails, emailCompactionHighWater)
		copy(trimmed, b.emails[len(b.emails)-maxRetainedEmails:])
		b.emails = trimmed
	}
	b.mu.Unlock()

	return msgID, nil
}

// checkFromIdentityLocked verifies the from address against registered identities.
// It checks exact email match first, then the domain portion as a fallback.
// Must be called with b.mu held for writing or reading.
func (b *InMemoryBackend) checkFromIdentityLocked(from string) error {
	if id, ok := b.identities[from]; ok && id.VerifiedForSending {
		return nil
	}
	if at := strings.LastIndex(from, "@"); at >= 0 {
		domain := from[at+1:]
		if id, ok := b.identities[domain]; ok && id.VerifiedForSending {
			return nil
		}
	}

	return fmt.Errorf("%w: identity not verified for sending: %s", ErrInvalidInput, from)
}

// ListEmails returns a copy of all captured emails.
func (b *InMemoryBackend) ListEmails() []Email {
	b.mu.RLock("ListEmails")
	defer b.mu.RUnlock()

	out := make([]Email, len(b.emails))
	copy(out, b.emails)

	return out
}

const sesv2DefaultMaxItems = 100

// BatchGetMetricData returns synthetic empty results for each query.
func (b *InMemoryBackend) BatchGetMetricData(
	queries []MetricDataQuery,
) ([]MetricDataResult, error) {
	now := time.Now().UTC().Truncate(time.Hour)
	results := make([]MetricDataResult, 0, len(queries))

	for _, q := range queries {
		results = append(results, MetricDataResult{
			ID:         q.ID,
			Timestamps: []time.Time{now},
			Values:     []float64{0},
		})
	}

	return results, nil
}

// CancelExportJob sets an export job status to CANCELLED.
func (b *InMemoryBackend) CancelExportJob(jobID string) error {
	b.mu.Lock("CancelExportJob")
	defer b.mu.Unlock()

	job, ok := b.exportJobs[jobID]
	if !ok {
		return fmt.Errorf("%w: export job %s not found", ErrNotFound, jobID)
	}

	job.JobStatus = exportJobStatusCancelled

	return nil
}

// CreateConfigurationSetEventDestination adds an event destination to a configuration set.
func (b *InMemoryBackend) CreateConfigurationSetEventDestination(
	configSetName, destName string,
	enabled bool,
	matchingEventTypes []string,
) (*EventDestination, error) {
	b.mu.Lock("CreateConfigurationSetEventDestination")
	defer b.mu.Unlock()

	if _, ok := b.configurationSets[configSetName]; !ok {
		return nil, fmt.Errorf("%w: configuration set %s not found", ErrNotFound, configSetName)
	}

	if _, ok := b.eventDestinations[configSetName]; !ok {
		b.eventDestinations[configSetName] = make(map[string]*EventDestination)
	}

	if _, exists := b.eventDestinations[configSetName][destName]; exists {
		return nil, fmt.Errorf(
			"%w: event destination %s already exists in config set %s",
			ErrAlreadyExists, destName, configSetName,
		)
	}

	types := make([]string, len(matchingEventTypes))
	copy(types, matchingEventTypes)

	dest := &EventDestination{
		Name:                 destName,
		ConfigurationSetName: configSetName,
		Enabled:              enabled,
		MatchingEventTypes:   types,
		CreatedAt:            time.Now(),
	}
	b.eventDestinations[configSetName][destName] = dest

	cp := *dest

	return &cp, nil
}

// CreateContact adds a contact to a contact list.
func (b *InMemoryBackend) CreateContact(
	contactListName, emailAddress string,
	topicPreferences []TopicPreference,
) (*Contact, error) {
	b.mu.Lock("CreateContact")
	defer b.mu.Unlock()

	if _, ok := b.contactLists[contactListName]; !ok {
		return nil, fmt.Errorf("%w: contact list %s not found", ErrNotFound, contactListName)
	}

	if _, ok := b.contacts[contactListName]; !ok {
		b.contacts[contactListName] = make(map[string]*Contact)
	}

	if _, exists := b.contacts[contactListName][emailAddress]; exists {
		return nil, fmt.Errorf(
			"%w: contact %s already exists in list %s",
			ErrAlreadyExists, emailAddress, contactListName,
		)
	}

	prefs := make([]TopicPreference, len(topicPreferences))
	copy(prefs, topicPreferences)

	now := time.Now()
	c := &Contact{
		EmailAddress:     emailAddress,
		ContactListName:  contactListName,
		TopicPreferences: prefs,
		CreatedAt:        now,
		LastUpdatedAt:    now,
	}
	b.contacts[contactListName][emailAddress] = c

	cp := *c

	return &cp, nil
}

// CreateContactList creates a new contact list.
func (b *InMemoryBackend) CreateContactList(name, description string) (*ContactList, error) {
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("%w: ContactListName is required", ErrInvalidInput)
	}

	b.mu.Lock("CreateContactList")
	defer b.mu.Unlock()

	if _, exists := b.contactLists[name]; exists {
		return nil, fmt.Errorf("%w: contact list %s already exists", ErrAlreadyExists, name)
	}

	now := time.Now()
	cl := &ContactList{
		Name:          name,
		Description:   description,
		Tags:          make(map[string]string),
		CreatedAt:     now,
		LastUpdatedAt: now,
	}
	b.contactLists[name] = cl

	cp := *cl

	return &cp, nil
}

// CreateCustomVerificationEmailTemplate creates a custom verification email template.
func (b *InMemoryBackend) CreateCustomVerificationEmailTemplate(
	in *CustomVerificationEmailTemplate,
) (*CustomVerificationEmailTemplate, error) {
	b.mu.Lock("CreateCustomVerificationEmailTemplate")
	defer b.mu.Unlock()

	if _, exists := b.customVerificationTemplates[in.TemplateName]; exists {
		return nil, fmt.Errorf(
			"%w: custom verification template %s already exists",
			ErrAlreadyExists, in.TemplateName,
		)
	}

	cp := *in
	b.customVerificationTemplates[in.TemplateName] = &cp

	out := *in

	return &out, nil
}

// CreateDedicatedIPPool creates a dedicated IP pool.
func (b *InMemoryBackend) CreateDedicatedIPPool(
	poolName, scalingMode string,
) (*DedicatedIPPool, error) {
	if strings.TrimSpace(poolName) == "" {
		return nil, fmt.Errorf("%w: PoolName is required", ErrInvalidInput)
	}

	if scalingMode != scalingModeStandard && scalingMode != scalingModeManaged {
		return nil, fmt.Errorf(
			"%w: ScalingMode must be %s or %s, got %s",
			ErrInvalidInput, scalingModeStandard, scalingModeManaged, scalingMode,
		)
	}

	b.mu.Lock("CreateDedicatedIPPool")
	defer b.mu.Unlock()

	if _, exists := b.dedicatedIPPools[poolName]; exists {
		return nil, fmt.Errorf(
			"%w: dedicated IP pool %s already exists",
			ErrAlreadyExists,
			poolName,
		)
	}

	pool := &DedicatedIPPool{
		PoolName:    poolName,
		ScalingMode: scalingMode,
	}
	b.dedicatedIPPools[poolName] = pool

	cp := *pool

	return &cp, nil
}

// CreateDeliverabilityTestReport creates a deliverability test report.
func (b *InMemoryBackend) CreateDeliverabilityTestReport(
	reportName, fromEmailAddress string,
) (*DeliverabilityTestReport, error) {
	reportID := uuid.New().String()

	report := &DeliverabilityTestReport{
		ReportID:                 reportID,
		ReportName:               reportName,
		FromEmailAddress:         fromEmailAddress,
		DeliverabilityTestStatus: deliverabilityTestStatusInProgress,
		CreateDate:               time.Now(),
	}

	b.mu.Lock("CreateDeliverabilityTestReport")
	b.deliverabilityTestReports[reportID] = report
	b.mu.Unlock()

	cp := *report

	return &cp, nil
}

// CreateEmailIdentityPolicy creates a policy for an email identity.
func (b *InMemoryBackend) CreateEmailIdentityPolicy(identity, policyName, policy string) error {
	b.mu.Lock("CreateEmailIdentityPolicy")
	defer b.mu.Unlock()

	if _, ok := b.identities[identity]; !ok {
		return fmt.Errorf("%w: identity %s not found", ErrNotFound, identity)
	}

	if _, ok := b.emailIdentityPolicies[identity]; !ok {
		b.emailIdentityPolicies[identity] = make(map[string]string)
	}

	if _, exists := b.emailIdentityPolicies[identity][policyName]; exists {
		return fmt.Errorf(
			"%w: policy %s already exists for identity %s",
			ErrAlreadyExists,
			policyName,
			identity,
		)
	}

	b.emailIdentityPolicies[identity][policyName] = policy

	return nil
}

// CreateEmailTemplate creates a new email template.
func (b *InMemoryBackend) CreateEmailTemplate(
	templateName string,
	content *EmailTemplateContent,
) (*EmailTemplate, error) {
	if strings.TrimSpace(templateName) == "" {
		return nil, fmt.Errorf("%w: TemplateName is required", ErrInvalidInput)
	}

	b.mu.Lock("CreateEmailTemplate")
	defer b.mu.Unlock()

	if _, exists := b.emailTemplates[templateName]; exists {
		return nil, fmt.Errorf(
			"%w: email template %s already exists",
			ErrAlreadyExists,
			templateName,
		)
	}

	var contentCopy *EmailTemplateContent
	if content != nil {
		c := *content
		contentCopy = &c
	}

	t := &EmailTemplate{
		TemplateName:    templateName,
		TemplateContent: contentCopy,
		CreatedAt:       time.Now(),
	}
	b.emailTemplates[templateName] = t

	cp := *t

	return &cp, nil
}

// AddContactListInternal creates a contact list directly (for tests).
func (b *InMemoryBackend) AddContactListInternal(name string) *ContactList {
	b.mu.Lock("AddContactListInternal")
	defer b.mu.Unlock()

	now := time.Now()
	cl := &ContactList{
		Name:          name,
		Tags:          make(map[string]string),
		CreatedAt:     now,
		LastUpdatedAt: now,
	}
	b.contactLists[name] = cl

	return cl
}

// AddExportJobInternal creates an export job directly (for tests).
func (b *InMemoryBackend) AddExportJobInternal(jobID, status string) *ExportJob {
	b.mu.Lock("AddExportJobInternal")
	defer b.mu.Unlock()

	job := &ExportJob{
		JobID:     jobID,
		JobStatus: status,
		CreatedAt: time.Now(),
	}
	b.exportJobs[jobID] = job

	return job
}

func (b *InMemoryBackend) TagResource(arn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if b.resourceTags[arn] == nil {
		b.resourceTags[arn] = make(map[string]string)
	}

	maps.Copy(b.resourceTags[arn], tags)

	return nil
}

func (b *InMemoryBackend) UntagResource(arn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	m := b.resourceTags[arn]
	for _, k := range tagKeys {
		delete(m, k)
	}

	return nil
}

func (b *InMemoryBackend) ListTagsForResource(arn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	out := make(map[string]string, len(b.resourceTags[arn]))
	maps.Copy(out, b.resourceTags[arn])

	return out, nil
}
