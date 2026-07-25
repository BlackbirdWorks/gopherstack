package sesv2

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const deliverabilityTestStatusInProgress = "IN_PROGRESS"

// ReputationEntity represents a SES v2 reputation entity (e.g. a configuration set
// or email identity tracked for reputation purposes).
type ReputationEntity struct {
	EntityRef             string `json:"entityRef"`
	EntityType            string `json:"entityType,omitempty"`
	CustomerManagedStatus string `json:"customerManagedStatus,omitempty"`
	ReputationPolicy      string `json:"reputationPolicy,omitempty"`
}

// DeliverabilityTestReport represents a deliverability test report.
type DeliverabilityTestReport struct {
	CreateDate               time.Time `json:"createDate"`
	ReportID                 string    `json:"reportId"`
	ReportName               string    `json:"reportName"`
	FromEmailAddress         string    `json:"fromEmailAddress"`
	DeliverabilityTestStatus string    `json:"deliverabilityTestStatus"`
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
	b.deliverabilityTestReports.Put(report)
	b.mu.Unlock()

	cp := *report

	return &cp, nil
}

// ---- deliverability ----

// GetDeliverabilityDashboardOptions reports the dashboard enablement state
// and subscribed-domain list previously set by PutDeliverabilityDashboardOption,
// matching GetDeliverabilityDashboardOptionsOutput
// (DashboardEnabled/AccountStatus/ActiveSubscribedDomains).
func (b *InMemoryBackend) GetDeliverabilityDashboardOptions() (map[string]any, error) {
	b.mu.RLock("GetDeliverabilityDashboardOptions")
	defer b.mu.RUnlock()

	status := "DISABLED"
	if b.deliverabilityDashboardEnabled {
		status = "ACTIVE"
	}

	domains := make([]map[string]any, len(b.deliverabilityDashboardDomains))
	copy(domains, b.deliverabilityDashboardDomains)

	return map[string]any{
		"DashboardEnabled":        b.deliverabilityDashboardEnabled,
		"AccountStatus":           status,
		"ActiveSubscribedDomains": domains,
	}, nil
}

// PutDeliverabilityDashboardOption persists the dashboard enablement state
// and subscribed-domain list so GetDeliverabilityDashboardOptions reflects
// it (previously a true no-op).
func (b *InMemoryBackend) PutDeliverabilityDashboardOption(enabled bool, subscribedDomains []string) error {
	b.mu.Lock("PutDeliverabilityDashboardOption")
	defer b.mu.Unlock()

	b.deliverabilityDashboardEnabled = enabled

	domains := make([]map[string]any, 0, len(subscribedDomains))
	for _, d := range subscribedDomains {
		domains = append(domains, map[string]any{
			"Domain":                d,
			"SubscriptionStartDate": awstime.Epoch(time.Now()),
		})
	}

	b.deliverabilityDashboardDomains = domains

	return nil
}

// GetDeliverabilityTestReport retrieves a test report.
func (b *InMemoryBackend) GetDeliverabilityTestReport(
	reportID string,
) (*DeliverabilityTestReport, error) {
	b.mu.RLock("GetDeliverabilityTestReport")
	defer b.mu.RUnlock()

	r, ok := b.deliverabilityTestReports.Get(reportID)
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

	snap := b.deliverabilityTestReports.Snapshot()

	items := make([]*DeliverabilityTestReport, 0, len(snap))
	for _, r := range snap {
		cp := *r
		items = append(items, &cp)
	}

	return page.New(items, nextToken, pageSize, sesv2DefaultMaxItems)
}

// GetDomainDeliverabilityCampaign returns deliverability data for a single
// campaign, matching types.DomainDeliverabilityCampaign. gopherstack has no
// real deliverability-dashboard data source (this requires opted-in
// production sending history AWS tracks server-side), so populated fields
// are zero-valued placeholders -- the wire shape/route are AWS-accurate even
// though the metrics themselves aren't meaningful in an emulator, the same
// tradeoff BatchGetMetricData makes.
func (b *InMemoryBackend) GetDomainDeliverabilityCampaign(campaignID string) (map[string]any, error) {
	now := awstime.Epoch(time.Now())

	return map[string]any{
		"CampaignId":        campaignID,
		"FromAddress":       "",
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

// EmailAddressInsightsConfidenceVerdict values (types.EmailAddressInsightsConfidenceVerdict).
const (
	confidenceHigh   = "HIGH"
	confidenceMedium = "MEDIUM"
	confidenceLow    = "LOW"
)

// onceRoleAddressLocalParts lazily initialises the set of common role-based
// mailbox local-parts, used by GetEmailAddressInsights' IsRoleAddress check
// (matches the check SES v2 documents: "role-based addresses (such as
// admin@, support@, or info@)").
//
//nolint:gochecknoglobals // read-only package-level lookup table
var onceRoleAddressLocalParts = sync.OnceValue(func() map[string]bool {
	return map[string]bool{
		"admin": true, "administrator": true, "support": true, "info": true,
		"sales": true, "contact": true, "help": true, "webmaster": true,
		"postmaster": true, "abuse": true, "noreply": true, "no-reply": true,
		"marketing": true, "billing": true, "office": true, "hello": true,
		"security": true, "privacy": true, "root": true,
	}
})

// emailSyntaxPattern is a practical (not exhaustive-RFC-5322) email address
// syntax check: local-part@domain-with-at-least-one-dot.
const emailSyntaxPatternSrc = `^[a-zA-Z0-9.!#$%&'*+/=?^_` + "`" + `{|}~-]+@` +
	`[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?` +
	`(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?)+$`

var emailSyntaxPattern = regexp.MustCompile(emailSyntaxPatternSrc)

// insightsVerdict builds an EmailAddressInsightsVerdict-shaped map.
func insightsVerdict(confidence string) map[string]any {
	return map[string]any{"ConfidenceVerdict": confidence}
}

// GetEmailAddressInsights runs the checks gopherstack can meaningfully
// perform without network access (RFC-shaped syntax validation and a
// role-address local-part lookup) and reports the checks that genuinely
// require external data (DNS, disposable-domain lists, mailbox probing) as
// MEDIUM-confidence placeholders -- the route and wire shape are
// AWS-accurate (types.MailboxValidation/EmailAddressInsightsMailboxEvaluations)
// even though those specific verdicts aren't backed by real validation.
func (b *InMemoryBackend) GetEmailAddressInsights(emailAddress string) (map[string]any, error) {
	validSyntax := emailSyntaxPattern.MatchString(emailAddress)

	syntaxConfidence, overallConfidence := confidenceHigh, confidenceHigh
	if !validSyntax {
		syntaxConfidence, overallConfidence = confidenceLow, confidenceLow
	}

	localPart, _, found := strings.Cut(emailAddress, "@")
	if !found {
		localPart = emailAddress
	}

	roleConfidence := confidenceLow
	if onceRoleAddressLocalParts()[strings.ToLower(localPart)] {
		roleConfidence = confidenceHigh
	}

	return map[string]any{
		"MailboxValidation": map[string]any{
			"IsValid": insightsVerdict(overallConfidence),
			"Evaluations": map[string]any{
				"HasValidSyntax":     insightsVerdict(syntaxConfidence),
				"IsRoleAddress":      insightsVerdict(roleConfidence),
				"HasValidDnsRecords": insightsVerdict(confidenceMedium),
				"IsDisposable":       insightsVerdict(confidenceMedium),
				"IsRandomInput":      insightsVerdict(confidenceMedium),
				"MailboxExists":      insightsVerdict(confidenceMedium),
			},
		},
	}, nil
}

// ListRecommendations returns an empty list -- gopherstack has no reputation
// analysis engine to generate real DKIM/SPF/DMARC/BIMI recommendations from,
// so (unlike GetEmailAddressInsights) there's no meaningful synthetic data to
// return; the route and NextToken/Recommendations wire shape are AWS-accurate.
func (b *InMemoryBackend) ListRecommendations(_ string, _ int) ([]map[string]any, string, error) {
	return []map[string]any{}, "", nil
}

// ---- reputation entities ----

// reputationEntityToMap renders a reputation entity as the AWS-shaped
// response map, field-diffed against types.ReputationEntity. Note
// CustomerManagedStatus is a *StatusRecord{Status,Cause,LastUpdatedTimestamp}
// (not a bare string) -- gopherstack only tracks Status, which is the only
// field UpdateReputationEntityCustomerManagedStatus lets a caller set.
// AwsSesManagedStatus is omitted: it's SES's own computed reputation-findings
// status, which gopherstack has no findings engine to derive.
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

	// SendingStatusAggregate is derived from CustomerManagedStatus (gopherstack
	// has no separate AWS-SES-managed status to combine it with) -- default
	// ENABLED, matching a freshly-tracked entity with no restrictions applied.
	aggregate := sendingStatusEnabled
	if e.CustomerManagedStatus != "" {
		aggregate = e.CustomerManagedStatus
	}

	m["SendingStatusAggregate"] = aggregate

	return m
}

// reputationEntityLocked returns the tracked reputation entity, creating an entry
// if it does not yet exist. Callers must hold the write lock.
func (b *InMemoryBackend) reputationEntityLocked(entityID string) *ReputationEntity {
	e, ok := b.reputationEntities.Get(entityID)
	if !ok {
		e = &ReputationEntity{EntityRef: entityID}
		b.reputationEntities.Put(e)
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

	if e, ok := b.reputationEntities.Get(entityID); ok {
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

	snap := b.reputationEntities.Snapshot()

	out := make([]map[string]any, 0, len(snap))
	for _, e := range snap {
		out = append(out, reputationEntityToMap(e))
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
