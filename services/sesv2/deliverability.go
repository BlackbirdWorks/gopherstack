package sesv2

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
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

// campaignIDFor derives a stable, deterministic campaign identifier from the
// (FromAddress, Subject) pair AWS's deliverability dashboard auto-groups
// sent messages by. gopherstack doesn't persist a separate campaign index --
// this is a pure function of the already-persisted send history in
// b.emails, so the same campaign is addressable by the same ID across
// calls/restarts without a snapshot-format change.
func campaignIDFor(from, subject string) string {
	sum := sha256.Sum256([]byte(from + "\x00" + subject))

	return "campaign-" + hex.EncodeToString(sum[:8])
}

// domainCampaign is the real (FromAddress/Subject/first-and-last-seen)
// portion of a deliverability-dashboard campaign gopherstack can derive from
// its own send history; see domainCampaignsLocked.
type domainCampaign struct {
	FirstSeenDateTime time.Time
	LastSeenDateTime  time.Time
	CampaignID        string
	FromAddress       string
	Subject           string
}

// domainCampaignsLocked groups gopherstack's real SendEmail history
// (b.emails) into deliverability-dashboard "campaigns" -- send events
// grouped by (FromAddress, Subject), the same grouping key real SES uses to
// auto-detect campaigns, optionally restricted to messages with at least
// one recipient in domain (empty domain means "no restriction", used by
// GetDomainDeliverabilityCampaign which has no domain parameter). Sorted by
// CampaignID for stable pagination. Must be called with b.mu held (read or
// write).
func (b *InMemoryBackend) domainCampaignsLocked(domain string) []domainCampaign {
	groups := make(map[string]*domainCampaign)
	order := make([]string, 0)

	for _, e := range b.emails {
		if domain != "" && !anyAddressInDomain(e.To, domain) {
			continue
		}

		id := campaignIDFor(e.From, e.Subject)

		c, ok := groups[id]
		if !ok {
			groups[id] = &domainCampaign{
				CampaignID:        id,
				FromAddress:       e.From,
				Subject:           e.Subject,
				FirstSeenDateTime: e.Timestamp,
				LastSeenDateTime:  e.Timestamp,
			}
			order = append(order, id)

			continue
		}

		if e.Timestamp.Before(c.FirstSeenDateTime) {
			c.FirstSeenDateTime = e.Timestamp
		}

		if e.Timestamp.After(c.LastSeenDateTime) {
			c.LastSeenDateTime = e.Timestamp
		}
	}

	sort.Strings(order)

	out := make([]domainCampaign, 0, len(order))
	for _, id := range order {
		out = append(out, *groups[id])
	}

	return out
}

// anyAddressInDomain reports whether any address in addrs belongs to domain
// (case-insensitive).
func anyAddressInDomain(addrs []string, domain string) bool {
	suffix := "@" + strings.ToLower(domain)
	for _, a := range addrs {
		if strings.HasSuffix(strings.ToLower(a), suffix) {
			return true
		}
	}

	return false
}

// domainDeliverabilityCampaignResponse renders a real derived campaign as
// the AWS-shaped response, matching types.DomainDeliverabilityCampaign.
// InboxCount/SpamCount/ReadRate/DeleteRate/ReadDeleteRate/ProjectedVolume/
// Esps/SendingIps require real inbox/spam placement tracking gopherstack
// doesn't have (this requires opted-in production sending history AWS
// tracks server-side) and stay honest zero-valued/empty placeholders -- the
// same tradeoff BatchGetMetricData makes for its own metrics.
func domainDeliverabilityCampaignResponse(c *domainCampaign) map[string]any {
	return map[string]any{
		"CampaignId":        c.CampaignID,
		"FromAddress":       c.FromAddress,
		keySubject:          c.Subject,
		"FirstSeenDateTime": awstime.Epoch(c.FirstSeenDateTime),
		"LastSeenDateTime":  awstime.Epoch(c.LastSeenDateTime),
		"InboxCount":        float64(0),
		"SpamCount":         float64(0),
		"ReadRate":          float64(0),
		"DeleteRate":        float64(0),
		"ReadDeleteRate":    float64(0),
		"ProjectedVolume":   float64(0),
		"Esps":              []any{},
		"SendingIps":        []any{},
	}
}

// GetDomainDeliverabilityCampaign returns deliverability data for a single
// campaign, matching types.DomainDeliverabilityCampaign. When campaignID
// matches a campaign gopherstack can actually derive from its own send
// history (see domainCampaignsLocked), CampaignId/FromAddress/Subject/
// FirstSeenDateTime/LastSeenDateTime are real. An ID that doesn't match any
// real send history (including any ID a caller makes up, since gopherstack
// exposes no other way to validate one) falls back to the same shape with
// the requested ID echoed back and every other field honestly zero/empty --
// gopherstack has no NotFoundException signal it could raise here without
// also rejecting IDs a real ListDomainDeliverabilityCampaigns call would
// have legitimately returned.
func (b *InMemoryBackend) GetDomainDeliverabilityCampaign(campaignID string) (map[string]any, error) {
	b.mu.RLock("GetDomainDeliverabilityCampaign")
	campaigns := b.domainCampaignsLocked("")
	b.mu.RUnlock()

	for _, c := range campaigns {
		if c.CampaignID == campaignID {
			return domainDeliverabilityCampaignResponse(&c), nil
		}
	}

	now := awstime.Epoch(time.Now())

	return map[string]any{
		"CampaignId":        campaignID,
		"FromAddress":       "",
		keySubject:          "",
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

// parseSESv2Timestamp parses a query-string timestamp in either RFC3339
// (the wire format real SES v2 clients use for the StartDate/EndDate query
// parameters on this op family, confirmed against serializers.go's
// smithytime.FormatDateTime) or a bare "YYYY-MM-DD" date (accepted for
// backend-direct callers/tests), returning ok=false if s is empty or
// matches neither.
func parseSESv2Timestamp(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}

	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, true
	}

	if t, err := time.Parse(time.DateOnly, s); err == nil {
		return t, true
	}

	return time.Time{}, false
}

// maxDailyVolumeDays caps the number of DailyVolume entries
// GetDomainStatisticsReport generates for a requested date range, so a
// caller-supplied multi-year range can't force an unbounded response.
const maxDailyVolumeDays = 366

// oneCalendarDay truncates a timestamp down to a whole UTC calendar day.
const oneCalendarDay = 24 * time.Hour

// GetDomainStatisticsReport reports volume statistics for a domain, matching
// GetDomainStatisticsReportOutput. Real SES v2 documents DailyVolumes as
// containing "data for each day, starting on the StartDate and ending on
// the EndDate" -- gopherstack honors that shape for real (one entry per day
// in the requested range, parsed via parseSESv2Timestamp) even though every
// actual statistic (VolumeStatistics/DomainIspPlacements/ReadRatePercent)
// requires mail-delivery-outcome (inbox vs spam) tracking gopherstack
// doesn't have and stays honest zero/empty per day -- same tradeoff
// BatchGetMetricData makes for its own metrics. An unparseable or reversed
// range yields an empty DailyVolumes list rather than a guess.
func (b *InMemoryBackend) GetDomainStatisticsReport(domain, startDate, endDate string) (map[string]any, error) {
	dailyVolumes := []any{}

	start, hasStart := parseSESv2Timestamp(startDate)
	end, hasEnd := parseSESv2Timestamp(endDate)

	if hasStart && hasEnd && !end.Before(start) {
		start = start.UTC().Truncate(oneCalendarDay)
		end = end.UTC().Truncate(oneCalendarDay)

		days := make([]any, 0)
		for d := start; !d.After(end) && len(days) < maxDailyVolumeDays; d = d.AddDate(0, 0, 1) {
			days = append(days, map[string]any{
				"StartDate": awstime.Epoch(d),
				"VolumeStatistics": map[string]any{
					"InboxRawCount":  float64(0),
					"SpamRawCount":   float64(0),
					"ProjectedInbox": float64(0),
					"ProjectedSpam":  float64(0),
				},
				"DomainIspPlacements": []any{},
			})
		}

		dailyVolumes = days
	}

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
		"DailyVolumes": dailyVolumes,
	}, nil
}

// ListDomainDeliverabilityCampaigns lists deliverability-dashboard campaigns
// for a subscribed domain, derived for real from gopherstack's own send
// history (see domainCampaignsLocked) and restricted to campaigns that
// overlap the requested [startDate, endDate] window when both parse via
// parseSESv2Timestamp. InboxCount/SpamCount/etc are honest placeholders,
// same as GetDomainDeliverabilityCampaign.
func (b *InMemoryBackend) ListDomainDeliverabilityCampaigns(
	startDate, endDate, domain, nextToken string,
) ([]map[string]any, string, error) {
	start, hasStart := parseSESv2Timestamp(startDate)
	end, hasEnd := parseSESv2Timestamp(endDate)

	b.mu.RLock("ListDomainDeliverabilityCampaigns")
	campaigns := b.domainCampaignsLocked(domain)
	b.mu.RUnlock()

	all := make([]map[string]any, 0, len(campaigns))

	for _, c := range campaigns {
		if hasStart && c.LastSeenDateTime.Before(start) {
			continue
		}

		if hasEnd && c.FirstSeenDateTime.After(end) {
			continue
		}

		all = append(all, domainDeliverabilityCampaignResponse(&c))
	}

	return paginateMaps(all, nextToken, 0, "CampaignId")
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

// Recommendation Type/Status/Impact values (types.RecommendationType/
// RecommendationStatus/RecommendationImpact) used by the recommendations
// gopherstack can derive for real -- see ListRecommendations.
const (
	recommendationTypeDKIM      = "DKIM"
	recommendationTypeSPF       = "SPF"
	recommendationTypeComplaint = "COMPLAINT"
	recommendationStatusOpen    = "OPEN"
	recommendationImpactHigh    = "HIGH"
)

// identityARN builds the ARN for an email identity:
// arn:{partition}:ses:{region}:{account}:identity/{identity}.
func (b *InMemoryBackend) identityARN(identity string) string {
	return arn.Build("ses", b.region, b.accountID, "identity/"+identity)
}

// newRecommendation builds an OPEN/HIGH-impact recommendation, the only
// Status/Impact combination gopherstack's derivation logic ever produces
// (see ListRecommendations).
func newRecommendation(resourceARN, recType, description string, now float64) recommendationOutput {
	return recommendationOutput{
		ResourceArn:          resourceARN,
		Type:                 recType,
		Status:               recommendationStatusOpen,
		Impact:               recommendationImpactHigh,
		Description:          description,
		CreatedTimestamp:     now,
		LastUpdatedTimestamp: now,
	}
}

// filterRecommendations applies ListRecommendations' Filter parameter
// (types.ListRecommendationsFilterKey: TYPE/STATUS/IMPACT/RESOURCE_ARN),
// ANDing together whichever keys are present, matching the documented
// "combinations of STATUS and IMPACT or STATUS and TYPE" filter semantics.
func filterRecommendations(all []recommendationOutput, filter map[string]string) []recommendationOutput {
	if len(filter) == 0 {
		return all
	}

	out := make([]recommendationOutput, 0, len(all))

	for _, r := range all {
		if v, ok := filter["TYPE"]; ok && v != r.Type {
			continue
		}

		if v, ok := filter["STATUS"]; ok && v != r.Status {
			continue
		}

		if v, ok := filter["IMPACT"]; ok && v != r.Impact {
			continue
		}

		if v, ok := filter["RESOURCE_ARN"]; ok && v != r.ResourceArn {
			continue
		}

		out = append(out, r)
	}

	return out
}

// ListRecommendations derives real recommendations from gopherstack's actual
// configuration state: identities with DKIM signing disabled (DKIM --
// CreateEmailIdentity enables it by default, so this only fires after an
// explicit PutEmailIdentityDkimAttributes(signingEnabled=false) call),
// identities with a custom MAIL FROM domain that hasn't reached SUCCESS
// status (SPF -- a custom MAIL FROM domain is how SES achieves SPF
// alignment; gopherstack doesn't simulate async MAIL FROM verification, so
// every configured MAIL FROM domain is honestly stuck at PENDING forever),
// and reputation entities with a DISABLED customer-managed status
// (COMPLAINT). DMARC/BIMI recommendations and reputation-finding-driven
// types (BOUNCE/FEEDBACK_3P/IP_LISTING) are never returned -- gopherstack
// has no DNS-record model and no bounce/complaint-rate tracking pipeline to
// derive those from, and fabricating them would be worse than omitting
// them. Recommendations are computed live on every call rather than
// persisted, so CreatedTimestamp/LastUpdatedTimestamp reflect "now" rather
// than a real first-detected time gopherstack doesn't track.
func (b *InMemoryBackend) ListRecommendations(
	filter map[string]string,
	nextToken string,
	pageSize int,
) ([]recommendationOutput, string, error) {
	b.mu.RLock("ListRecommendations")

	now := awstime.Epoch(time.Now())
	all := make([]recommendationOutput, 0)

	for _, ei := range b.identities.Snapshot() {
		resourceARN := b.identityARN(ei.Identity)

		if !ei.DkimSigningEnabled {
			all = append(all, newRecommendation(
				resourceARN, recommendationTypeDKIM,
				"DKIM signing is not enabled for this identity.", now,
			))
		}

		// verificationStatusSuccess's value ("SUCCESS") is MailFromDomainStatus's
		// success value too -- gopherstack only ever sets MailFromDomainStatus to
		// "" or mailFromStatusPending (no async verification simulation), so this
		// fires for every identity that has ever configured a custom MAIL FROM
		// domain.
		if ei.MailFromDomainStatus != "" && ei.MailFromDomainStatus != verificationStatusSuccess {
			all = append(all, newRecommendation(
				resourceARN, recommendationTypeSPF,
				"The custom MAIL FROM domain for this identity has not reached SUCCESS status.", now,
			))
		}
	}

	for _, e := range b.reputationEntities.Snapshot() {
		if e.CustomerManagedStatus == sendingStatusDisabled {
			all = append(all, newRecommendation(
				e.EntityRef, recommendationTypeComplaint,
				"Sending has been disabled for this entity by a customer-managed status update.", now,
			))
		}
	}

	b.mu.RUnlock()

	all = filterRecommendations(all, filter)

	sort.Slice(all, func(i, j int) bool {
		if all[i].ResourceArn != all[j].ResourceArn {
			return all[i].ResourceArn < all[j].ResourceArn
		}

		return all[i].Type < all[j].Type
	})

	pg := page.New(all, nextToken, pageSize, sesv2DefaultMaxItems)

	return pg.Data, pg.Next, nil
}

// ---- reputation entities ----

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
func (b *InMemoryBackend) GetReputationEntity(entityID string) (reputationEntityOutput, error) {
	b.mu.RLock("GetReputationEntity")
	defer b.mu.RUnlock()

	if e, ok := b.reputationEntities.Get(entityID); ok {
		return toReputationEntityOutput(e), nil
	}

	return toReputationEntityOutput(&ReputationEntity{EntityRef: entityID}), nil
}

// filterReputationEntities applies the ListReputationEntities filter map.
// ENTITY_TYPE and REPUTATION_IMPACT are not applied: this backend never
// populates ReputationEntity.EntityType (nothing assigns it), and there is
// no reputation-impact field on the model at all.
func filterReputationEntities(all []reputationEntityOutput, filter map[string]string) []reputationEntityOutput {
	if len(filter) == 0 {
		return all
	}

	out := make([]reputationEntityOutput, 0, len(all))

	for _, e := range all {
		if v, ok := filter["SENDING_STATUS"]; ok && v != e.SendingStatusAggregate {
			continue
		}

		if v, ok := filter["ENTITY_REFERENCE_PREFIX"]; ok && !strings.HasPrefix(e.ReputationEntityReference, v) {
			continue
		}

		out = append(out, e)
	}

	return out
}

// ListReputationEntities returns tracked reputation entities, optionally
// filtered and paginated.
func (b *InMemoryBackend) ListReputationEntities(
	filter map[string]string,
	nextToken string,
	pageSize int,
) ([]reputationEntityOutput, string, error) {
	b.mu.RLock("ListReputationEntities")
	defer b.mu.RUnlock()

	snap := b.reputationEntities.Snapshot()

	all := make([]reputationEntityOutput, 0, len(snap))
	for _, e := range snap {
		all = append(all, toReputationEntityOutput(e))
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].ReputationEntityReference < all[j].ReputationEntityReference
	})

	all = filterReputationEntities(all, filter)

	pg := page.New(all, nextToken, pageSize, sesv2DefaultMaxItems)

	return pg.Data, pg.Next, nil
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
