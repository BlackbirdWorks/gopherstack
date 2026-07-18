package sesv2

import (
	"fmt"
	"time"

	"github.com/google/uuid"

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

// ListRecommendations returns empty list.
func (b *InMemoryBackend) ListRecommendations(_ string, _ int) ([]map[string]any, string, error) {
	return []map[string]any{}, "", nil
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
