package support

import (
	"fmt"
	"sync"
	"time"
)

const (
	keyStatusField         = "Status"
	fieldRegion            = "Region"
	categorySecurity       = "security"
	categoryCostOptimizing = "cost_optimizing"

	checkRefreshStatusNone       = "none"
	checkRefreshStatusEnqueued   = "enqueued"
	checkRefreshStatusProcessing = "processing"
	checkRefreshStatusSuccess    = "success"

	// defaultResourcesProcessed is the default count of processed resources for static check results.
	defaultResourcesProcessed = int64(10)
	// refreshMillisDefault is the default milliseconds until next refreshable after a refresh is enqueued.
	refreshMillisDefault = int64(3_600_000)

	// maxCheckRefreshStatuses caps the number of tracked refresh statuses.
	maxCheckRefreshStatuses = 1000
)

// trustedAdvisorChecks returns a static list of mock Trusted Advisor checks.
func trustedAdvisorChecks() []TrustedAdvisorCheck {
	return []TrustedAdvisorCheck{
		{
			ID:          "Pfx0RwqBli",
			Name:        "Service Limits",
			Description: "Checks for service usage that is more than 80% of the service limit.",
			Category:    "service_limits",
			Metadata: []string{
				fieldRegion,
				"Service",
				"Limit Name",
				"Limit Amount",
				"Current Usage",
				keyStatusField,
			},
		},
		{
			ID:   "DAvU99Dc4C",
			Name: "Low Utilization Amazon EC2 Instances",
			Description: "Checks the Amazon Elastic Compute Cloud (Amazon EC2) instances that were " +
				"running at any time during the last 14 days.",
			Category: "cost_optimizing",
			Metadata: []string{
				"Region/AZ",
				"Instance ID",
				"Instance Name",
				"Instance Type",
				"Estimated Monthly Savings",
			},
		},
		{
			ID:   "N430c450f2",
			Name: "Unassociated Elastic IP Addresses",
			Description: "Checks for Elastic IP addresses (EIPs) that are not associated with a running " +
				"Amazon Elastic Compute Cloud (Amazon EC2) instance.",
			Category: "cost_optimizing",
			Metadata: []string{fieldRegion, "IP Address"},
		},
		{
			ID:          "hjLMh88uM8",
			Name:        "MFA on Root Account",
			Description: "Checks the root account and warns if multi-factor authentication (MFA) is not enabled.",
			Category:    categorySecurity,
			Metadata:    []string{keyStatusField},
		},
		{
			ID:          "H7IgqkgtmV",
			Name:        "IAM Use",
			Description: "Checks for your use of AWS Identity and Access Management (IAM).",
			Category:    categorySecurity,
			Metadata:    []string{keyStatusField},
		},
		{
			ID:          "1iG5NDGVre",
			Name:        "Amazon S3 Bucket Permissions",
			Description: "Checks buckets in Amazon Simple Storage Service (Amazon S3) that have open access permissions.",
			Category:    categorySecurity,
			Metadata: []string{
				fieldRegion,
				"Bucket Name",
				"ACL Allows List",
				"ACL Allows Upload/Delete",
				"Policy Allows Access",
			},
		},
		{
			ID:          "R365s2Qddf",
			Name:        "Amazon RDS Multi-AZ",
			Description: "Checks for Amazon RDS DB instances that are deployed in a single Availability Zone.",
			Category:    "fault_tolerance",
			Metadata:    []string{fieldRegion, "DB Instance", "Multi-AZ"},
		},
		{
			ID:          "xSqX82fQu",
			Name:        "Amazon EC2 Availability Zone Balance",
			Description: "Checks the distribution of Amazon EC2 instances across Availability Zones in a region.",
			Category:    "fault_tolerance",
			Metadata:    []string{fieldRegion, "Availability Zone", "Instance Count"},
		},
	}
}

// DescribeTrustedAdvisorChecks returns the static list of Trusted Advisor checks.
func (b *InMemoryBackend) DescribeTrustedAdvisorChecks() []TrustedAdvisorCheck {
	return trustedAdvisorChecks()
}

// DescribeTrustedAdvisorCheckRefreshStatuses returns refresh statuses for the given check IDs.
// Uses RLock for the common read-only case; upgrades to a write lock only when state advancement
// is required (simulates the async progression: enqueued → processing → success).
func (b *InMemoryBackend) DescribeTrustedAdvisorCheckRefreshStatuses(
	checkIDs []string,
) []TrustedAdvisorCheckRefreshStatus {
	b.mu.RLock()
	needsWrite := false

	for _, id := range checkIDs {
		if s, ok := b.checkRefreshStatuses.Get(id); ok && s.Status != checkRefreshStatusSuccess {
			needsWrite = true

			break
		}
	}

	if !needsWrite {
		out := make([]TrustedAdvisorCheckRefreshStatus, 0, len(checkIDs))
		for _, id := range checkIDs {
			if s, ok := b.checkRefreshStatuses.Get(id); ok {
				out = append(out, *s)
			} else {
				out = append(out, TrustedAdvisorCheckRefreshStatus{
					CheckID:                    id,
					Status:                     checkRefreshStatusNone,
					MillisUntilNextRefreshable: 0,
				})
			}
		}
		b.mu.RUnlock()

		return out
	}

	b.mu.RUnlock()
	b.mu.Lock()
	defer b.mu.Unlock()

	out := make([]TrustedAdvisorCheckRefreshStatus, 0, len(checkIDs))
	for _, id := range checkIDs {
		if s, ok := b.checkRefreshStatuses.Get(id); ok {
			switch s.PollCount {
			case 0:
				s.PollCount++
			case 1:
				s.Status = checkRefreshStatusProcessing
				s.PollCount++
			default:
				s.Status = checkRefreshStatusSuccess
				s.MillisUntilNextRefreshable = 0
			}
			out = append(out, *s)
		} else {
			out = append(out, TrustedAdvisorCheckRefreshStatus{
				CheckID:                    id,
				Status:                     checkRefreshStatusNone,
				MillisUntilNextRefreshable: 0,
			})
		}
	}

	return out
}

// DescribeTrustedAdvisorCheckResult returns the result for the given Trusted Advisor check.
// Stored results (written by RefreshTrustedAdvisorCheck) take precedence over the static default.
func (b *InMemoryBackend) DescribeTrustedAdvisorCheckResult(checkID, _ string) *TrustedAdvisorCheckResult {
	b.mu.RLock()
	if stored, ok := b.checkResults.Get(checkID); ok {
		cp := *stored
		b.mu.RUnlock()

		return &cp
	}
	b.mu.RUnlock()

	// Default: cost_optimizing checks show "warning" to indicate potential savings;
	// all other categories show "ok".
	status := "ok"
	for _, c := range trustedAdvisorChecks() {
		if c.ID == checkID && c.Category == categoryCostOptimizing {
			status = "warning"

			break
		}
	}

	return &TrustedAdvisorCheckResult{
		CheckID:          checkID,
		Status:           status,
		Timestamp:        time.Now().UTC().Format(time.RFC3339),
		FlaggedResources: []TrustedAdvisorResourceDetail{},
		ResourcesSummary: TrustedAdvisorResourcesSummary{
			ResourcesProcessed:  defaultResourcesProcessed,
			ResourcesFlagged:    0,
			ResourcesIgnored:    0,
			ResourcesSuppressed: 0,
		},
		CategorySpecificSummary: &TrustedAdvisorCategorySpecificSummary{},
	}
}

// DescribeTrustedAdvisorCheckSummaries returns summaries for the given check IDs,
// derived from stored check results where available.
func (b *InMemoryBackend) DescribeTrustedAdvisorCheckSummaries(checkIDs []string) []TrustedAdvisorCheckSummary {
	b.mu.RLock()
	defer b.mu.RUnlock()

	ts := time.Now().UTC().Format(time.RFC3339)
	out := make([]TrustedAdvisorCheckSummary, 0, len(checkIDs))

	for _, id := range checkIDs {
		if stored, ok := b.checkResults.Get(id); ok {
			out = append(out, TrustedAdvisorCheckSummary{
				CheckID:                 id,
				Status:                  stored.Status,
				Timestamp:               stored.Timestamp,
				ResourcesSummary:        stored.ResourcesSummary,
				HasFlaggedResources:     stored.ResourcesSummary.ResourcesFlagged > 0,
				CategorySpecificSummary: stored.CategorySpecificSummary,
			})
		} else {
			out = append(out, TrustedAdvisorCheckSummary{
				CheckID:             id,
				Status:              "ok",
				Timestamp:           ts,
				HasFlaggedResources: false,
				ResourcesSummary: TrustedAdvisorResourcesSummary{
					ResourcesProcessed:  defaultResourcesProcessed,
					ResourcesFlagged:    0,
					ResourcesIgnored:    0,
					ResourcesSuppressed: 0,
				},
				CategorySpecificSummary: &TrustedAdvisorCategorySpecificSummary{},
			})
		}
	}

	return out
}

// RefreshTrustedAdvisorCheck enqueues a refresh for the given Trusted Advisor check.
// Evicts a random entry when the status map is at capacity to prevent unbounded growth.
func (b *InMemoryBackend) RefreshTrustedAdvisorCheck(checkID string) (*TrustedAdvisorCheckRefreshStatus, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if existing, ok := b.checkRefreshStatuses.Get(checkID); ok && time.Since(existing.RefreshTime) < time.Hour {
		cp := *existing

		return &cp, nil
	}

	if b.checkRefreshStatuses.Len() >= maxCheckRefreshStatuses {
		var evictID string

		b.checkRefreshStatuses.Range(func(s *TrustedAdvisorCheckRefreshStatus) bool {
			evictID = s.CheckID

			return false
		})

		if evictID != "" {
			b.checkRefreshStatuses.Delete(evictID)
		}
	}

	status := &TrustedAdvisorCheckRefreshStatus{
		CheckID:                    checkID,
		Status:                     checkRefreshStatusEnqueued,
		MillisUntilNextRefreshable: refreshMillisDefault,
		RefreshTime:                time.Now(),
	}
	b.checkRefreshStatuses.Put(status)

	cp := *status

	return &cp, nil
}

var (
	checkIDSetOnce sync.Once           //nolint:gochecknoglobals // sync.Once lazy init for static check ID set
	checkIDSet     map[string]struct{} //nolint:gochecknoglobals // populated exactly once by initCheckIDSet
)

func initCheckIDSet() {
	checkIDSetOnce.Do(func() {
		checks := trustedAdvisorChecks()
		checkIDSet = make(map[string]struct{}, len(checks))

		for _, c := range checks {
			checkIDSet[c.ID] = struct{}{}
		}
	})
}

func validCheckID(checkID string) bool {
	initCheckIDSet()
	_, ok := checkIDSet[checkID]

	return ok
}

func validateCheckIDs(checkIDs []string) error {
	if len(checkIDs) == 0 {
		return fmt.Errorf("%w: checkIds is required", ErrValidation)
	}
	for _, checkID := range checkIDs {
		if !validCheckID(checkID) {
			return fmt.Errorf("%w: invalid checkId %s", ErrValidation, checkID)
		}
	}

	return nil
}
