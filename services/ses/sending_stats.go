package ses

import (
	"slices"
	"sort"
	"time"
)

// maxSendQuota24Hours is the simulated 24-hour send quota returned by GetSendQuota.
const maxSendQuota24Hours = 200

// maxSendRate is the simulated max send rate (emails/second) returned by
// GetSendQuota and enforced by checkSendingAllowedLocked -- both read this
// same constant so the advertised and enforced rate can never drift
// (gopherstack-a6y).
const maxSendRate = 1

// sendQuotaWindow is the rolling window Max24HourSend is measured over.
const sendQuotaWindow = 24 * time.Hour

// sentInWindowLocked returns the count of emails whose Timestamp falls
// within window of now. b.emails is append-ordered by increasing Timestamp,
// so iterating backward lets us stop at the first entry older than the
// cutoff. Shared by the 24-hour (Max24HourSend) and per-second (MaxSendRate)
// quota checks. When excludeSimulatorOnly is true, rows whose
// Email.SimulatorOnly is set are skipped -- real AWS SES mailbox-simulator
// sends "don't affect your daily sending quota" but "are limited by your
// account's maximum sending rate" (see allRecipientsAreSimulator's doc
// comment, email_sending.go), so the 24-hour caller passes true and the
// per-second caller passes false.
//
// The caller MUST hold b.mu for reading or writing.
func (b *InMemoryBackend) sentInWindowLocked(window time.Duration, excludeSimulatorOnly bool) int {
	cutoff := time.Now().UTC().Add(-window)
	sent := 0

	for _, v := range slices.Backward(b.emails) {
		if v.Timestamp.Before(cutoff) {
			break
		}

		if excludeSimulatorOnly && v.SimulatorOnly {
			continue
		}

		sent++
	}

	return sent
}

// sentLast24HoursLocked returns the count of non-simulator-only emails sent
// within the past 24 hours (see sentInWindowLocked).
//
// The caller MUST hold b.mu for reading or writing.
func (b *InMemoryBackend) sentLast24HoursLocked() int {
	return b.sentInWindowLocked(sendQuotaWindow, true)
}

// sentLastSecondLocked returns the count of emails (including
// simulator-only ones) sent within the past second (see sentInWindowLocked).
//
// The caller MUST hold b.mu for reading or writing.
func (b *InMemoryBackend) sentLastSecondLocked() int {
	return b.sentInWindowLocked(time.Second, false)
}

// GetSendQuota returns simulated quota values.
// SentLast24Hours counts only emails sent within the past 24 hours.
func (b *InMemoryBackend) GetSendQuota() SendQuota {
	b.mu.RLock("GetSendQuota")
	defer b.mu.RUnlock()

	return SendQuota{
		Max24HourSend:   maxSendQuota24Hours,
		MaxSendRate:     maxSendRate,
		SentLast24Hours: float64(b.sentLast24HoursLocked()),
	}
}

// sendStatisticsDays is the number of days of send history returned by GetSendStatistics,
// matching real AWS SES behavior (last 2 weeks / 14 days).
const sendStatisticsDays = 14

// GetSendStatistics returns aggregated send data points (one per hour) for the last 14 days,
// matching real AWS SES behavior.
func (b *InMemoryBackend) GetSendStatistics() []SendDataPoint {
	b.mu.RLock("GetSendStatistics")
	defer b.mu.RUnlock()

	cutoff := time.Now().UTC().Add(-sendStatisticsDays * 24 * time.Hour)

	// Aggregate emails into hourly buckets within the 14-day window.
	// Rejects has no simulable AWS SES trigger in this backend (no content/virus
	// scanning exists) and is deliberately left at zero rather than fabricated.
	buckets := make(map[time.Time]SendDataPoint)

	for _, e := range b.emails {
		if e.Timestamp.Before(cutoff) {
			continue
		}

		hour := e.Timestamp.UTC().Truncate(time.Hour)
		p := buckets[hour]
		p.Timestamp = hour
		p.DeliveryAttempts++

		if e.Bounced {
			p.Bounces++
		}

		if e.Complained {
			p.Complaints++
		}

		buckets[hour] = p
	}

	result := make([]SendDataPoint, 0, len(buckets))
	for _, p := range buckets {
		result = append(result, p)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Timestamp.Before(result[j].Timestamp)
	})

	return result
}

// UpdateAccountSendingEnabled persists the account-level sending enabled flag.
func (b *InMemoryBackend) UpdateAccountSendingEnabled(enabled bool) {
	b.mu.Lock("UpdateAccountSendingEnabled")
	defer b.mu.Unlock()

	b.accountSendingEnabled = enabled
}

// GetAccountSendingEnabled returns the account-level sending enabled flag.
func (b *InMemoryBackend) GetAccountSendingEnabled() bool {
	b.mu.RLock("GetAccountSendingEnabled")
	defer b.mu.RUnlock()

	return b.accountSendingEnabled
}
