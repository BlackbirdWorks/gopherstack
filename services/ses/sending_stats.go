package ses

import (
	"slices"
	"sort"
	"time"
)

// maxSendQuota24Hours is the simulated 24-hour send quota returned by GetSendQuota.
const maxSendQuota24Hours = 200

// maxSendRate is the simulated max send rate (emails/second) returned by GetSendQuota.
const maxSendRate = 1

// sentLast24HoursLocked returns the count of emails sent within the past 24
// hours. b.emails is append-ordered by increasing Timestamp, so iterating
// backward lets us stop at the first entry older than the cutoff.
//
// The caller MUST hold b.mu for reading or writing.
func (b *InMemoryBackend) sentLast24HoursLocked() int {
	cutoff := time.Now().UTC().Add(-24 * time.Hour)
	sent := 0

	for _, v := range slices.Backward(b.emails) {
		if v.Timestamp.Before(cutoff) {
			break
		}

		sent++
	}

	return sent
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
