package ocicore

import "time"

const (
	retentionDays  = "DAYS"
	retentionYears = "YEARS"
)

// RetentionIn is the retention part of create and update calls for backups.
type RetentionIn struct {
	RetentionPeriod              *RetentionDuration `json:"retentionPeriod"`
	IsPreventDeletionEnabled     *bool              `json:"isPreventDeletionEnabled"`
	IsRetentionLockEnabled       *bool              `json:"isRetentionLockEnabled"`
	IsIndefiniteRetentionEnabled *bool              `json:"isIndefiniteRetentionEnabled"`
}

func (d RetentionDuration) expiry(from time.Time) time.Time {
	if d.RetentionTimeUnit == retentionYears {
		return from.AddDate(d.RetentionTimeAmount, 0, 0)
	}

	return from.AddDate(0, 0, d.RetentionTimeAmount)
}

// apply validates the retention settings and stores them; created is the backup's creation time.
func (r *Retention) apply(in RetentionIn, created time.Time) *APIError {
	if d := in.RetentionPeriod; d != nil {
		if d.RetentionTimeUnit != retentionDays && d.RetentionTimeUnit != retentionYears {
			return errParam("retentionTimeUnit must be YEARS or DAYS")
		}

		if d.RetentionTimeAmount < 1 {
			return errParam("retentionTimeAmount must be at least 1")
		}
	}

	if r.IsRetentionLockEnabled && in.IsRetentionLockEnabled != nil && !*in.IsRetentionLockEnabled {
		return errConflict("A retention lock cannot be removed once it is enabled")
	}

	if d := in.RetentionPeriod; d != nil {
		at := d.expiry(created)
		r.RetentionPeriod, r.TimeRetentionExpiresAt = d, &at
	}

	if in.IsPreventDeletionEnabled != nil {
		r.IsPreventDeletionEnabled = *in.IsPreventDeletionEnabled
	}

	if in.IsRetentionLockEnabled != nil {
		r.IsRetentionLockEnabled = *in.IsRetentionLockEnabled
	}

	if in.IsIndefiniteRetentionEnabled != nil {
		r.IsIndefiniteRetentionEnabled = *in.IsIndefiniteRetentionEnabled
	}

	return nil
}

// protected reports whether retention settings forbid deleting the backup now.
func (r *Retention) protected(now time.Time) bool {
	if r.IsIndefiniteRetentionEnabled {
		return true
	}

	if !r.IsPreventDeletionEnabled && !r.IsRetentionLockEnabled {
		return false
	}

	return r.TimeRetentionExpiresAt == nil || now.Before(*r.TimeRetentionExpiresAt)
}

func (b *InMemoryBackend) retentionBlock(what, id string, r *Retention) *APIError {
	if r.protected(b.nowUTC()) {
		return errConflict("The " + what + " " + id + " is protected by its retention settings and cannot be deleted")
	}

	return nil
}
