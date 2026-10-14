package arczonalshift

import "time"

const (
	statusActive   = "ACTIVE"
	statusExpired  = "EXPIRED"
	statusCanceled = "CANCELED"

	shiftZonal    = "ZONAL_SHIFT"
	shiftPractice = "PRACTICE_RUN"
	shiftAutoshft = "ZONAL_AUTOSHIFT"

	outcomePending     = "PENDING"
	outcomeSucceeded   = "SUCCEEDED"
	outcomeFailed      = "FAILED"
	outcomeInterrupted = "INTERRUPTED"

	autoshiftEnabled  = "ENABLED"
	autoshiftDisabled = "DISABLED"

	alarmStateAlarm = "ALARM"

	appliedYes = "APPLIED"
	appliedNo  = "NOT_APPLIED"
)

// Shift is a customer-started zonal shift or practice run.
type Shift struct {
	StartTime  time.Time  `json:"startTime"`
	ExpiryTime time.Time  `json:"expiryTime"`
	CanceledAt *time.Time `json:"canceledAt,omitempty"`
	ID         string     `json:"id"`
	Resource   string     `json:"resource"`
	AwayFrom   string     `json:"awayFrom"`
	Comment    string     `json:"comment"`
	Type       string     `json:"type"`
	Outcome    string     `json:"outcome,omitempty"`
}

// Alarm is a practice-run alarm condition.
type Alarm struct {
	AlarmIdentifier string `json:"alarmIdentifier"`
	Type            string `json:"type"`
}

// PracticeConfig is a resource's practice run configuration.
type PracticeConfig struct {
	Resource       string   `json:"resource"`
	OutcomeAlarms  []Alarm  `json:"outcomeAlarms"`
	BlockingAlarms []Alarm  `json:"blockingAlarms,omitempty"`
	AllowedWindows []string `json:"allowedWindows,omitempty"`
	BlockedWindows []string `json:"blockedWindows,omitempty"`
	BlockedDates   []string `json:"blockedDates,omitempty"`
}

// ResourceSettings is the per-resource autoshift status; present once any setting is changed.
type ResourceSettings struct {
	Resource  string `json:"resource"`
	Autoshift string `json:"autoshift"`
}

// Settings holds account-level options.
type Settings struct {
	ID       string `json:"id"`
	Observer string `json:"observer"`
}
