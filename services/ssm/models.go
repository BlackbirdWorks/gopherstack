package ssm

import (
	"time"
)

// UnixTimeFloat returns a unix timestamp float required by some AWS SDKs.
const nanoToSeconds = 1e9

func UnixTimeFloat(t time.Time) float64 {
	return float64(t.UnixNano()) / nanoToSeconds
}

// Document type constants.
const (
	DocumentTypeCommand    = "Command"
	DocumentTypeAutomation = "Automation"
	DocumentTypePolicy     = "Policy"
	DocumentTypeSession    = "Session"
)
