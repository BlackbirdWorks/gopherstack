package accessanalyzer

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
)

const (
	maxAnalyzerNameLen    = 255
	maxUnusedAccessAgeDay = 365
)

var analyzerNameRe = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.-]*$`)

func isValidAnalyzerType(t AnalyzerType) bool {
	switch t {
	case AnalyzerTypeAccount, AnalyzerTypeOrganization, AnalyzerTypeAccountUnusedAccess,
		AnalyzerTypeOrganizationUnusedAccess, "ACCOUNT_INTERNAL_ACCESS", "ORGANIZATION_INTERNAL_ACCESS":
		return true
	}

	return false
}

// validateCreateAnalyzer enforces the CreateAnalyzer name pattern (1-255,
// [A-Za-z][A-Za-z0-9_.-]*), the Type enum, and that the configuration union
// member matches the analyzer type.
func validateCreateAnalyzer(name string, t AnalyzerType, config json.RawMessage) error {
	if len(name) > maxAnalyzerNameLen || !analyzerNameRe.MatchString(name) {
		return fmt.Errorf("%w: analyzerName must match [A-Za-z][A-Za-z0-9_.-]* (1-255 characters)", ErrValidation)
	}

	return validateAnalyzerTypeAndConfig(t, config)
}

func validateAnalyzerTypeAndConfig(t AnalyzerType, config json.RawMessage) error {
	if !isValidAnalyzerType(t) {
		return fmt.Errorf("%w: unsupported analyzer type %q", ErrValidation, t)
	}

	if len(config) == 0 || string(config) == "null" {
		return nil
	}

	var members map[string]json.RawMessage
	if err := json.Unmarshal(config, &members); err != nil {
		return fmt.Errorf("%w: malformed configuration", ErrValidation)
	}

	if raw, ok := members["unusedAccess"]; ok {
		if !strings.HasSuffix(string(t), "_UNUSED_ACCESS") {
			return fmt.Errorf("%w: unusedAccess configuration requires an unused access analyzer type", ErrValidation)
		}

		return validateUnusedAccessAge(raw)
	}

	if _, ok := members["internalAccess"]; ok && !strings.HasSuffix(string(t), "_INTERNAL_ACCESS") {
		return fmt.Errorf("%w: internalAccess configuration requires an internal access analyzer type", ErrValidation)
	}

	return nil
}

func validateUnusedAccessAge(raw json.RawMessage) error {
	var cfg struct {
		UnusedAccessAge *int `json:"unusedAccessAge"`
	}

	if err := json.Unmarshal(raw, &cfg); err != nil {
		return fmt.Errorf("%w: malformed unusedAccess configuration", ErrValidation)
	}

	if cfg.UnusedAccessAge != nil && (*cfg.UnusedAccessAge < 1 || *cfg.UnusedAccessAge > maxUnusedAccessAgeDay) {
		return fmt.Errorf("%w: unusedAccessAge must be between 1 and 365", ErrValidation)
	}

	return nil
}

// SetAnalyzerCreateDelay keeps new analyzers in CREATING for d (default 0:
// immediately ACTIVE).
func (b *InMemoryBackend) SetAnalyzerCreateDelay(d time.Duration) {
	b.mu.Lock("SetAnalyzerCreateDelay")
	defer b.mu.Unlock()

	b.analyzerCreateDelay = d
}

func (b *InMemoryBackend) analyzerView(a *Analyzer) *Analyzer {
	cp := copyAnalyzer(a)
	if b.analyzerCreateDelay > 0 && time.Since(cp.CreatedAt) < b.analyzerCreateDelay {
		cp.Status = AnalyzerStatusCreating
	}

	return cp
}
