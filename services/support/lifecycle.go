package support

import (
	"fmt"
	"net/mail"
	"time"
)

const caseStatusWorkInProgress = "work-in-progress"

// SetCaseProgressDelay sets how long a new case stays "opened" before it turns work-in-progress; zero disables.
func (b *InMemoryBackend) SetCaseProgressDelay(d time.Duration) {
	b.mu.Lock("SetCaseProgressDelay")
	defer b.mu.Unlock()
	b.caseProgressDelay = d
}

// effectiveCaseStatus derives the status a reader sees. Caller holds b.mu.
func (b *InMemoryBackend) effectiveCaseStatus(cs *Case) string {
	if b.caseProgressDelay > 0 && cs.Status == caseStatusOpened && time.Since(cs.CreatedTime) >= b.caseProgressDelay {
		return caseStatusWorkInProgress
	}

	return cs.Status
}

func validateCCEmails(emails []string) error {
	for _, e := range emails {
		addr, err := mail.ParseAddress(e)
		if err != nil || addr.Address != e {
			return fmt.Errorf("%w: ccEmailAddresses entry %q is not a valid email address", ErrValidation, e)
		}
	}

	return nil
}
