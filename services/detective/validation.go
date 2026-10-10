package detective

import (
	"fmt"
	"regexp"
	"time"
	"unicode/utf8"
)

const maxInvitationMessageLen = 1000

var graphARNRe = regexp.MustCompile(`^arn:aws[-\w]{0,10}?:detective:[-\w]{2,20}?:\d{12}?:graph:[0-9a-f]{32}$`)

// SetLifecycleDelay keeps new investigations RUNNING for d before they are
// SUCCESSFUL (default 0: immediately SUCCESSFUL).
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()

	b.lifecycleDelay = d
}

// requireGraphLocked rejects a malformed graph ARN as a ValidationException
// and an unknown one as ResourceNotFoundException.
func (b *InMemoryBackend) requireGraphLocked(graphARN string) error {
	if !graphARNRe.MatchString(graphARN) {
		return fmt.Errorf("%w: GraphArn is not a valid behavior graph ARN", ErrValidation)
	}

	if !b.graphs.Has(graphARN) {
		return ErrGraphNotFound
	}

	return nil
}

func pageLimit(maxResults int32, ceiling int) (int, error) {
	if maxResults < 0 || int(maxResults) > ceiling {
		return 0, fmt.Errorf("%w: MaxResults must be between 1 and %d", ErrValidation, ceiling)
	}

	if maxResults == 0 {
		return ceiling, nil
	}

	return int(maxResults), nil
}

func validateInvitationMessage(msg string) error {
	if utf8.RuneCountInString(msg) > maxInvitationMessageLen {
		return fmt.Errorf("%w: Message can contain at most %d characters", ErrValidation, maxInvitationMessageLen)
	}

	return nil
}

// settleInvestigationsLocked completes RUNNING investigations whose window has elapsed.
func (b *InMemoryBackend) settleInvestigationsLocked(now time.Time) {
	for _, inv := range b.investigations.All() {
		if inv.Status == investigationStatusRunning && !now.Before(inv.CreatedTime.Add(b.lifecycleDelay)) {
			inv.Status = investigationStatusSucceeded
		}
	}
}
