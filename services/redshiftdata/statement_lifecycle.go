package redshiftdata

import (
	"context"
	"fmt"
	"time"
)

const (
	statusSubmitted       = "SUBMITTED"
	statusStarted         = "STARTED"
	statusPicked          = "PICKED"
	halfDivisor           = 2
	statementPollInterval = 5 * time.Millisecond
)

// SetLifecycleDelay sets how long a statement takes to run; zero (default) finishes it instantly.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()
	b.lifecycleDelay = d
}

func isOpenStatus(status string) bool {
	return status == statusSubmitted || status == statusPicked || status == statusStarted
}

// initialStatementStatus is the status a new statement is created with.
func (b *InMemoryBackend) initialStatementStatus() string {
	if b.lifecycleDelay > 0 {
		return statusSubmitted
	}

	return statusFinished
}

// settleStatements advances every open statement to the status its age implies. Caller must hold b.mu for writing.
func (b *InMemoryBackend) settleStatements(now time.Time) {
	if b.lifecycleDelay <= 0 {
		return
	}

	for _, st := range b.stores {
		for _, stmt := range st.statements {
			if isOpenStatus(stmt.Status) {
				b.settleStatement(stmt, now)
			}
		}
	}
}

func (b *InMemoryBackend) settleStatement(stmt *Statement, now time.Time) {
	delay := b.lifecycleDelay
	age := now.Sub(stmt.CreatedAt)

	switch {
	case delay <= 0 || age >= delay:
		finishedAt := stmt.CreatedAt.Add(max(delay, 0))
		stmt.Status = statusFinished
		stmt.UpdatedAt = finishedAt
		stmt.DurationMs = max(delay.Milliseconds(), mockStatementDurationMs)
		stmt.ResultRows = demoResultRows
		stmt.ResultSize = demoResultSize

		for i := range stmt.SubStatements {
			sub := &stmt.SubStatements[i]
			sub.Status = statusFinished
			sub.UpdatedAt = finishedAt
			sub.DurationMs = stmt.DurationMs / int64(len(stmt.SubStatements))
		}
	case age >= delay/halfDivisor && stmt.Status != statusStarted:
		stmt.Status = statusStarted
		stmt.UpdatedAt = stmt.CreatedAt.Add(delay / halfDivisor)

		if len(stmt.SubStatements) > 0 {
			stmt.SubStatements[0].Status = statusStarted
			stmt.SubStatements[0].UpdatedAt = stmt.UpdatedAt
		}
	}
}

// abortSubStatements marks every unfinished sub-statement ABORTED.
func abortSubStatements(stmt *Statement, at time.Time) {
	for i := range stmt.SubStatements {
		sub := &stmt.SubStatements[i]
		if sub.Status != statusFinished && sub.Status != statusFailed {
			sub.Status = statusAborted
			sub.UpdatedAt = at
		}
	}
}

const maxWaitTimeSeconds = 30

// validateWaitTimeSeconds enforces the documented 0..30 second bound on WaitTimeSeconds.
func validateWaitTimeSeconds(seconds int32) error {
	if seconds < 0 || seconds > maxWaitTimeSeconds {
		return fmt.Errorf("%w: WaitTimeSeconds must be between 0 and %d", ErrValidation, maxWaitTimeSeconds)
	}

	return nil
}

// WaitForStatement blocks until the statement leaves SUBMITTED/PICKED/STARTED, wait elapses, or ctx ends.
func (b *InMemoryBackend) WaitForStatement(
	ctx context.Context, id string, wait time.Duration,
) (*Statement, error) {
	deadline := time.NewTimer(wait)
	defer deadline.Stop()

	tick := time.NewTicker(statementPollInterval)
	defer tick.Stop()

	for {
		stmt, err := b.DescribeStatement(ctx, id)
		if err != nil || !isOpenStatus(stmt.Status) || wait <= 0 {
			return stmt, err
		}

		select {
		case <-ctx.Done():
			return stmt, nil
		case <-deadline.C:
			return b.DescribeStatement(ctx, id)
		case <-tick.C:
		}
	}
}
