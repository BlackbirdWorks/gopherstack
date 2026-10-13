package apprunner

import (
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// ListOperations returns operations for a service with pagination.
func (b *InMemoryBackend) ListOperations(
	serviceArn string,
	maxResults int32,
	nextToken string,
) ([]*OperationSummary, string, error) {
	b.mu.RLock("ListOperations")
	defer b.mu.RUnlock()

	svc, ok := b.services.Get(serviceArn)
	if !ok {
		return nil, "", fmt.Errorf("service %s not found: %w", serviceArn, ErrNotFound)
	}

	all := make([]*OperationSummary, 0, len(svc.Operations))
	for _, op := range svc.Operations {
		s := op.toSummary()
		if b.operationActive(op) {
			s.Status = opStatusInProgress
			s.EndedAt = time.Time{}
		}

		all = append(all, &s)
	}

	limit := int(maxResults)
	pg := page.New(all, nextToken, limit, defaultMaxResults)

	return pg.Data, pg.Next, nil
}

const statusOperationInProgress = "OPERATION_IN_PROGRESS"

const opStatusInProgress = "IN_PROGRESS"

func (b *InMemoryBackend) now() time.Time {
	if b.clock != nil {
		return b.clock()
	}

	return time.Now()
}

// SetOperationDelay sets how long a service reports OPERATION_IN_PROGRESS after each
// create, update, pause, resume, deployment or delete. Zero (the default) settles instantly.
func (b *InMemoryBackend) SetOperationDelay(d time.Duration) {
	b.mu.Lock("SetOperationDelay")
	defer b.mu.Unlock()

	b.operationDelay = d
}

// SetClock overrides the backend clock for deterministic lifecycle tests; nil restores time.Now.
func (b *InMemoryBackend) SetClock(clock func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.clock = clock
}

func (b *InMemoryBackend) operationActive(op *storedOperation) bool {
	return b.operationDelay > 0 && b.now().Before(op.StartedAt.Add(b.operationDelay))
}

// effectiveStatus is the service status clients see: OPERATION_IN_PROGRESS while the latest operation is dwelling.
func (b *InMemoryBackend) effectiveStatus(svc *storedService) string {
	if n := len(svc.Operations); n > 0 && b.operationActive(svc.Operations[n-1]) {
		return statusOperationInProgress
	}

	return svc.Status
}

func (b *InMemoryBackend) serviceView(svc *storedService) Service {
	v := svc.toService()
	v.Status = b.effectiveStatus(svc)

	return v
}
