package apprunner

import (
	"fmt"

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
		all = append(all, &s)
	}

	limit := int(maxResults)
	pg := page.New(all, nextToken, limit, defaultMaxResults)

	return pg.Data, pg.Next, nil
}
