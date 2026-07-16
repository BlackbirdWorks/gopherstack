package redshift

import (
	"fmt"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// ---------------------------------------------------------------------------
// Serverless Usage Limits
// ---------------------------------------------------------------------------

// CreateServerlessUsageLimit creates a serverless usage limit.
func (b *InMemoryBackend) CreateServerlessUsageLimit(
	resourceArn, usageType, period, breachAction string,
	amount int64,
) (*ServerlessUsageLimit, error) {
	b.mu.Lock("CreateServerlessUsageLimit")
	defer b.mu.Unlock()

	id := randomHex(slIDHexBytes)
	ulArn := arn.Build("redshift-serverless", b.region, b.accountID, "usagelimit/"+id)

	ul := &ServerlessUsageLimit{
		UsageLimitArn: ulArn,
		UsageLimitID:  id,
		ResourceArn:   resourceArn,
		UsageType:     usageType,
		Amount:        amount,
		Period:        period,
		BreachAction:  breachAction,
	}
	b.slUsageLimits.Put(ul)
	b.slUsageLimitIdx.insert(id)

	return cloneServerlessUsageLimit(ul), nil
}

// GetServerlessUsageLimit returns a serverless usage limit by ID.
func (b *InMemoryBackend) GetServerlessUsageLimit(
	usageLimitID string,
) (*ServerlessUsageLimit, error) {
	b.mu.RLock("GetServerlessUsageLimit")
	defer b.mu.RUnlock()

	ul, ok := b.slUsageLimits.Get(usageLimitID)
	if !ok {
		return nil, fmt.Errorf(
			"%w: usage limit %q not found",
			ErrUsageLimitSLNotFound,
			usageLimitID,
		)
	}

	return cloneServerlessUsageLimit(ul), nil
}

// ListServerlessUsageLimits returns all serverless usage limits.
//
//nolint:dupl // pagination pattern is structurally identical across serverless resource types
func (b *InMemoryBackend) ListServerlessUsageLimits(
	resourceArn string,
	maxResults int,
	nextToken string,
) ([]*ServerlessUsageLimit, string) {
	b.mu.RLock("ListServerlessUsageLimits")
	defer b.mu.RUnlock()

	// Iterate the pre-sorted index so results are ordered without re-sorting.
	keys := b.slUsageLimitIdx.ordered()
	list := make([]*ServerlessUsageLimit, 0, len(keys))

	for _, id := range keys {
		ul, ok := b.slUsageLimits.Get(id)
		if !ok {
			continue
		}

		if resourceArn == "" || ul.ResourceArn == resourceArn {
			list = append(list, cloneServerlessUsageLimit(ul))
		}
	}

	if maxResults <= 0 {
		maxResults = serverlessDefaultPageSize()
	}

	startIdx := 0
	if nextToken != "" {
		if n, err := strconv.Atoi(nextToken); err == nil {
			startIdx = n
		}
	}

	if startIdx >= len(list) {
		return []*ServerlessUsageLimit{}, ""
	}

	end := startIdx + maxResults
	var outToken string

	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}

// UpdateServerlessUsageLimit updates a serverless usage limit.
func (b *InMemoryBackend) UpdateServerlessUsageLimit(
	usageLimitID, breachAction string,
	amount int64,
) (*ServerlessUsageLimit, error) {
	b.mu.Lock("UpdateServerlessUsageLimit")
	defer b.mu.Unlock()

	ul, ok := b.slUsageLimits.Get(usageLimitID)
	if !ok {
		return nil, fmt.Errorf(
			"%w: usage limit %q not found",
			ErrUsageLimitSLNotFound,
			usageLimitID,
		)
	}

	if amount > 0 {
		ul.Amount = amount
	}

	if breachAction != "" {
		ul.BreachAction = breachAction
	}

	return cloneServerlessUsageLimit(ul), nil
}

// DeleteServerlessUsageLimit deletes a serverless usage limit.
func (b *InMemoryBackend) DeleteServerlessUsageLimit(
	usageLimitID string,
) (*ServerlessUsageLimit, error) {
	b.mu.Lock("DeleteServerlessUsageLimit")
	defer b.mu.Unlock()

	ul, ok := b.slUsageLimits.Get(usageLimitID)
	if !ok {
		return nil, fmt.Errorf(
			"%w: usage limit %q not found",
			ErrUsageLimitSLNotFound,
			usageLimitID,
		)
	}

	cp := cloneServerlessUsageLimit(ul)
	b.slUsageLimits.Delete(usageLimitID)
	b.slUsageLimitIdx.remove(usageLimitID)

	return cp, nil
}

func cloneServerlessUsageLimit(ul *ServerlessUsageLimit) *ServerlessUsageLimit {
	cp := *ul

	return &cp
}
