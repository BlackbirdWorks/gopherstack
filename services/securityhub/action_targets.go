package securityhub

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) actionTargetARN(id string) string {
	return arn.Build("securityhub", b.region, b.accountID, fmt.Sprintf("action/custom/%s", id))
}

func (b *InMemoryBackend) CreateActionTarget(name, description, id string) (string, error) {
	b.mu.Lock("CreateActionTarget")
	defer b.mu.Unlock()

	if !b.hubEnabled {
		return "", ErrHubNotEnabled
	}

	arn := b.actionTargetARN(id)

	if b.actionTargets.Has(arn) {
		return "", fmt.Errorf("%w: %s", ErrAlreadyExists, arn)
	}

	b.actionTargets.Put(&ActionTarget{
		ActionTargetArn: arn,
		Name:            name,
		Description:     description,
	})

	return arn, nil
}

func (b *InMemoryBackend) DescribeActionTargets(
	actionTargetArns []string,
	nextToken string,
	maxResults int,
) ([]*ActionTarget, string) {
	b.mu.RLock("DescribeActionTargets")
	defer b.mu.RUnlock()
	results := filterOrAll(actionTargetArns, b.actionTargets)

	return paginateSlice(results, nextToken, maxResults, maxDefaultResults)
}

func (b *InMemoryBackend) UpdateActionTarget(actionTargetArn, name, description string) error {
	b.mu.Lock("UpdateActionTarget")
	defer b.mu.Unlock()

	if !b.hubEnabled {
		return ErrHubNotEnabled
	}

	at, ok := b.actionTargets.Get(actionTargetArn)
	if !ok {
		return fmt.Errorf("%w: action target %s", ErrNotFound, actionTargetArn)
	}

	if name != "" {
		at.Name = name
	}

	if description != "" {
		at.Description = description
	}

	return nil
}

func (b *InMemoryBackend) DeleteActionTarget(actionTargetArn string) (string, error) {
	b.mu.Lock("DeleteActionTarget")
	defer b.mu.Unlock()

	if !b.hubEnabled {
		return "", ErrHubNotEnabled
	}

	if !b.actionTargets.Delete(actionTargetArn) {
		return "", fmt.Errorf("%w: action target %s", ErrNotFound, actionTargetArn)
	}

	return actionTargetArn, nil
}
