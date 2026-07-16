package stepfunctions

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions/asl"
)

// SetStateMachineConfigurations sets optional tracing, logging, and encryption configuration
// for a state machine. Any nil argument leaves the corresponding field unchanged.
func (b *InMemoryBackend) SetStateMachineConfigurations(
	arn string,
	tracing *TracingConfiguration,
	logging *LoggingConfiguration,
	encryption *EncryptionConfiguration,
) error {
	b.mu.Lock("SetStateMachineConfigurations")
	defer b.mu.Unlock()

	sm, ok := b.stateMachines.Get(arn)
	if !ok || sm == nil {
		return fmt.Errorf("%w: %s", ErrStateMachineDoesNotExist, arn)
	}

	if tracing != nil {
		sm.TracingConfiguration = tracing
	}

	if logging != nil {
		sm.LoggingConfiguration = logging
	}

	if encryption != nil {
		sm.EncryptionConfiguration = encryption
	}

	return nil
}

// CreateStateMachine creates and stores a new state machine in the caller's region.
func (b *InMemoryBackend) CreateStateMachine(
	ctx context.Context,
	name, definition, roleArn, smType string,
) (*StateMachine, error) {
	if smType == "" {
		smType = "STANDARD"
	}

	if err := validateName(name, maxStateMachineNameLen); err != nil {
		return nil, err
	}

	if err := validateRoleARN(roleArn); err != nil {
		return nil, err
	}

	// Validate the definition before storing.
	if _, err := asl.Parse(definition); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidDefinition, err)
	}

	region := getRegionFromContext(ctx, b.region)
	smARN := b.smARN(region, name)

	b.mu.Lock("CreateStateMachine")
	defer b.mu.Unlock()

	nameIdx := b.regionNameIndex(region)
	if existingARN, exists := nameIdx[name]; exists {
		if sm, _ := b.stateMachines.Get(existingARN); sm != nil && sm.Status != statusDeleting {
			// AWS idempotency: same name+definition+type+roleArn → return existing without error.
			if sm.Definition == definition && sm.Type == smType && sm.RoleArn == roleArn {
				cp := *sm

				return &cp, nil
			}

			return nil, fmt.Errorf("%w: %s", ErrStateMachineAlreadyExists, name)
		}
	}

	sm := &StateMachine{
		CreationDate:    float64(time.Now().Unix()),
		Name:            name,
		StateMachineArn: smARN,
		Type:            smType,
		Status:          statusActive,
		Definition:      definition,
		RoleArn:         roleArn,
	}
	b.stateMachines.Put(sm)
	nameIdx[name] = smARN

	return sm, nil
}

// DeleteStateMachine marks a state machine as DELETING then removes it.
func (b *InMemoryBackend) DeleteStateMachine(arn string) error {
	b.mu.Lock("DeleteStateMachine")
	defer b.mu.Unlock()

	sm, exists := b.stateMachines.Get(arn)
	if !exists {
		return fmt.Errorf("%w: %s", ErrStateMachineDoesNotExist, arn)
	}

	sm.Status = statusDeleting
	b.stateMachines.Delete(arn)

	smRegion := regionFromARN(arn, b.region)
	delete(b.nameIndex[smRegion], sm.Name)

	// Cancel running goroutines and clean up all executions and history for this SM.
	// Cloned first: b.executions.Delete below mutates the executionsByStateMachine
	// index this slice is backed by, so iterating the live index result while
	// deleting from it would be unsafe.
	execs := slices.Clone(b.executionsByStateMachine.Get(arn))
	for _, exec := range execs {
		execARN := exec.ExecutionArn

		if cancelFn, ok := b.cancelFns[execARN]; ok {
			cancelFn()
			delete(b.cancelFns, execARN)
			// Only tombstone executions whose goroutines are still running; completed
			// executions have already cleaned up their own tombstones.
			b.deletedExecs[execARN] = true
		}

		// Removes the execution (and its inline history) from the table and,
		// via the index, from the former smExecutions bookkeeping too.
		b.executions.Delete(execARN)
		delete(b.executionDefinitions, execARN)
		delete(b.historyTruncated, execARN)
	}

	delete(b.smExecsByStatus, arn)

	// Remove all versions for this state machine. Cloned first for the same
	// reason as executions above: b.versions.Delete mutates the
	// versionsByStateMachine index this slice is backed by.
	versions := slices.Clone(b.versionsByStateMachine.Get(arn))
	for _, v := range versions {
		b.versions.Delete(v.StateMachineVersionArn)
	}

	// Remove all aliases for this state machine.
	for _, aARN := range b.smAliases[arn] {
		b.aliases.Delete(aARN)
	}
	delete(b.smAliases, arn)

	return nil
}

// ListStateMachines returns state machines in the caller's region with optional pagination.
func (b *InMemoryBackend) ListStateMachines(
	ctx context.Context,
	nextToken string,
	maxResults int,
) ([]StateMachine, string, error) {
	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("ListStateMachines")
	defer b.mu.RUnlock()

	all := make([]StateMachine, 0, b.stateMachines.Len())
	for _, sm := range b.stateMachines.All() {
		if regionFromARN(sm.StateMachineArn, b.region) != region {
			continue
		}

		all = append(all, *sm)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	sms, token := paginate(all, nextToken, maxResults)

	return sms, token, nil
}

// DescribeStateMachine returns details for a single state machine.
func (b *InMemoryBackend) DescribeStateMachine(arn string) (*StateMachine, error) {
	b.mu.RLock("DescribeStateMachine")
	defer b.mu.RUnlock()

	sm, exists := b.stateMachines.Get(arn)
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrStateMachineDoesNotExist, arn)
	}

	cp := *sm

	return &cp, nil
}

// UpdateStateMachine updates a state machine's definition and/or roleArn.
// It returns the update timestamp (Unix epoch seconds).
func (b *InMemoryBackend) UpdateStateMachine(smARN, definition, roleArn string) (float64, error) {
	// Validate the new definition before acquiring the lock.
	if definition != "" {
		if _, err := asl.Parse(definition); err != nil {
			return 0, fmt.Errorf("%w: %w", ErrInvalidDefinition, err)
		}
	}

	if roleArn != "" {
		if err := validateRoleARN(roleArn); err != nil {
			return 0, err
		}
	}

	b.mu.Lock("UpdateStateMachine")
	defer b.mu.Unlock()

	sm, exists := b.stateMachines.Get(smARN)
	if !exists {
		return 0, fmt.Errorf("%w: %s", ErrStateMachineDoesNotExist, smARN)
	}

	if definition != "" {
		sm.Definition = definition
	}

	if roleArn != "" {
		sm.RoleArn = roleArn
	}

	sm.UpdatedDate = float64(time.Now().Unix())

	return sm.UpdatedDate, nil
}

func validateRoleARN(roleArn string) error {
	const arnParts = 6

	if roleArn == "" {
		return fmt.Errorf("%w: roleArn is required", ErrValidation)
	}

	if !strings.HasPrefix(roleArn, "arn:") {
		return fmt.Errorf("%w: roleArn must be an ARN", ErrInvalidRoleArn)
	}

	if strings.ContainsAny(roleArn, " \t\r\n") {
		return fmt.Errorf("%w: roleArn must not contain whitespace", ErrInvalidRoleArn)
	}

	parts := strings.Split(roleArn, ":")
	if len(parts) == arnParts {
		if parts[2] == "" || parts[5] == "" {
			return fmt.Errorf("%w: roleArn must include service and resource", ErrInvalidRoleArn)
		}
	}

	return nil
}
