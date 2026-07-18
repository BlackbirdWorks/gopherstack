package stepfunctions

import (
	"fmt"
	"sort"
	"time"
)

// PublishStateMachineVersion creates an immutable snapshot version of a state machine.
func (b *InMemoryBackend) PublishStateMachineVersion(
	smARN, description, revisionID string,
) (*StateMachineVersion, error) {
	b.mu.Lock("PublishStateMachineVersion")
	defer b.mu.Unlock()

	sm, exists := b.stateMachines.Get(smARN)
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrStateMachineDoesNotExist, smARN)
	}

	versionNum := len(b.versionsByStateMachine.Get(smARN)) + 1
	vARN := b.versionARN(smARN, sm.Name, versionNum)

	v := &StateMachineVersion{
		StateMachineVersionArn: vARN,
		StateMachineArn:        smARN,
		Name:                   sm.Name,
		Definition:             sm.Definition,
		RoleArn:                sm.RoleArn,
		Type:                   sm.Type,
		Status:                 statusActive,
		Description:            description,
		RevisionID:             revisionID,
		CreationDate:           float64(time.Now().Unix()),
	}

	// Put also inserts v into the versionsByStateMachine index, replacing the
	// former manual b.smVersions[smARN] append.
	b.versions.Put(v)

	cp := *v

	return &cp, nil
}

// DescribeStateMachineVersion returns details for a specific version.
func (b *InMemoryBackend) DescribeStateMachineVersion(
	versionARN string,
) (*StateMachineVersion, error) {
	b.mu.RLock("DescribeStateMachineVersion")
	defer b.mu.RUnlock()

	v, exists := b.versions.Get(versionARN)
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrStateMachineVersionDoesNotExist, versionARN)
	}

	cp := *v

	return &cp, nil
}

// DeleteStateMachineVersion removes a specific version.
func (b *InMemoryBackend) DeleteStateMachineVersion(versionARN string) error {
	b.mu.Lock("DeleteStateMachineVersion")
	defer b.mu.Unlock()

	if !b.versions.Has(versionARN) {
		return fmt.Errorf("%w: %s", ErrStateMachineVersionDoesNotExist, versionARN)
	}

	// Delete also removes v from the versionsByStateMachine index, replacing
	// the former manual b.smVersions[smARN] filter-and-rebuild.
	b.versions.Delete(versionARN)

	return nil
}

// ListStateMachineVersions returns all versions for a state machine.
func (b *InMemoryBackend) ListStateMachineVersions(
	smARN, nextToken string, maxResults int,
) ([]StateMachineVersion, string, error) {
	b.mu.RLock("ListStateMachineVersions")
	defer b.mu.RUnlock()

	if !b.stateMachines.Has(smARN) {
		return nil, "", fmt.Errorf("%w: %s", ErrStateMachineDoesNotExist, smARN)
	}

	vers := b.versionsByStateMachine.Get(smARN)
	all := make([]StateMachineVersion, 0, len(vers))
	for _, v := range vers {
		all = append(all, *v)
	}

	// Return newest first.
	sort.Slice(all, func(i, j int) bool { return all[i].CreationDate > all[j].CreationDate })

	versions, token := paginate(all, nextToken, maxResults)

	return versions, token, nil
}
