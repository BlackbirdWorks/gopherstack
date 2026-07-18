package accessanalyzer

import (
	"sort"
	"time"

	"github.com/google/uuid"
)

// StartPolicyGeneration creates a new policy generation job.
func (b *InMemoryBackend) StartPolicyGeneration(principalArn string) (*PolicyGeneration, error) {
	b.mu.Lock("StartPolicyGeneration")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	completed := now

	pg := &PolicyGeneration{
		JobID:        uuid.NewString(),
		PrincipalArn: principalArn,
		Status:       PolicyGenerationStatusSucceeded,
		StartedOn:    now,
		CompletedOn:  &completed,
	}

	b.policyGenerations.Put(pg)

	return copyPolicyGeneration(pg), nil
}

// GetPolicyGeneration returns a policy generation job by ID.
func (b *InMemoryBackend) GetPolicyGeneration(jobID string) (*PolicyGeneration, error) {
	b.mu.RLock("GetPolicyGeneration")
	defer b.mu.RUnlock()

	pg, ok := b.policyGenerations.Get(jobID)
	if !ok {
		return nil, ErrPolicyGenerationNotFound
	}

	return copyPolicyGeneration(pg), nil
}

// CancelPolicyGeneration cancels a policy generation job.
func (b *InMemoryBackend) CancelPolicyGeneration(jobID string) error {
	b.mu.Lock("CancelPolicyGeneration")
	defer b.mu.Unlock()

	pg, ok := b.policyGenerations.Get(jobID)
	if !ok {
		return ErrPolicyGenerationNotFound
	}

	pg.Status = PolicyGenerationStatusCanceled

	return nil
}

// ListPolicyGenerations returns all policy generation jobs, optionally filtered by principalArn.
func (b *InMemoryBackend) ListPolicyGenerations(principalArn string) ([]*PolicyGeneration, error) {
	b.mu.RLock("ListPolicyGenerations")
	defer b.mu.RUnlock()

	all := b.policyGenerations.All()
	result := make([]*PolicyGeneration, 0, len(all))

	for _, pg := range all {
		if principalArn != "" && pg.PrincipalArn != principalArn {
			continue
		}

		result = append(result, copyPolicyGeneration(pg))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].JobID < result[j].JobID
	})

	return result, nil
}
