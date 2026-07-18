package bedrock

import (
	"fmt"
	"sort"
)

// CreateFoundationModelAgreement creates a foundation model access agreement.
func (b *InMemoryBackend) CreateFoundationModelAgreement(
	modelID string,
) (*FoundationModelAgreement, error) {
	b.mu.Lock("CreateFoundationModelAgreement")
	defer b.mu.Unlock()

	if modelID == "" {
		return nil, fmt.Errorf("%w: modelId is required", ErrValidation)
	}

	agreement := &FoundationModelAgreement{
		ModelID: modelID,
	}
	b.foundationModelAgreements.Put(agreement)
	cp := *agreement

	return &cp, nil
}

// ListFoundationModelAgreementOffers returns all foundation model agreements.
func (b *InMemoryBackend) ListFoundationModelAgreementOffers() []*FoundationModelAgreement {
	b.mu.RLock("ListFoundationModelAgreementOffers")
	defer b.mu.RUnlock()

	agreements := make([]*FoundationModelAgreement, 0, b.foundationModelAgreements.Len())
	for _, a := range b.foundationModelAgreements.All() {
		cp := *a
		agreements = append(agreements, &cp)
	}

	sort.Slice(agreements, func(i, k int) bool {
		return agreements[i].ModelID < agreements[k].ModelID
	})

	return agreements
}

// DeleteFoundationModelAgreement removes an agreement by model ID.
func (b *InMemoryBackend) DeleteFoundationModelAgreement(modelID string) error {
	b.mu.Lock("DeleteFoundationModelAgreement")
	defer b.mu.Unlock()

	if _, ok := b.foundationModelAgreements.Get(modelID); !ok {
		return fmt.Errorf("%w: foundation model agreement for %s not found", ErrNotFound, modelID)
	}

	b.foundationModelAgreements.Delete(modelID)

	return nil
}

// GetUseCaseForModelAccess returns the current use case configuration.
func (b *InMemoryBackend) GetUseCaseForModelAccess() map[string]any {
	b.mu.RLock("GetUseCaseForModelAccess")
	defer b.mu.RUnlock()

	return map[string]any{
		"useCaseType":        b.useCaseType,
		"useCaseDescription": b.useCaseDescription,
	}
}

// PutUseCaseForModelAccess stores the use case configuration.
func (b *InMemoryBackend) PutUseCaseForModelAccess(useCaseType, description string) {
	b.mu.Lock("PutUseCaseForModelAccess")
	defer b.mu.Unlock()

	b.useCaseType = useCaseType
	b.useCaseDescription = description
}
