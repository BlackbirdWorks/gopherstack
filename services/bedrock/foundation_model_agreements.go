package bedrock

import (
	"fmt"
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

// ListFoundationModelAgreementOffers returns the catalog of agreement offers
// available for modelID.
//
// Real AWS: this is a catalog lookup ("what offers exist for this model")
// keyed by a required modelId PATH parameter -- it has nothing to do with
// agreements a caller has already created via CreateFoundationModelAgreement.
// gopherstack previously implemented this as "list every agreement this
// account has created," a different resource entirely (and returned only
// {modelId} per entry, missing the required offerToken/termDetails fields).
// Since gopherstack does not model a real per-model offer catalog, this
// returns one deterministic, wire-shape-valid offer per known model ID.
func (b *InMemoryBackend) ListFoundationModelAgreementOffers(modelID string) []*FoundationModelAgreementOffer {
	b.mu.RLock("ListFoundationModelAgreementOffers")
	defer b.mu.RUnlock()

	if modelID == "" {
		return nil
	}

	return []*FoundationModelAgreementOffer{
		{
			OfferToken:   "offer-token-" + modelID,
			OfferID:      "offer-id-" + modelID,
			LegalTermURL: "https://aws.amazon.com/bedrock/model-terms/" + modelID,
		},
	}
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
