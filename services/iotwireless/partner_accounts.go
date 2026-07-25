package iotwireless

import (
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func partnerAccountARN(accountID, region, partnerAccountID string) string {
	return arn.Build("iotwireless", region, accountID, fmt.Sprintf("PartnerAccount/%s", partnerAccountID))
}

// AssociateAwsAccountWithPartnerAccount stores a partner account association and returns its ARN.
func (b *InMemoryBackend) AssociateAwsAccountWithPartnerAccount(
	accountID, region, partnerAccountID string,
	tags map[string]string,
) (string, error) {
	b.mu.Lock("AssociateAwsAccountWithPartnerAccount")
	defer b.mu.Unlock()

	arn := partnerAccountARN(accountID, region, partnerAccountID)
	b.partnerAccounts[partnerAccountID] = arn
	b.storeResourceTagsLocked(arn, tags)

	return arn, nil
}

// GetPartnerAccount returns the ARN for a partner account.
func (b *InMemoryBackend) GetPartnerAccount(partnerAccountID string) (string, error) {
	b.mu.RLock("GetPartnerAccount")
	defer b.mu.RUnlock()

	arn, ok := b.partnerAccounts[partnerAccountID]
	if !ok {
		return "", ErrPartnerAccountNotFound
	}

	return arn, nil
}

// ListPartnerAccounts returns all partner account ARNs.
func (b *InMemoryBackend) ListPartnerAccounts() map[string]string {
	b.mu.RLock("ListPartnerAccounts")
	defer b.mu.RUnlock()

	result := make(map[string]string, len(b.partnerAccounts))
	maps.Copy(result, b.partnerAccounts)

	return result
}

// DisassociateAwsAccountFromPartnerAccount removes a partner account association.
func (b *InMemoryBackend) DisassociateAwsAccountFromPartnerAccount(partnerAccountID string) error {
	b.mu.Lock("DisassociateAwsAccountFromPartnerAccount")
	defer b.mu.Unlock()

	if _, ok := b.partnerAccounts[partnerAccountID]; !ok {
		return ErrPartnerAccountNotFound
	}

	delete(b.partnerAccounts, partnerAccountID)

	return nil
}
