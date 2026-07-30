package ec2

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// ---- BYOASN ----

// ProvisionIpamByoasn provisions a public ASN for use with an IPAM's BYOIP CIDRs.
func (b *InMemoryBackend) ProvisionIpamByoasn(ipamID, asn string) (*IpamByoasn, error) {
	if ipamID == "" || asn == "" {
		return nil, fmt.Errorf("%w: IpamId and Asn are required", ErrInvalidParameter)
	}

	b.mu.Lock("ProvisionIpamByoasn")
	defer b.mu.Unlock()

	if _, ok := b.ipams.Get(ipamID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamNotFound, ipamID)
	}

	byoasn := &IpamByoasn{
		Asn:    asn,
		IpamID: ipamID,
		State:  ipamByoasnStateProvisioned,
	}
	b.ipamByoasns.Put(byoasn)

	cp := *byoasn

	return &cp, nil
}

// DeprovisionIpamByoasn releases a previously-provisioned BYOASN.
func (b *InMemoryBackend) DeprovisionIpamByoasn(ipamID, asn string) (*IpamByoasn, error) {
	if ipamID == "" || asn == "" {
		return nil, fmt.Errorf("%w: IpamId and Asn are required", ErrInvalidParameter)
	}

	b.mu.Lock("DeprovisionIpamByoasn")
	defer b.mu.Unlock()

	byoasn, ok := b.ipamByoasns.Get(asn)
	if !ok || byoasn.IpamID != ipamID {
		return nil, fmt.Errorf("%w: %s", ErrIpamByoasnNotFound, asn)
	}
	b.ipamByoasns.Delete(asn)

	cp := *byoasn
	cp.State = ipamByoasnStateDeprovisioned

	return &cp, nil
}

// DescribeIpamByoasn returns all BYOASNs provisioned across all IPAMs.
func (b *InMemoryBackend) DescribeIpamByoasn() []*IpamByoasn {
	b.mu.RLock("DescribeIpamByoasn")
	defer b.mu.RUnlock()

	out := make([]*IpamByoasn, 0, b.ipamByoasns.Len())
	for _, a := range b.ipamByoasns.All() {
		cp := *a
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Asn < out[j].Asn })

	return out
}

// AssociateIpamByoasn associates a provisioned BYOASN with a BYOIP CIDR.
func (b *InMemoryBackend) AssociateIpamByoasn(asn, cidr string) (*IpamAsnAssociation, error) {
	if asn == "" || cidr == "" {
		return nil, fmt.Errorf("%w: Asn and Cidr are required", ErrInvalidParameter)
	}

	b.mu.Lock("AssociateIpamByoasn")
	defer b.mu.Unlock()

	if _, ok := b.ipamByoasns.Get(asn); !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamByoasnNotFound, asn)
	}

	assoc := &IpamAsnAssociation{Asn: asn, Cidr: cidr, State: ipamAsnAssocStateAssociated}
	b.ipamAsnAssociations.Put(assoc)

	cp := *assoc

	return &cp, nil
}

// DisassociateIpamByoasn removes the association between a BYOASN and a BYOIP CIDR.
func (b *InMemoryBackend) DisassociateIpamByoasn(asn, cidr string) (*IpamAsnAssociation, error) {
	if asn == "" || cidr == "" {
		return nil, fmt.Errorf("%w: Asn and Cidr are required", ErrInvalidParameter)
	}

	b.mu.Lock("DisassociateIpamByoasn")
	defer b.mu.Unlock()

	key := asn + "|" + cidr
	assoc, ok := b.ipamAsnAssociations.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: %s / %s", ErrIpamAsnAssociationNotFound, asn, cidr)
	}
	b.ipamAsnAssociations.Delete(key)

	cp := *assoc
	cp.State = ipamAsnAssocStateDisassociated

	return &cp, nil
}

// ---- External Resource Verification Tokens ----

// CreateIpamExternalResourceVerificationToken creates a token an external (non-AWS) resource
// owner can use to prove ownership of a resource so IPAM can monitor its CIDR.
func (b *InMemoryBackend) CreateIpamExternalResourceVerificationToken(
	ipamID string,
) (*IpamExternalResourceVerificationToken, error) {
	if ipamID == "" {
		return nil, fmt.Errorf("%w: IpamId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateIpamExternalResourceVerificationToken")
	defer b.mu.Unlock()

	ipam, ok := b.ipams.Get(ipamID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamNotFound, ipamID)
	}

	id := "ipam-ext-res-verification-token-" + uuid.New().String()[:8]
	now := time.Now().UTC()
	token := &IpamExternalResourceVerificationToken{
		IpamExternalResourceVerificationTokenID: id,
		IpamExternalResourceVerificationTokenARN: "arn:aws:ec2:" + b.Region + ":" + b.AccountID +
			":ipam-external-resource-verification-token/" + id,
		IpamID:     ipamID,
		IpamARN:    ipam.IpamARN,
		IpamRegion: b.Region,
		State:      ipamStateCreateComplete,
		Status:     ipamTokenStatusValid,
		TokenName:  newIPAMVerificationTokenName(),
		TokenValue: uuid.New().String(),
		NotAfter:   now.Add(ipamVerificationTokenValidity),
	}
	b.ipamVerificationTokens.Put(token)

	cp := *token

	return &cp, nil
}

// DeleteIpamExternalResourceVerificationToken removes a verification token.
func (b *InMemoryBackend) DeleteIpamExternalResourceVerificationToken(
	id string,
) (*IpamExternalResourceVerificationToken, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: IpamExternalResourceVerificationTokenId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteIpamExternalResourceVerificationToken")
	defer b.mu.Unlock()

	token, ok := b.ipamVerificationTokens.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamVerificationTokenNotFound, id)
	}
	b.ipamVerificationTokens.Delete(id)
	delete(b.tags, id)

	cp := *token
	cp.State = ipamStateDeleteComplete

	return &cp, nil
}

// DescribeIpamExternalResourceVerificationTokens returns verification tokens, optionally
// filtered by IDs.
func (b *InMemoryBackend) DescribeIpamExternalResourceVerificationTokens(
	ids []string,
) []*IpamExternalResourceVerificationToken {
	b.mu.RLock("DescribeIpamExternalResourceVerificationTokens")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*IpamExternalResourceVerificationToken, 0, b.ipamVerificationTokens.Len())

	for _, t := range b.ipamVerificationTokens.All() {
		if len(idSet) > 0 && !idSet[t.IpamExternalResourceVerificationTokenID] {
			continue
		}

		cp := *t
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].IpamExternalResourceVerificationTokenID < out[j].IpamExternalResourceVerificationTokenID
	})

	return out
}
