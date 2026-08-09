package cloudfront

import (
	"crypto/sha256"
	"fmt"
	"maps"
	"sort"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// anycastIPListARN builds an ARN for an Anycast IP list.
func (b *InMemoryBackend) anycastIPListARN(id string) string {
	return arn.Build("cloudfront", "", b.accountID, fmt.Sprintf("anycast-ip-list/%s", id))
}

// generateAnycastIPs derives a deterministic, unique-looking set of IPv4 addresses for an
// Anycast IP list from its ID and requested count, standing in for the real static IPs AWS
// allocates from its Anycast address pool.
func generateAnycastIPs(id string, ipCount int32) []string {
	ips := make([]string, 0, ipCount)
	for i := range ipCount {
		sum := sha256.Sum256(fmt.Appendf(nil, "%s-anycast-%d", id, i))
		// 15.0.0.0/8 is one of the real ranges AWS documents for CloudFront Anycast static IPs;
		// used here purely as a plausible, non-conflicting prefix for generated addresses.
		ips = append(ips, fmt.Sprintf("15.%d.%d.%d", sum[0], sum[1], sum[2]))
	}

	return ips
}

// CreateAnycastIPList creates a new Anycast IP list. Name must be unique among existing anycast
// IP lists. tags, if provided (first element only), seeds the list's tags.
func (b *InMemoryBackend) CreateAnycastIPList(
	name string, ipCount int32, tags ...map[string]string,
) (*AnycastIPList, error) {
	b.mu.Lock("CreateAnycastIpList")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if ipCount <= 0 {
		return nil, fmt.Errorf("%w: IpCount must be greater than 0", ErrValidation)
	}

	if ipCount > maxAnycastIPCount {
		return nil, fmt.Errorf(
			"%w: IpCount must not exceed %d", ErrValidation, maxAnycastIPCount,
		)
	}

	if _, exists := b.anycastIPListByName[name]; exists {
		return nil, fmt.Errorf("%w: anycast IP list with name %q already exists", ErrAlreadyExists, name)
	}

	id := generateID()
	list := &AnycastIPList{
		ID:         id,
		ARN:        b.anycastIPListARN(id),
		Name:       name,
		Status:     statusDeployed,
		ETag:       uuid.NewString(),
		IPCount:    ipCount,
		AnycastIPs: generateAnycastIPs(id, ipCount),
		Tags:       make(map[string]string),
	}
	if len(tags) > 0 {
		maps.Copy(list.Tags, tags[0])
	}
	b.anycastIPLists.Put(list)
	b.anycastIPListARNs[list.ARN] = id
	b.anycastIPListByName[name] = id

	return b.copyAnycastIPList(list), nil
}

// copyAnycastIPList returns a deep copy of an AnycastIPList. Must be called with the lock held.
func (b *InMemoryBackend) copyAnycastIPList(list *AnycastIPList) *AnycastIPList {
	cp := *list
	cp.AnycastIPs = append([]string(nil), list.AnycastIPs...)
	if list.Tags != nil {
		cp.Tags = make(map[string]string, len(list.Tags))
		maps.Copy(cp.Tags, list.Tags)
	}

	return &cp
}

func (b *InMemoryBackend) GetAnycastIPList(id string) (*AnycastIPList, error) {
	b.mu.RLock("GetAnycastIPList")
	defer b.mu.RUnlock()

	list, ok := b.anycastIPLists.Get(id)
	if !ok {
		return nil, ErrAnycastIPListNotFound
	}

	return b.copyAnycastIPList(list), nil
}

func (b *InMemoryBackend) ListAnycastIPLists() []*AnycastIPList {
	b.mu.RLock("ListAnycastIPLists")
	defer b.mu.RUnlock()

	out := make([]*AnycastIPList, 0, b.anycastIPLists.Len())
	for _, list := range b.anycastIPLists.All() {
		out = append(out, b.copyAnycastIPList(list))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out
}

// isValidAnycastIPAddressType reports whether ipAddressType is one of the values
// UpdateAnycastIpListInput accepts, or empty (unset) (cloudfront@v1.67.4 types/enums.go:427-434).
func isValidAnycastIPAddressType(ipAddressType string) bool {
	switch ipAddressType {
	case "", "ipv4", "ipv6", "dualstack":
		return true
	default:
		return false
	}
}

// UpdateAnycastIPList updates the IpAddressType of an existing Anycast IP list. IpCount is not
// a member of UpdateAnycastIpListInput (cloudfront@v1.67.4 api_op_UpdateAnycastIpList.go:28-56)
// -- a real client can never change the IP count via this operation, so it is left untouched.
func (b *InMemoryBackend) UpdateAnycastIPList(id, ipAddressType string) (*AnycastIPList, error) {
	b.mu.Lock("UpdateAnycastIPList")
	defer b.mu.Unlock()

	list, ok := b.anycastIPLists.Get(id)
	if !ok {
		return nil, ErrAnycastIPListNotFound
	}
	if !isValidAnycastIPAddressType(ipAddressType) {
		return nil, fmt.Errorf("%w: IpAddressType must be one of ipv4, ipv6, dualstack", ErrValidation)
	}
	if ipAddressType != "" {
		list.IPAddressType = ipAddressType
	}
	list.ETag = uuid.NewString()

	return b.copyAnycastIPList(list), nil
}

func (b *InMemoryBackend) DeleteAnycastIPList(id string) error {
	b.mu.Lock("DeleteAnycastIPList")
	defer b.mu.Unlock()

	list, ok := b.anycastIPLists.Get(id)
	if !ok {
		return ErrAnycastIPListNotFound
	}
	delete(b.anycastIPListByName, list.Name)
	delete(b.anycastIPListARNs, list.ARN)
	b.anycastIPLists.Delete(id)

	return nil
}

// ---------------------------------------------------------------------------
// ManagedCertificateDetails (per-distribution-tenant)
// ---------------------------------------------------------------------------
