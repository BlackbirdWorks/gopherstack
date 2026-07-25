package eks

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// CreateEksAnywhereSubscription creates a new EKS Anywhere subscription. term
// is required by the real API (CreateEksAnywhereSubscriptionInput.Term) --
// callers must validate it before calling this method.
func (b *InMemoryBackend) CreateEksAnywhereSubscription(
	name string,
	term SubscriptionTerm,
	autoRenew bool,
	licenseQuantity int32,
	licenseType string,
	kv map[string]string,
) (*AnywhereSubscription, error) {
	b.mu.Lock("CreateEksAnywhereSubscription")
	defer b.mu.Unlock()

	id := stableID(name + strconv.FormatInt(time.Now().UnixNano(), 10))
	subARN := arn.Build("eks", b.region, b.accountID, "eks-anywhere-subscription/"+id)

	t := tags.New("eks.subscription." + id + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	now := time.Now().UTC()
	termCopy := term

	sub := &AnywhereSubscription{
		ID:              id,
		ARN:             subARN,
		Name:            name,
		Status:          statusActive,
		LicenseType:     licenseType,
		LicenseQuantity: licenseQuantity,
		CreatedAt:       now,
		EffectiveDate:   now,
		ExpirationDate:  now.AddDate(0, int(termCopy.Duration), 0),
		Term:            &termCopy,
		AutoRenew:       autoRenew,
		Tags:            t,
	}
	b.subscriptions.Put(sub)
	cp := *sub

	return &cp, nil
}

// DeleteEksAnywhereSubscription removes a subscription by ID.
func (b *InMemoryBackend) DeleteEksAnywhereSubscription(id string) (*AnywhereSubscription, error) {
	b.mu.Lock("DeleteEksAnywhereSubscription")
	defer b.mu.Unlock()

	sub, ok := b.subscriptions.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: subscription %s not found", ErrNotFound, id)
	}

	cp := *sub

	if sub.Tags != nil {
		sub.Tags.Close()
	}

	b.subscriptions.Delete(id)

	cp.Status = statusDeleting

	return &cp, nil
}

// DescribeEksAnywhereSubscription returns a subscription by ID.
func (b *InMemoryBackend) DescribeEksAnywhereSubscription(id string) (*AnywhereSubscription, error) {
	b.mu.RLock("DescribeEksAnywhereSubscription")
	defer b.mu.RUnlock()

	sub, ok := b.subscriptions.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: subscription %s not found", ErrNotFound, id)
	}

	cp := *sub

	return &cp, nil
}

// ListEksAnywhereSubscriptions returns all subscription IDs sorted.
func (b *InMemoryBackend) ListEksAnywhereSubscriptions() []*AnywhereSubscription {
	b.mu.RLock("ListEksAnywhereSubscriptions")
	defer b.mu.RUnlock()

	items := b.subscriptions.All()
	list := make([]*AnywhereSubscription, 0, len(items))

	for _, sub := range items {
		cp := *sub
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// UpdateEksAnywhereSubscription updates a subscription.
func (b *InMemoryBackend) UpdateEksAnywhereSubscription(
	id string, licenseQuantity *int32, licenseType string,
) (*AnywhereSubscription, error) {
	b.mu.Lock("UpdateEksAnywhereSubscription")
	defer b.mu.Unlock()

	sub, ok := b.subscriptions.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: subscription %s not found", ErrNotFound, id)
	}

	if licenseQuantity != nil {
		sub.LicenseQuantity = *licenseQuantity
	}

	if licenseType != "" {
		sub.LicenseType = licenseType
	}

	cp := *sub

	return &cp, nil
}

// AddSubscriptionInternal inserts a pre-built EKS Anywhere subscription into the backend.
// Intended only for test seeding.
func (b *InMemoryBackend) AddSubscriptionInternal(sub *AnywhereSubscription) {
	b.mu.Lock("AddSubscriptionInternal")
	defer b.mu.Unlock()

	if sub.Tags == nil {
		sub.Tags = tags.New("eks.subscription." + sub.ID + ".tags")
	}

	b.subscriptions.Put(sub)
}

// ListAllSubscriptions returns all EKS Anywhere subscriptions.
func (b *InMemoryBackend) ListAllSubscriptions() []*AnywhereSubscription {
	b.mu.RLock("ListAllSubscriptions")
	defer b.mu.RUnlock()

	items := b.subscriptions.All()
	list := make([]*AnywhereSubscription, 0, len(items))

	for _, s := range items {
		cp := *s
		list = append(list, &cp)
	}

	return list
}
