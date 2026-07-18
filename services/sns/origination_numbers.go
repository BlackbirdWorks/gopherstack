package sns

import (
	"sort"
)

// ListOriginationNumbers returns a paginated list of origination phone numbers for the
// backend's region, a next-page token (empty when the last page is reached), and any error.
// maxResults controls the page size; 0 means the default (30). Values exceeding 30 are clamped,
// matching AWS SNS limits for this operation.
//
// AWS SNS exposes no public "create origination number" API: origination numbers are
// provisioned by buying phone numbers through Pinpoint / AWS End User Messaging. A fresh
// account therefore legitimately has none, so an empty list is AWS-accurate. State is
// populated internally via [InMemoryBackend.SeedOriginationNumber] (used by tests).
func (b *InMemoryBackend) ListOriginationNumbers(
	nextToken string,
	maxResults int,
) ([]XMLOriginationPhone, string, error) {
	b.mu.RLock("ListOriginationNumbers")
	defer b.mu.RUnlock()

	all := b.sortedOriginationNumbers()

	offset, err := decodeToken(nextToken)
	if err != nil {
		return nil, "", ErrInvalidParameter
	}

	size := resolvePageSize(maxResults, defaultListOriginationNumbersResults, maxListOriginationNumbersResults)
	nums, next := paginate(all, offset, size)

	return nums, next, nil
}

// sortedOriginationNumbers returns the origination numbers for the backend's region sorted
// by phone number. Must be called with at least RLock held.
func (b *InMemoryBackend) sortedOriginationNumbers() []XMLOriginationPhone {
	src := b.originationNumbers[b.region]
	nums := make([]XMLOriginationPhone, len(src))
	copy(nums, src)

	sort.Slice(nums, func(i, j int) bool {
		return nums[i].PhoneNumber < nums[j].PhoneNumber
	})

	return nums
}

// SeedOriginationNumber populates an origination phone number for the backend's region.
// It exists because AWS SNS provides no public API to create origination numbers (they are
// provisioned via Pinpoint / AWS End User Messaging); this internal helper lets tests and
// fixtures populate state that [InMemoryBackend.ListOriginationNumbers] then returns honestly.
func (b *InMemoryBackend) SeedOriginationNumber(num XMLOriginationPhone) {
	b.mu.Lock("SeedOriginationNumber")
	defer b.mu.Unlock()

	if b.originationNumbers == nil {
		b.originationNumbers = make(map[string][]XMLOriginationPhone)
	}

	b.originationNumbers[b.region] = append(b.originationNumbers[b.region], num)
}
