package outposts

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// quoteValidityWindow is how long a newly-created quote remains CREATED
// before this backend flips it to EXPIRED (see expireQuoteIfNeededLocked).
// The SDK does not specify a validity window anywhere; 30 days is a
// defensible, documented placeholder -- see PARITY.md.
const quoteValidityWindow = 30 * 24 * time.Hour

const requiredCountryCodeLen = 2

func isValidQuoteCapacityType(v string) bool {
	switch v {
	case QuoteCapacityTypeEC2, QuoteCapacityTypeEBS, QuoteCapacityTypeS3:
		return true
	default:
		return false
	}
}

func validateRequestedCapacities(capacities []quoteCapacityWire) error {
	if len(capacities) == 0 {
		return validationError("RequestedCapacities is required")
	}

	for _, c := range capacities {
		if !isValidQuoteCapacityType(c.QuoteCapacityType) {
			return validationError("invalid QuoteCapacityType: " + c.QuoteCapacityType)
		}

		if c.Quantity <= 0 {
			return validationError("RequestedCapacities[].Quantity must be positive")
		}

		if c.Unit == "" {
			return validationError("RequestedCapacities[].Unit is required")
		}
	}

	return nil
}

func toQuoteCapacityModel(w quoteCapacityWire) QuoteCapacity {
	return QuoteCapacity(w)
}

func toQuoteConstraintModel(w quoteConstraintWire) QuoteConstraint {
	return QuoteConstraint(w)
}

// siteForOutpostLocked resolves outpost's Site, or nil if outpost is nil or
// its Site no longer exists. Callers must hold b.mu.
func (b *InMemoryBackend) siteForOutpostLocked(outpost *Outpost) *Site {
	if outpost == nil {
		return nil
	}

	s, ok := b.sites.Get(outpost.SiteID)
	if !ok {
		return nil
	}

	return s
}

// buildOrderingRequirementsLocked resolves outpost's Site and delegates to
// buildOrderingRequirements (see ordering_requirements.go for the full
// 17-check bucketing). Callers must hold b.mu.
func (b *InMemoryBackend) buildOrderingRequirementsLocked(
	outpostID string,
	outpost *Outpost,
	countryCode string,
) []OrderingRequirement {
	return buildOrderingRequirements(
		outpostID,
		outpost,
		b.siteForOutpostLocked(outpost),
		countryCode,
	)
}

// CreateQuote creates a new quote, optionally associated with an existing
// Outpost.
func (b *InMemoryBackend) CreateQuote(req *createQuoteRequest) (*Quote, error) {
	if len(req.CountryCode) != requiredCountryCodeLen {
		return nil, validationError("CountryCode must be a 2-letter ISO-3166 code")
	}

	if err := validateRequestedCapacities(req.RequestedCapacities); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateQuote")
	defer b.mu.Unlock()

	var (
		outpost   *Outpost
		outpostID string
	)

	if req.OutpostIdentifier != "" {
		o, ok := b.resolveOutpostLocked(req.OutpostIdentifier)
		if !ok {
			return nil, notFoundError(resourceOutpost, req.OutpostIdentifier)
		}

		outpost = o
		outpostID = o.ID
	}

	now := time.Now().UTC()

	q := &Quote{
		ID:                      newQuoteID(),
		AccountID:               b.accountID,
		CountryCode:             req.CountryCode,
		Description:             req.Description,
		Status:                  QuoteStatusCreated,
		CreatedDate:             now,
		ExpirationDate:          now.Add(quoteValidityWindow),
		QuoteOptionID:           newQuoteOptionID(),
		RequestedPaymentOptions: cloneStrs(req.RequestedPaymentOptions),
		RequestedPaymentTerms:   cloneStrs(req.RequestedPaymentTerms),
		OrderingRequirements: b.buildOrderingRequirementsLocked(
			outpostID,
			outpost,
			req.CountryCode,
		),
		PricingOptions: buildPricingOptions(
			req.RequestedPaymentOptions,
			req.RequestedPaymentTerms,
		),
	}

	if outpost != nil {
		q.OutpostID = outpost.ID
		q.OutpostARN = outpost.ARN
	}

	for _, c := range req.RequestedCapacities {
		q.RequestedCapacities = append(q.RequestedCapacities, toQuoteCapacityModel(c))
	}

	for _, c := range req.RequestedConstraints {
		q.RequestedConstraints = append(q.RequestedConstraints, toQuoteConstraintModel(c))
	}

	b.quotes.Put(q)

	return q.clone(), nil
}

// clone returns a deep copy of q, so the returned Quote shares no mutable
// memory with the backend's stored copy -- every slice field below is
// reference-typed and, defensively, none of them should be shared between a
// caller's copy and the backend's stored *Quote.
func (q *Quote) clone() *Quote {
	cp := *q
	cp.RequestedCapacities = append([]QuoteCapacity(nil), q.RequestedCapacities...)
	cp.RequestedConstraints = append([]QuoteConstraint(nil), q.RequestedConstraints...)
	cp.RequestedPaymentOptions = cloneStrs(q.RequestedPaymentOptions)
	cp.RequestedPaymentTerms = cloneStrs(q.RequestedPaymentTerms)
	cp.OrderingRequirements = append([]OrderingRequirement(nil), q.OrderingRequirements...)
	cp.PricingOptions = append([]PricingOption(nil), q.PricingOptions...)

	return &cp
}

// expireQuoteIfNeededLocked flips q to EXPIRED if its validity window has
// elapsed. Callers must hold b.mu (write lock).
func expireQuoteIfNeededLocked(q *Quote) {
	if q.Status == QuoteStatusCreated && !q.ExpirationDate.IsZero() &&
		time.Now().After(q.ExpirationDate) {
		q.Status = QuoteStatusExpired
	}
}

// GetQuote returns a copy of the quote identified by idOrARN (a Quote ID, or
// an "arn:...:quote/<id>"-shaped identifier -- see resolveQuoteLocked).
func (b *InMemoryBackend) GetQuote(idOrARN string) (*Quote, error) {
	b.mu.Lock("GetQuote")
	defer b.mu.Unlock()

	q, ok := b.resolveQuoteLocked(idOrARN)
	if !ok {
		return nil, notFoundError(resourceQuote, idOrARN)
	}

	expireQuoteIfNeededLocked(q)

	return q.clone(), nil
}

// UpdateQuote applies a partial update to the quote with the given ID.
// UpdateQuoteInput's own wire error set has no ConflictException (verified
// via serializers/deserializers -- see PARITY.md), so this never rejects
// based on the quote's current status, even ORDER_SUBMITTED/EXPIRED --
// there is no wire-accurate error to signal that with.
func (b *InMemoryBackend) UpdateQuote(idOrARN string, req *updateQuoteRequest) (*Quote, error) {
	b.mu.Lock("UpdateQuote")
	defer b.mu.Unlock()

	q, ok := b.resolveQuoteLocked(idOrARN)
	if !ok {
		return nil, notFoundError(resourceQuote, idOrARN)
	}

	if req.CountryCode != "" {
		q.CountryCode = req.CountryCode
	}

	if req.Description != "" {
		q.Description = req.Description
	}

	if len(req.RequestedCapacities) > 0 {
		q.RequestedCapacities = nil
		for _, c := range req.RequestedCapacities {
			q.RequestedCapacities = append(q.RequestedCapacities, toQuoteCapacityModel(c))
		}
	}

	if len(req.RequestedConstraints) > 0 {
		q.RequestedConstraints = nil
		for _, c := range req.RequestedConstraints {
			q.RequestedConstraints = append(q.RequestedConstraints, toQuoteConstraintModel(c))
		}
	}

	if len(req.RequestedPaymentOptions) > 0 {
		q.RequestedPaymentOptions = cloneStrs(req.RequestedPaymentOptions)
	}

	if len(req.RequestedPaymentTerms) > 0 {
		q.RequestedPaymentTerms = cloneStrs(req.RequestedPaymentTerms)
	}

	if err := b.applyQuoteOutpostIdentifierLocked(q, req.OutpostIdentifier); err != nil {
		return nil, err
	}

	q.PricingOptions = buildPricingOptions(q.RequestedPaymentOptions, q.RequestedPaymentTerms)
	q.OrderingRequirements = b.buildOrderingRequirementsLocked(
		q.OutpostID,
		b.quoteOutpostLocked(q),
		q.CountryCode,
	)

	expireQuoteIfNeededLocked(q)

	return q.clone(), nil
}

// applyQuoteOutpostIdentifierLocked implements UpdateQuoteInput.OutpostIdentifier's
// tri-state semantics: nil means "omitted, no change"; a non-nil empty
// string means "clear the association" (per the SDK's own doc comment);
// anything else must resolve to a real Outpost. Callers must hold b.mu.
func (b *InMemoryBackend) applyQuoteOutpostIdentifierLocked(
	q *Quote,
	outpostIdentifier *string,
) error {
	if outpostIdentifier == nil {
		return nil
	}

	if *outpostIdentifier == "" {
		q.OutpostID = ""
		q.OutpostARN = ""

		return nil
	}

	o, ok := b.resolveOutpostLocked(*outpostIdentifier)
	if !ok {
		return notFoundError(resourceOutpost, *outpostIdentifier)
	}

	q.OutpostID = o.ID
	q.OutpostARN = o.ARN

	return nil
}

// quoteOutpostLocked resolves the Outpost currently associated with q, or
// nil if it has none. Callers must hold b.mu.
func (b *InMemoryBackend) quoteOutpostLocked(q *Quote) *Outpost {
	if q.OutpostID == "" {
		return nil
	}

	o, ok := b.outposts.Get(q.OutpostID)
	if !ok {
		return nil
	}

	return o
}

// DeleteQuote deletes the quote identified by idOrARN.
func (b *InMemoryBackend) DeleteQuote(idOrARN string) error {
	b.mu.Lock("DeleteQuote")
	defer b.mu.Unlock()

	q, ok := b.resolveQuoteLocked(idOrARN)
	if !ok {
		return notFoundError(resourceQuote, idOrARN)
	}

	b.quotes.Delete(q.ID)

	return nil
}

// ListQuotes returns a page of quotes. ListQuotesInput has no filters at
// all besides pagination (verified -- see PARITY.md).
func (b *InMemoryBackend) ListQuotes(token string, limit int) page.Page[*Quote] {
	b.mu.Lock("ListQuotes")
	defer b.mu.Unlock()

	all := b.quotes.Snapshot()
	for _, q := range all {
		expireQuoteIfNeededLocked(q)
	}

	return page.New(all, token, limit, defaultPageLimit)
}
