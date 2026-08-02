package outposts

import "time"

// CreateRenewalResult is CreateRenewal's flat response shape (also the
// idempotency-cache value type -- see store.go's renewalIdempotency). It is
// exported (unlike CreateRenewal's unexported request type) to match this
// package's convention: CreateOutpost, CreateQuote, CreateSite, and
// StartConnection all return an exported result type while taking an
// unexported request DTO.
type CreateRenewalResult struct {
	Currency              string
	OutpostID             string
	PaymentOption         string
	PaymentTerm           string
	MonthlyRecurringPrice float32
	UpfrontPrice          float32
}

func isValidPaymentOption(v string) bool {
	switch v {
	case PaymentOptionAllUpfront, PaymentOptionPartialUpfront, PaymentOptionNoUpfront:
		return true
	default:
		return false
	}
}

func isValidPaymentTerm(v string) bool {
	switch v {
	case PaymentTermOneYear, PaymentTermThreeYears, PaymentTermFiveYears:
		return true
	default:
		return false
	}
}

// CreateRenewal computes and records a renewal subscription for the Outpost
// identified by idOrARN. A non-empty ClientToken is honored as an
// idempotency key (see PARITY.md trap #8): a retried request with the same
// (Outpost, ClientToken) pair replays the original response without
// recomputing or re-recording a new subscription.
func (b *InMemoryBackend) CreateRenewal(idOrARN string, req *createRenewalRequest) (*CreateRenewalResult, error) {
	if !isValidPaymentOption(req.PaymentOption) {
		return nil, validationError("invalid PaymentOption: " + req.PaymentOption)
	}

	if !isValidPaymentTerm(req.PaymentTerm) {
		return nil, validationError("invalid PaymentTerm: " + req.PaymentTerm)
	}

	b.mu.Lock("CreateRenewal")
	defer b.mu.Unlock()

	o, ok := b.resolveOutpostLocked(idOrARN)
	if !ok {
		return nil, notFoundError(resourceOutpost, idOrARN)
	}

	cacheKey := o.ID + "::" + req.ClientToken
	if req.ClientToken != "" {
		if cached, cachedOK := b.renewalIdempotency[cacheKey]; cachedOK {
			return &cached, nil
		}
	}

	monthly, upfront := computePricing(req.PaymentOption, req.PaymentTerm)
	now := time.Now().UTC()
	endDate := now.AddDate(termYears(req.PaymentTerm), 0, 0)

	o.PaymentOption = req.PaymentOption
	o.PaymentTerm = req.PaymentTerm
	o.ContractEndDate = endDate
	o.Subscriptions = append(o.Subscriptions, Subscription{
		SubscriptionID:        newSubscriptionID(),
		SubscriptionType:      SubscriptionTypeRenewal,
		SubscriptionStatus:    SubscriptionStatusActive,
		Currency:              currencyUSD,
		BeginDate:             now,
		EndDate:               endDate,
		MonthlyRecurringPrice: float64(monthly),
		UpfrontPrice:          float64(upfront),
	})

	result := CreateRenewalResult{
		Currency:              currencyUSD,
		OutpostID:             o.ID,
		PaymentOption:         req.PaymentOption,
		PaymentTerm:           req.PaymentTerm,
		MonthlyRecurringPrice: monthly,
		UpfrontPrice:          upfront,
	}

	if req.ClientToken != "" {
		b.renewalIdempotency[cacheKey] = result
	}

	return &result, nil
}

// GetRenewalPricing returns the renewal pricing options for the Outpost
// identified by idOrARN. Only an ACTIVE Outpost can be priced -- an Outpost
// pending decommission returns UNABLE_TO_PRICE with no options, a chosen,
// documented interpretation of an otherwise-unspecified case (see
// PARITY.md).
func (b *InMemoryBackend) GetRenewalPricing(idOrARN string) (string, []PricingOption, error) {
	b.mu.RLock("GetRenewalPricing")
	defer b.mu.RUnlock()

	o, ok := b.resolveOutpostLocked(idOrARN)
	if !ok {
		return "", nil, notFoundError(resourceOutpost, idOrARN)
	}

	if o.LifeCycleStatus != LifeCycleStatusActive {
		return PricingResultUnableToPrice, nil, nil
	}

	return PricingResultPriced, buildPricingOptions(nil, nil), nil
}
