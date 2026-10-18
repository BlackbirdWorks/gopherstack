package marketplacemetering

import (
	"context"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

var tagTextPattern = regexp.MustCompile(`^[a-zA-Z0-9+ \-=._:\\/@]+$`)

// MeterUsageInput is the MeterUsage request.
type MeterUsageInput struct {
	Timestamp        *awstime.EpochTime `json:"Timestamp,omitempty"`
	UsageQuantity    *int32             `json:"UsageQuantity,omitempty"`
	DryRun           *bool              `json:"DryRun,omitempty"`
	ProductCode      string             `json:"ProductCode"`
	UsageDimension   string             `json:"UsageDimension"`
	ClientToken      string             `json:"ClientToken,omitempty"`
	UsageAllocations []UsageAllocation  `json:"UsageAllocations,omitempty"`
}

// MeterUsageOutput is the MeterUsage response.
type MeterUsageOutput struct {
	MeteringRecordID string `json:"MeteringRecordId,omitempty"`
}

// BatchMeterUsageInput is the BatchMeterUsage request.
type BatchMeterUsageInput struct {
	ProductCode  string        `json:"ProductCode"`
	UsageRecords []UsageRecord `json:"UsageRecords"`
}

// BatchMeterUsageOutput is the BatchMeterUsage response.
type BatchMeterUsageOutput struct {
	Results            []UsageRecordResult `json:"Results"`
	UnprocessedRecords []UsageRecord       `json:"UnprocessedRecords"`
}

func validateTag(t Tag) error {
	if t.Key == "" || len(t.Key) > maxTagKeyLen || !tagTextPattern.MatchString(t.Key) ||
		t.Value == "" || len(t.Value) > maxTagValueLen || !tagTextPattern.MatchString(t.Value) {
		return fail(ErrInvalidTag, "tag %q=%q is not valid", t.Key, t.Value)
	}

	return nil
}

func tagSetKey(tags []Tag) string {
	parts := make([]string, 0, len(tags))
	for _, t := range tags {
		parts = append(parts, t.Key+"="+t.Value)
	}

	slices.Sort(parts)

	return strings.Join(parts, "&")
}

// validateAllocations checks that allocations add up to quantity and carry unique, valid tag sets.
func validateAllocations(allocs []UsageAllocation, quantity int32) error {
	if len(allocs) == 0 {
		return nil
	}

	if len(allocs) > maxAllocations {
		return fail(ErrInvalidUsageAllocations, "at most %d usage allocations are allowed", maxAllocations)
	}

	var sum int64

	seen := map[string]bool{}

	for _, a := range allocs {
		if a.AllocatedUsageQuantity < 0 {
			return fail(ErrInvalidUsageAllocations, "AllocatedUsageQuantity must not be negative")
		}

		if len(a.Tags) > maxAllocationTags {
			return fail(ErrInvalidTag, "a usage allocation can have at most %d tags", maxAllocationTags)
		}

		for _, t := range a.Tags {
			if err := validateTag(t); err != nil {
				return err
			}
		}

		key := tagSetKey(a.Tags)
		if seen[key] {
			return fail(ErrInvalidUsageAllocations, "each usage allocation must have a unique set of tags")
		}

		seen[key] = true
		sum += int64(a.AllocatedUsageQuantity)
	}

	if sum != int64(quantity) {
		return fail(ErrInvalidUsageAllocations,
			"the usage allocations add up to %d but UsageQuantity is %d", sum, quantity)
	}

	return nil
}

func (b *InMemoryBackend) checkWindow(ts time.Time) error {
	now := b.now()
	if ts.Before(now.Add(-meterPastWindow)) || ts.After(now.Add(meterFutureWindow)) {
		return fail(ErrTimestampOutOfBounds,
			"Timestamp must be within the past %s and the next %s", meterPastWindow, meterFutureWindow)
	}

	return nil
}

func (p *Product) hasDimension(name string) bool {
	if !p.Declared || len(p.Dimensions) == 0 {
		return true
	}

	return slices.ContainsFunc(p.Dimensions, func(d Dimension) bool { return d.Name == name })
}

func validDimension(name string) bool { return name != "" && len(name) <= maxDimensionLen }

// validateMeterUsage checks the request without touching state and returns the effective quantity.
func validateMeterUsage(in *MeterUsageInput) (int32, error) {
	if !validProductCode(in.ProductCode) {
		return 0, fail(ErrInvalidProductCode, "ProductCode is not valid")
	}

	if !validDimension(in.UsageDimension) {
		return 0, fail(ErrInvalidUsageDimension, "UsageDimension must be 1-%d characters", maxDimensionLen)
	}

	if in.Timestamp == nil || in.Timestamp.IsZero() {
		return 0, fail(ErrValidation, "Timestamp is required")
	}

	var quantity int32

	if in.UsageQuantity != nil {
		quantity = *in.UsageQuantity
	}

	if quantity < 0 {
		return 0, fail(ErrValidation, "UsageQuantity must not be negative")
	}

	return quantity, validateAllocations(in.UsageAllocations, quantity)
}

// checkEntitledLocked verifies the dimension and, for declared products, the caller's subscription.
func (b *InMemoryBackend) checkEntitledLocked(in *MeterUsageInput, account string) error {
	p, ok := b.products.Get(in.ProductCode)
	if !ok {
		return nil
	}

	if !p.hasDimension(in.UsageDimension) {
		return fail(ErrInvalidUsageDimension, "dimension %q is not defined for the product", in.UsageDimension)
	}

	if _, subscribed := b.subscriptionLocked(in.ProductCode, account, ""); p.Declared && !subscribed {
		return fail(ErrCustomerNotEntitled, "account %s is not subscribed to the product", account)
	}

	return nil
}

// MeterUsage records hourly usage of one dimension for the calling account's product subscription.
// A repeat of the same (product, customer, dimension, timestamp) with the same quantity returns the original record;
// with another quantity it fails with DuplicateRequestException.
func (b *InMemoryBackend) MeterUsage(ctx context.Context, in *MeterUsageInput) (*MeterUsageOutput, error) {
	quantity, err := validateMeterUsage(in)
	if err != nil {
		return nil, err
	}

	account := b.callerAccount(ctx)

	b.mu.Lock("MeterUsage")
	defer b.mu.Unlock()

	if err = b.checkWindow(in.Timestamp.Time); err != nil {
		return nil, err
	}

	if err = b.checkEntitledLocked(in, account); err != nil {
		return nil, err
	}

	customer := customerID(in.ProductCode, account)
	if s, ok := b.subscriptionLocked(in.ProductCode, account, ""); ok {
		customer = s.CustomerIdentifier
	}

	key := usageDedupKey(in.ProductCode, customer, in.UsageDimension, in.Timestamp.Unix())
	if old, dup := b.usage.Get(key); dup {
		if old.Quantity != quantity {
			return nil, fail(ErrDuplicateRequest,
				"usage for this dimension and timestamp was already metered with quantity %d", old.Quantity)
		}

		return &MeterUsageOutput{MeteringRecordID: old.ID}, nil
	}

	if in.DryRun != nil && *in.DryRun {
		return nil, fail(ErrDryRunOperation, "Request would have succeeded, but DryRun flag is set")
	}

	b.productLocked(in.ProductCode)

	rec := &StoredUsage{
		ID: awsid.NewN("", recordIDLen), ProductCode: in.ProductCode, CustomerIdentifier: customer,
		CustomerAWSAccountID: account, Dimension: in.UsageDimension, Quantity: quantity,
		Timestamp: *in.Timestamp, RecordedAt: awstime.NewEpochTime(b.now()), Operation: opMeterUsage,
		Allocations: slices.Clone(in.UsageAllocations),
	}
	b.usage.Put(rec)

	return &MeterUsageOutput{MeteringRecordID: rec.ID}, nil
}

// BatchMeterUsage meters up to 25 customer records of one product; rejected records come back unprocessed.
func (b *InMemoryBackend) BatchMeterUsage(in *BatchMeterUsageInput) (*BatchMeterUsageOutput, error) {
	if !validProductCode(in.ProductCode) {
		return nil, fail(ErrInvalidProductCode, "ProductCode is required and must be valid")
	}

	if len(in.UsageRecords) == 0 || len(in.UsageRecords) > maxBatchRecords {
		return nil, fail(ErrValidation, "UsageRecords must hold between 1 and %d records", maxBatchRecords)
	}

	for i := range in.UsageRecords {
		r := &in.UsageRecords[i]
		if r.CustomerIdentifier == "" && r.CustomerAWSAccountID == "" && r.LicenseArn == "" {
			return nil, fail(ErrInvalidCustomerIdentifier, "record %d names no customer", i)
		}
	}

	b.mu.Lock("BatchMeterUsage")
	defer b.mu.Unlock()

	out := &BatchMeterUsageOutput{Results: []UsageRecordResult{}, UnprocessedRecords: []UsageRecord{}}

	for _, r := range in.UsageRecords {
		res, ok := b.meterRecordLocked(in.ProductCode, r)
		if !ok {
			out.UnprocessedRecords = append(out.UnprocessedRecords, r)

			continue
		}

		out.Results = append(out.Results, res)
	}

	return out, nil
}

// meterRecordLocked returns false when the record cannot be processed and must be returned to the caller.
func (b *InMemoryBackend) meterRecordLocked(productCode string, r UsageRecord) (UsageRecordResult, bool) {
	rec := r

	var quantity int32
	if r.Quantity != nil {
		quantity = *r.Quantity
	}

	if !validDimension(r.Dimension) || r.Timestamp == nil || r.Timestamp.IsZero() || quantity < 0 ||
		b.checkWindow(r.Timestamp.Time) != nil || validateAllocations(r.UsageAllocations, quantity) != nil {
		return UsageRecordResult{}, false
	}

	p := b.productLocked(productCode)
	if !p.hasDimension(r.Dimension) {
		return UsageRecordResult{}, false
	}

	sub, subscribed := b.subscriptionLocked(productCode, r.CustomerAWSAccountID, r.CustomerIdentifier)
	if p.Declared && !subscribed {
		return UsageRecordResult{UsageRecord: &rec, Status: statusNotSubscribed}, true
	}

	customer, account := r.CustomerIdentifier, r.CustomerAWSAccountID

	if subscribed {
		customer, account = sub.CustomerIdentifier, sub.CustomerAWSAccountID
	} else if customer == "" {
		customer = customerID(productCode, account)
	}

	key := usageDedupKey(productCode, customer, r.Dimension, r.Timestamp.Unix())
	if old, dup := b.usage.Get(key); dup {
		return UsageRecordResult{UsageRecord: &rec, MeteringRecordID: old.ID, Status: statusDuplicate}, true
	}

	stored := &StoredUsage{
		ID: awsid.NewN("", recordIDLen), ProductCode: productCode, CustomerIdentifier: customer,
		CustomerAWSAccountID: account, Dimension: r.Dimension, Quantity: quantity, Timestamp: *r.Timestamp,
		RecordedAt: awstime.NewEpochTime(b.now()), Operation: opBatchMeterUsage,
		Allocations: slices.Clone(r.UsageAllocations),
	}
	b.usage.Put(stored)

	return UsageRecordResult{UsageRecord: &rec, MeteringRecordID: stored.ID, Status: statusSuccess}, true
}

// UsageRecords returns the metering records stored for a product, oldest first.
func (b *InMemoryBackend) UsageRecords(productCode string) []StoredUsage {
	b.mu.RLock("UsageRecords")
	defer b.mu.RUnlock()

	var out []StoredUsage

	for _, u := range b.usage.All() {
		if u.ProductCode == productCode {
			out = append(out, *u)
		}
	}

	slices.SortFunc(out, func(a, c StoredUsage) int {
		if d := a.Timestamp.Compare(c.Timestamp.Time); d != 0 {
			return d
		}

		return strings.Compare(a.ID, c.ID)
	})

	return out
}
