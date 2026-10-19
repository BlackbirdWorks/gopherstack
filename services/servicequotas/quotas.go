package servicequotas

import (
	"context"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// QuotaView is a catalogued quota with the value that applies to the caller.
type QuotaView struct {
	Service *ServiceDef
	Def     *QuotaDef
	ARN     string
	Value   float64
	Applied bool
}

func paginate[T any](all []T, token string, maxResults *int32) (page.Page[T], error) {
	limit := defaultPageSize

	if maxResults != nil {
		if *maxResults < 1 || *maxResults > maxPageSize {
			return page.Page[T]{}, fail(ErrIllegalArgument, "MaxResults must be between 1 and %d", maxPageSize)
		}

		limit = int(*maxResults)
	}

	if err := page.ValidateToken(token); err != nil || (token != "" && page.DecodeToken(token) > len(all)) {
		return page.Page[T]{}, fail(ErrInvalidPaginationToken, "the NextToken is invalid")
	}

	return page.New(all, token, limit, defaultPageSize), nil
}

func (b *InMemoryBackend) lookupQuotaLocked(serviceCode, quotaCode string) (*ServiceDef, *QuotaDef, error) {
	if serviceCode == "" || quotaCode == "" {
		return nil, nil, fail(ErrIllegalArgument, "ServiceCode and QuotaCode are required")
	}

	svc, def, ok := b.catalog.Quota(serviceCode, quotaCode)
	if !ok {
		if svc == nil {
			return nil, nil, fail(ErrNoSuchResource, "The service %q was not found", serviceCode)
		}

		return nil, nil, fail(ErrNoSuchResource, "The quota %q was not found for service %q", quotaCode, serviceCode)
	}

	return svc, def, nil
}

func (b *InMemoryBackend) appliedValueLocked(s scope, svc string, def *QuotaDef) (float64, bool) {
	if av, ok := b.applied.Get(appliedKey(s, svc, def.Code)); ok {
		return av.Value, true
	}

	return def.Default, false
}

func (b *InMemoryBackend) viewLocked(s scope, svc *ServiceDef, def *QuotaDef) QuotaView {
	v, applied := b.appliedValueLocked(s, svc.Code, def)

	return QuotaView{Service: svc, Def: def, Value: v, Applied: applied, ARN: s.quotaARN(svc.Code, def.Code)}
}

func checkContext(contextID string) error {
	if contextID != "" {
		return fail(ErrIllegalArgument, "The quota does not support resource-level values; ContextId is not valid")
	}

	return nil
}

// ListServices returns the services that have quotas.
func (b *InMemoryBackend) ListServices(token string, maxResults *int32) (page.Page[ServiceDef], error) {
	return paginate(slices.Clone(b.catalog.Services()), token, maxResults)
}

// ListServiceQuotas returns the applied quotas of a service.
func (b *InMemoryBackend) ListServiceQuotas(
	ctx context.Context, serviceCode, quotaCode, level, token string, maxResults *int32,
) (page.Page[QuotaView], error) {
	if err := validateLevel(level); err != nil {
		return page.Page[QuotaView]{}, err
	}

	b.mu.Lock("ListServiceQuotas")
	defer b.mu.Unlock()

	b.settleLocked()

	svc, ok := b.catalog.Service(serviceCode)
	if !ok {
		return page.Page[QuotaView]{}, fail(ErrNoSuchResource, "The service %q was not found", serviceCode)
	}

	s := b.scopeOf(ctx)
	views := make([]QuotaView, 0, len(svc.Quotas))

	for i := range svc.Quotas {
		if quotaCode != "" && svc.Quotas[i].Code != quotaCode {
			continue
		}

		if level == levelResource {
			continue
		}

		views = append(views, b.viewLocked(s, svc, &svc.Quotas[i]))
	}

	return paginate(views, token, maxResults)
}

// ListDefaultQuotas returns the AWS default quotas of a service.
func (b *InMemoryBackend) ListDefaultQuotas(
	ctx context.Context, serviceCode, token string, maxResults *int32,
) (page.Page[QuotaView], error) {
	svc, ok := b.catalog.Service(serviceCode)
	if !ok {
		return page.Page[QuotaView]{}, fail(ErrNoSuchResource, "The service %q was not found", serviceCode)
	}

	region := b.scopeOf(ctx).region
	views := make([]QuotaView, 0, len(svc.Quotas))

	for i := range svc.Quotas {
		views = append(views, QuotaView{
			Service: svc, Def: &svc.Quotas[i], Value: svc.Quotas[i].Default,
			ARN: defaultQuotaARN(region, svc.Code, svc.Quotas[i].Code),
		})
	}

	return paginate(views, token, maxResults)
}

// GetServiceQuota returns the applied value of one quota.
func (b *InMemoryBackend) GetServiceQuota(
	ctx context.Context,
	serviceCode, quotaCode, contextID string,
) (QuotaView, error) {
	if err := checkContext(contextID); err != nil {
		return QuotaView{}, err
	}

	b.mu.Lock("GetServiceQuota")
	defer b.mu.Unlock()

	b.settleLocked()

	svc, def, err := b.lookupQuotaLocked(serviceCode, quotaCode)
	if err != nil {
		return QuotaView{}, err
	}

	return b.viewLocked(b.scopeOf(ctx), svc, def), nil
}

// GetDefaultQuota returns the AWS default of one quota.
func (b *InMemoryBackend) GetDefaultQuota(ctx context.Context, serviceCode, quotaCode string) (QuotaView, error) {
	b.mu.RLock("GetDefaultQuota")
	defer b.mu.RUnlock()

	svc, def, err := b.lookupQuotaLocked(serviceCode, quotaCode)
	if err != nil {
		return QuotaView{}, err
	}

	return QuotaView{
		Service: svc, Def: def, Value: def.Default,
		ARN: defaultQuotaARN(b.scopeOf(ctx).region, svc.Code, def.Code),
	}, nil
}

func validateLevel(level string) error {
	switch level {
	case "", levelAccount, levelResource, levelAll:
		return nil
	default:
		return fail(ErrIllegalArgument, "QuotaAppliedAtLevel %q is not valid", level)
	}
}
