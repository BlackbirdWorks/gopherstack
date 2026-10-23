package servicequotas

// QuotaLimit returns the value applied to a catalogued quota, for services that enforce their own limits.
// Empty account or region fall back to the backend's.
func (b *InMemoryBackend) QuotaLimit(account, region, serviceCode, quotaCode string) (float64, bool) {
	b.mu.Lock("QuotaLimit")
	defer b.mu.Unlock()

	b.settleLocked()

	svc, def, ok := b.catalog.Quota(serviceCode, quotaCode)
	if !ok {
		return 0, false
	}

	s := scope{account: account, region: region}
	if s.account == "" {
		s.account = b.accountID
	}

	if s.region == "" {
		s.region = b.region
	}

	v, _ := b.appliedValueLocked(s, svc.Code, def)

	return v, true
}
