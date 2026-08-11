package rolesanywhere

import "context"

// validCertificateField reports whether f is one of the values the real
// CertificateField enum shape allows (botocore's
// rolesanywhere/2018-05-10/service-2.json "CertificateField" shape).
func validCertificateField(f string) bool {
	return f == "x509Subject" || f == "x509Issuer" || f == "x509SAN"
}

// PutAttributeMapping adds or replaces a certificate field mapping on a
// profile. certificateField must be one of the CertificateField enum values
// and rules must be non-nil with every entry carrying a non-empty specifier,
// matching validateOpPutAttributeMappingInput/validateMappingRule.
func (b *InMemoryBackend) PutAttributeMapping(
	ctx context.Context,
	profileID, certificateField string,
	rules []MappingRule,
) (*Profile, error) {
	if !validCertificateField(certificateField) || rules == nil {
		return nil, ErrValidation
	}

	for _, r := range rules {
		if r.Specifier == "" {
			return nil, ErrValidation
		}
	}

	b.mu.Lock("PutAttributeMapping")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	p, exists := b.profiles.Get(regionKey(region, profileID))
	if !exists {
		return nil, ErrProfileNotFound
	}

	amStore := b.attributeMappingsStore(region)
	mappings := amStore[profileID]
	updated := false

	for i, m := range mappings {
		if m.CertificateField == certificateField {
			mappings[i].MappingRules = append([]MappingRule(nil), rules...)
			updated = true

			break
		}
	}

	if !updated {
		mappings = append(mappings, AttributeMapping{
			CertificateField: certificateField,
			MappingRules:     append([]MappingRule(nil), rules...),
		})
	}

	amStore[profileID] = mappings

	return copyProfile(p), nil
}

// DeleteAttributeMapping removes a certificate field mapping (and optional
// specifiers) from a profile. certificateField must be one of the
// CertificateField enum values (same shape PutAttributeMapping validates),
// per validateOpDeleteAttributeMappingInput.
func (b *InMemoryBackend) DeleteAttributeMapping(
	ctx context.Context,
	profileID, certificateField string,
	specifiers []string,
) (*Profile, error) {
	if !validCertificateField(certificateField) {
		return nil, ErrValidation
	}

	b.mu.Lock("DeleteAttributeMapping")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	p, exists := b.profiles.Get(regionKey(region, profileID))
	if !exists {
		return nil, ErrProfileNotFound
	}

	amStore := b.attributeMappingsStore(region)

	if len(specifiers) == 0 {
		amStore[profileID] = removeFieldMapping(amStore[profileID], certificateField)
	} else {
		amStore[profileID] = removeSpecifiers(amStore[profileID], certificateField, specifiers)
	}

	return copyProfile(p), nil
}

// removeFieldMapping returns mappings with the named certificateField removed entirely.
func removeFieldMapping(mappings []AttributeMapping, certificateField string) []AttributeMapping {
	filtered := mappings[:0]

	for _, m := range mappings {
		if m.CertificateField != certificateField {
			filtered = append(filtered, m)
		}
	}

	return filtered
}

// removeSpecifiers returns mappings with the named specifiers removed from certificateField's rules.
func removeSpecifiers(mappings []AttributeMapping, certificateField string, specifiers []string) []AttributeMapping {
	specSet := make(map[string]bool, len(specifiers))

	for _, s := range specifiers {
		specSet[s] = true
	}

	for i, m := range mappings {
		if m.CertificateField != certificateField {
			continue
		}

		filtered := m.MappingRules[:0]

		for _, r := range m.MappingRules {
			if !specSet[r.Specifier] {
				filtered = append(filtered, r)
			}
		}

		mappings[i].MappingRules = filtered
	}

	return mappings
}

// GetAttributeMappings returns the attribute mappings for a profile.
func (b *InMemoryBackend) GetAttributeMappings(ctx context.Context, profileID string) []AttributeMapping {
	b.mu.RLock("GetAttributeMappings")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	amStore := b.attributeMappings[region]

	if amStore == nil {
		return nil
	}

	src := amStore[profileID]
	out := make([]AttributeMapping, len(src))
	copy(out, src)

	return out
}
