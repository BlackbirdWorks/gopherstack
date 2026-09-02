package redshift

import (
	"fmt"
	"strconv"
	"time"
)

// ---------------------------------------------------------------------------
// Serverless custom domain associations
// ---------------------------------------------------------------------------

// CreateCustomDomainAssociationSL associates a custom domain name with a
// serverless workgroup. customDomainName/customDomainCertificateArn/
// workgroupName are all required by the real CreateCustomDomainAssociationInput
// (confirmed against CreateCustomDomainAssociationRequest in service-2.json).
// Also mirrors the association onto the workgroup's own customDomain* fields
// (real Workgroup carries them directly), so a subsequent GetWorkgroup/
// ListWorkgroups reflects the association without a separate lookup.
func (b *InMemoryBackend) CreateCustomDomainAssociationSL(
	customDomainName, customDomainCertificateArn, workgroupName string,
) (*ServerlessCustomDomainAssociation, error) {
	b.mu.Lock("CreateCustomDomainAssociationSL")
	defer b.mu.Unlock()

	wg, ok := b.slWorkgroups.Get(workgroupName)
	if !ok {
		return nil, fmt.Errorf("%w: workgroup %q not found", ErrWorkgroupNotFound, workgroupName)
	}

	if _, exists := b.slCustomDomains.Get(customDomainName); exists {
		return nil, fmt.Errorf(
			"%w: custom domain %q is already associated with a workgroup",
			ErrCustomDomainSLConflict, customDomainName,
		)
	}

	expiry := time.Now().Add(slCertExpiryDays * 24 * time.Hour)

	assoc := &ServerlessCustomDomainAssociation{
		CustomDomainName:                  customDomainName,
		CustomDomainCertificateArn:        customDomainCertificateArn,
		CustomDomainCertificateExpiryTime: &expiry,
		WorkgroupName:                     workgroupName,
	}
	b.slCustomDomains.Put(assoc)
	b.slCustomDomainIdx.insert(customDomainName)

	wg.CustomDomainName = customDomainName
	wg.CustomDomainCertificateArn = customDomainCertificateArn
	wg.CustomDomainCertificateExpiryTime = &expiry

	return cloneCustomDomainAssociationSL(assoc), nil
}

// GetCustomDomainAssociationSL returns a custom domain association, validating
// it belongs to workgroupName the way GetCustomDomainAssociationInput's two
// required fields (customDomainName, workgroupName) imply.
func (b *InMemoryBackend) GetCustomDomainAssociationSL(
	customDomainName, workgroupName string,
) (*ServerlessCustomDomainAssociation, error) {
	b.mu.RLock("GetCustomDomainAssociationSL")
	defer b.mu.RUnlock()

	assoc, ok := b.slCustomDomains.Get(customDomainName)
	if !ok || assoc.WorkgroupName != workgroupName {
		return nil, fmt.Errorf(
			"%w: custom domain %q not associated with workgroup %q",
			ErrCustomDomainSLNotFound, customDomainName, workgroupName,
		)
	}

	return cloneCustomDomainAssociationSL(assoc), nil
}

// ListCustomDomainAssociationsSL returns associations, optionally filtered by
// customDomainName or customDomainCertificateArn (both optional filters on
// ListCustomDomainAssociationsRequest).
//
//nolint:dupl // pagination pattern is structurally identical across serverless resource types
func (b *InMemoryBackend) ListCustomDomainAssociationsSL(
	customDomainName, customDomainCertificateArn string,
	maxResults int,
	nextToken string,
) ([]*ServerlessCustomDomainAssociation, string) {
	b.mu.RLock("ListCustomDomainAssociationsSL")
	defer b.mu.RUnlock()

	keys := b.slCustomDomainIdx.ordered()
	list := make([]*ServerlessCustomDomainAssociation, 0, len(keys))

	for _, name := range keys {
		assoc, ok := b.slCustomDomains.Get(name)
		if !ok {
			continue
		}

		if customDomainName != "" && assoc.CustomDomainName != customDomainName {
			continue
		}

		if customDomainCertificateArn != "" && assoc.CustomDomainCertificateArn != customDomainCertificateArn {
			continue
		}

		list = append(list, cloneCustomDomainAssociationSL(assoc))
	}

	if maxResults <= 0 {
		maxResults = serverlessDefaultPageSize()
	}

	startIdx := decodeServerlessPageToken(nextToken)

	if startIdx >= len(list) {
		return []*ServerlessCustomDomainAssociation{}, ""
	}

	end := startIdx + maxResults
	var outToken string

	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}

// UpdateCustomDomainAssociationSL updates an association's certificate ARN --
// the only mutable field on UpdateCustomDomainAssociationInput besides the two
// identifying fields (confirmed against UpdateCustomDomainAssociationRequest).
func (b *InMemoryBackend) UpdateCustomDomainAssociationSL(
	customDomainName, workgroupName, customDomainCertificateArn string,
) (*ServerlessCustomDomainAssociation, error) {
	b.mu.Lock("UpdateCustomDomainAssociationSL")
	defer b.mu.Unlock()

	assoc, ok := b.slCustomDomains.Get(customDomainName)
	if !ok || assoc.WorkgroupName != workgroupName {
		return nil, fmt.Errorf(
			"%w: custom domain %q not associated with workgroup %q",
			ErrCustomDomainSLNotFound, customDomainName, workgroupName,
		)
	}

	expiry := time.Now().Add(slCertExpiryDays * 24 * time.Hour)
	assoc.CustomDomainCertificateArn = customDomainCertificateArn
	assoc.CustomDomainCertificateExpiryTime = &expiry

	if wg, wgOK := b.slWorkgroups.Get(workgroupName); wgOK {
		wg.CustomDomainCertificateArn = customDomainCertificateArn
		wg.CustomDomainCertificateExpiryTime = &expiry
	}

	return cloneCustomDomainAssociationSL(assoc), nil
}

// DeleteCustomDomainAssociationSL removes a custom domain association and
// clears the mirrored fields on its workgroup.
func (b *InMemoryBackend) DeleteCustomDomainAssociationSL(
	customDomainName, workgroupName string,
) (*ServerlessCustomDomainAssociation, error) {
	b.mu.Lock("DeleteCustomDomainAssociationSL")
	defer b.mu.Unlock()

	assoc, ok := b.slCustomDomains.Get(customDomainName)
	if !ok || assoc.WorkgroupName != workgroupName {
		return nil, fmt.Errorf(
			"%w: custom domain %q not associated with workgroup %q",
			ErrCustomDomainSLNotFound, customDomainName, workgroupName,
		)
	}

	cp := cloneCustomDomainAssociationSL(assoc)
	b.slCustomDomains.Delete(customDomainName)
	b.slCustomDomainIdx.remove(customDomainName)

	if wg, wgOK := b.slWorkgroups.Get(workgroupName); wgOK && wg.CustomDomainName == customDomainName {
		wg.CustomDomainName = ""
		wg.CustomDomainCertificateArn = ""
		wg.CustomDomainCertificateExpiryTime = nil
	}

	return cp, nil
}

// WorkgroupNameByCustomDomainSL resolves a custom domain name to its
// associated workgroup name. Used by GetCredentials, whose real
// GetCredentialsRequest accepts either customDomainName or workgroupName --
// "The custom domain name or the workgroup name must be included in the
// request" (confirmed against GetCredentialsRequest's documentation).
func (b *InMemoryBackend) WorkgroupNameByCustomDomainSL(customDomainName string) (string, error) {
	b.mu.RLock("WorkgroupNameByCustomDomainSL")
	defer b.mu.RUnlock()

	assoc, ok := b.slCustomDomains.Get(customDomainName)
	if !ok {
		return "", fmt.Errorf("%w: custom domain %q not found", ErrCustomDomainSLNotFound, customDomainName)
	}

	return assoc.WorkgroupName, nil
}

func cloneCustomDomainAssociationSL(a *ServerlessCustomDomainAssociation) *ServerlessCustomDomainAssociation {
	cp := *a

	if a.CustomDomainCertificateExpiryTime != nil {
		t := *a.CustomDomainCertificateExpiryTime
		cp.CustomDomainCertificateExpiryTime = &t
	}

	return &cp
}
