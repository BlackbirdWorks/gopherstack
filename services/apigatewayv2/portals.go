package apigatewayv2

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"time"
)

// CreatePortal creates a new portal.
func (b *InMemoryBackend) CreatePortal(input CreatePortalInput) (*Portal, error) {
	b.mu.Lock("CreatePortal")
	defer b.mu.Unlock()

	id := randomID()
	portal := &Portal{
		PortalID: id,
		LogoURI:  input.LogoURI,
		Tags:     copyTags(input.Tags),
		Status:   "ACTIVE",
	}

	b.portals.Put(portal)

	cp := *portal

	return &cp, nil
}

// CreatePortalProduct creates a new portal product.
func (b *InMemoryBackend) CreatePortalProduct(input CreatePortalProductInput) (*PortalProduct, error) {
	if input.DisplayName == "" {
		return nil, fmt.Errorf("%w: displayName is required", ErrBadRequest)
	}

	b.mu.Lock("CreatePortalProduct")
	defer b.mu.Unlock()

	id := randomID()
	product := &PortalProduct{
		PortalProductID: id,
		DisplayName:     input.DisplayName,
		Description:     input.Description,
		Tags:            copyTags(input.Tags),
	}

	b.portalProducts.Put(product)

	cp := *product

	return &cp, nil
}

// CreateProductPage creates a new product page for a portal product.
func (b *InMemoryBackend) CreateProductPage(
	portalProductID string,
	_ CreateProductPageInput,
) (*ProductPage, error) {
	b.mu.Lock("CreateProductPage")
	defer b.mu.Unlock()

	if !b.portalProducts.Has(portalProductID) {
		return nil, ErrPortalProductNotFound
	}

	now := isoTime{time.Now()}
	id := randomID()
	page := &ProductPage{
		ProductPageID:   id,
		PortalProductID: portalProductID,
		LastModified:    &now,
	}

	b.productPages.Put(page)

	cp := *page

	return &cp, nil
}

// CreateProductRestEndpointPage creates a new product REST endpoint page for a portal product.
func (b *InMemoryBackend) CreateProductRestEndpointPage(
	portalProductID string,
	_ CreateProductRestEndpointPageInput,
) (*ProductRestEndpointPage, error) {
	b.mu.Lock("CreateProductRestEndpointPage")
	defer b.mu.Unlock()

	if !b.portalProducts.Has(portalProductID) {
		return nil, ErrPortalProductNotFound
	}

	now := isoTime{time.Now()}
	id := randomID()
	page := &ProductRestEndpointPage{
		ProductRestEndpointPageID: id,
		PortalProductID:           portalProductID,
		LastModified:              &now,
	}

	b.productREPages.Put(page)

	cp := *page

	return &cp, nil
}

// GetPortalProductSharingPolicy gets sharing policy for a portal product.
func (b *InMemoryBackend) GetPortalProductSharingPolicy(portalProductID string) (*PortalProductSharingPolicy, error) {
	b.mu.RLock("GetPortalProductSharingPolicy")
	defer b.mu.RUnlock()

	if !b.portalProducts.Has(portalProductID) {
		return nil, ErrPortalProductNotFound
	}

	return &PortalProductSharingPolicy{PolicyDocument: b.portalProductSharingPolicies[portalProductID]}, nil
}

// PutPortalProductSharingPolicy stores sharing policy for a portal product.
func (b *InMemoryBackend) PutPortalProductSharingPolicy(
	portalProductID, policyDocument string,
) (*PortalProductSharingPolicy, error) {
	b.mu.Lock("PutPortalProductSharingPolicy")
	defer b.mu.Unlock()

	if !b.portalProducts.Has(portalProductID) {
		return nil, ErrPortalProductNotFound
	}
	b.portalProductSharingPolicies[portalProductID] = policyDocument

	return &PortalProductSharingPolicy{PolicyDocument: policyDocument}, nil
}

// DeletePortalProductSharingPolicy deletes sharing policy for a portal product.
func (b *InMemoryBackend) DeletePortalProductSharingPolicy(portalProductID string) error {
	b.mu.Lock("DeletePortalProductSharingPolicy")
	defer b.mu.Unlock()

	if !b.portalProducts.Has(portalProductID) {
		return ErrPortalProductNotFound
	}
	delete(b.portalProductSharingPolicies, portalProductID)

	return nil
}

// GetPortal retrieves a portal by ID.
func (b *InMemoryBackend) GetPortal(portalID string) (*Portal, error) {
	b.mu.RLock("GetPortal")
	defer b.mu.RUnlock()

	p, ok := b.portals.Get(portalID)
	if !ok {
		return nil, ErrPortalNotFound
	}

	cp := *p

	return &cp, nil
}

// ListPortals retrieves all portals.
func (b *InMemoryBackend) ListPortals() ([]Portal, error) {
	b.mu.RLock("ListPortals")
	defer b.mu.RUnlock()

	all := b.portals.All()
	result := make([]Portal, 0, len(all))

	for _, p := range all {
		result = append(result, *p)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].PortalID < result[j].PortalID
	})

	return result, nil
}

// GetPortalProduct retrieves a portal product by ID.
func (b *InMemoryBackend) GetPortalProduct(portalProductID string) (*PortalProduct, error) {
	b.mu.RLock("GetPortalProduct")
	defer b.mu.RUnlock()

	pp, ok := b.portalProducts.Get(portalProductID)
	if !ok {
		return nil, ErrPortalProductNotFound
	}

	cp := *pp

	return &cp, nil
}

// ListPortalProducts retrieves all portal products.
func (b *InMemoryBackend) ListPortalProducts() ([]PortalProduct, error) {
	b.mu.RLock("ListPortalProducts")
	defer b.mu.RUnlock()

	all := b.portalProducts.All()
	result := make([]PortalProduct, 0, len(all))

	for _, pp := range all {
		result = append(result, *pp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].PortalProductID < result[j].PortalProductID
	})

	return result, nil
}

// ListProductPages retrieves all product pages for a portal product.
func (b *InMemoryBackend) ListProductPages(portalProductID string) ([]ProductPage, error) {
	b.mu.RLock("ListProductPages")
	defer b.mu.RUnlock()

	if !b.portalProducts.Has(portalProductID) {
		return nil, ErrPortalProductNotFound
	}

	pages := b.productPagesByPortalProduct.Get(portalProductID)
	result := make([]ProductPage, 0, len(pages))

	for _, p := range pages {
		result = append(result, *p)
	}

	return result, nil
}

// ListProductRestEndpointPages retrieves all product REST endpoint pages for a portal product.
func (b *InMemoryBackend) ListProductRestEndpointPages(portalProductID string) ([]ProductRestEndpointPage, error) {
	b.mu.RLock("ListProductRestEndpointPages")
	defer b.mu.RUnlock()

	if !b.portalProducts.Has(portalProductID) {
		return nil, ErrPortalProductNotFound
	}

	pages := b.productREPagesByPortalProduct.Get(portalProductID)
	result := make([]ProductRestEndpointPage, 0, len(pages))

	for _, p := range pages {
		result = append(result, *p)
	}

	return result, nil
}

// UpdatePortal updates fields on an existing portal.
func (b *InMemoryBackend) UpdatePortal(portalID string, input UpdatePortalInput) (*Portal, error) {
	b.mu.Lock("UpdatePortal")
	defer b.mu.Unlock()

	p, ok := b.portals.Get(portalID)
	if !ok {
		return nil, ErrPortalNotFound
	}

	if input.Tags != nil {
		if p.Tags == nil {
			p.Tags = make(map[string]string)
		}
		maps.Copy(p.Tags, input.Tags)
	}

	if input.LogoURI != "" {
		p.LogoURI = input.LogoURI
	}
	if input.Status != "" {
		p.Status = input.Status
	}

	cp := *p

	return &cp, nil
}

// UpdatePortalProduct updates fields on an existing portal product.
func (b *InMemoryBackend) UpdatePortalProduct(
	portalProductID string,
	input UpdatePortalProductInput,
) (*PortalProduct, error) {
	b.mu.Lock("UpdatePortalProduct")
	defer b.mu.Unlock()

	pp, ok := b.portalProducts.Get(portalProductID)
	if !ok {
		return nil, ErrPortalProductNotFound
	}

	if input.Tags != nil {
		if pp.Tags == nil {
			pp.Tags = make(map[string]string)
		}
		maps.Copy(pp.Tags, input.Tags)
	}

	if input.DisplayName != "" {
		pp.DisplayName = input.DisplayName
	}

	if input.Description != "" {
		pp.Description = input.Description
	}

	cp := *pp

	return &cp, nil
}

// UpdateProductPage updates a product page.
func (b *InMemoryBackend) UpdateProductPage(
	portalProductID, pageID string,
	input UpdateProductPageInput,
) (*ProductPage, error) {
	b.mu.Lock("UpdateProductPage")
	defer b.mu.Unlock()

	if !b.portalProducts.Has(portalProductID) {
		return nil, ErrPortalProductNotFound
	}

	page, ok := b.productPages.Get(productPageKey(portalProductID, pageID))
	if !ok {
		return nil, ErrProductPageNotFound
	}

	now := isoTime{time.Now()}
	if input.DisplayContent != nil {
		page.DisplayContent = input.DisplayContent
	}
	page.LastModified = &now

	cp := *page

	return &cp, nil
}

// UpdateProductRestEndpointPage updates a product REST endpoint page.
func (b *InMemoryBackend) UpdateProductRestEndpointPage(
	portalProductID, pageID string,
	input UpdateProductRestEndpointPageInput,
) (*ProductRestEndpointPage, error) {
	b.mu.Lock("UpdateProductRestEndpointPage")
	defer b.mu.Unlock()

	if !b.portalProducts.Has(portalProductID) {
		return nil, ErrPortalProductNotFound
	}

	page, ok := b.productREPages.Get(productREPageKey(portalProductID, pageID))
	if !ok {
		return nil, ErrProductREPageNotFound
	}

	now := isoTime{time.Now()}
	if input.DisplayContent != nil {
		page.DisplayContent = input.DisplayContent
	}
	page.LastModified = &now

	cp := *page

	return &cp, nil
}

// DeletePortal removes a portal by ID.
func (b *InMemoryBackend) DeletePortal(portalID string) error {
	b.mu.Lock("DeletePortal")
	defer b.mu.Unlock()

	if !b.portals.Delete(portalID) {
		return ErrPortalNotFound
	}

	return nil
}

// DeletePortalProduct removes a portal product and all its associated pages.
func (b *InMemoryBackend) DeletePortalProduct(portalProductID string) error {
	b.mu.Lock("DeletePortalProduct")
	defer b.mu.Unlock()

	if !b.portalProducts.Delete(portalProductID) {
		return ErrPortalProductNotFound
	}

	for _, p := range slices.Clone(b.productPagesByPortalProduct.Get(portalProductID)) {
		b.productPages.Delete(productPageKey(portalProductID, p.ProductPageID))
	}

	for _, p := range slices.Clone(b.productREPagesByPortalProduct.Get(portalProductID)) {
		b.productREPages.Delete(productREPageKey(portalProductID, p.ProductRestEndpointPageID))
	}

	// Clean up the sharing policy entry so deleting a product does not leak its
	// policy document (AWS removes the associated sharing policy on delete).
	delete(b.portalProductSharingPolicies, portalProductID)

	return nil
}

// GetProductPage retrieves a specific product page.
func (b *InMemoryBackend) GetProductPage(portalProductID, pageID string) (*ProductPage, error) {
	b.mu.RLock("GetProductPage")
	defer b.mu.RUnlock()

	if !b.portalProducts.Has(portalProductID) {
		return nil, ErrPortalProductNotFound
	}

	p, ok := b.productPages.Get(productPageKey(portalProductID, pageID))
	if !ok {
		return nil, ErrProductPageNotFound
	}

	cp := *p

	return &cp, nil
}

// GetProductRestEndpointPage retrieves a specific product REST endpoint page.
func (b *InMemoryBackend) GetProductRestEndpointPage(portalProductID, pageID string) (*ProductRestEndpointPage, error) {
	b.mu.RLock("GetProductRestEndpointPage")
	defer b.mu.RUnlock()

	if !b.portalProducts.Has(portalProductID) {
		return nil, ErrPortalProductNotFound
	}

	p, ok := b.productREPages.Get(productREPageKey(portalProductID, pageID))
	if !ok {
		return nil, ErrProductREPageNotFound
	}

	cp := *p

	return &cp, nil
}

// DeleteProductPage removes a product page from a portal product.
func (b *InMemoryBackend) DeleteProductPage(portalProductID, pageID string) error {
	b.mu.Lock("DeleteProductPage")
	defer b.mu.Unlock()

	if !b.portalProducts.Has(portalProductID) {
		return ErrPortalProductNotFound
	}

	if !b.productPages.Delete(productPageKey(portalProductID, pageID)) {
		return ErrProductPageNotFound
	}

	return nil
}

// DeleteProductRestEndpointPage removes a product REST endpoint page from a portal product.
func (b *InMemoryBackend) DeleteProductRestEndpointPage(portalProductID, pageID string) error {
	b.mu.Lock("DeleteProductRestEndpointPage")
	defer b.mu.Unlock()

	if !b.portalProducts.Has(portalProductID) {
		return ErrPortalProductNotFound
	}

	if !b.productREPages.Delete(productREPageKey(portalProductID, pageID)) {
		return ErrProductREPageNotFound
	}

	return nil
}
