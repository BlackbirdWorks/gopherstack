package servicecatalog

import (
	"cmp"
	"context"
	"slices"
	"strings"
)

const (
	maxDisplayName = 100
	maxProvider    = 50
	maxDescription = 2000
	maxPrincipals  = 1000
)

func registerPortfolioOps(m map[string]opFunc, b *InMemoryBackend) {
	m["CreatePortfolio"] = op(b.CreatePortfolio)
	m["DescribePortfolio"] = op(b.DescribePortfolio)
	m["UpdatePortfolio"] = op(b.UpdatePortfolio)
	m["DeletePortfolio"] = op(b.DeletePortfolio)
	m["ListPortfolios"] = op(b.ListPortfolios)
	m["AssociatePrincipalWithPortfolio"] = op(b.AssociatePrincipalWithPortfolio)
	m["DisassociatePrincipalFromPortfolio"] = op(b.DisassociatePrincipalFromPortfolio)
	m["ListPrincipalsForPortfolio"] = op(b.ListPrincipalsForPortfolio)
	m["AssociateProductWithPortfolio"] = op(b.AssociateProductWithPortfolio)
	m["DisassociateProductFromPortfolio"] = op(b.DisassociateProductFromPortfolio)
	m["ListPortfoliosForProduct"] = op(b.ListPortfoliosForProduct)
}

func (b *InMemoryBackend) portfolioDetail(p *Portfolio) *PortfolioDetail {
	return &PortfolioDetail{
		ARN: p.ARN, CreatedTime: ts(p.CreatedTime), Description: p.Description, DisplayName: p.DisplayName,
		ID: p.ID, ProviderName: p.ProviderName,
	}
}

// ownedPortfolioLocked returns a portfolio the caller's account owns in the caller's Region.
func (b *InMemoryBackend) ownedPortfolioLocked(account, region, id string) (*Portfolio, error) {
	p, ok := b.portfolios.Get(id)
	if !ok || p.AccountID != account || p.Region != region {
		return nil, notFound("portfolio", id)
	}

	return p, nil
}

// accessiblePortfolioLocked returns a portfolio the caller owns or has accepted a share of.
func (b *InMemoryBackend) accessiblePortfolioLocked(account, region, id string) (*Portfolio, error) {
	p, ok := b.portfolios.Get(id)
	if !ok || p.Region != region {
		return nil, notFound("portfolio", id)
	}

	if p.AccountID == account || b.acceptedShareLocked(p.ID, account) {
		return p, nil
	}

	return nil, notFound("portfolio", id)
}

func (b *InMemoryBackend) acceptedShareLocked(portfolioID, account string) bool {
	for _, s := range b.shares.All() {
		if s.PortfolioID == portfolioID && s.Accepted && s.Recipient == account &&
			(s.Type == shareAccount || s.Type == shareMember) {
			return true
		}
	}

	return false
}

// accessiblePortfoliosLocked lists the portfolios the caller owns or has accepted, ordered by ID.
func (b *InMemoryBackend) accessiblePortfoliosLocked(account, region string) []*Portfolio {
	var out []*Portfolio

	for _, p := range b.portfolios.All() {
		if p.Region == region && (p.AccountID == account || b.acceptedShareLocked(p.ID, account)) {
			out = append(out, p)
		}
	}

	slices.SortFunc(out, func(x, y *Portfolio) int { return cmp.Compare(x.ID, y.ID) })

	return out
}

// CreatePortfolioInput is the CreatePortfolio request.
type CreatePortfolioInput struct {
	DisplayName      string `json:"DisplayName"`
	ProviderName     string `json:"ProviderName"`
	Description      string `json:"Description,omitempty"`
	IdempotencyToken string `json:"IdempotencyToken,omitempty"`
	Tags             []Tag  `json:"Tags,omitempty"`
}

// CreatePortfolioOutput is the CreatePortfolio response.
type CreatePortfolioOutput struct {
	PortfolioDetail *PortfolioDetail `json:"PortfolioDetail"`
	Tags            []Tag            `json:"Tags,omitempty"`
}

// CreatePortfolio creates a portfolio.
func (b *InMemoryBackend) CreatePortfolio(
	ctx context.Context,
	in *CreatePortfolioInput,
) (*CreatePortfolioOutput, error) {
	if in.DisplayName == "" || len(in.DisplayName) > maxDisplayName {
		return nil, invalid("DisplayName must be 1-%d characters", maxDisplayName)
	}

	if in.ProviderName == "" || len(in.ProviderName) > maxProvider {
		return nil, invalid("ProviderName must be 1-%d characters", maxProvider)
	}

	if len(in.Description) > maxDescription {
		return nil, invalid("Description must be at most %d characters", maxDescription)
	}

	tags := tagsToMap(in.Tags)
	if err := validateTags(tags); err != nil {
		return nil, err
	}

	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	p, err := idemCreate(b, "CreatePortfolio", in.IdempotencyToken, in,
		func(p *Portfolio) string { return p.ID },
		func(id string) (*Portfolio, error) {
			b.mu.RLock("CreatePortfolio")
			defer b.mu.RUnlock()

			return b.ownedPortfolioLocked(account, region, id)
		},
		func() (*Portfolio, error) {
			b.mu.Lock("CreatePortfolio")
			defer b.mu.Unlock()

			id := newID("port-")
			p := &Portfolio{
				ID: id, ARN: b.portfolioARN(account, region, id), AccountID: account, Region: region,
				DisplayName: in.DisplayName, Description: in.Description, ProviderName: in.ProviderName,
				CreatedTime: b.now(), Tags: tags,
			}
			b.portfolios.Put(p)

			return p, nil
		},
	)
	if err != nil {
		return nil, err
	}

	return &CreatePortfolioOutput{PortfolioDetail: b.portfolioDetail(p), Tags: mapToTags(p.Tags)}, nil
}

// DescribePortfolioInput is the DescribePortfolio request.
type DescribePortfolioInput struct {
	ID string `json:"Id"`
}

// DescribePortfolioOutput is the DescribePortfolio response.
type DescribePortfolioOutput struct {
	PortfolioDetail *PortfolioDetail  `json:"PortfolioDetail"`
	Tags            []Tag             `json:"Tags"`
	TagOptions      []TagOptionDetail `json:"TagOptions"`
	Budgets         []BudgetDetail    `json:"Budgets"`
}

// DescribePortfolio returns a portfolio with its tags, tag options and budgets.
func (b *InMemoryBackend) DescribePortfolio(
	ctx context.Context,
	in *DescribePortfolioInput,
) (*DescribePortfolioOutput, error) {
	b.mu.RLock("DescribePortfolio")
	defer b.mu.RUnlock()

	p, err := b.accessiblePortfolioLocked(b.callerAccount(ctx), b.callerRegion(ctx), in.ID)
	if err != nil {
		return nil, err
	}

	return &DescribePortfolioOutput{
		PortfolioDetail: b.portfolioDetail(p), Tags: orEmpty(mapToTags(p.Tags)),
		TagOptions: orEmpty(b.tagOptionsForLocked(p.ID)), Budgets: orEmpty(b.budgetsForLocked(p.ID)),
	}, nil
}

func orEmpty[T any](s []T) []T {
	if s == nil {
		return []T{}
	}

	return s
}

// UpdatePortfolioInput is the UpdatePortfolio request.
type UpdatePortfolioInput struct {
	ID           string   `json:"Id"`
	DisplayName  string   `json:"DisplayName,omitempty"`
	Description  *string  `json:"Description,omitempty"`
	ProviderName string   `json:"ProviderName,omitempty"`
	AddTags      []Tag    `json:"AddTags,omitempty"`
	RemoveTags   []string `json:"RemoveTags,omitempty"`
}

// UpdatePortfolio edits a portfolio's attributes and tags.
func (b *InMemoryBackend) UpdatePortfolio(
	ctx context.Context,
	in *UpdatePortfolioInput,
) (*CreatePortfolioOutput, error) {
	if len(in.DisplayName) > maxDisplayName || len(in.ProviderName) > maxProvider ||
		(in.Description != nil && len(*in.Description) > maxDescription) {
		return nil, invalid("an attribute exceeds its maximum length")
	}

	b.mu.Lock("UpdatePortfolio")
	defer b.mu.Unlock()

	p, err := b.ownedPortfolioLocked(b.callerAccount(ctx), b.callerRegion(ctx), in.ID)
	if err != nil {
		return nil, err
	}

	tags, err := mergeTags(p.Tags, tagsToMap(in.AddTags), in.RemoveTags)
	if err != nil {
		return nil, err
	}

	p.Tags = tags

	if in.DisplayName != "" {
		p.DisplayName = in.DisplayName
	}

	if in.ProviderName != "" {
		p.ProviderName = in.ProviderName
	}

	if in.Description != nil {
		p.Description = *in.Description
	}

	return &CreatePortfolioOutput{PortfolioDetail: b.portfolioDetail(p), Tags: mapToTags(p.Tags)}, nil
}

// EmptyOutput is the response of operations without output members.
type EmptyOutput struct{}

// DeletePortfolio deletes a portfolio that has no products, principals or shares.
func (b *InMemoryBackend) DeletePortfolio(ctx context.Context, in *DescribePortfolioInput) (*EmptyOutput, error) {
	b.mu.Lock("DeletePortfolio")
	defer b.mu.Unlock()

	p, err := b.ownedPortfolioLocked(b.callerAccount(ctx), b.callerRegion(ctx), in.ID)
	if err != nil {
		return nil, err
	}

	switch {
	case len(b.associationsOfPortfolioLocked(p.ID)) > 0:
		return nil, fail(ErrResourceInUse, "portfolio %s still has associated products", p.ID)
	case len(b.principalsOfPortfolioLocked(p.ID)) > 0:
		return nil, fail(ErrResourceInUse, "portfolio %s still has associated principals", p.ID)
	case len(b.sharesOfPortfolioLocked(p.ID)) > 0:
		return nil, fail(ErrResourceInUse, "portfolio %s is still shared", p.ID)
	}

	b.removeLinksLocked(p.ID)
	b.portfolios.Delete(p.ID)

	return &EmptyOutput{}, nil
}

// ListPortfoliosInput is the ListPortfolios request.
type ListPortfoliosInput struct {
	Paging
}

// ListPortfoliosOutput is the ListPortfolios response.
type ListPortfoliosOutput struct {
	NextPageToken    string             `json:"NextPageToken,omitempty"`
	PortfolioDetails []*PortfolioDetail `json:"PortfolioDetails"`
}

// ListPortfolios lists the portfolios the caller's account owns in the Region.
func (b *InMemoryBackend) ListPortfolios(ctx context.Context, in *ListPortfoliosInput) (*ListPortfoliosOutput, error) {
	b.mu.RLock("ListPortfolios")
	defer b.mu.RUnlock()

	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	var owned []*Portfolio

	for _, p := range b.accessiblePortfoliosLocked(account, region) {
		if p.AccountID == account {
			owned = append(owned, p)
		}
	}

	pg, err := pageOf(owned, in.Paging)
	if err != nil {
		return nil, err
	}

	return &ListPortfoliosOutput{PortfolioDetails: b.portfolioDetails(pg.Data), NextPageToken: pg.Next}, nil
}

func (b *InMemoryBackend) portfolioDetails(ps []*Portfolio) []*PortfolioDetail {
	out := make([]*PortfolioDetail, 0, len(ps))
	for _, p := range ps {
		out = append(out, b.portfolioDetail(p))
	}

	return out
}

func (b *InMemoryBackend) associationsOfPortfolioLocked(portfolioID string) []*Association {
	var out []*Association

	for _, a := range b.associations.All() {
		if a.PortfolioID == portfolioID {
			out = append(out, a)
		}
	}

	return out
}

func (b *InMemoryBackend) principalsOfPortfolioLocked(portfolioID string) []*Principal {
	var out []*Principal

	for _, p := range b.principals.All() {
		if p.PortfolioID == portfolioID {
			out = append(out, p)
		}
	}

	slices.SortFunc(out, func(x, y *Principal) int { return cmp.Compare(x.PrincipalARN, y.PrincipalARN) })

	return out
}

// removeLinksLocked drops tag option and budget links of a deleted resource.
func (b *InMemoryBackend) removeLinksLocked(resourceID string) {
	for _, l := range b.tagOptionLink.All() {
		if l.ResourceID == resourceID {
			b.tagOptionLink.Delete(l.Key)
		}
	}

	for _, l := range b.budgetLinks.All() {
		if l.ResourceID == resourceID {
			b.budgetLinks.Delete(l.Key)
		}
	}
}

// AssociatePrincipalWithPortfolioInput is the AssociatePrincipalWithPortfolio request.
type AssociatePrincipalWithPortfolioInput struct {
	PortfolioID   string `json:"PortfolioId"`
	PrincipalARN  string `json:"PrincipalARN"`
	PrincipalType string `json:"PrincipalType"`
}

// AssociatePrincipalWithPortfolio lets an IAM principal (or pattern) launch the portfolio's products.
func (b *InMemoryBackend) AssociatePrincipalWithPortfolio(
	ctx context.Context, in *AssociatePrincipalWithPortfolioInput,
) (*EmptyOutput, error) {
	if in.PrincipalType != "IAM" && in.PrincipalType != "IAM_PATTERN" {
		return nil, invalid("PrincipalType must be IAM or IAM_PATTERN")
	}

	if !strings.HasPrefix(in.PrincipalARN, "arn:") || len(in.PrincipalARN) > 1000 {
		return nil, invalid("PrincipalARN %q is not a valid ARN", in.PrincipalARN)
	}

	account := b.callerAccount(ctx)

	b.mu.Lock("AssociatePrincipalWithPortfolio")
	defer b.mu.Unlock()

	p, err := b.accessiblePortfolioLocked(account, b.callerRegion(ctx), in.PortfolioID)
	if err != nil {
		return nil, err
	}

	key := p.ID + "|" + account + "|" + in.PrincipalARN
	if _, exists := b.principals.Get(key); !exists {
		if len(b.principalsOfPortfolioLocked(p.ID)) >= maxPrincipals {
			return nil, fail(ErrLimitExceeded, "a portfolio can have at most %d principals", maxPrincipals)
		}
	}

	b.principals.Put(&Principal{
		Key: key, PortfolioID: p.ID, AccountID: account, PrincipalARN: in.PrincipalARN, PrincipalType: in.PrincipalType,
	})

	return &EmptyOutput{}, nil
}

// DisassociatePrincipalFromPortfolioInput is the DisassociatePrincipalFromPortfolio request.
type DisassociatePrincipalFromPortfolioInput struct {
	PortfolioID   string `json:"PortfolioId"`
	PrincipalARN  string `json:"PrincipalARN"`
	PrincipalType string `json:"PrincipalType,omitempty"`
}

// DisassociatePrincipalFromPortfolio removes a principal association.
func (b *InMemoryBackend) DisassociatePrincipalFromPortfolio(
	ctx context.Context, in *DisassociatePrincipalFromPortfolioInput,
) (*EmptyOutput, error) {
	account := b.callerAccount(ctx)

	b.mu.Lock("DisassociatePrincipalFromPortfolio")
	defer b.mu.Unlock()

	p, err := b.accessiblePortfolioLocked(account, b.callerRegion(ctx), in.PortfolioID)
	if err != nil {
		return nil, err
	}

	b.principals.Delete(p.ID + "|" + account + "|" + in.PrincipalARN)

	return &EmptyOutput{}, nil
}

// ListPrincipalsForPortfolioInput is the ListPrincipalsForPortfolio request.
type ListPrincipalsForPortfolioInput struct {
	PortfolioID string `json:"PortfolioId"`
	Paging
}

// PrincipalWire is the Principal shape.
type PrincipalWire struct {
	PrincipalARN  string `json:"PrincipalARN"`
	PrincipalType string `json:"PrincipalType"`
}

// ListPrincipalsForPortfolioOutput is the ListPrincipalsForPortfolio response.
type ListPrincipalsForPortfolioOutput struct {
	NextPageToken string          `json:"NextPageToken,omitempty"`
	Principals    []PrincipalWire `json:"Principals"`
}

// ListPrincipalsForPortfolio lists the principals the caller's account associated with a portfolio.
func (b *InMemoryBackend) ListPrincipalsForPortfolio(
	ctx context.Context, in *ListPrincipalsForPortfolioInput,
) (*ListPrincipalsForPortfolioOutput, error) {
	account := b.callerAccount(ctx)

	b.mu.RLock("ListPrincipalsForPortfolio")
	defer b.mu.RUnlock()

	p, err := b.accessiblePortfolioLocked(account, b.callerRegion(ctx), in.PortfolioID)
	if err != nil {
		return nil, err
	}

	var mine []PrincipalWire

	for _, pr := range b.principalsOfPortfolioLocked(p.ID) {
		if pr.AccountID == account {
			mine = append(mine, PrincipalWire{PrincipalARN: pr.PrincipalARN, PrincipalType: pr.PrincipalType})
		}
	}

	pg, err := pageOf(mine, in.Paging)
	if err != nil {
		return nil, err
	}

	return &ListPrincipalsForPortfolioOutput{Principals: orEmpty(pg.Data), NextPageToken: pg.Next}, nil
}

// AssociateProductWithPortfolioInput is the AssociateProductWithPortfolio request.
type AssociateProductWithPortfolioInput struct {
	PortfolioID       string `json:"PortfolioId"`
	ProductID         string `json:"ProductId"`
	SourcePortfolioID string `json:"SourcePortfolioId,omitempty"`
}

// AssociateProductWithPortfolio adds a product to a portfolio.
func (b *InMemoryBackend) AssociateProductWithPortfolio(
	ctx context.Context, in *AssociateProductWithPortfolioInput,
) (*EmptyOutput, error) {
	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	b.mu.Lock("AssociateProductWithPortfolio")
	defer b.mu.Unlock()

	p, err := b.ownedPortfolioLocked(account, region, in.PortfolioID)
	if err != nil {
		return nil, err
	}

	prod, err := b.ownedProductLocked(account, region, in.ProductID)
	if err != nil {
		if in.SourcePortfolioID == "" {
			return nil, err
		}

		prod, err = b.productInSourcePortfolioLocked(account, region, in.SourcePortfolioID, in.ProductID)
		if err != nil {
			return nil, err
		}
	}

	key := p.ID + "|" + prod.ID
	b.associations.Put(&Association{Key: key, PortfolioID: p.ID, ProductID: prod.ID})

	return &EmptyOutput{}, nil
}

func (b *InMemoryBackend) productInSourcePortfolioLocked(
	account, region, portfolioID, productID string,
) (*Product, error) {
	if _, err := b.accessiblePortfolioLocked(account, region, portfolioID); err != nil {
		return nil, err
	}

	if _, ok := b.associations.Get(portfolioID + "|" + productID); !ok {
		return nil, notFound("product", productID)
	}

	prod, ok := b.products.Get(productID)
	if !ok {
		return nil, notFound("product", productID)
	}

	return prod, nil
}

// DisassociateProductFromPortfolioInput is the DisassociateProductFromPortfolio request.
type DisassociateProductFromPortfolioInput struct {
	PortfolioID string `json:"PortfolioId"`
	ProductID   string `json:"ProductId"`
}

// DisassociateProductFromPortfolio removes a product from a portfolio.
func (b *InMemoryBackend) DisassociateProductFromPortfolio(
	ctx context.Context, in *DisassociateProductFromPortfolioInput,
) (*EmptyOutput, error) {
	b.mu.Lock("DisassociateProductFromPortfolio")
	defer b.mu.Unlock()

	p, err := b.ownedPortfolioLocked(b.callerAccount(ctx), b.callerRegion(ctx), in.PortfolioID)
	if err != nil {
		return nil, err
	}

	key := p.ID + "|" + in.ProductID
	if _, ok := b.associations.Get(key); !ok {
		return nil, notFound("product", in.ProductID)
	}

	for _, c := range b.constraints.All() {
		if c.PortfolioID == p.ID && c.ProductID == in.ProductID {
			return nil, fail(
				ErrResourceInUse,
				"constraint %s still applies to product %s in portfolio %s",
				c.ID,
				c.ProductID,
				p.ID,
			)
		}
	}

	b.associations.Delete(key)

	return &EmptyOutput{}, nil
}

// ListPortfoliosForProductInput is the ListPortfoliosForProduct request.
type ListPortfoliosForProductInput struct {
	ProductID string `json:"ProductId"`
	Paging
}

// ListPortfoliosForProduct lists the portfolios that contain a product.
func (b *InMemoryBackend) ListPortfoliosForProduct(
	ctx context.Context, in *ListPortfoliosForProductInput,
) (*ListPortfoliosOutput, error) {
	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	b.mu.RLock("ListPortfoliosForProduct")
	defer b.mu.RUnlock()

	if _, err := b.ownedProductLocked(account, region, in.ProductID); err != nil {
		return nil, err
	}

	var out []*Portfolio

	for _, p := range b.accessiblePortfoliosLocked(account, region) {
		if _, ok := b.associations.Get(p.ID + "|" + in.ProductID); ok {
			out = append(out, p)
		}
	}

	pg, err := pageOf(out, in.Paging)
	if err != nil {
		return nil, err
	}

	return &ListPortfoliosOutput{PortfolioDetails: b.portfolioDetails(pg.Data), NextPageToken: pg.Next}, nil
}
