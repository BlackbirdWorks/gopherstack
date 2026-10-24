package servicecatalog

import (
	"cmp"
	"context"
	"regexp"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
)

const maxProductText = 8191

func registerProductOps(m map[string]opFunc, b *InMemoryBackend) {
	m["CreateProduct"] = op(b.CreateProduct)
	m["DescribeProductAsAdmin"] = op(b.DescribeProductAsAdmin)
	m["UpdateProduct"] = op(b.UpdateProduct)
	m["DeleteProduct"] = op(b.DeleteProduct)
	m["SearchProductsAsAdmin"] = op(b.SearchProductsAsAdmin)
	m["DescribeProduct"] = op(b.DescribeProduct)
	m["DescribeProductView"] = op(b.DescribeProductView)
	m["SearchProducts"] = op(b.SearchProducts)
	m["CopyProduct"] = op(b.CopyProduct)
	m["DescribeCopyProductStatus"] = op(b.DescribeCopyProductStatus)
	m["ListLaunchPaths"] = op(b.ListLaunchPaths)
}

func (b *InMemoryBackend) ownedProductLocked(account, region, id string) (*Product, error) {
	p, ok := b.products.Get(id)
	if !ok || p.AccountID != account || p.Region != region {
		return nil, notFound("product", id)
	}

	return p, nil
}

func (b *InMemoryBackend) ownedProductByIDOrNameLocked(account, region, id, name string) (*Product, error) {
	if id != "" {
		return b.ownedProductLocked(account, region, id)
	}

	if name == "" {
		return nil, invalid("either Id or Name is required")
	}

	for _, p := range b.products.All() {
		if p.AccountID == account && p.Region == region && p.Name == name {
			return p, nil
		}
	}

	return nil, notFound("product", name)
}

func (b *InMemoryBackend) artifactsOfLocked(productID string) []*Artifact {
	var out []*Artifact

	for _, a := range b.artifacts.All() {
		if a.ProductID == productID {
			out = append(out, a)
		}
	}

	slices.SortFunc(out, func(x, y *Artifact) int {
		if c := x.CreatedTime.Compare(y.CreatedTime); c != 0 {
			return c
		}

		return cmp.Compare(x.ID, y.ID)
	})

	return out
}

func (b *InMemoryBackend) summaryOfLocked(p *Product) *ProductViewSummary {
	return &ProductViewSummary{
		Distributor: p.Distributor, ID: p.ViewID, Name: p.Name, Owner: p.Owner, ProductID: p.ID,
		ShortDescription: p.Description, SupportDescription: p.SupportDescription, SupportEmail: p.SupportEmail,
		SupportURL: p.SupportURL, Type: p.Type, HasDefaultPath: b.pathCountLocked(p.ID) == 1,
	}
}

func (b *InMemoryBackend) pathCountLocked(productID string) int {
	n := 0

	for _, a := range b.associations.All() {
		if a.ProductID == productID {
			n++
		}
	}

	return n
}

func (b *InMemoryBackend) detailOfLocked(p *Product) *ProductViewDetail {
	d := &ProductViewDetail{
		CreatedTime: ts(p.CreatedTime), ProductARN: p.ARN, Status: p.Status, ProductViewSummary: b.summaryOfLocked(p),
	}

	if sc := p.SourceConnection; sc != nil {
		d.SourceConnection = &SourceConnectionDetail{
			Type: sc.Type, ConnectionParameters: &SourceConnectionParameters{CodeStar: &CodeStarParameters{
				ArtifactPath:  sc.ArtifactPath,
				Branch:        sc.Branch,
				ConnectionArn: sc.ConnectionArn,
				Repository:    sc.Repository,
			}},
		}
	}

	return d
}

func artifactDetail(a *Artifact) *ProvisioningArtifactDetail {
	return &ProvisioningArtifactDetail{
		Active: new(!a.Inactive), CreatedTime: ts(a.CreatedTime), Description: a.Description,
		Guidance: a.Guidance, ID: a.ID, Name: a.Name, SourceRevision: a.SourceRevision, Type: a.Type,
	}
}

func newSourceConnection(in *SourceConnectionRequest) (*SourceConnection, error) {
	if in.Type != "" && in.Type != "CODESTAR" {
		return nil, invalid("SourceConnection Type must be CODESTAR")
	}

	sc := &SourceConnection{Type: strOr(in.Type, "CODESTAR")}
	if cp := in.ConnectionParameters; cp != nil && cp.CodeStar != nil {
		sc.ConnectionArn, sc.Repository = cp.CodeStar.ConnectionArn, cp.CodeStar.Repository
		sc.Branch, sc.ArtifactPath = cp.CodeStar.Branch, cp.CodeStar.ArtifactPath
	}

	return sc, nil
}

// CreateProductInput is the CreateProduct request.
type CreateProductInput struct {
	ProvisioningArtifactParameters *ProvisioningArtifactProperties `json:"ProvisioningArtifactParameters"`
	SourceConnection               *SourceConnectionRequest        `json:"SourceConnection,omitempty"`
	Name                           string                          `json:"Name"`
	Owner                          string                          `json:"Owner,omitempty"`
	ProductType                    string                          `json:"ProductType"`
	Description                    string                          `json:"Description,omitempty"`
	Distributor                    string                          `json:"Distributor,omitempty"`
	SupportDescription             string                          `json:"SupportDescription,omitempty"`
	SupportEmail                   string                          `json:"SupportEmail,omitempty"`
	SupportURL                     string                          `json:"SupportUrl,omitempty"`
	IdempotencyToken               string                          `json:"IdempotencyToken,omitempty"`
	Tags                           []Tag                           `json:"Tags,omitempty"`
}

// CreateProductOutput is the CreateProduct response.
type CreateProductOutput struct {
	ProductViewDetail          *ProductViewDetail          `json:"ProductViewDetail"`
	ProvisioningArtifactDetail *ProvisioningArtifactDetail `json:"ProvisioningArtifactDetail"`
	Tags                       []Tag                       `json:"Tags,omitempty"`
}

func validProductType(t string) bool {
	return t == productTypeCFN || t == productTypeTFOpen || t == productTypeTFCloud || t == productTypeExternal
}

func validateProductText(in *CreateProductInput) error {
	texts := []string{
		in.Name, in.Owner, in.Description, in.Distributor, in.SupportDescription, in.SupportEmail, in.SupportURL,
	}

	for _, v := range texts {
		if len(v) > maxProductText {
			return invalid("an attribute exceeds its maximum length of %d", maxProductText)
		}
	}

	if in.Name == "" {
		return invalid("Name is required")
	}

	return nil
}

// CreateProduct creates a product with its first provisioning artifact.
func (b *InMemoryBackend) CreateProduct(ctx context.Context, in *CreateProductInput) (*CreateProductOutput, error) {
	if err := validateProductText(in); err != nil {
		return nil, err
	}

	if !validProductType(in.ProductType) {
		return nil, invalid(
			"ProductType must be CLOUD_FORMATION_TEMPLATE, TERRAFORM_OPEN_SOURCE, TERRAFORM_CLOUD or EXTERNAL",
		)
	}

	if in.ProvisioningArtifactParameters == nil {
		return nil, invalid("ProvisioningArtifactParameters is required")
	}

	var (
		conn *SourceConnection
		err  error
	)

	if in.SourceConnection != nil {
		if conn, err = newSourceConnection(in.SourceConnection); err != nil {
			return nil, err
		}
	}

	tags := tagsToMap(in.Tags)
	if err = validateTags(tags); err != nil {
		return nil, err
	}

	art, err := b.newArtifact(ctx, in.ProductType, in.ProvisioningArtifactParameters)
	if err != nil {
		return nil, err
	}

	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	prod, err := idemCreate(b, "CreateProduct", in.IdempotencyToken, in,
		func(p *Product) string { return p.ID },
		func(id string) (*Product, error) {
			b.mu.RLock("CreateProduct")
			defer b.mu.RUnlock()

			return b.ownedProductLocked(account, region, id)
		},
		func() (*Product, error) {
			b.mu.Lock("CreateProduct")
			defer b.mu.Unlock()

			id := newID("prod-")
			p := &Product{
				ID: id, ViewID: newID("prodview-"), ARN: b.productARN(account, region, id), AccountID: account,
				Region: region, Name: in.Name, Owner: in.Owner, Description: in.Description,
				Distributor: in.Distributor, SupportDescription: in.SupportDescription, SupportEmail: in.SupportEmail,
				SupportURL: in.SupportURL, Type: in.ProductType, Status: statusAvailable, CreatedTime: b.now(),
				Tags: tags, SourceConnection: conn,
			}
			art.ProductID, art.ID, art.CreatedTime = id, newID("pa-"), p.CreatedTime
			b.products.Put(p)
			b.artifacts.Put(art)

			return p, nil
		},
	)
	if err != nil {
		return nil, err
	}

	b.mu.RLock("CreateProduct")
	defer b.mu.RUnlock()

	first := b.artifactsOfLocked(prod.ID)

	var detail *ProvisioningArtifactDetail
	if len(first) > 0 {
		detail = artifactDetail(first[0])
	}

	return &CreateProductOutput{
		ProductViewDetail: b.detailOfLocked(prod), ProvisioningArtifactDetail: detail, Tags: mapToTags(prod.Tags),
	}, nil
}

// DescribeProductAsAdminInput is the DescribeProductAsAdmin request.
type DescribeProductAsAdminInput struct {
	ID                string `json:"Id,omitempty"`
	Name              string `json:"Name,omitempty"`
	SourcePortfolioID string `json:"SourcePortfolioId,omitempty"`
}

// DescribeProductAsAdminOutput is the DescribeProductAsAdmin response.
type DescribeProductAsAdminOutput struct {
	ProductViewDetail             *ProductViewDetail            `json:"ProductViewDetail"`
	ProvisioningArtifactSummaries []ProvisioningArtifactSummary `json:"ProvisioningArtifactSummaries"`
	Tags                          []Tag                         `json:"Tags"`
	TagOptions                    []TagOptionDetail             `json:"TagOptions"`
	Budgets                       []BudgetDetail                `json:"Budgets"`
}

// DescribeProductAsAdmin returns a product the caller's account owns, with its versions and tags.
func (b *InMemoryBackend) DescribeProductAsAdmin(
	ctx context.Context,
	in *DescribeProductAsAdminInput,
) (*DescribeProductAsAdminOutput, error) {
	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	b.mu.RLock("DescribeProductAsAdmin")
	defer b.mu.RUnlock()

	var (
		p   *Product
		err error
	)

	if in.SourcePortfolioID != "" && in.ID != "" {
		p, err = b.productInSourcePortfolioLocked(account, region, in.SourcePortfolioID, in.ID)
	} else {
		p, err = b.ownedProductByIDOrNameLocked(account, region, in.ID, in.Name)
	}

	if err != nil {
		return nil, err
	}

	sums := []ProvisioningArtifactSummary{}

	for _, a := range b.artifactsOfLocked(p.ID) {
		sums = append(sums, ProvisioningArtifactSummary{
			CreatedTime: ts(a.CreatedTime), Description: a.Description, ID: a.ID, Name: a.Name,
			ProvisioningArtifactMetadata: a.Info,
		})
	}

	return &DescribeProductAsAdminOutput{
		ProductViewDetail: b.detailOfLocked(p), ProvisioningArtifactSummaries: sums, Tags: orEmpty(mapToTags(p.Tags)),
		TagOptions: orEmpty(b.tagOptionsForLocked(p.ID)), Budgets: orEmpty(b.budgetsForLocked(p.ID)),
	}, nil
}

// UpdateProductInput is the UpdateProduct request.
type UpdateProductInput struct {
	SourceConnection   *SourceConnectionRequest `json:"SourceConnection,omitempty"`
	Description        *string                  `json:"Description,omitempty"`
	Distributor        *string                  `json:"Distributor,omitempty"`
	SupportDescription *string                  `json:"SupportDescription,omitempty"`
	SupportEmail       *string                  `json:"SupportEmail,omitempty"`
	SupportURL         *string                  `json:"SupportUrl,omitempty"`
	ID                 string                   `json:"Id"`
	Name               string                   `json:"Name,omitempty"`
	Owner              string                   `json:"Owner,omitempty"`
	AddTags            []Tag                    `json:"AddTags,omitempty"`
	RemoveTags         []string                 `json:"RemoveTags,omitempty"`
}

// UpdateProductOutput is the UpdateProduct response.
type UpdateProductOutput struct {
	ProductViewDetail *ProductViewDetail `json:"ProductViewDetail"`
	Tags              []Tag              `json:"Tags,omitempty"`
}

// UpdateProduct edits a product's attributes and tags.
func (b *InMemoryBackend) UpdateProduct(ctx context.Context, in *UpdateProductInput) (*UpdateProductOutput, error) {
	var conn *SourceConnection

	if in.SourceConnection != nil {
		var err error
		if conn, err = newSourceConnection(in.SourceConnection); err != nil {
			return nil, err
		}
	}

	b.mu.Lock("UpdateProduct")
	defer b.mu.Unlock()

	p, err := b.ownedProductLocked(b.callerAccount(ctx), b.callerRegion(ctx), in.ID)
	if err != nil {
		return nil, err
	}

	tags, err := mergeTags(p.Tags, tagsToMap(in.AddTags), in.RemoveTags)
	if err != nil {
		return nil, err
	}

	p.Tags = tags

	if in.Name != "" {
		p.Name = in.Name
	}

	if in.Owner != "" {
		p.Owner = in.Owner
	}

	setIf(&p.Description, in.Description)
	setIf(&p.Distributor, in.Distributor)
	setIf(&p.SupportDescription, in.SupportDescription)
	setIf(&p.SupportEmail, in.SupportEmail)
	setIf(&p.SupportURL, in.SupportURL)

	if conn != nil {
		p.SourceConnection = conn
	}

	return &UpdateProductOutput{ProductViewDetail: b.detailOfLocked(p), Tags: mapToTags(p.Tags)}, nil
}

func setIf(dst *string, v *string) {
	if v != nil {
		*dst = *v
	}
}

// DeleteProductInput is the DeleteProduct request.
type DeleteProductInput struct {
	ID string `json:"Id"`
}

// DeleteProduct deletes a product that is in no portfolio and has no provisioned products.
func (b *InMemoryBackend) DeleteProduct(ctx context.Context, in *DeleteProductInput) (*EmptyOutput, error) {
	b.mu.Lock("DeleteProduct")
	defer b.mu.Unlock()

	p, err := b.ownedProductLocked(b.callerAccount(ctx), b.callerRegion(ctx), in.ID)
	if err != nil {
		return nil, err
	}

	if b.pathCountLocked(p.ID) > 0 {
		return nil, fail(ErrResourceInUse, "product %s is still associated with a portfolio", p.ID)
	}

	for _, pp := range b.provisioned.All() {
		if pp.ProductID == p.ID {
			return nil, fail(ErrResourceInUse, "product %s still has provisioned products", p.ID)
		}
	}

	for _, a := range b.artifactsOfLocked(p.ID) {
		b.artifacts.Delete(a.ID)
	}

	for _, l := range b.actionLinks.All() {
		if l.ProductID == p.ID {
			b.actionLinks.Delete(l.Key)
		}
	}

	b.removeLinksLocked(p.ID)
	b.products.Delete(p.ID)

	return &EmptyOutput{}, nil
}

// ProductFilters carries the shared product-search request members.
type ProductFilters struct {
	Filters   map[string][]string `json:"Filters,omitempty"`
	SortBy    string              `json:"SortBy,omitempty"`
	SortOrder string              `json:"SortOrder,omitempty"`
	Paging
}

func (f *ProductFilters) validate() error {
	for k := range f.Filters {
		switch k {
		case "FullTextSearch", filterOwner, filterProductType, "SourceProductId":
		default:
			return invalid("unsupported filter %q", k)
		}
	}

	switch f.SortBy {
	case "", "Title", "VersionCount", "CreationDate":
	default:
		return invalid("SortBy must be Title, VersionCount or CreationDate")
	}

	switch f.SortOrder {
	case "", orderAscending, orderDescending:
	default:
		return invalid("SortOrder must be ASCENDING or DESCENDING")
	}

	return nil
}

func (b *InMemoryBackend) matchesFilters(p *Product, f map[string][]string) bool {
	for k, vals := range f {
		if len(vals) == 0 {
			continue
		}

		if !slices.ContainsFunc(vals, func(v string) bool { return matchesFilterValue(p, k, v) }) {
			return false
		}
	}

	return true
}

func matchesFilterValue(p *Product, key, v string) bool {
	switch key {
	case "FullTextSearch":
		return containsFold(p.Name, v) || containsFold(p.Owner, v) || containsFold(p.Description, v) ||
			containsFold(p.Distributor, v)
	case filterOwner:
		return strings.EqualFold(p.Owner, v)
	case filterProductType:
		return p.Type == v
	case "SourceProductId":
		return p.SourceProductID == v
	}

	return false
}

func (b *InMemoryBackend) sortProductsLocked(ps []*Product, sortBy, order string) {
	slices.SortFunc(ps, func(x, y *Product) int {
		var c int

		switch sortBy {
		case "Title":
			c = cmp.Compare(strings.ToLower(x.Name), strings.ToLower(y.Name))
		case "VersionCount":
			c = cmp.Compare(len(b.artifactsOfLocked(x.ID)), len(b.artifactsOfLocked(y.ID)))
		case "CreationDate":
			c = x.CreatedTime.Compare(y.CreatedTime)
		}

		if c == 0 {
			c = cmp.Compare(x.ID, y.ID)
		}

		if order == orderDescending {
			return -c
		}

		return c
	})
}

// SearchProductsAsAdminInput is the SearchProductsAsAdmin request.
type SearchProductsAsAdminInput struct {
	PortfolioID   string `json:"PortfolioId,omitempty"`
	ProductSource string `json:"ProductSource,omitempty"`
	ProductFilters
}

// SearchProductsAsAdminOutput is the SearchProductsAsAdmin response.
type SearchProductsAsAdminOutput struct {
	NextPageToken      string               `json:"NextPageToken,omitempty"`
	ProductViewDetails []*ProductViewDetail `json:"ProductViewDetails"`
}

// SearchProductsAsAdmin searches the products the caller's account owns.
func (b *InMemoryBackend) SearchProductsAsAdmin(
	ctx context.Context,
	in *SearchProductsAsAdminInput,
) (*SearchProductsAsAdminOutput, error) {
	if err := in.validate(); err != nil {
		return nil, err
	}

	if in.ProductSource != "" && in.ProductSource != shareAccount {
		return nil, invalid("ProductSource must be ACCOUNT")
	}

	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	b.mu.RLock("SearchProductsAsAdmin")
	defer b.mu.RUnlock()

	if in.PortfolioID != "" {
		if _, err := b.accessiblePortfolioLocked(account, region, in.PortfolioID); err != nil {
			return nil, err
		}
	}

	var hits []*Product

	for _, p := range b.products.All() {
		if p.AccountID != account || p.Region != region || !b.matchesFilters(p, in.Filters) {
			continue
		}

		if in.PortfolioID != "" {
			if _, ok := b.associations.Get(in.PortfolioID + "|" + p.ID); !ok {
				continue
			}
		}

		hits = append(hits, p)
	}

	b.sortProductsLocked(hits, in.SortBy, in.SortOrder)

	pg, err := pageOf(hits, in.Paging)
	if err != nil {
		return nil, err
	}

	out := make([]*ProductViewDetail, 0, len(pg.Data))
	for _, p := range pg.Data {
		out = append(out, b.detailOfLocked(p))
	}

	return &SearchProductsAsAdminOutput{ProductViewDetails: out, NextPageToken: pg.Next}, nil
}

// userCanLaunchLocked reports whether the caller's identity is associated with the portfolio.
func (b *InMemoryBackend) userCanLaunchLocked(ctx context.Context, portfolioID, account string) bool {
	caller := awsmeta.CallerArn(ctx)
	if caller == "" || strings.HasSuffix(caller, ":root") {
		return true
	}

	p, ok := b.portfolios.Get(portfolioID)
	if !ok {
		return false
	}

	return slices.ContainsFunc(b.principalsForAccountLocked(p, account), func(pr *Principal) bool {
		return principalMatches(pr, caller)
	})
}

func principalMatches(pr *Principal, caller string) bool {
	if pr.PrincipalType != "IAM_PATTERN" {
		return pr.PrincipalARN == caller
	}

	pat := "^" + strings.ReplaceAll(strings.ReplaceAll(regexp.QuoteMeta(pr.PrincipalARN), `\*`, ".*"), `\?`, ".") + "$"
	re, err := regexp.Compile(pat)

	return err == nil && re.MatchString(caller)
}

// launchPathsLocked lists the portfolios through which the caller can launch a product.
func (b *InMemoryBackend) launchPathsLocked(ctx context.Context, account, region, productID string) []*Portfolio {
	var out []*Portfolio

	for _, p := range b.accessiblePortfoliosLocked(account, region) {
		if _, ok := b.associations.Get(p.ID + "|" + productID); ok && b.userCanLaunchLocked(ctx, p.ID, account) {
			out = append(out, p)
		}
	}

	return out
}

// visibleProductsLocked lists the products the caller can see through accessible portfolios.
func (b *InMemoryBackend) visibleProductsLocked(ctx context.Context, account, region string) []*Product {
	seen := map[string]bool{}

	var out []*Product

	for _, p := range b.accessiblePortfoliosLocked(account, region) {
		if !b.userCanLaunchLocked(ctx, p.ID, account) {
			continue
		}

		for _, a := range b.associationsOfPortfolioLocked(p.ID) {
			if prod, ok := b.products.Get(a.ProductID); ok && !seen[prod.ID] {
				seen[prod.ID] = true
				out = append(out, prod)
			}
		}
	}

	return out
}

func (b *InMemoryBackend) visibleProductLocked(
	ctx context.Context,
	account, region, id, name string,
) (*Product, error) {
	for _, p := range b.visibleProductsLocked(ctx, account, region) {
		if (id != "" && p.ID == id) || (id == "" && name != "" && p.Name == name) {
			return p, nil
		}
	}

	if id == "" && name == "" {
		return nil, invalid("either Id or Name is required")
	}

	return nil, notFound("product", strOr(id, name))
}

// SearchProductsInput is the SearchProducts request.
type SearchProductsInput struct {
	ProductFilters
}

// ProductViewAggregationValue is the ProductViewAggregationValue shape.
type ProductViewAggregationValue struct {
	Value            string `json:"Value"`
	ApproximateCount int32  `json:"ApproximateCount"`
}

// SearchProductsOutput is the SearchProducts response.
type SearchProductsOutput struct {
	ProductViewAggregations map[string][]ProductViewAggregationValue `json:"ProductViewAggregations"`
	NextPageToken           string                                   `json:"NextPageToken,omitempty"`
	ProductViewSummaries    []*ProductViewSummary                    `json:"ProductViewSummaries"`
}

// SearchProducts searches the products the caller can launch.
func (b *InMemoryBackend) SearchProducts(ctx context.Context, in *SearchProductsInput) (*SearchProductsOutput, error) {
	if err := in.validate(); err != nil {
		return nil, err
	}

	b.mu.RLock("SearchProducts")
	defer b.mu.RUnlock()

	var hits []*Product

	for _, p := range b.visibleProductsLocked(ctx, b.callerAccount(ctx), b.callerRegion(ctx)) {
		if b.matchesFilters(p, in.Filters) {
			hits = append(hits, p)
		}
	}

	b.sortProductsLocked(hits, in.SortBy, in.SortOrder)

	aggs := map[string][]ProductViewAggregationValue{
		filterOwner:       aggregate(hits, func(p *Product) string { return p.Owner }),
		filterProductType: aggregate(hits, func(p *Product) string { return p.Type }),
		"Vendor":          aggregate(hits, func(p *Product) string { return p.Distributor }),
	}

	pg, err := pageOf(hits, in.Paging)
	if err != nil {
		return nil, err
	}

	out := make([]*ProductViewSummary, 0, len(pg.Data))
	for _, p := range pg.Data {
		out = append(out, b.summaryOfLocked(p))
	}

	return &SearchProductsOutput{ProductViewSummaries: out, ProductViewAggregations: aggs, NextPageToken: pg.Next}, nil
}

func aggregate(ps []*Product, key func(*Product) string) []ProductViewAggregationValue {
	counts := map[string]int32{}

	for _, p := range ps {
		if k := key(p); k != "" {
			counts[k]++
		}
	}

	out := make([]ProductViewAggregationValue, 0, len(counts))
	for _, k := range sortedKeys(counts) {
		out = append(out, ProductViewAggregationValue{Value: k, ApproximateCount: counts[k]})
	}

	return out
}

// LaunchPath is the LaunchPath shape.
type LaunchPath struct {
	ID   string `json:"Id"`
	Name string `json:"Name"`
}

// DescribeProductInput is the DescribeProduct request.
type DescribeProductInput struct {
	ID   string `json:"Id,omitempty"`
	Name string `json:"Name,omitempty"`
}

// DescribeProductOutput is the DescribeProduct response.
type DescribeProductOutput struct {
	ProductViewSummary    *ProductViewSummary    `json:"ProductViewSummary"`
	ProvisioningArtifacts []ProvisioningArtifact `json:"ProvisioningArtifacts"`
	LaunchPaths           []LaunchPath           `json:"LaunchPaths"`
	Budgets               []BudgetDetail         `json:"Budgets"`
}

// DescribeProduct returns a product the caller can launch.
func (b *InMemoryBackend) DescribeProduct(
	ctx context.Context,
	in *DescribeProductInput,
) (*DescribeProductOutput, error) {
	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	b.mu.RLock("DescribeProduct")
	defer b.mu.RUnlock()

	p, err := b.visibleProductLocked(ctx, account, region, in.ID, in.Name)
	if err != nil {
		return nil, err
	}

	return b.describeVisibleLocked(ctx, account, region, p), nil
}

func (b *InMemoryBackend) describeVisibleLocked(
	ctx context.Context,
	account, region string,
	p *Product,
) *DescribeProductOutput {
	launch := b.launchPathsLocked(ctx, account, region, p.ID)

	paths := make([]LaunchPath, 0, len(launch))
	for _, pf := range launch {
		paths = append(paths, LaunchPath{ID: pf.ID, Name: pf.DisplayName})
	}

	arts := []ProvisioningArtifact{}

	for _, a := range b.artifactsOfLocked(p.ID) {
		if !a.Inactive {
			arts = append(arts, ProvisioningArtifact{
				CreatedTime: ts(
					a.CreatedTime,
				),
				Description: a.Description,
				Guidance:    a.Guidance,
				ID:          a.ID,
				Name:        a.Name,
			})
		}
	}

	return &DescribeProductOutput{
		ProductViewSummary: b.summaryOfLocked(p), ProvisioningArtifacts: arts, LaunchPaths: paths,
		Budgets: orEmpty(b.budgetsForLocked(p.ID)),
	}
}

// DescribeProductViewInput is the DescribeProductView request.
type DescribeProductViewInput struct {
	ID string `json:"Id"`
}

// DescribeProductViewOutput is the DescribeProductView response.
type DescribeProductViewOutput struct {
	ProductViewSummary    *ProductViewSummary    `json:"ProductViewSummary"`
	ProvisioningArtifacts []ProvisioningArtifact `json:"ProvisioningArtifacts"`
}

// DescribeProductView returns a product by its view ID.
func (b *InMemoryBackend) DescribeProductView(
	ctx context.Context,
	in *DescribeProductViewInput,
) (*DescribeProductViewOutput, error) {
	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	b.mu.RLock("DescribeProductView")
	defer b.mu.RUnlock()

	for _, p := range b.visibleProductsLocked(ctx, account, region) {
		if p.ViewID == in.ID || p.ID == in.ID {
			d := b.describeVisibleLocked(ctx, account, region, p)

			return &DescribeProductViewOutput{
				ProductViewSummary:    d.ProductViewSummary,
				ProvisioningArtifacts: d.ProvisioningArtifacts,
			}, nil
		}
	}

	return nil, notFound("product view", in.ID)
}

// ListLaunchPathsInput is the ListLaunchPaths request.
type ListLaunchPathsInput struct {
	ProductID string `json:"ProductId"`
	Paging
}

// LaunchPathSummary is the LaunchPathSummary shape.
type LaunchPathSummary struct {
	ID                  string              `json:"Id"`
	Name                string              `json:"Name"`
	ConstraintSummaries []ConstraintSummary `json:"ConstraintSummaries"`
	Tags                []Tag               `json:"Tags"`
}

// ListLaunchPathsOutput is the ListLaunchPaths response.
type ListLaunchPathsOutput struct {
	NextPageToken       string              `json:"NextPageToken,omitempty"`
	LaunchPathSummaries []LaunchPathSummary `json:"LaunchPathSummaries"`
}

// ListLaunchPaths lists the portfolios through which a product can be launched.
func (b *InMemoryBackend) ListLaunchPaths(
	ctx context.Context,
	in *ListLaunchPathsInput,
) (*ListLaunchPathsOutput, error) {
	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	b.mu.RLock("ListLaunchPaths")
	defer b.mu.RUnlock()

	p, err := b.visibleProductLocked(ctx, account, region, in.ProductID, "")
	if err != nil {
		return nil, err
	}

	var sums []LaunchPathSummary

	for _, pf := range b.launchPathsLocked(ctx, account, region, p.ID) {
		sums = append(sums, LaunchPathSummary{
			ID: pf.ID, Name: pf.DisplayName, Tags: orEmpty(mapToTags(pf.Tags)),
			ConstraintSummaries: orEmpty(b.constraintSummariesLocked(pf.ID, p.ID)),
		})
	}

	pg, err := pageOf(sums, in.Paging)
	if err != nil {
		return nil, err
	}

	return &ListLaunchPathsOutput{LaunchPathSummaries: orEmpty(pg.Data), NextPageToken: pg.Next}, nil
}

// resolvePathLocked picks the launch path for a product from an optional PathId or PathName.
func (b *InMemoryBackend) resolvePathLocked(
	ctx context.Context, account, region, productID, pathID, pathName string,
) (*Portfolio, error) {
	cands := b.launchPathsLocked(ctx, account, region, productID)

	switch {
	case pathID != "":
		for _, c := range cands {
			if c.ID == pathID {
				return c, nil
			}
		}

		return nil, invalid("path %s is not a launch path of product %s", pathID, productID)
	case pathName != "":
		for _, c := range cands {
			if c.DisplayName == pathName {
				return c, nil
			}
		}

		return nil, invalid("path %q is not a launch path of product %s", pathName, productID)
	case len(cands) == 1:
		return cands[0], nil
	case len(cands) == 0:
		return nil, invalid("product %s has no launch path available to the caller", productID)
	default:
		return nil, invalid("product %s has multiple launch paths; specify PathId or PathName", productID)
	}
}
