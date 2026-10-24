package servicecatalog

import (
	"cmp"
	"context"
	"slices"
	"strings"
)

const (
	maxTagOptionKey   = 128
	maxTagOptionValue = 256
	maxTagOptions     = 10000
)

func registerTagOptionOps(m map[string]opFunc, b *InMemoryBackend) {
	m["CreateTagOption"] = op(b.CreateTagOption)
	m["UpdateTagOption"] = op(b.UpdateTagOption)
	m["DeleteTagOption"] = op(b.DeleteTagOption)
	m["DescribeTagOption"] = op(b.DescribeTagOption)
	m["ListTagOptions"] = op(b.ListTagOptions)
	m["AssociateTagOptionWithResource"] = op(b.AssociateTagOptionWithResource)
	m["DisassociateTagOptionFromResource"] = op(b.DisassociateTagOptionFromResource)
	m["ListResourcesForTagOption"] = op(b.ListResourcesForTagOption)
	m["AssociateBudgetWithResource"] = op(b.AssociateBudgetWithResource)
	m["DisassociateBudgetFromResource"] = op(b.DisassociateBudgetFromResource)
	m["ListBudgetsForResource"] = op(b.ListBudgetsForResource)
}

func tagOptionDetail(t *TagOption) TagOptionDetail {
	return TagOptionDetail{Active: new(t.Active), ID: t.ID, Key: t.Key, Owner: t.AccountID, Value: t.Value}
}

func (b *InMemoryBackend) tagOptionLocked(account, region, id string) (*TagOption, error) {
	t, ok := b.tagOptions.Get(id)
	if !ok || t.AccountID != account || t.Region != region {
		return nil, notFound("tag option", id)
	}

	return t, nil
}

func (b *InMemoryBackend) tagOptionsForLocked(resourceID string) []TagOptionDetail {
	var out []TagOptionDetail

	for _, l := range b.tagOptionLink.All() {
		if l.ResourceID == resourceID {
			if t, ok := b.tagOptions.Get(l.TagOptionID); ok {
				out = append(out, tagOptionDetail(t))
			}
		}
	}

	slices.SortFunc(out, func(x, y TagOptionDetail) int { return cmp.Compare(x.ID, y.ID) })

	return out
}

// CreateTagOptionInput is the CreateTagOption request.
type CreateTagOptionInput struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// TagOptionOutput is the response of tag option create, update and describe operations.
type TagOptionOutput struct {
	TagOptionDetail *TagOptionDetail `json:"TagOptionDetail"`
}

func validateTagOption(key, value string) error {
	if key == "" || len(key) > maxTagOptionKey || len(value) == 0 || len(value) > maxTagOptionValue ||
		strings.HasPrefix(strings.ToLower(key), "aws:") {
		return invalid(
			"Key must be 1-%d and Value 1-%d characters and Key must not start with aws:",
			maxTagOptionKey,
			maxTagOptionValue,
		)
	}

	return nil
}

// CreateTagOption creates a tag option.
func (b *InMemoryBackend) CreateTagOption(ctx context.Context, in *CreateTagOptionInput) (*TagOptionOutput, error) {
	if err := validateTagOption(in.Key, in.Value); err != nil {
		return nil, err
	}

	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	b.mu.Lock("CreateTagOption")
	defer b.mu.Unlock()

	n := 0

	for _, t := range b.tagOptions.All() {
		if t.AccountID == account && t.Region == region {
			if t.Key == in.Key && t.Value == in.Value {
				return nil, fail(ErrDuplicateResource, "tag option %s=%s already exists", in.Key, in.Value)
			}

			n++
		}
	}

	if n >= maxTagOptions {
		return nil, fail(ErrLimitExceeded, "at most %d tag options are allowed", maxTagOptions)
	}

	t := &TagOption{ID: newID("tag-"), AccountID: account, Region: region, Key: in.Key, Value: in.Value, Active: true}
	b.tagOptions.Put(t)
	d := tagOptionDetail(t)

	return &TagOptionOutput{TagOptionDetail: &d}, nil
}

// UpdateTagOptionInput is the UpdateTagOption request.
type UpdateTagOptionInput struct {
	Active *bool  `json:"Active,omitempty"`
	ID     string `json:"Id"`
	Value  string `json:"Value,omitempty"`
}

// UpdateTagOption changes a tag option's value or active flag.
func (b *InMemoryBackend) UpdateTagOption(ctx context.Context, in *UpdateTagOptionInput) (*TagOptionOutput, error) {
	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	b.mu.Lock("UpdateTagOption")
	defer b.mu.Unlock()

	t, err := b.tagOptionLocked(account, region, in.ID)
	if err != nil {
		return nil, err
	}

	if in.Value != "" {
		if err = validateTagOption(t.Key, in.Value); err != nil {
			return nil, err
		}

		for _, o := range b.tagOptions.All() {
			if o.ID != t.ID && o.AccountID == account && o.Region == region && o.Key == t.Key && o.Value == in.Value {
				return nil, fail(ErrDuplicateResource, "tag option %s=%s already exists", t.Key, in.Value)
			}
		}

		t.Value = in.Value
	}

	if in.Active != nil {
		t.Active = *in.Active
	}

	d := tagOptionDetail(t)

	return &TagOptionOutput{TagOptionDetail: &d}, nil
}

// DescribeTagOptionInput is the DescribeTagOption and DeleteTagOption request.
type DescribeTagOptionInput struct {
	ID string `json:"Id"`
}

// DeleteTagOption deletes a tag option that is not associated with any resource.
func (b *InMemoryBackend) DeleteTagOption(ctx context.Context, in *DescribeTagOptionInput) (*EmptyOutput, error) {
	b.mu.Lock("DeleteTagOption")
	defer b.mu.Unlock()

	t, err := b.tagOptionLocked(b.callerAccount(ctx), b.callerRegion(ctx), in.ID)
	if err != nil {
		return nil, err
	}

	for _, l := range b.tagOptionLink.All() {
		if l.TagOptionID == t.ID {
			return nil, fail(ErrResourceInUse, "tag option %s is associated with %s", t.ID, l.ResourceID)
		}
	}

	b.tagOptions.Delete(t.ID)

	return &EmptyOutput{}, nil
}

// DescribeTagOption returns a tag option.
func (b *InMemoryBackend) DescribeTagOption(ctx context.Context, in *DescribeTagOptionInput) (*TagOptionOutput, error) {
	b.mu.RLock("DescribeTagOption")
	defer b.mu.RUnlock()

	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	t, err := b.tagOptionLocked(account, region, in.ID)
	if err != nil {
		if shared, ok := b.sharedTagOptionsLocked(account, region)[in.ID]; ok {
			t, err = shared, nil
		}
	}

	if err != nil {
		return nil, err
	}

	d := tagOptionDetail(t)

	return &TagOptionOutput{TagOptionDetail: &d}, nil
}

// ListTagOptionsFilters is the ListTagOptionsFilters shape.
type ListTagOptionsFilters struct {
	Active *bool  `json:"Active,omitempty"`
	Key    string `json:"Key,omitempty"`
	Value  string `json:"Value,omitempty"`
}

// ListTagOptionsInput is the ListTagOptions request.
type ListTagOptionsInput struct {
	Filters *ListTagOptionsFilters `json:"Filters,omitempty"`
	Paging
}

// ListTagOptionsOutput is the ListTagOptions response.
type ListTagOptionsOutput struct {
	PageToken        string            `json:"PageToken,omitempty"`
	TagOptionDetails []TagOptionDetail `json:"TagOptionDetails"`
}

// ListTagOptions lists tag options, optionally filtered by key, value and active flag.
func (b *InMemoryBackend) ListTagOptions(ctx context.Context, in *ListTagOptionsInput) (*ListTagOptionsOutput, error) {
	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	b.mu.RLock("ListTagOptions")
	defer b.mu.RUnlock()

	var all []TagOptionDetail

	shared := b.sharedTagOptionsLocked(account, region)

	for _, t := range b.tagOptions.All() {
		if _, isShared := shared[t.ID]; (t.AccountID != account || t.Region != region) && !isShared {
			continue
		}

		if f := in.Filters; f != nil && ((f.Key != "" && f.Key != t.Key) || (f.Value != "" && f.Value != t.Value) ||
			(f.Active != nil && *f.Active != t.Active)) {
			continue
		}

		all = append(all, tagOptionDetail(t))
	}

	slices.SortFunc(all, func(x, y TagOptionDetail) int { return cmp.Compare(x.ID, y.ID) })

	pg, err := pageOf(all, in.Paging)
	if err != nil {
		return nil, err
	}

	return &ListTagOptionsOutput{TagOptionDetails: orEmpty(pg.Data), PageToken: pg.Next}, nil
}

// TagOptionResourceInput is the AssociateTagOptionWithResource and DisassociateTagOptionFromResource request.
type TagOptionResourceInput struct {
	ResourceID  string `json:"ResourceId"`
	TagOptionID string `json:"TagOptionId"`
}

// resourceExistsLocked reports whether id names a portfolio or product the caller can manage.
func (b *InMemoryBackend) resourceExistsLocked(account, region, id string) error {
	if _, err := b.ownedPortfolioLocked(account, region, id); err == nil {
		return nil
	}

	if _, err := b.ownedProductLocked(account, region, id); err == nil {
		return nil
	}

	return notFound("portfolio or product", id)
}

// AssociateTagOptionWithResource attaches a tag option to a portfolio or product.
func (b *InMemoryBackend) AssociateTagOptionWithResource(
	ctx context.Context,
	in *TagOptionResourceInput,
) (*EmptyOutput, error) {
	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	b.mu.Lock("AssociateTagOptionWithResource")
	defer b.mu.Unlock()

	t, err := b.tagOptionLocked(account, region, in.TagOptionID)
	if err != nil {
		return nil, err
	}

	if err = b.resourceExistsLocked(account, region, in.ResourceID); err != nil {
		return nil, err
	}

	for _, l := range b.tagOptionLink.All() {
		if l.ResourceID != in.ResourceID || l.TagOptionID == t.ID {
			continue
		}

		if o, ok := b.tagOptions.Get(l.TagOptionID); ok && o.Key == t.Key {
			return nil, invalid("resource %s already has a tag option with key %s", in.ResourceID, t.Key)
		}
	}

	b.tagOptionLink.Put(&TagOptionLink{Key: t.ID + "|" + in.ResourceID, TagOptionID: t.ID, ResourceID: in.ResourceID})

	return &EmptyOutput{}, nil
}

// DisassociateTagOptionFromResource detaches a tag option from a portfolio or product.
func (b *InMemoryBackend) DisassociateTagOptionFromResource(
	ctx context.Context,
	in *TagOptionResourceInput,
) (*EmptyOutput, error) {
	b.mu.Lock("DisassociateTagOptionFromResource")
	defer b.mu.Unlock()

	if _, err := b.tagOptionLocked(b.callerAccount(ctx), b.callerRegion(ctx), in.TagOptionID); err != nil {
		return nil, err
	}

	if !b.tagOptionLink.Delete(in.TagOptionID + "|" + in.ResourceID) {
		return nil, notFound("tag option association", in.TagOptionID+" and "+in.ResourceID)
	}

	return &EmptyOutput{}, nil
}

// ListResourcesForTagOptionInput is the ListResourcesForTagOption request.
type ListResourcesForTagOptionInput struct {
	TagOptionID  string `json:"TagOptionId"`
	ResourceType string `json:"ResourceType,omitempty"`
	Paging
}

// ResourceDetail is the ResourceDetail shape.
type ResourceDetail struct {
	CreatedTime *float64 `json:"CreatedTime,omitempty"`
	ARN         string   `json:"ARN,omitempty"`
	Description string   `json:"Description,omitempty"`
	ID          string   `json:"Id"`
	Name        string   `json:"Name"`
}

// ListResourcesForTagOptionOutput is the ListResourcesForTagOption response.
type ListResourcesForTagOptionOutput struct {
	PageToken       string           `json:"PageToken,omitempty"`
	ResourceDetails []ResourceDetail `json:"ResourceDetails"`
}

// ListResourcesForTagOption lists the portfolios and products a tag option is attached to.
func (b *InMemoryBackend) ListResourcesForTagOption(
	ctx context.Context,
	in *ListResourcesForTagOptionInput,
) (*ListResourcesForTagOptionOutput, error) {
	if in.ResourceType != "" && in.ResourceType != kindPortfolio && in.ResourceType != kindProduct {
		return nil, invalid("ResourceType must be Portfolio or Product")
	}

	b.mu.RLock("ListResourcesForTagOption")
	defer b.mu.RUnlock()

	t, err := b.tagOptionLocked(b.callerAccount(ctx), b.callerRegion(ctx), in.TagOptionID)
	if err != nil {
		return nil, err
	}

	var all []ResourceDetail

	for _, l := range b.tagOptionLink.All() {
		if l.TagOptionID != t.ID {
			continue
		}

		wantsPortfolio := in.ResourceType == "" || in.ResourceType == kindPortfolio
		wantsProduct := in.ResourceType == "" || in.ResourceType == kindProduct

		if p, ok := b.portfolios.Get(l.ResourceID); ok && wantsPortfolio {
			all = append(all, ResourceDetail{
				ARN: p.ARN, CreatedTime: ts(p.CreatedTime), Description: p.Description, ID: p.ID, Name: p.DisplayName,
			})
		} else if pr, isProd := b.products.Get(l.ResourceID); isProd && wantsProduct {
			all = append(all, ResourceDetail{
				ARN: pr.ARN, CreatedTime: ts(pr.CreatedTime), Description: pr.Description, ID: pr.ID, Name: pr.Name,
			})
		}
	}

	slices.SortFunc(all, func(x, y ResourceDetail) int { return cmp.Compare(x.ID, y.ID) })

	pg, err := pageOf(all, in.Paging)
	if err != nil {
		return nil, err
	}

	return &ListResourcesForTagOptionOutput{ResourceDetails: orEmpty(pg.Data), PageToken: pg.Next}, nil
}

// BudgetResourceInput is the AssociateBudgetWithResource and DisassociateBudgetFromResource request.
type BudgetResourceInput struct {
	BudgetName string `json:"BudgetName"`
	ResourceID string `json:"ResourceId"`
}

func (b *InMemoryBackend) budgetsForLocked(resourceID string) []BudgetDetail {
	var out []BudgetDetail

	for _, l := range b.budgetLinks.All() {
		if l.ResourceID == resourceID {
			out = append(out, BudgetDetail{BudgetName: l.BudgetName})
		}
	}

	slices.SortFunc(out, func(x, y BudgetDetail) int { return cmp.Compare(x.BudgetName, y.BudgetName) })

	return out
}

// AssociateBudgetWithResource attaches a budget to a portfolio or product.
func (b *InMemoryBackend) AssociateBudgetWithResource(
	ctx context.Context,
	in *BudgetResourceInput,
) (*EmptyOutput, error) {
	if in.BudgetName == "" || len(in.BudgetName) > 100 {
		return nil, invalid("BudgetName must be 1-100 characters")
	}

	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	b.mu.RLock("AssociateBudgetWithResource")
	checker := b.budgets
	b.mu.RUnlock()

	if checker != nil && !checker.BudgetExists(ctx, in.BudgetName) {
		return nil, notFound("budget", in.BudgetName)
	}

	b.mu.Lock("AssociateBudgetWithResource")
	defer b.mu.Unlock()

	if err := b.resourceExistsLocked(account, region, in.ResourceID); err != nil {
		return nil, err
	}

	key := in.BudgetName + "|" + in.ResourceID
	if _, ok := b.budgetLinks.Get(key); ok {
		return nil, fail(ErrDuplicateResource, "budget %s is already associated with %s", in.BudgetName, in.ResourceID)
	}

	b.budgetLinks.Put(&BudgetLink{Key: key, BudgetName: in.BudgetName, ResourceID: in.ResourceID})

	return &EmptyOutput{}, nil
}

// DisassociateBudgetFromResource detaches a budget from a portfolio or product.
func (b *InMemoryBackend) DisassociateBudgetFromResource(
	ctx context.Context,
	in *BudgetResourceInput,
) (*EmptyOutput, error) {
	b.mu.Lock("DisassociateBudgetFromResource")
	defer b.mu.Unlock()

	if err := b.resourceExistsLocked(b.callerAccount(ctx), b.callerRegion(ctx), in.ResourceID); err != nil {
		return nil, err
	}

	if !b.budgetLinks.Delete(in.BudgetName + "|" + in.ResourceID) {
		return nil, notFound("budget association", in.BudgetName)
	}

	return &EmptyOutput{}, nil
}

// ListBudgetsForResourceInput is the ListBudgetsForResource request.
type ListBudgetsForResourceInput struct {
	ResourceID string `json:"ResourceId"`
	Paging
}

// ListBudgetsForResourceOutput is the ListBudgetsForResource response.
type ListBudgetsForResourceOutput struct {
	NextPageToken string         `json:"NextPageToken,omitempty"`
	Budgets       []BudgetDetail `json:"Budgets"`
}

// ListBudgetsForResource lists the budgets attached to a portfolio or product.
func (b *InMemoryBackend) ListBudgetsForResource(
	ctx context.Context,
	in *ListBudgetsForResourceInput,
) (*ListBudgetsForResourceOutput, error) {
	b.mu.RLock("ListBudgetsForResource")
	defer b.mu.RUnlock()

	if err := b.resourceExistsLocked(b.callerAccount(ctx), b.callerRegion(ctx), in.ResourceID); err != nil {
		return nil, err
	}

	pg, err := pageOf(b.budgetsForLocked(in.ResourceID), in.Paging)
	if err != nil {
		return nil, err
	}

	return &ListBudgetsForResourceOutput{Budgets: orEmpty(pg.Data), NextPageToken: pg.Next}, nil
}
