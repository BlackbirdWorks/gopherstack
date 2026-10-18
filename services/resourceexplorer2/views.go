package resourceexplorer2

import (
	"cmp"
	"context"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const propertyTags = "tags"

// SearchFilter is a view or ListResources filter.
type SearchFilter struct {
	FilterString string `json:"FilterString"`
}

// IncludedProperty names a property returned by searches through a view.
type IncludedProperty struct {
	Name string `json:"Name"`
}

// ViewWire is the View shape.
type ViewWire struct {
	LastUpdatedAt      time.Time          `json:"LastUpdatedAt"`
	Filters            *SearchFilter      `json:"Filters,omitempty"`
	Owner              string             `json:"Owner"`
	Scope              string             `json:"Scope"`
	ViewArn            string             `json:"ViewArn"`
	ViewName           string             `json:"ViewName"`
	IncludedProperties []IncludedProperty `json:"IncludedProperties,omitempty"`
}

// ViewError is a BatchGetView failure.
type ViewError struct {
	ErrorMessage string `json:"ErrorMessage"`
	ViewArn      string `json:"ViewArn"`
}

// CreateViewInput carries CreateView arguments.
type CreateViewInput struct {
	Filters            *SearchFilter
	Tags               map[string]string
	ViewName           string
	ClientToken        string
	Scope              string
	IncludedProperties []IncludedProperty
}

func toWire(v *View) ViewWire {
	w := ViewWire{
		LastUpdatedAt: v.LastUpdatedAt, Owner: v.AccountID, Scope: v.Scope, ViewArn: v.ARN, ViewName: v.Name,
		Filters: &SearchFilter{FilterString: v.FilterString},
	}

	for _, p := range v.IncludedProperties {
		w.IncludedProperties = append(w.IncludedProperties, IncludedProperty{Name: p})
	}

	return w
}

func validateProperties(in []IncludedProperty) ([]string, error) {
	var out []string

	for _, p := range in {
		if p.Name != propertyTags {
			return nil, invalid("IncludedProperties name %q is not supported; only %q is", p.Name, propertyTags)
		}

		if !slices.Contains(out, p.Name) {
			out = append(out, p.Name)
		}
	}

	return out, nil
}

func validateFilter(f *SearchFilter) (string, error) {
	if f == nil {
		return "", nil
	}

	if _, err := parseQuery(f.FilterString); err != nil {
		return "", err
	}

	return f.FilterString, nil
}

func (b *InMemoryBackend) viewsIn(account, region string) []*View {
	var out []*View

	for _, v := range b.views.All() {
		if v.AccountID == account && v.Region == region {
			out = append(out, v)
		}
	}

	slices.SortFunc(out, func(x, y *View) int { return cmp.Compare(x.ARN, y.ARN) })

	return out
}

func (b *InMemoryBackend) removeViewsLocked(account, region string) {
	for _, v := range b.viewsIn(account, region) {
		b.views.Delete(v.ARN)
	}

	b.defaults.Delete(defaultKey(account, region))
}

func (b *InMemoryBackend) viewLocked(account, region, viewARN string) (*View, error) {
	v, ok := b.views.Get(viewARN)
	if !ok || v.AccountID != account || v.Region != region {
		return nil, notFound("view %s not found", viewARN)
	}

	return v, nil
}

// CreateView creates a view in the caller's Region.
func (b *InMemoryBackend) CreateView(ctx context.Context, in *CreateViewInput) (*ViewWire, error) {
	if !viewNameRE.MatchString(in.ViewName) {
		return nil, invalid("ViewName must be 1-%d letters, digits or hyphens", maxViewName)
	}

	filter, err := validateFilter(in.Filters)
	if err != nil {
		return nil, err
	}

	props, err := validateProperties(in.IncludedProperties)
	if err != nil {
		return nil, err
	}

	if err = validateTags(in.Tags); err != nil {
		return nil, err
	}

	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	scope := in.Scope
	if scope == "" {
		scope = arn.BuildGlobal("iam", "", account, "root")
	} else if !strings.HasPrefix(scope, "arn:") {
		return nil, invalid("Scope %q is not an ARN", scope)
	}

	v, err := idempotency.Create(b.idem, "CreateView", in.ClientToken, idempotency.Fingerprint(in),
		func(v *View) string { return v.ARN },
		func(id string) (*View, error) {
			b.mu.RLock("CreateView")
			defer b.mu.RUnlock()

			return b.viewLocked(account, region, id)
		},
		func() (*View, error) {
			return b.createView(account, region, &View{
				Name:               in.ViewName,
				Scope:              scope,
				FilterString:       filter,
				IncludedProperties: props,
				Tags:               maps.Clone(in.Tags),
			})
		},
	)
	if err != nil {
		return nil, wrapIdem(err)
	}

	w := toWire(v)

	return &w, nil
}

func (b *InMemoryBackend) createView(account, region string, v *View) (*View, error) {
	b.mu.Lock("CreateView")
	defer b.mu.Unlock()

	b.purgeLocked()

	if b.indexIn(account, region) == nil {
		return nil, notFound("there is no index in %s; create one before creating views", region)
	}

	existing := b.viewsIn(account, region)
	if len(existing) >= maxViewsPerReg {
		return nil, quotaExceeded("at most %d views are allowed per Region", maxViewsPerReg)
	}

	for _, o := range existing {
		if strs.Equal(o.Name, v.Name) {
			return nil, conflict("a view named %s already exists", v.Name)
		}
	}

	v.ARN = arn.Build(signingName, region, account, "view/"+v.Name+"/"+uuid.NewString())
	v.Region, v.AccountID, v.LastUpdatedAt = region, account, b.now()
	b.views.Put(v)

	return v, nil
}

// GetView returns a view and its tags.
func (b *InMemoryBackend) GetView(ctx context.Context, viewARN string) (*ViewWire, map[string]string, error) {
	if viewARN == "" {
		return nil, nil, invalid("ViewArn is required")
	}

	b.mu.RLock("GetView")
	defer b.mu.RUnlock()

	v, err := b.viewLocked(b.callerAccount(ctx), b.callerRegion(ctx), viewARN)
	if err != nil {
		return nil, nil, err
	}

	w := toWire(v)

	return &w, maps.Clone(v.Tags), nil
}

// UpdateView changes the filter and included properties of a view; nil arguments are left alone.
func (b *InMemoryBackend) UpdateView(
	ctx context.Context, viewARN string, filters *SearchFilter, props []IncludedProperty, propsSet bool,
) (*ViewWire, error) {
	if viewARN == "" {
		return nil, invalid("ViewArn is required")
	}

	filter, err := validateFilter(filters)
	if err != nil {
		return nil, err
	}

	included, err := validateProperties(props)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateView")
	defer b.mu.Unlock()

	v, err := b.viewLocked(b.callerAccount(ctx), b.callerRegion(ctx), viewARN)
	if err != nil {
		return nil, err
	}

	if filters != nil {
		v.FilterString = filter
	}

	if propsSet {
		v.IncludedProperties = included
	}

	v.LastUpdatedAt = b.now()
	w := toWire(v)

	return &w, nil
}

// DeleteView removes a view, clearing it as the default when it was.
func (b *InMemoryBackend) DeleteView(ctx context.Context, viewARN string) (string, error) {
	if viewARN == "" {
		return "", invalid("ViewArn is required")
	}

	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	b.mu.Lock("DeleteView")
	defer b.mu.Unlock()

	if _, err := b.viewLocked(account, region, viewARN); err != nil {
		return "", err
	}

	b.views.Delete(viewARN)

	if d, ok := b.defaults.Get(defaultKey(account, region)); ok && d.ViewARN == viewARN {
		b.defaults.Delete(d.Key)
	}

	return viewARN, nil
}

// ListViews lists the view ARNs of the caller's Region.
func (b *InMemoryBackend) ListViews(ctx context.Context, token string, maxResults int) (page.Page[string], error) {
	b.mu.RLock("ListViews")
	defer b.mu.RUnlock()

	views := b.viewsIn(b.callerAccount(ctx), b.callerRegion(ctx))

	arns := make([]string, 0, len(views))
	for _, v := range views {
		arns = append(arns, v.ARN)
	}

	return paginate(arns, token, maxResults, maxViewPage)
}

// BatchGetView returns the requested views and an error entry for each that cannot be returned.
func (b *InMemoryBackend) BatchGetView(ctx context.Context, arns []string) ([]ViewWire, []ViewError, error) {
	if len(arns) == 0 || len(arns) > maxBatchViews {
		return nil, nil, invalid("ViewArns must contain between 1 and %d ARNs", maxBatchViews)
	}

	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	b.mu.RLock("BatchGetView")
	defer b.mu.RUnlock()

	var (
		views []ViewWire
		errs  []ViewError
	)

	for _, a := range arns {
		v, err := b.viewLocked(account, region, a)
		if err != nil {
			errs = append(errs, ViewError{ErrorMessage: "the view was not found", ViewArn: a})

			continue
		}

		views = append(views, toWire(v))
	}

	return views, errs, nil
}

// AssociateDefaultView makes a view the default for the caller's Region.
func (b *InMemoryBackend) AssociateDefaultView(ctx context.Context, viewARN string) (string, error) {
	if viewARN == "" {
		return "", invalid("ViewArn is required")
	}

	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	b.mu.Lock("AssociateDefaultView")
	defer b.mu.Unlock()

	if _, err := b.viewLocked(account, region, viewARN); err != nil {
		return "", err
	}

	b.defaults.Put(&DefaultView{Key: defaultKey(account, region), ViewARN: viewARN})

	return viewARN, nil
}

// DisassociateDefaultView clears the default view of the caller's Region.
func (b *InMemoryBackend) DisassociateDefaultView(ctx context.Context) {
	b.mu.Lock("DisassociateDefaultView")
	defer b.mu.Unlock()

	b.defaults.Delete(defaultKey(b.callerAccount(ctx), b.callerRegion(ctx)))
}

// GetDefaultView returns the default view ARN of the caller's Region, or "".
func (b *InMemoryBackend) GetDefaultView(ctx context.Context) string {
	b.mu.RLock("GetDefaultView")
	defer b.mu.RUnlock()

	if d, ok := b.defaults.Get(defaultKey(b.callerAccount(ctx), b.callerRegion(ctx))); ok {
		return d.ViewARN
	}

	return ""
}
