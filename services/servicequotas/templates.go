package servicequotas

import (
	"cmp"
	"context"
	"fmt"
	"regexp"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

var regionRE = regexp.MustCompile(`^[a-z]{2}(-[a-z]+)+-\d$`)

// TemplateView is a template entry with the quota it targets.
type TemplateView struct {
	Service *ServiceDef
	Def     *QuotaDef
	Item    TemplateItem
}

func (b *InMemoryBackend) orgCheckLocked(s scope) error {
	if b.orgs == nil {
		return fail(ErrNoAvailableOrganization, "The account is not a member of an organization")
	}

	info, ok := b.orgs.OrganizationOf(s.account)
	if !ok {
		return fail(ErrNoAvailableOrganization, "The account is not a member of an organization")
	}

	if info.ManagementAccountID != s.account {
		return fail(ErrAccessDenied, "Only the organization's management account can manage the quota request template")
	}

	if !info.AllFeatures {
		return fail(ErrOrganizationNotInAllFeaturesMode, "The organization does not have all features enabled")
	}

	if !info.ServiceAccess {
		return fail(ErrAWSServiceAccessNotEnabled,
			"Service access for servicequotas.amazonaws.com is not enabled in the organization")
	}

	return nil
}

func templateKey(s scope, region, serviceCode, quotaCode string) string {
	return fmt.Sprintf("%s|%s|%s|%s", s.account, region, serviceCode, quotaCode)
}

func validateTemplateRegion(region string) error {
	if !regionRE.MatchString(region) {
		return fail(ErrIllegalArgument, "AwsRegion %q is not a valid AWS Region", region)
	}

	return nil
}

func (b *InMemoryBackend) templateViewLocked(t *TemplateItem) TemplateView {
	svc, def, _ := b.catalog.Quota(t.ServiceCode, t.QuotaCode)

	return TemplateView{Service: svc, Def: def, Item: *t}
}

// PutTemplateItem adds or replaces a quota increase request in the template.
func (b *InMemoryBackend) PutTemplateItem(
	ctx context.Context, region, serviceCode, quotaCode string, desired *float64,
) (TemplateView, error) {
	if desired == nil {
		return TemplateView{}, fail(ErrIllegalArgument, "DesiredValue is required")
	}

	if err := validateTemplateRegion(region); err != nil {
		return TemplateView{}, err
	}

	b.mu.Lock("PutServiceQuotaIncreaseRequestIntoTemplate")
	defer b.mu.Unlock()

	s := b.scopeOf(ctx)
	if err := b.orgCheckLocked(s); err != nil {
		return TemplateView{}, err
	}

	_, def, err := b.lookupQuotaLocked(serviceCode, quotaCode)
	if err != nil {
		return TemplateView{}, err
	}

	if !def.Adjustable {
		return TemplateView{}, fail(ErrIllegalArgument, "The quota %q is not adjustable", quotaCode)
	}

	if *desired <= def.Default {
		return TemplateView{}, fail(ErrIllegalArgument,
			"DesiredValue %v must be greater than the default value %v", *desired, def.Default)
	}

	item := &TemplateItem{
		Key: templateKey(s, region, serviceCode, quotaCode), Account: s.account, AwsRegion: region,
		ServiceCode: serviceCode, QuotaCode: quotaCode, DesiredValue: *desired,
	}
	b.templates.Put(item)

	return b.templateViewLocked(item), nil
}

func (b *InMemoryBackend) templateItemLocked(s scope, region, serviceCode, quotaCode string) (*TemplateItem, error) {
	if err := validateTemplateRegion(region); err != nil {
		return nil, err
	}

	if _, _, err := b.lookupQuotaLocked(serviceCode, quotaCode); err != nil {
		return nil, err
	}

	t, ok := b.templates.Get(templateKey(s, region, serviceCode, quotaCode))
	if !ok {
		return nil, fail(ErrNoSuchResource, "No template entry exists for quota %q in %s", quotaCode, region)
	}

	return t, nil
}

// GetTemplateItem returns one template entry.
func (b *InMemoryBackend) GetTemplateItem(
	ctx context.Context,
	region, serviceCode, quotaCode string,
) (TemplateView, error) {
	b.mu.RLock("GetServiceQuotaIncreaseRequestFromTemplate")
	defer b.mu.RUnlock()

	s := b.scopeOf(ctx)
	if err := b.orgCheckLocked(s); err != nil {
		return TemplateView{}, err
	}

	t, err := b.templateItemLocked(s, region, serviceCode, quotaCode)
	if err != nil {
		return TemplateView{}, err
	}

	return b.templateViewLocked(t), nil
}

// DeleteTemplateItem removes one template entry.
func (b *InMemoryBackend) DeleteTemplateItem(ctx context.Context, region, serviceCode, quotaCode string) error {
	b.mu.Lock("DeleteServiceQuotaIncreaseRequestFromTemplate")
	defer b.mu.Unlock()

	s := b.scopeOf(ctx)
	if err := b.orgCheckLocked(s); err != nil {
		return err
	}

	t, err := b.templateItemLocked(s, region, serviceCode, quotaCode)
	if err != nil {
		return err
	}

	b.templates.Delete(t.Key)

	return nil
}

// ListTemplateItems lists template entries, optionally narrowed by Region and service.
func (b *InMemoryBackend) ListTemplateItems(
	ctx context.Context, region, serviceCode, token string, maxResults *int32,
) (page.Page[TemplateView], error) {
	b.mu.RLock("ListServiceQuotaIncreaseRequestsInTemplate")
	defer b.mu.RUnlock()

	s := b.scopeOf(ctx)
	if err := b.orgCheckLocked(s); err != nil {
		return page.Page[TemplateView]{}, err
	}

	if region != "" {
		if err := validateTemplateRegion(region); err != nil {
			return page.Page[TemplateView]{}, err
		}
	}

	if serviceCode != "" {
		if _, ok := b.catalog.Service(serviceCode); !ok {
			return page.Page[TemplateView]{}, fail(ErrNoSuchResource, "The service %q was not found", serviceCode)
		}
	}

	rows := make([]*TemplateItem, 0)

	for _, t := range b.templates.All() {
		if t.Account == s.account && (region == "" || t.AwsRegion == region) &&
			(serviceCode == "" || t.ServiceCode == serviceCode) {
			rows = append(rows, t)
		}
	}

	slices.SortFunc(rows, func(a, c *TemplateItem) int { return cmp.Compare(a.Key, c.Key) })

	views := make([]TemplateView, len(rows))
	for i, t := range rows {
		views[i] = b.templateViewLocked(t)
	}

	return paginate(views, token, maxResults)
}

// AssociateTemplate associates the template with the organization.
func (b *InMemoryBackend) AssociateTemplate(ctx context.Context) error {
	return b.setAssociation(ctx, "AssociateServiceQuotaTemplate", assocAssociated)
}

// DisassociateTemplate disassociates the template from the organization.
func (b *InMemoryBackend) DisassociateTemplate(ctx context.Context) error {
	return b.setAssociation(ctx, "DisassociateServiceQuotaTemplate", assocDisassociated)
}

func (b *InMemoryBackend) setAssociation(ctx context.Context, op, status string) error {
	b.mu.Lock(op)
	defer b.mu.Unlock()

	s := b.scopeOf(ctx)
	if err := b.orgCheckLocked(s); err != nil {
		return err
	}

	cur := assocDisassociated
	if a, ok := b.associations.Get(s.account); ok {
		cur = a.Status
	}

	if status == assocDisassociated && cur != assocAssociated {
		return fail(
			ErrServiceQuotaTemplateNotInUse,
			"The quota request template is not associated with the organization",
		)
	}

	b.associations.Put(&Association{Account: s.account, Status: status})

	return nil
}

// TemplateAssociation returns the association status of the template.
func (b *InMemoryBackend) TemplateAssociation(ctx context.Context) (string, error) {
	b.mu.RLock("GetAssociationForServiceQuotaTemplate")
	defer b.mu.RUnlock()

	s := b.scopeOf(ctx)
	if err := b.orgCheckLocked(s); err != nil {
		return "", err
	}

	if a, ok := b.associations.Get(s.account); ok {
		return a.Status, nil
	}

	return assocDisassociated, nil
}
