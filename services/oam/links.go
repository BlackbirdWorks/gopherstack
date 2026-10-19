package oam

import (
	"cmp"
	"context"
	"maps"
	"slices"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	resourceTypeLogGroup = "AWS::Logs::LogGroup"
	resourceTypeMetric   = "AWS::CloudWatch::Metric"
	maxLabelLen          = 255
)

func validResourceTypes() []string {
	return []string{
		resourceTypeMetric, resourceTypeLogGroup, "AWS::XRay::Trace", "AWS::ApplicationInsights::Application",
		"AWS::InternetMonitor::Monitor", "AWS::ApplicationSignals::Service",
		"AWS::ApplicationSignals::ServiceLevelObjective",
	}
}

func normalizeResourceTypes(in []string) ([]string, error) {
	if len(in) == 0 {
		return nil, fail(ErrMissingParameter, "ResourceTypes is required")
	}

	valid := validResourceTypes()
	out := make([]string, 0, len(in))

	for _, t := range in {
		if !slices.Contains(valid, t) {
			return nil, fail(ErrInvalidParameter, "ResourceType %q is not supported", t)
		}

		if !slices.Contains(out, t) {
			out = append(out, t)
		}
	}

	return out, nil
}

func validateLinkConfig(cfg LinkConfig, types []string) error {
	if cfg.LogGroupFilter != nil {
		if !slices.Contains(types, resourceTypeLogGroup) {
			return fail(
				ErrInvalidParameter,
				"LogGroupConfiguration requires the %s resource type",
				resourceTypeLogGroup,
			)
		}

		if err := validateFilter(*cfg.LogGroupFilter, "LogGroupName"); err != nil {
			return err
		}
	}

	if cfg.MetricFilter != nil {
		if !slices.Contains(types, resourceTypeMetric) {
			return fail(ErrInvalidParameter, "MetricConfiguration requires the %s resource type", resourceTypeMetric)
		}

		if err := validateFilter(*cfg.MetricFilter, "Namespace"); err != nil {
			return err
		}
	}

	return nil
}

func (b *InMemoryBackend) resolveLabelLocked(template, account string) (string, error) {
	if template == "" {
		return "", fail(ErrMissingParameter, "LabelTemplate is required")
	}

	if len(template) > maxLabelLen {
		return "", fail(ErrInvalidParameter, "LabelTemplate must be at most %d characters", maxLabelLen)
	}

	info := AccountInfo{Name: account, Email: account}

	if b.accounts != nil {
		if found, ok := b.accounts.AccountInfo(account); ok {
			info = found
		}
	}

	noDomain, _, _ := strings.Cut(info.Email, "@")
	label := template

	// Longest variable first so $AccountEmailNoDomain is not clobbered by $AccountEmail.
	for _, r := range [][2]string{
		{"$AccountEmailNoDomain", noDomain}, {"$AccountEmail", info.Email}, {"$AccountName", info.Name},
	} {
		label = strings.ReplaceAll(label, r[0], r[1])
	}

	if strings.Contains(label, "$") {
		return "", fail(ErrInvalidParameter, "LabelTemplate %q uses an unknown variable", template)
	}

	return label, nil
}

func linkARN(s scope, id string) string {
	return arn.Build(serviceName, s.region, s.account, "link/"+id)
}

func cloneLink(l *Link) *Link {
	cp := *l
	cp.Tags = maps.Clone(l.Tags)
	cp.ResourceTypes = slices.Clone(l.ResourceTypes)

	return &cp
}

func (b *InMemoryBackend) linkInScopeLocked(s scope, identifier string) (*Link, error) {
	if identifier == "" {
		return nil, fail(ErrMissingParameter, "Identifier is required")
	}

	if strings.HasPrefix(identifier, "arn:") {
		if v, ok := b.links.Get(identifier); ok && v.Account == s.account && v.Region == s.region {
			return v, nil
		}
	} else {
		for _, v := range b.links.All() {
			if v.ID == identifier && v.Account == s.account && v.Region == s.region {
				return v, nil
			}
		}
	}

	return nil, fail(ErrResourceNotFound, "The link %q was not found", identifier)
}

// LinkInput carries the editable fields of CreateLink.
type LinkInput struct {
	Config        LinkConfig
	Tags          map[string]string
	SinkARN       string
	LabelTemplate string
	ResourceTypes []string
}

// CreateLink links the caller's account and Region to a sink, subject to the sink policy.
func (b *InMemoryBackend) CreateLink(ctx context.Context, in LinkInput) (*Link, error) {
	if in.SinkARN == "" {
		return nil, fail(ErrMissingParameter, "SinkIdentifier is required")
	}

	types, err := normalizeResourceTypes(in.ResourceTypes)
	if err != nil {
		return nil, err
	}

	if err = validateLinkConfig(in.Config, types); err != nil {
		return nil, err
	}

	if err = validateTags(in.Tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateLink")
	defer b.mu.Unlock()

	sink, ok := b.sinks.Get(in.SinkARN)
	if !ok {
		return nil, fail(ErrResourceNotFound, "The sink %q was not found", in.SinkARN)
	}

	s := b.scopeOf(ctx)

	label, err := b.resolveLabelLocked(in.LabelTemplate, s.account)
	if err != nil {
		return nil, err
	}

	if !b.sinkPolicyAllowsLocked(sink, s.account, actionCreateLink, types) {
		return nil, fail(
			ErrInvalidParameter,
			"The sink policy does not allow account %s to create a link with these resource types",
			s.account,
		)
	}

	for _, l := range b.links.All() {
		if l.SinkARN == sink.ARN && l.Account == s.account && l.Region == s.region {
			return nil, fail(ErrConflict, "A link from this account and Region to the sink already exists")
		}
	}

	id := uuid.NewString()
	link := &Link{
		ARN: linkARN(s, id), ID: id, Label: label, LabelTemplate: in.LabelTemplate, SinkARN: sink.ARN,
		Account: s.account, Region: s.region, ResourceTypes: types, Tags: maps.Clone(in.Tags),
		LogGroupFilter: in.Config.LogGroupFilter, MetricFilter: in.Config.MetricFilter, Seq: b.nextSeqLocked(),
	}
	b.links.Put(link)

	return cloneLink(link), nil
}

// GetLink returns a link by ARN or ID.
func (b *InMemoryBackend) GetLink(ctx context.Context, identifier string) (*Link, error) {
	b.mu.RLock("GetLink")
	defer b.mu.RUnlock()

	v, err := b.linkInScopeLocked(b.scopeOf(ctx), identifier)
	if err != nil {
		return nil, err
	}

	return cloneLink(v), nil
}

// ListLinks returns the caller's links in creation order.
func (b *InMemoryBackend) ListLinks(ctx context.Context, token string, maxResults *int32) (page.Page[*Link], error) {
	b.mu.RLock("ListLinks")
	defer b.mu.RUnlock()

	s := b.scopeOf(ctx)
	rows := make([]*Link, 0)

	for _, v := range b.links.All() {
		if v.Account == s.account && v.Region == s.region {
			rows = append(rows, cloneLink(v))
		}
	}

	slices.SortFunc(rows, func(a, c *Link) int { return cmp.Compare(a.Seq, c.Seq) })

	return paginate(rows, token, maxResults)
}

// ListAttachedLinks returns the links attached to one of the caller's sinks.
func (b *InMemoryBackend) ListAttachedLinks(
	ctx context.Context, sinkIdentifier, token string, maxResults *int32,
) (page.Page[*Link], error) {
	b.mu.RLock("ListAttachedLinks")
	defer b.mu.RUnlock()

	sink, err := b.sinkInScopeLocked(b.scopeOf(ctx), sinkIdentifier)
	if err != nil {
		return page.Page[*Link]{}, err
	}

	rows := make([]*Link, 0)

	for _, v := range b.links.All() {
		if v.SinkARN == sink.ARN {
			rows = append(rows, cloneLink(v))
		}
	}

	slices.SortFunc(rows, func(a, c *Link) int { return cmp.Compare(a.Seq, c.Seq) })

	return paginate(rows, token, maxResults)
}

// UpdateLink replaces a link's resource types and, when given, its filters.
func (b *InMemoryBackend) UpdateLink(
	ctx context.Context, identifier string, resourceTypes []string, cfg *LinkConfig,
) (*Link, error) {
	types, err := normalizeResourceTypes(resourceTypes)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateLink")
	defer b.mu.Unlock()

	link, err := b.linkInScopeLocked(b.scopeOf(ctx), identifier)
	if err != nil {
		return nil, err
	}

	next := LinkConfig{LogGroupFilter: link.LogGroupFilter, MetricFilter: link.MetricFilter}
	if cfg != nil {
		next = *cfg
	}

	if err = validateLinkConfig(next, types); err != nil {
		return nil, err
	}

	if sink, ok := b.sinks.Get(link.SinkARN); ok &&
		!b.sinkPolicyAllowsLocked(sink, link.Account, actionUpdateLink, types) {
		return nil, fail(
			ErrInvalidParameter,
			"The sink policy does not allow account %s to update the link with these resource types",
			link.Account,
		)
	}

	link.ResourceTypes = types
	link.LogGroupFilter, link.MetricFilter = next.LogGroupFilter, next.MetricFilter

	return cloneLink(link), nil
}

// DeleteLink removes a link.
func (b *InMemoryBackend) DeleteLink(ctx context.Context, identifier string) error {
	b.mu.Lock("DeleteLink")
	defer b.mu.Unlock()

	v, err := b.linkInScopeLocked(b.scopeOf(ctx), identifier)
	if err != nil {
		return err
	}

	b.links.Delete(v.ARN)

	return nil
}
