package amp

import (
	"cmp"
	"context"
	"slices"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// ScraperInput holds Create/UpdateScraper parameters; empty fields are unchanged on update.
type ScraperInput struct {
	Tags         map[string]string
	Destination  Destination
	ID           string
	Alias        string
	ClientToken  string
	SourceRole   string
	TargetRole   string
	ScrapeConfig []byte
	Exporters    []string
	Source       Source
	HasExporters bool
	HasRoles     bool
}

func (b *InMemoryBackend) wireScraper(s *Scraper) wScraper {
	out := wScraper{
		Tags: cloneTags(s.Tags), Alias: s.Alias, Arn: s.ARN, RoleArn: s.RoleARN, ScraperID: s.ID,
		Status: wCode{StatusCode: b.state(s.Pending)}, CreatedAt: epoch(s.CreatedAt),
		LastModifiedAt:      epoch(s.LastModifiedAt),
		ScrapeConfiguration: wScrapeConfig{ConfigurationBlob: s.ScrapeConfig},
	}

	if s.Destination.WorkspaceARN != "" {
		out.Destination.AmpConfiguration = &wAmpDestination{WorkspaceArn: s.Destination.WorkspaceARN}
	} else {
		out.Destination.CloudWatchConfiguration = &wCloudWatchDestination{DatasetArn: s.Destination.DatasetARN}
	}

	if s.Source.VPC {
		out.Source.VpcConfiguration = &wVPC{
			SubnetIDs:        s.Source.SubnetIDs,
			SecurityGroupIDs: nonNil(s.Source.SecurityGroupIDs),
		}
	} else {
		out.Source.EksConfiguration = &wEks{
			ClusterArn: s.Source.ClusterARN, SubnetIDs: s.Source.SubnetIDs, SecurityGroupIDs: s.Source.SecurityGroupIDs,
		}
	}

	for _, e := range s.Exporters {
		out.Exporters = append(out.Exporters, wExporter{OpenSearchConfiguration: wOpenSearch{DomainArn: e}})
	}

	if s.SourceRole != "" || s.TargetRole != "" {
		out.RoleConfiguration = &wRoleConfig{SourceRoleArn: s.SourceRole, TargetRoleArn: s.TargetRole}
	}

	return out
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}

	return s
}

func (b *InMemoryBackend) scraperLocked(sc scope, id string) (*Scraper, error) {
	s, ok := b.scrapers.Get(id)
	if !ok || !sc.owns(s.AccountID, s.Region) || b.gone(s.Pending) {
		return nil, notFound(resScraper, id, "Scraper %s does not exist", id)
	}

	return s, nil
}

func (b *InMemoryBackend) validateDestination(sc scope, d Destination) error {
	switch {
	case (d.WorkspaceARN == "") == (d.DatasetARN == ""):
		return invalid("destination must set exactly one of ampConfiguration or cloudWatchConfiguration")
	case d.DatasetARN != "":
		if !datasetRe.MatchString(d.DatasetARN) {
			return invalid("datasetArn %q is not a CloudWatch dataset ARN", d.DatasetARN)
		}

		return nil
	}

	m := workspaceRe.FindStringSubmatch(d.WorkspaceARN)
	if m == nil {
		return invalid("workspaceArn %q is not a workspace ARN", d.WorkspaceARN)
	}

	if _, err := b.workspaceLocked(sc, m[3]); err != nil || m[1] != sc.region || m[2] != sc.account {
		return notFound(resWorkspace, m[3], "Workspace %s does not exist", m[3])
	}

	return nil
}

func (b *InMemoryBackend) validateSource(sc scope, src Source) error {
	if err := validateSubnetsAndGroups(src.SubnetIDs, src.SecurityGroupIDs); err != nil {
		return err
	}

	if src.VPC {
		return nil
	}

	m := eksClusterRe.FindStringSubmatch(src.ClusterARN)
	if m == nil {
		return invalid("clusterArn %q is not an EKS cluster ARN", src.ClusterARN)
	}

	if b.clusters != nil && (m[2] != sc.account || !b.clusters.ClusterExists(m[1], m[3])) {
		return notFound("cluster", src.ClusterARN, "EKS cluster %s does not exist", src.ClusterARN)
	}

	return nil
}

func validateScraperExtras(in ScraperInput) error {
	if len(in.Exporters) > maxExporters {
		return invalid("at most %d exporter can be configured", maxExporters)
	}

	for _, e := range in.Exporters {
		if !domainRe.MatchString(e) {
			return invalid("domainArn %q is not an OpenSearch domain ARN", e)
		}
	}

	for _, r := range []string{in.SourceRole, in.TargetRole} {
		if r != "" && !roleRe.MatchString(r) {
			return invalid("role ARN %q is not an IAM role ARN", r)
		}
	}

	return validateAlias(in.Alias)
}

// CreateScraper creates a collector in CREATING state.
func (b *InMemoryBackend) CreateScraper(ctx context.Context, in ScraperInput) (wScraper, error) {
	for _, err := range []error{
		validateScrapeConfig(in.ScrapeConfig), validateTags(in.Tags), validateScraperExtras(in),
	} {
		if err != nil {
			return wScraper{}, err
		}
	}

	sc := b.scopeOf(ctx)

	b.mu.Lock("CreateScraper")
	defer b.mu.Unlock()

	b.purgeLocked()

	if err := b.validateDestination(sc, in.Destination); err != nil {
		return wScraper{}, err
	}

	if err := b.validateSource(sc, in.Source); err != nil {
		return wScraper{}, err
	}

	s, err := idempotency.Create(b.idem, "CreateScraper", in.ClientToken, idempotency.Fingerprint(in),
		func(s *Scraper) string { return s.ID },
		func(id string) (*Scraper, error) { return b.scraperLocked(sc, id) },
		func() (*Scraper, error) {
			id := "s-" + uuid.NewString()
			now := b.now()
			s := &Scraper{
				ID: id, ARN: sc.arn("scraper/" + id), AccountID: sc.account, Region: sc.region, Alias: in.Alias,
				RoleARN: sc.roleARN(id), ScrapeConfig: in.ScrapeConfig, Destination: in.Destination,
				Source: in.Source, Exporters: slices.Clone(in.Exporters), Tags: cloneTags(in.Tags),
				SourceRole: in.SourceRole, TargetRole: in.TargetRole,
				CreatedAt: now, LastModifiedAt: now, Pending: b.pend(statusCreating),
			}
			b.scrapers.Put(s)

			return s, nil
		})
	if err != nil {
		return wScraper{}, mapIdem(err)
	}

	return b.wireScraper(s), nil
}

func (s scope) roleARN(id string) string {
	return "arn:aws:iam::" + s.account + ":role/aws-service-role/scraper.aps.amazonaws.com/" +
		"AWSServiceRoleForAmazonPrometheusScraper_" + strings.ReplaceAll(strings.TrimPrefix(id, "s-"), "-", "")
}

// UpdateScraper changes a scraper's alias, configuration, destination, exporters or role configuration.
func (b *InMemoryBackend) UpdateScraper(ctx context.Context, in ScraperInput) (wScraper, error) {
	if err := validateScraperExtras(in); err != nil {
		return wScraper{}, err
	}

	if len(in.ScrapeConfig) > 0 {
		if err := validateScrapeConfig(in.ScrapeConfig); err != nil {
			return wScraper{}, err
		}
	}

	sc := b.scopeOf(ctx)

	b.mu.Lock("UpdateScraper")
	defer b.mu.Unlock()

	b.purgeLocked()

	s, err := b.scraperLocked(sc, in.ID)
	if err != nil {
		return wScraper{}, err
	}

	if ierr := b.idle(resScraper, in.ID, s.Pending); ierr != nil {
		return wScraper{}, ierr
	}

	if in.Destination != (Destination{}) {
		if ierr := b.validateDestination(sc, in.Destination); ierr != nil {
			return wScraper{}, ierr
		}

		s.Destination = in.Destination
	}

	if in.Alias != "" {
		s.Alias = in.Alias
	}

	if len(in.ScrapeConfig) > 0 {
		s.ScrapeConfig = in.ScrapeConfig
	}

	if in.HasExporters {
		s.Exporters = slices.Clone(in.Exporters)
	}

	if in.HasRoles {
		s.SourceRole, s.TargetRole = in.SourceRole, in.TargetRole
	}

	s.LastModifiedAt = b.now()
	s.Pending = b.pend(statusUpdating)

	return b.wireScraper(s), nil
}

// DescribeScraper returns a scraper.
func (b *InMemoryBackend) DescribeScraper(ctx context.Context, id string) (wScraper, error) {
	b.mu.RLock("DescribeScraper")
	defer b.mu.RUnlock()

	s, err := b.scraperLocked(b.scopeOf(ctx), id)
	if err != nil {
		return wScraper{}, err
	}

	return b.wireScraper(s), nil
}

// ListScrapers lists scrapers matching filters (alias, status, sourceArn, destinationArn).
func (b *InMemoryBackend) ListScrapers(
	ctx context.Context, filters map[string][]string, token string, maxResults int,
) (page.Page[wScraper], error) {
	sc := b.scopeOf(ctx)

	b.mu.RLock("ListScrapers")
	defer b.mu.RUnlock()

	var items []*Scraper

	b.scrapers.Range(func(s *Scraper) bool {
		if sc.owns(s.AccountID, s.Region) && !b.gone(s.Pending) && b.scraperMatches(s, filters) {
			items = append(items, s)
		}

		return true
	})

	slices.SortFunc(items, func(x, y *Scraper) int { return cmp.Compare(x.ID, y.ID) })

	pg, err := restroute.Paginate(items, token, maxResults, listDefault, listMax)
	if err != nil {
		return page.Page[wScraper]{}, err
	}

	out := page.Page[wScraper]{Next: pg.Next, Data: make([]wScraper, 0, len(pg.Data))}
	for _, s := range pg.Data {
		out.Data = append(out.Data, b.wireScraper(s))
	}

	return out, nil
}

func (b *InMemoryBackend) scraperMatches(s *Scraper, filters map[string][]string) bool {
	checks := map[string]string{
		"alias": s.Alias, "status": b.state(s.Pending), "sourceArn": s.Source.ClusterARN,
		"destinationArn": cmp.Or(s.Destination.WorkspaceARN, s.Destination.DatasetARN),
	}

	for key, want := range filters {
		have, known := checks[key]
		if !known || len(want) == 0 {
			continue
		}

		if !slices.Contains(want, have) {
			return false
		}
	}

	return true
}

// DeleteScraper starts scraper deletion.
func (b *InMemoryBackend) DeleteScraper(ctx context.Context, id string) (string, error) {
	b.mu.Lock("DeleteScraper")
	defer b.mu.Unlock()

	b.purgeLocked()

	s, err := b.scraperLocked(b.scopeOf(ctx), id)
	if err != nil {
		return "", err
	}

	if s.Pending.Kind != statusDeleting {
		if ierr := b.idle(resScraper, id, s.Pending); ierr != nil {
			return "", ierr
		}

		s.Pending = b.pend(statusDeleting)
	}

	return statusDeleting, nil
}

// ScraperLoggingInput holds UpdateScraperLoggingConfiguration parameters.
type ScraperLoggingInput struct {
	ID          string
	LogGroupARN string
	Components  []ScraperComponent
}

// UpdateScraperLoggingConfiguration sets a scraper's log delivery.
func (b *InMemoryBackend) UpdateScraperLoggingConfiguration(
	ctx context.Context,
	in ScraperLoggingInput,
) (wStatus, error) {
	if err := validateLogGroup(in.LogGroupARN); err != nil {
		return wStatus{}, err
	}

	for _, c := range in.Components {
		if !validComponentType(c.Type) {
			return wStatus{}, invalid("scraper component type %q is not valid", c.Type)
		}
	}

	b.mu.Lock("UpdateScraperLoggingConfiguration")
	defer b.mu.Unlock()

	b.purgeLocked()

	s, err := b.scraperLocked(b.scopeOf(ctx), in.ID)
	if err != nil {
		return wStatus{}, err
	}

	kind := statusUpdating
	if s.Logging == nil {
		kind = statusCreating
	}

	s.Logging = &ScraperLogging{
		LogGroupARN: in.LogGroupARN, Components: slices.Clone(in.Components),
		ModifiedAt: b.now(), Pending: b.pend(kind),
	}

	return wStatus{StatusCode: kind}, nil
}

// DescribeScraperLoggingConfiguration returns a scraper's log delivery.
func (b *InMemoryBackend) DescribeScraperLoggingConfiguration(ctx context.Context, id string) (wScraperLogging, error) {
	b.mu.RLock("DescribeScraperLoggingConfiguration")
	defer b.mu.RUnlock()

	s, err := b.scraperLocked(b.scopeOf(ctx), id)
	if err != nil {
		return wScraperLogging{}, err
	}

	l := s.Logging
	if l == nil || b.gone(l.Pending) {
		return wScraperLogging{}, notFound(resScraper, id, "No logging configuration exists for scraper %s", id)
	}

	out := wScraperLogging{
		ScraperID: id, Status: b.wireStatus(l.Pending), ModifiedAt: epoch(l.ModifiedAt),
		LoggingDestination: map[string]wCloudWatch{"cloudWatchLogs": {LogGroupArn: l.LogGroupARN}},
		ScraperComponents:  []wScraperComponent{},
	}

	for _, c := range l.Components {
		wc := wScraperComponent{Type: c.Type}
		if len(c.Options) > 0 {
			wc.Config = &wComponentConfig{Options: c.Options}
		}

		out.ScraperComponents = append(out.ScraperComponents, wc)
	}

	return out, nil
}

// DeleteScraperLoggingConfiguration starts deletion of a scraper's log delivery.
func (b *InMemoryBackend) DeleteScraperLoggingConfiguration(ctx context.Context, id string) error {
	b.mu.Lock("DeleteScraperLoggingConfiguration")
	defer b.mu.Unlock()

	b.purgeLocked()

	s, err := b.scraperLocked(b.scopeOf(ctx), id)
	if err != nil {
		return err
	}

	if s.Logging == nil || b.gone(s.Logging.Pending) {
		return notFound(resScraper, id, "No logging configuration exists for scraper %s", id)
	}

	s.Logging.Pending = b.pend(statusDeleting)

	return nil
}
