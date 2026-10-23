package gcpmonitoring

import (
	"maps"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	fiveMinutes          = 5 * time.Minute
	tenMinutes           = 10 * time.Minute
	fifteenMinutes       = 15 * time.Minute
	errUnsupportedM      = "unsupported content matcher"
	defaultUptimePeriod  = time.Minute
	defaultUptimeTimeout = 10 * time.Second
	maxUptimeTimeout     = time.Minute
	maxSlug              = 40
	httpPort             = 80
	httpsPort            = 443
	maxPort              = 65535
)

var (
	slugRe = regexp.MustCompile(`[^a-z0-9]+`)
)

type uptimeMatcher = monitoringpb.UptimeCheckConfig_ContentMatcher

func cloneUptime(c *monitoringpb.UptimeCheckConfig) *monitoringpb.UptimeCheckConfig {
	out, _ := proto.Clone(c).(*monitoringpb.UptimeCheckConfig)

	return out
}

func uptimeID(displayName string) string {
	slug := strings.Trim(slugRe.ReplaceAllString(strings.ToLower(displayName), "-"), "-")
	if len(slug) > maxSlug {
		slug = strings.Trim(slug[:maxSlug], "-")
	}

	if slug == "" {
		slug = "check"
	}

	return slug + "-" + newHexSuffix()
}

func uptimeHost(c *monitoringpb.UptimeCheckConfig) string {
	return c.GetMonitoredResource().GetLabels()[labelHost]
}

func validatePeriod(c *monitoringpb.UptimeCheckConfig) error {
	p := c.GetPeriod().AsDuration()

	if slices.Contains([]time.Duration{time.Minute, fiveMinutes, tenMinutes, fifteenMinutes}, p) {
		return nil
	}

	return errInvalid("Field uptime_check_config.period had an invalid value of %q: "+
		"the period must be 1, 5, 10 or 15 minutes.", p)
}

func validateMatchers(ms []*uptimeMatcher) error {
	for _, m := range ms {
		switch m.GetMatcher() {
		case monitoringpb.UptimeCheckConfig_ContentMatcher_CONTENT_MATCHER_OPTION_UNSPECIFIED,
			monitoringpb.UptimeCheckConfig_ContentMatcher_CONTAINS_STRING,
			monitoringpb.UptimeCheckConfig_ContentMatcher_NOT_CONTAINS_STRING:
		case monitoringpb.UptimeCheckConfig_ContentMatcher_MATCHES_REGEX,
			monitoringpb.UptimeCheckConfig_ContentMatcher_NOT_MATCHES_REGEX:
			if _, err := regexp.Compile(m.GetContent()); err != nil || len(m.GetContent()) > maxRegexLen {
				return errInvalid("Field uptime_check_config.content_matchers had an invalid regular expression.")
			}
		default:
			return errInvalid("Field uptime_check_config.content_matchers had an invalid value: %s: %s.",
				errUnsupportedM, m.GetMatcher())
		}
	}

	return nil
}

func validatePort(port int32) error {
	if port < 0 || port > maxPort {
		return errInvalid("Field uptime_check_config port had an invalid value of %d.", port)
	}

	return nil
}

// normalizeUptime validates a config and fills the defaults the service reports.
func normalizeUptime(in *monitoringpb.UptimeCheckConfig) (*monitoringpb.UptimeCheckConfig, error) {
	c := cloneUptime(in)

	if c.GetDisplayName() == "" || len(c.GetDisplayName()) > maxDisplayName {
		return nil, errInvalid(
			"Field uptime_check_config.display_name had an invalid value: a display name is required.",
		)
	}

	//nolint:staticcheck // the deprecated field must still be rejected
	if c.GetIsInternal() || len(c.GetInternalCheckers()) > 0 || c.GetResourceGroup() != nil {
		return nil, errInvalid("Field uptime_check_config had an invalid value: internal checkers and resource " +
			"groups are not supported.")
	}

	res := c.GetMonitoredResource()
	if res.GetType() != typeUptimeURL || uptimeHost(c) == "" {
		return nil, errInvalid("Field uptime_check_config.monitored_resource had an invalid value: " +
			"an uptime_url resource with a host label is required.")
	}

	if c.GetPeriod() == nil {
		c.Period = durationpb.New(defaultUptimePeriod)
	}

	if c.GetTimeout() == nil {
		c.Timeout = durationpb.New(defaultUptimeTimeout)
	}

	if t := c.GetTimeout().AsDuration(); t < time.Second || t > maxUptimeTimeout {
		return nil, errInvalid("Field uptime_check_config.timeout had an invalid value of %q: "+
			"the timeout must be between 1 and 60 seconds.", t)
	}

	if err := validatePeriod(c); err != nil {
		return nil, err
	}

	if err := validateMatchers(c.GetContentMatchers()); err != nil {
		return nil, err
	}

	return c, normalizeRequestType(c)
}

func normalizeRequestType(c *monitoringpb.UptimeCheckConfig) error {
	switch rt := c.GetCheckRequestType().(type) {
	case *monitoringpb.UptimeCheckConfig_HttpCheck_:
		h := rt.HttpCheck
		if err := validatePort(h.GetPort()); err != nil {
			return err
		}

		if h.GetPort() == 0 {
			h.Port = httpPort
			if h.GetUseSsl() {
				h.Port = httpsPort
			}
		}

		if h.GetPath() == "" {
			h.Path = "/"
		}

		if h.GetRequestMethod() == monitoringpb.UptimeCheckConfig_HttpCheck_METHOD_UNSPECIFIED {
			h.RequestMethod = monitoringpb.UptimeCheckConfig_HttpCheck_GET
		}

		if h.GetRequestMethod() == monitoringpb.UptimeCheckConfig_HttpCheck_POST &&
			h.GetContentType() == monitoringpb.UptimeCheckConfig_HttpCheck_TYPE_UNSPECIFIED {
			h.ContentType = monitoringpb.UptimeCheckConfig_HttpCheck_URL_ENCODED
		}

		return nil
	case *monitoringpb.UptimeCheckConfig_TcpCheck_:
		if rt.TcpCheck.GetPort() <= 0 {
			return errInvalid("Field uptime_check_config.tcp_check.port had an invalid value: a port is required.")
		}

		return validatePort(rt.TcpCheck.GetPort())
	}

	return errInvalid("Field uptime_check_config had an invalid value: an http_check or tcp_check is required.")
}

// CreateUptimeCheckConfig stores a check; it is probed on the next Tick.
func (b *InMemoryBackend) CreateUptimeCheckConfig(
	req *monitoringpb.CreateUptimeCheckConfigRequest,
) (*monitoringpb.UptimeCheckConfig, error) {
	project, err := projectOf(req.GetParent())
	if err != nil {
		return nil, err
	}

	if req.GetUptimeCheckConfig() == nil {
		return nil, errInvalid("uptime_check_config is required.")
	}

	c, err := normalizeUptime(req.GetUptimeCheckConfig())
	if err != nil {
		return nil, err
	}

	c.Name = projectScope + project + "/" + collUptime + "/" + uptimeID(c.GetDisplayName())
	c.GetMonitoredResource().Labels = withProject(c.GetMonitoredResource().GetLabels(), project)

	b.mu.Lock("CreateUptimeCheckConfig")
	defer b.mu.Unlock()

	b.uptime[c.GetName()] = &uptimeRec{pb: c}

	return cloneUptime(c), nil
}

func withProject(labels map[string]string, project string) map[string]string {
	out := make(map[string]string, len(labels)+1)
	maps.Copy(out, labels)

	if out[labelProject] == "" {
		out[labelProject] = project
	}

	return out
}

// GetUptimeCheckConfig returns one check.
func (b *InMemoryBackend) GetUptimeCheckConfig(name string) (*monitoringpb.UptimeCheckConfig, error) {
	if _, _, err := splitName(name, collUptime); err != nil {
		return nil, err
	}

	b.mu.RLock("GetUptimeCheckConfig")
	defer b.mu.RUnlock()

	rec, ok := b.uptime[name]
	if !ok {
		return nil, errNotFound("Uptime check %s does not exist.", name)
	}

	return cloneUptime(rec.pb), nil
}

type uptimeEnv struct {
	c *monitoringpb.UptimeCheckConfig
}

func (e uptimeEnv) values(path string) []string {
	switch path {
	case fldDisplayName:
		return []string{e.c.GetDisplayName()}
	case fldName:
		return []string{e.c.GetName()}
	}

	return nil
}

// ListUptimeCheckConfigs pages the project's checks ordered by name.
func (b *InMemoryBackend) ListUptimeCheckConfigs(
	req *monitoringpb.ListUptimeCheckConfigsRequest,
) ([]*monitoringpb.UptimeCheckConfig, string, int32, error) {
	project, err := projectOf(req.GetParent())
	if err != nil {
		return nil, "", 0, err
	}

	if err = pageTokenOK(req.GetPageToken()); err != nil {
		return nil, "", 0, err
	}

	f, err := parseFilter(req.GetFilter())
	if err != nil {
		return nil, "", 0, err
	}

	prefix := projectScope + project + "/" + collUptime + "/"

	b.mu.RLock("ListUptimeCheckConfigs")

	var all []*monitoringpb.UptimeCheckConfig

	for name, rec := range b.uptime {
		if strings.HasPrefix(name, prefix) && f.matches(uptimeEnv{c: rec.pb}) {
			all = append(all, cloneUptime(rec.pb))
		}
	}

	b.mu.RUnlock()

	sort.Slice(all, func(i, j int) bool { return all[i].GetName() < all[j].GetName() })
	pg := page.New(all, req.GetPageToken(), clampPage(req.GetPageSize()), defaultPage)

	return pg.Data, pg.Next, int32(len(all)), nil //nolint:gosec // bounded by the check count
}

// UpdateUptimeCheckConfig replaces the fields in the mask (the whole config when empty).
func (b *InMemoryBackend) UpdateUptimeCheckConfig(
	req *monitoringpb.UpdateUptimeCheckConfigRequest,
) (*monitoringpb.UptimeCheckConfig, error) {
	in := req.GetUptimeCheckConfig()

	project, _, err := splitName(in.GetName(), collUptime)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateUptimeCheckConfig")
	defer b.mu.Unlock()

	rec, ok := b.uptime[in.GetName()]
	if !ok {
		return nil, errNotFound("Uptime check %s does not exist.", in.GetName())
	}

	next := cloneUptime(rec.pb)
	if err = applyUptimeMask(next, in, req.GetUpdateMask().GetPaths()); err != nil {
		return nil, err
	}

	if next, err = normalizeUptime(next); err != nil {
		return nil, err
	}

	next.Name = rec.pb.GetName()
	next.GetMonitoredResource().Labels = withProject(next.GetMonitoredResource().GetLabels(), project)
	rec.pb, rec.lastRun = next, time.Time{}

	return cloneUptime(next), nil
}

func applyUptimeMask(next, in *monitoringpb.UptimeCheckConfig, paths []string) error {
	if len(paths) == 0 {
		name := next.GetName()
		*next = *cloneUptime(in)
		next.Name = name

		return nil
	}

	src := cloneUptime(in)

	for _, p := range paths {
		switch p {
		case fldDisplayName:
			next.DisplayName = src.GetDisplayName()
		case "monitored_resource":
			next.Resource = src.GetResource()
		case "http_check", "tcp_check":
			next.CheckRequestType = src.GetCheckRequestType()
		case "period":
			next.Period = src.GetPeriod()
		case "timeout":
			next.Timeout = src.GetTimeout()
		case "content_matchers":
			next.ContentMatchers = src.GetContentMatchers()
		case "selected_regions":
			next.SelectedRegions = src.GetSelectedRegions()
		case "checker_type":
			next.CheckerType = src.GetCheckerType()
		case fldUserLabels:
			next.UserLabels = src.GetUserLabels()
		default:
			return errInvalid("Field update_mask had an invalid value of %q: the field cannot be updated.", p)
		}
	}

	return nil
}

// DeleteUptimeCheckConfig removes a check.
func (b *InMemoryBackend) DeleteUptimeCheckConfig(name string) error {
	if _, _, err := splitName(name, collUptime); err != nil {
		return err
	}

	b.mu.Lock("DeleteUptimeCheckConfig")
	defer b.mu.Unlock()

	if _, ok := b.uptime[name]; !ok {
		return errNotFound("Uptime check %s does not exist.", name)
	}

	delete(b.uptime, name)

	return nil
}

// ListUptimeCheckIps lists the checker locations; probes originate from the emulator itself.
func (b *InMemoryBackend) ListUptimeCheckIps() []*monitoringpb.UptimeCheckIp {
	out := make([]*monitoringpb.UptimeCheckIp, 0, len(checkerLocations))

	for _, l := range checkerLocations {
		out = append(out, &monitoringpb.UptimeCheckIp{Region: l.region, Location: l.city, IpAddress: "127.0.0.1"})
	}

	return out
}
