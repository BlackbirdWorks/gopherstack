package rum

import (
	"cmp"
	"context"
	"maps"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

const (
	maxNameLen    = 255
	maxDomainLen  = 253
	maxDomainList = 5
)

const (
	timeLayoutRFC  = time.RFC3339
	errTagsTooMany = "a resource can have at most 50 tags"
)

var namePattern = regexp.MustCompile(`^[\.\-_/#A-Za-z0-9]+$`)

func validTelemetry(t string) bool {
	return t == telemetryErrors || t == telemetryPerformance || t == telemetryHTTP
}

func validPlatform(p string) bool {
	return p == platformWeb || p == platformAndroid || p == platformIOS
}

type wireCustomEvents struct {
	Status string `json:"Status"`
}

type wireSourceMaps struct {
	JavaScriptSourceMaps *SourceMaps `json:"JavaScriptSourceMaps,omitempty"`
}

type wireCwLog struct {
	CwLogGroup   string `json:"CwLogGroup,omitempty"`
	CwLogEnabled bool   `json:"CwLogEnabled"`
}

type wireDataStorage struct {
	CwLog *wireCwLog `json:"CwLog,omitempty"`
}

type wireMonitor struct {
	AppMonitorConfiguration    *MonitorConfig    `json:"AppMonitorConfiguration,omitempty"`
	CustomEvents               *wireCustomEvents `json:"CustomEvents,omitempty"`
	DataStorage                *wireDataStorage  `json:"DataStorage,omitempty"`
	DeobfuscationConfiguration *wireSourceMaps   `json:"DeobfuscationConfiguration,omitempty"`
	Tags                       map[string]string `json:"Tags,omitempty"`
	Created                    string            `json:"Created"`
	Domain                     string            `json:"Domain,omitempty"`
	ID                         string            `json:"Id"`
	LastModified               string            `json:"LastModified"`
	Name                       string            `json:"Name"`
	Platform                   string            `json:"Platform"`
	State                      string            `json:"State"`
	DomainList                 []string          `json:"DomainList,omitempty"`
}

// WireSummary is an entry of ListAppMonitors.
type WireSummary struct {
	Created      string `json:"Created"`
	ID           string `json:"Id"`
	LastModified string `json:"LastModified"`
	Name         string `json:"Name"`
	Platform     string `json:"Platform"`
	State        string `json:"State"`
}

func logGroupOf(m *AppMonitor) string {
	return logGroupPrefix + m.Name + m.ID[:logGroupIDChars]
}

func monitorState(m *AppMonitor) string {
	if m.Active {
		return stateActive
	}

	return stateCreated
}

func wireMonitorOf(m *AppMonitor) wireMonitor {
	out := wireMonitor{
		AppMonitorConfiguration: m.Config,
		CustomEvents:            &wireCustomEvents{Status: m.CustomEvents},
		DataStorage:             &wireDataStorage{CwLog: &wireCwLog{CwLogEnabled: m.CwLogEnabled}},
		Created:                 m.Created.Format(timeLayoutRFC),
		Domain:                  m.Domain,
		DomainList:              m.DomainList,
		ID:                      m.ID,
		LastModified:            m.LastModified.Format(timeLayoutRFC),
		Name:                    m.Name,
		Platform:                m.Platform,
		State:                   monitorState(m),
		Tags:                    m.Tags,
	}

	if m.CwLogEnabled {
		out.DataStorage.CwLog.CwLogGroup = logGroupOf(m)
	}

	if m.SourceMaps != nil {
		out.DeobfuscationConfiguration = &wireSourceMaps{JavaScriptSourceMaps: m.SourceMaps}
	}

	return out
}

// CreateMonitorInput holds CreateAppMonitor parameters.
type CreateMonitorInput struct {
	Config       *MonitorConfig
	CustomEvents *wireCustomEvents
	SourceMaps   *wireSourceMaps
	Tags         map[string]string
	CwLogEnabled *bool
	Name         string
	Platform     string
	Domain       string
	DomainList   []string
}

func validateDomains(domain string, list []string, required bool) error {
	switch {
	case domain != "" && len(list) > 0:
		return invalid("specify either Domain or DomainList, not both")
	case required && domain == "" && len(list) == 0:
		return invalid("Domain or DomainList is required")
	case len(domain) > maxDomainLen:
		return invalid("Domain must be at most %d characters", maxDomainLen)
	case len(list) > maxDomainList:
		return invalid("DomainList can contain at most %d domains", maxDomainList)
	}

	for _, d := range list {
		if d == "" || len(d) > maxDomainLen {
			return invalid("DomainList entries must be 1-%d characters", maxDomainLen)
		}
	}

	return nil
}

func validateConfig(c *MonitorConfig) error {
	if c == nil {
		return nil
	}

	if c.SessionSampleRate < 0 || c.SessionSampleRate > 1 {
		return invalid("SessionSampleRate must be between 0 and 1")
	}

	if len(c.IncludedPages) > 0 && len(c.ExcludedPages) > 0 {
		return invalid("IncludedPages and ExcludedPages cannot be specified together")
	}

	for _, t := range c.Telemetries {
		if !validTelemetry(t) {
			return invalid("Telemetries entries must be one of errors, performance, http")
		}
	}

	return nil
}

func validateCustomEvents(c *wireCustomEvents) (string, error) {
	switch {
	case c == nil:
		return statusDisabled, nil
	case c.Status == statusEnabled || c.Status == statusDisabled:
		return c.Status, nil
	default:
		return "", invalid("CustomEvents.Status must be %s or %s", statusEnabled, statusDisabled)
	}
}

func sourceMapsOf(w *wireSourceMaps) *SourceMaps {
	if w == nil || w.JavaScriptSourceMaps == nil {
		return nil
	}

	cp := *w.JavaScriptSourceMaps

	return &cp
}

func validateSourceMaps(w *wireSourceMaps) error {
	sm := sourceMapsOf(w)
	if sm == nil {
		return nil
	}

	if sm.Status != statusEnabled && sm.Status != statusDisabled {
		return invalid("JavaScriptSourceMaps.Status must be %s or %s", statusEnabled, statusDisabled)
	}

	if sm.Status == statusEnabled && sm.S3URI == "" {
		return invalid("JavaScriptSourceMaps.S3Uri is required when source maps are enabled")
	}

	return nil
}

func validateMonitorTags(t map[string]string) error {
	if len(t) > maxTags {
		return invalid("%s", errTagsTooMany)
	}

	for k, v := range t {
		if k == "" || len(k) > 128 || len(v) > 256 || strings.HasPrefix(strings.ToLower(k), "aws:") {
			return invalid("tag keys must be 1-128 characters, values at most 256, and not start with aws:")
		}
	}

	return nil
}

func (b *InMemoryBackend) monitorByNameLocked(sc scope, name string) (*AppMonitor, error) {
	var found *AppMonitor

	b.monitors.Range(func(m *AppMonitor) bool {
		if m.Name == name && m.Account == sc.account && m.Region == sc.region {
			found = m

			return false
		}

		return true
	})

	if found == nil {
		return nil, notFound(name, "App monitor %s does not exist", name)
	}

	return found, nil
}

// ensureLogGroupLocked creates the monitor's CloudWatch Logs group when delivery is enabled.
func (b *InMemoryBackend) ensureLogGroupLocked(ctx context.Context, m *AppMonitor) error {
	if !m.CwLogEnabled || b.logs == nil {
		return nil
	}

	if err := b.logs.EnsureLogGroup(ctx, m.Region, logGroupOf(m)); err != nil {
		return internalError("could not create the CloudWatch Logs group: %v", err)
	}

	return nil
}

// CreateAppMonitor creates an app monitor and returns its ID.
func (b *InMemoryBackend) CreateAppMonitor(ctx context.Context, in CreateMonitorInput) (string, error) {
	if in.Name == "" || len(in.Name) > maxNameLen || !namePattern.MatchString(in.Name) {
		return "", invalid("Name must be 1-%d characters of letters, digits and . - _ / #", maxNameLen)
	}

	platform := cmp.Or(in.Platform, platformWeb)
	if !validPlatform(platform) {
		return "", invalid("Platform must be Web, Android or iOS")
	}

	if err := validateDomains(in.Domain, in.DomainList, platform == platformWeb); err != nil {
		return "", err
	}

	if err := validateConfig(in.Config); err != nil {
		return "", err
	}

	if err := validateMonitorTags(in.Tags); err != nil {
		return "", err
	}

	custom, err := validateCustomEvents(in.CustomEvents)
	if err != nil {
		return "", err
	}

	if err = validateSourceMaps(in.SourceMaps); err != nil {
		return "", err
	}

	sc := b.scopeOf(ctx)

	b.mu.Lock("CreateAppMonitor")
	defer b.mu.Unlock()

	if _, lookup := b.monitorByNameLocked(sc, in.Name); lookup == nil {
		return "", conflict(resAppMonitor, in.Name, "App monitor %s already exists", in.Name)
	}

	if b.monitorCountLocked(sc) >= maxAppMonitors {
		return "", restroute.QuotaExceeded("at most %d app monitors are allowed per region", maxAppMonitors)
	}

	now := b.now()
	m := &AppMonitor{
		ID: uuid.NewString(), Name: in.Name, Account: sc.account, Region: sc.region, Platform: platform,
		Domain: in.Domain, DomainList: in.DomainList, CustomEvents: custom, SourceMaps: sourceMapsOf(in.SourceMaps),
		Config: in.Config, Tags: cloneMap(in.Tags), Created: now, LastModified: now,
		CwLogEnabled: in.CwLogEnabled != nil && *in.CwLogEnabled,
	}

	if err = b.ensureLogGroupLocked(ctx, m); err != nil {
		return "", err
	}

	b.monitors.Put(m)

	return m.ID, nil
}

func (b *InMemoryBackend) monitorCountLocked(sc scope) int {
	n := 0

	b.monitors.Range(func(m *AppMonitor) bool {
		if m.Account == sc.account && m.Region == sc.region {
			n++
		}

		return true
	})

	return n
}

func cloneMap(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}

	return maps.Clone(m)
}

// getAppMonitor returns one app monitor.
func (b *InMemoryBackend) getAppMonitor(ctx context.Context, name string) (wireMonitor, error) {
	sc := b.scopeOf(ctx)

	b.mu.RLock("getAppMonitor")
	defer b.mu.RUnlock()

	m, err := b.monitorByNameLocked(sc, name)
	if err != nil {
		return wireMonitor{}, err
	}

	return wireMonitorOf(m), nil
}

// ListAppMonitors pages the caller's app monitors.
func (b *InMemoryBackend) ListAppMonitors(
	ctx context.Context, token string, maxResults int,
) (page.Page[WireSummary], error) {
	sc := b.scopeOf(ctx)

	b.mu.RLock("ListAppMonitors")
	defer b.mu.RUnlock()

	var all []*AppMonitor

	b.monitors.Range(func(m *AppMonitor) bool {
		if m.Account == sc.account && m.Region == sc.region {
			all = append(all, m)
		}

		return true
	})

	slices.SortFunc(all, func(a, z *AppMonitor) int { return cmp.Compare(a.Name, z.Name) })

	sums := make([]WireSummary, len(all))
	for i, m := range all {
		sums[i] = WireSummary{
			Created: m.Created.Format(timeLayoutRFC), ID: m.ID, LastModified: m.LastModified.Format(timeLayoutRFC),
			Name: m.Name, Platform: m.Platform, State: monitorState(m),
		}
	}

	return restroute.Paginate(sums, token, maxResults, defaultPageSize, maxPageSize)
}

// UpdateMonitorInput holds UpdateAppMonitor parameters; nil fields are unchanged.
type UpdateMonitorInput struct {
	Config       *MonitorConfig
	CustomEvents *wireCustomEvents
	SourceMaps   *wireSourceMaps
	CwLogEnabled *bool
	Name         string
	Domain       string
	DomainList   []string
}

// UpdateAppMonitor changes the supplied parts of an app monitor.
func (b *InMemoryBackend) UpdateAppMonitor(ctx context.Context, in UpdateMonitorInput) error {
	if err := validateDomains(in.Domain, in.DomainList, false); err != nil {
		return err
	}

	if err := validateConfig(in.Config); err != nil {
		return err
	}

	custom := ""

	if in.CustomEvents != nil {
		c, err := validateCustomEvents(in.CustomEvents)
		if err != nil {
			return err
		}

		custom = c
	}

	if err := validateSourceMaps(in.SourceMaps); err != nil {
		return err
	}

	sc := b.scopeOf(ctx)

	b.mu.Lock("UpdateAppMonitor")
	defer b.mu.Unlock()

	m, err := b.monitorByNameLocked(sc, in.Name)
	if err != nil {
		return err
	}

	cp := *m
	if in.Domain != "" || len(in.DomainList) > 0 {
		cp.Domain, cp.DomainList = in.Domain, in.DomainList
	}

	if in.Config != nil {
		cp.Config = in.Config
	}

	if custom != "" {
		cp.CustomEvents = custom
	}

	if sm := sourceMapsOf(in.SourceMaps); sm != nil {
		cp.SourceMaps = sm
	}

	if in.CwLogEnabled != nil {
		cp.CwLogEnabled = *in.CwLogEnabled
	}

	if err = b.ensureLogGroupLocked(ctx, &cp); err != nil {
		return err
	}

	cp.LastModified = b.now()
	b.monitors.Put(&cp)

	return nil
}

// DeleteAppMonitor removes an app monitor with its events, destinations and metric definitions.
func (b *InMemoryBackend) DeleteAppMonitor(ctx context.Context, name string) error {
	sc := b.scopeOf(ctx)

	b.mu.Lock("DeleteAppMonitor")
	defer b.mu.Unlock()

	m, err := b.monitorByNameLocked(sc, name)
	if err != nil {
		return err
	}

	for _, e := range b.eventsByApp.Get(m.ID) {
		b.events.Delete(e.Key)
	}

	for _, d := range b.defsByApp.Get(m.ID) {
		b.definitions.Delete(d.ID)
	}

	for _, d := range b.destinations.All() {
		if d.AppID == m.ID {
			b.destinations.Delete(destinationKey(d))
		}
	}

	b.monitors.Delete(m.ID)

	return nil
}
