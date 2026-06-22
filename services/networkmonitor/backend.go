package networkmonitor

import (
	"context"
	"fmt"
	"maps"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

var (
	// ErrNotFound is returned when a monitor or probe does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a monitor already exists.
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrAlreadyExists)
	// ErrValidation is returned for invalid input parameters.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

const (
	monitorStateActive  = "ACTIVE"
	monitorStatePending = "PENDING"
	monitorStateDeleted = "DELETED"

	probeStateActive  = "ACTIVE"
	probeStatePending = "PENDING"

	defaultAggregationPeriod = int64(60)
	minAggregationPeriod     = int64(30)

	networkmonitorService = "networkmonitor"

	arnColonParts  = 6
	probePathParts = 2
)

var monitorNameRE = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,200}$`)

// StorageBackend is the interface for the Network Monitor in-memory backend.
type StorageBackend interface {
	CreateMonitor(
		ctx context.Context,
		name string,
		aggregationPeriod *int64,
		probes []createMonitorProbeInput,
		tags map[string]string,
	) (*Monitor, error)
	DeleteMonitor(ctx context.Context, name string) error
	GetMonitor(ctx context.Context, name string) (*Monitor, error)
	UpdateMonitor(ctx context.Context, name string, aggregationPeriod int64) (*Monitor, error)
	ListMonitors(
		ctx context.Context,
		state, nextToken string,
		maxResults int,
	) ([]monitorSummary, string, error)
	CreateProbe(
		ctx context.Context,
		monitorName string,
		probe *probeInput,
		tags map[string]string,
	) (*Probe, error)
	DeleteProbe(ctx context.Context, monitorName, probeID string) error
	GetProbe(ctx context.Context, monitorName, probeID string) (*Probe, error)
	UpdateProbe(
		ctx context.Context,
		monitorName, probeID string,
		req *updateProbeRequest,
	) (*Probe, error)
	ListTagsForResource(ctx context.Context, resourceARN string) (map[string]string, error)
	TagResource(ctx context.Context, resourceARN string, tags map[string]string) error
	UntagResource(ctx context.Context, resourceARN string, tagKeys []string) error
}

// InMemoryBackend is the in-memory implementation of StorageBackend.
// Resources are nested by region for isolation.
type InMemoryBackend struct {
	monitors      map[string]map[string]*Monitor
	arnIndex      map[string]map[string]string
	accountID     string
	defaultRegion string
	nextProbeSeq  int64
	mu            sync.RWMutex
}

var _ StorageBackend = (*InMemoryBackend)(nil)

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(region, accountID string) *InMemoryBackend {
	return &InMemoryBackend{
		monitors:      make(map[string]map[string]*Monitor),
		arnIndex:      make(map[string]map[string]string),
		accountID:     accountID,
		defaultRegion: region,
	}
}

// Reset clears all backend state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.monitors = make(map[string]map[string]*Monitor)
	b.arnIndex = make(map[string]map[string]string)
	b.nextProbeSeq = 0
}

func (b *InMemoryBackend) regionMonitors(region string) map[string]*Monitor {
	if _, ok := b.monitors[region]; !ok {
		b.monitors[region] = make(map[string]*Monitor)
	}

	return b.monitors[region]
}

func (b *InMemoryBackend) regionARNIndex(region string) map[string]string {
	if _, ok := b.arnIndex[region]; !ok {
		b.arnIndex[region] = make(map[string]string)
	}

	return b.arnIndex[region]
}

func (b *InMemoryBackend) buildMonitorARN(region, monitorName string) string {
	return arn.Build(networkmonitorService, region, b.accountID, "monitor/"+monitorName)
}

func (b *InMemoryBackend) buildProbeARN(region, monitorName, probeID string) string {
	return arn.Build(
		networkmonitorService,
		region,
		b.accountID,
		fmt.Sprintf("probe/%s/%s", monitorName, probeID),
	)
}

func (b *InMemoryBackend) nextProbeID() string {
	b.nextProbeSeq++

	return fmt.Sprintf("probe-%08d", b.nextProbeSeq)
}

// CreateMonitor creates a new monitor.
func (b *InMemoryBackend) CreateMonitor(
	ctx context.Context,
	name string,
	aggregationPeriod *int64,
	probeInputs []createMonitorProbeInput,
	tags map[string]string,
) (*Monitor, error) {
	if err := validateMonitorName(name); err != nil {
		return nil, err
	}

	region := getRegion(ctx, b.defaultRegion)
	period := defaultAggregationPeriod

	if aggregationPeriod != nil {
		if *aggregationPeriod != 30 && *aggregationPeriod != 60 {
			return nil, fmt.Errorf("%w: aggregationPeriod must be 30 or 60", ErrValidation)
		}

		period = *aggregationPeriod
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	rm := b.regionMonitors(region)

	if _, exists := rm[name]; exists {
		return nil, fmt.Errorf("%w: monitor %q already exists", ErrAlreadyExists, name)
	}

	now := time.Now().UTC()
	monARN := b.buildMonitorARN(region, name)

	var probes []*Probe

	for _, pi := range probeInputs {
		if err := validateProbeInput(pi.Destination, pi.Protocol, pi.SourceArn, pi.DestinationPort); err != nil {
			return nil, err
		}

		probeID := b.nextProbeID()
		probeARN := b.buildProbeARN(region, name, probeID)
		af := detectAddressFamily(pi.Destination)

		probeTags := make(map[string]string, len(pi.Tags))
		maps.Copy(probeTags, pi.Tags)

		probes = append(probes, &Probe{
			ProbeID:         probeID,
			ProbeArn:        probeARN,
			SourceArn:       pi.SourceArn,
			Destination:     pi.Destination,
			Protocol:        pi.Protocol,
			DestinationPort: pi.DestinationPort,
			PacketSize:      pi.PacketSize,
			State:           probeStateActive,
			AddressFamily:   af,
			CreatedAt:       &now,
			ModifiedAt:      &now,
			Tags:            probeTags,
		})
	}

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	m := &Monitor{
		MonitorArn:        monARN,
		MonitorName:       name,
		State:             monitorStateActive,
		AggregationPeriod: period,
		Probes:            probes,
		Tags:              tagsCopy,
		CreatedAt:         &now,
		ModifiedAt:        &now,
	}

	rm[name] = m
	b.regionARNIndex(region)[monARN] = name

	return monitorCopy(m), nil
}

// DeleteMonitor deletes a monitor and its probes.
func (b *InMemoryBackend) DeleteMonitor(ctx context.Context, name string) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	rm := b.regionMonitors(region)

	m, exists := rm[name]
	if !exists {
		return fmt.Errorf("%w: monitor %q not found", ErrNotFound, name)
	}

	delete(b.regionARNIndex(region), m.MonitorArn)
	delete(rm, name)

	return nil
}

// GetMonitor returns a monitor by name.
func (b *InMemoryBackend) GetMonitor(ctx context.Context, name string) (*Monitor, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock()
	defer b.mu.RUnlock()

	rm := b.monitors[region]

	m, exists := rm[name]
	if !exists {
		return nil, fmt.Errorf("%w: monitor %q not found", ErrNotFound, name)
	}

	return monitorCopy(m), nil
}

// UpdateMonitor updates a monitor's aggregation period.
func (b *InMemoryBackend) UpdateMonitor(
	ctx context.Context,
	name string,
	aggregationPeriod int64,
) (*Monitor, error) {
	if aggregationPeriod != 30 && aggregationPeriod != 60 {
		return nil, fmt.Errorf("%w: aggregationPeriod must be 30 or 60", ErrValidation)
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	rm := b.monitors[region]

	m, exists := rm[name]
	if !exists {
		return nil, fmt.Errorf("%w: monitor %q not found", ErrNotFound, name)
	}

	now := time.Now().UTC()
	m.AggregationPeriod = aggregationPeriod
	m.ModifiedAt = &now

	return monitorCopy(m), nil
}

// ListMonitors returns a filtered, paginated list of monitor summaries.
func (b *InMemoryBackend) ListMonitors(
	ctx context.Context,
	state, nextToken string,
	maxResults int,
) ([]monitorSummary, string, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock()
	defer b.mu.RUnlock()

	rm := b.monitors[region]

	names := collections.SortedKeys(rm)

	startIdx := 0

	if nextToken != "" {
		for i, n := range names {
			if n > nextToken {
				startIdx = i

				break
			}
		}
	}

	if maxResults <= 0 || maxResults > 100 {
		maxResults = 100
	}

	var summaries []monitorSummary

	for i := startIdx; i < len(names) && len(summaries) < maxResults; i++ {
		m := rm[names[i]]
		if state != "" && !strings.EqualFold(m.State, state) {
			continue
		}

		period := m.AggregationPeriod
		s := monitorSummary{
			MonitorArn:        m.MonitorArn,
			MonitorName:       m.MonitorName,
			State:             m.State,
			AggregationPeriod: &period,
			Tags:              maps.Clone(m.Tags),
		}

		summaries = append(summaries, s)
	}

	var outToken string

	if len(summaries) == maxResults && startIdx+maxResults < len(names) {
		outToken = summaries[len(summaries)-1].MonitorName
	}

	if summaries == nil {
		summaries = []monitorSummary{}
	}

	return summaries, outToken, nil
}

// CreateProbe adds a probe to an existing monitor.
func (b *InMemoryBackend) CreateProbe(
	ctx context.Context,
	monitorName string,
	pi *probeInput,
	tags map[string]string,
) (*Probe, error) {
	if pi == nil {
		return nil, fmt.Errorf("%w: probe is required", ErrValidation)
	}

	if err := validateProbeInput(pi.Destination, pi.Protocol, pi.SourceArn, pi.DestinationPort); err != nil {
		return nil, err
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	rm := b.monitors[region]

	m, exists := rm[monitorName]
	if !exists {
		return nil, fmt.Errorf("%w: monitor %q not found", ErrNotFound, monitorName)
	}

	now := time.Now().UTC()
	probeID := b.nextProbeID()
	probeARN := b.buildProbeARN(region, monitorName, probeID)
	af := detectAddressFamily(pi.Destination)

	tagsCopy := make(map[string]string, len(pi.Tags)+len(tags))
	maps.Copy(tagsCopy, pi.Tags)
	maps.Copy(tagsCopy, tags)

	probe := &Probe{
		ProbeID:         probeID,
		ProbeArn:        probeARN,
		SourceArn:       pi.SourceArn,
		Destination:     pi.Destination,
		Protocol:        pi.Protocol,
		DestinationPort: pi.DestinationPort,
		PacketSize:      pi.PacketSize,
		State:           probeStateActive,
		AddressFamily:   af,
		CreatedAt:       &now,
		ModifiedAt:      &now,
		Tags:            tagsCopy,
	}

	m.Probes = append(m.Probes, probe)
	m.ModifiedAt = &now

	return probeCopy(probe), nil
}

// DeleteProbe removes a probe from a monitor.
func (b *InMemoryBackend) DeleteProbe(ctx context.Context, monitorName, probeID string) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	rm := b.monitors[region]

	m, exists := rm[monitorName]
	if !exists {
		return fmt.Errorf("%w: monitor %q not found", ErrNotFound, monitorName)
	}

	idx := findProbeIndex(m.Probes, probeID)
	if idx < 0 {
		return fmt.Errorf("%w: probe %q not found in monitor %q", ErrNotFound, probeID, monitorName)
	}

	now := time.Now().UTC()
	m.Probes = append(m.Probes[:idx], m.Probes[idx+1:]...)
	m.ModifiedAt = &now

	return nil
}

// GetProbe returns a probe from a monitor.
func (b *InMemoryBackend) GetProbe(
	ctx context.Context,
	monitorName, probeID string,
) (*Probe, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock()
	defer b.mu.RUnlock()

	rm := b.monitors[region]

	m, exists := rm[monitorName]
	if !exists {
		return nil, fmt.Errorf("%w: monitor %q not found", ErrNotFound, monitorName)
	}

	idx := findProbeIndex(m.Probes, probeID)
	if idx < 0 {
		return nil, fmt.Errorf(
			"%w: probe %q not found in monitor %q",
			ErrNotFound,
			probeID,
			monitorName,
		)
	}

	return probeCopy(m.Probes[idx]), nil
}

// UpdateProbe updates a probe's attributes.
func (b *InMemoryBackend) UpdateProbe(
	ctx context.Context,
	monitorName, probeID string,
	req *updateProbeRequest,
) (*Probe, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: update request is required", ErrValidation)
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	rm := b.monitors[region]

	m, exists := rm[monitorName]
	if !exists {
		return nil, fmt.Errorf("%w: monitor %q not found", ErrNotFound, monitorName)
	}

	idx := findProbeIndex(m.Probes, probeID)
	if idx < 0 {
		return nil, fmt.Errorf(
			"%w: probe %q not found in monitor %q",
			ErrNotFound,
			probeID,
			monitorName,
		)
	}

	probe := m.Probes[idx]
	now := time.Now().UTC()

	if req.Destination != "" {
		probe.Destination = req.Destination
		probe.AddressFamily = detectAddressFamily(req.Destination)
	}

	if req.Protocol != "" {
		probe.Protocol = req.Protocol
	}

	if req.DestinationPort != nil {
		probe.DestinationPort = req.DestinationPort
	}

	if req.PacketSize != nil {
		probe.PacketSize = req.PacketSize
	}

	if req.State != "" {
		probe.State = req.State
	}

	if req.Tags != nil {
		maps.Copy(probe.Tags, req.Tags)
	}

	probe.ModifiedAt = &now
	m.ModifiedAt = &now

	return probeCopy(probe), nil
}

// ListTagsForResource returns tags for a monitor or probe by ARN.
func (b *InMemoryBackend) ListTagsForResource(
	ctx context.Context,
	resourceARN string,
) (map[string]string, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.lookupTagsByARN(region, resourceARN)
}

// TagResource adds or updates tags on a monitor or probe.
func (b *InMemoryBackend) TagResource(
	ctx context.Context,
	resourceARN string,
	tags map[string]string,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	m, probe, err := b.findResourceByARN(region, resourceARN)
	if err != nil {
		return err
	}

	if probe != nil {
		if probe.Tags == nil {
			probe.Tags = make(map[string]string)
		}

		maps.Copy(probe.Tags, tags)
	} else if m != nil {
		if m.Tags == nil {
			m.Tags = make(map[string]string)
		}

		maps.Copy(m.Tags, tags)
	}

	return nil
}

// UntagResource removes tags from a monitor or probe.
func (b *InMemoryBackend) UntagResource(
	ctx context.Context,
	resourceARN string,
	tagKeys []string,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	m, probe, err := b.findResourceByARN(region, resourceARN)
	if err != nil {
		return err
	}

	if probe != nil {
		for _, k := range tagKeys {
			delete(probe.Tags, k)
		}
	} else if m != nil {
		for _, k := range tagKeys {
			delete(m.Tags, k)
		}
	}

	return nil
}

func (b *InMemoryBackend) lookupTagsByARN(region, resourceARN string) (map[string]string, error) {
	m, probe, err := b.findResourceByARN(region, resourceARN)
	if err != nil {
		return nil, err
	}

	if probe != nil {
		return maps.Clone(probe.Tags), nil
	}

	return maps.Clone(m.Tags), nil
}

// findResourceByARN resolves an ARN to either a monitor or a probe (not both).
// Must be called with b.mu held (read or write).
func (b *InMemoryBackend) findResourceByARN(region, resourceARN string) (*Monitor, *Probe, error) {
	// ARN formats:
	//   monitor: arn:aws:networkmonitor:{region}:{acct}:monitor/{name}
	//   probe:   arn:aws:networkmonitor:{region}:{acct}:probe/{monitorName}/{probeId}
	parts := strings.SplitN(resourceARN, ":", arnColonParts)
	if len(parts) < arnColonParts {
		return nil, nil, fmt.Errorf("%w: invalid resource ARN", ErrNotFound)
	}

	resource := parts[arnColonParts-1]
	rm := b.monitors[region]

	if monitorName, ok := strings.CutPrefix(resource, "monitor/"); ok {
		m, exists := rm[monitorName]
		if !exists {
			return nil, nil, fmt.Errorf("%w: resource %q not found", ErrNotFound, resourceARN)
		}

		return m, nil, nil
	}

	if rest, ok := strings.CutPrefix(resource, "probe/"); ok {
		segments := strings.SplitN(rest, "/", probePathParts)
		if len(segments) != probePathParts {
			return nil, nil, fmt.Errorf("%w: invalid probe ARN", ErrNotFound)
		}

		monitorName, probeID := segments[0], segments[1]

		m, exists := rm[monitorName]
		if !exists {
			return nil, nil, fmt.Errorf("%w: resource %q not found", ErrNotFound, resourceARN)
		}

		idx := findProbeIndex(m.Probes, probeID)
		if idx < 0 {
			return nil, nil, fmt.Errorf("%w: resource %q not found", ErrNotFound, resourceARN)
		}

		return nil, m.Probes[idx], nil
	}

	return nil, nil, fmt.Errorf("%w: resource %q not found", ErrNotFound, resourceARN)
}

func findProbeIndex(probes []*Probe, probeID string) int {
	for i, p := range probes {
		if p.ProbeID == probeID {
			return i
		}
	}

	return -1
}

func validateMonitorName(name string) error {
	if !monitorNameRE.MatchString(name) {
		return fmt.Errorf("%w: monitorName must match [a-zA-Z0-9_-]{1,200}", ErrValidation)
	}

	return nil
}

func validateProbeInput(destination, protocol, sourceARN string, destPort *int32) error {
	if destination == "" {
		return fmt.Errorf("%w: probe destination is required", ErrValidation)
	}

	if protocol == "" {
		return fmt.Errorf("%w: probe protocol is required", ErrValidation)
	}

	proto := strings.ToUpper(protocol)
	if proto != "TCP" && proto != "ICMP" {
		return fmt.Errorf("%w: probe protocol must be TCP or ICMP", ErrValidation)
	}

	if sourceARN == "" {
		return fmt.Errorf("%w: probe sourceArn is required", ErrValidation)
	}

	if proto == "TCP" && destPort == nil {
		return fmt.Errorf("%w: destinationPort is required when protocol is TCP", ErrValidation)
	}

	if destPort != nil && (*destPort < 1 || *destPort > 65535) {
		return fmt.Errorf("%w: destinationPort must be between 1 and 65535", ErrValidation)
	}

	return nil
}

// detectAddressFamily returns "IPV4" or "IPV6" based on destination format.
func detectAddressFamily(destination string) string {
	if strings.Contains(destination, ":") {
		return "IPV6"
	}

	return "IPV4"
}

func monitorCopy(m *Monitor) *Monitor {
	if m == nil {
		return nil
	}

	cp := *m
	cp.Tags = maps.Clone(m.Tags)

	if m.Probes != nil {
		cp.Probes = make([]*Probe, len(m.Probes))
		for i, p := range m.Probes {
			cp.Probes[i] = probeCopy(p)
		}
	}

	if m.CreatedAt != nil {
		t := *m.CreatedAt
		cp.CreatedAt = &t
	}

	if m.ModifiedAt != nil {
		t := *m.ModifiedAt
		cp.ModifiedAt = &t
	}

	return &cp
}

func probeCopy(p *Probe) *Probe {
	if p == nil {
		return nil
	}

	cp := *p
	cp.Tags = maps.Clone(p.Tags)

	if p.CreatedAt != nil {
		t := *p.CreatedAt
		cp.CreatedAt = &t
	}

	if p.ModifiedAt != nil {
		t := *p.ModifiedAt
		cp.ModifiedAt = &t
	}

	if p.DestinationPort != nil {
		v := *p.DestinationPort
		cp.DestinationPort = &v
	}

	if p.PacketSize != nil {
		v := *p.PacketSize
		cp.PacketSize = &v
	}

	return &cp
}
