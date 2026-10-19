package connect

import (
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

const (
	metricInQueue = "CONTACTS_IN_QUEUE"
	metricQueued  = "CONTACTS_QUEUED"
	metricHandled = "CONTACTS_HANDLED"
	statSum       = "SUM"
	unitCount     = "COUNT"
	unitSeconds   = "SECONDS"
	dimQueue      = "QUEUE"
	dimChannel    = "CHANNEL"
	dimProfile    = "ROUTING_PROFILE"
	dimStatus     = "AGENT_STATUS"
	maxMetricPage = 100
)

// MetricFilters narrows metric data; unsupported filters are rejected.
type MetricFilters struct {
	Queues          []string `json:"Queues"`
	Channels        []string `json:"Channels"`
	RoutingProfiles []string `json:"RoutingProfiles"`
	AgentStatuses   []string `json:"AgentStatuses"`
	Other           []string `json:"-"`
	Unsupported     []string `json:"RoutingStepExpressions"`
	Subtypes        []string `json:"Subtypes"`
	ValidationTests []string `json:"ValidationTestTypes"`
}

// CurrentMetric names one real-time metric.
type CurrentMetric struct {
	Name string `json:"Name"`
	Unit string `json:"Unit"`
}

// CurrentMetricSort orders metric results.
type CurrentMetricSort struct {
	SortByMetric string `json:"SortByMetric"`
	SortOrder    string `json:"SortOrder"`
}

// GetCurrentMetricDataInput is the GetCurrentMetricData request.
type GetCurrentMetricDataInput struct {
	Filters   *MetricFilters
	NextToken string
	instPath
	CurrentMetrics []CurrentMetric
	Groupings      []string
	SortCriteria   []CurrentMetricSort
	MaxResults     int32
}

// Reference names a resource by ID and ARN.
type Reference struct {
	Arn string `json:"Arn,omitempty"`
	ID  string `json:"Id,omitempty"`
}

// Dimensions identifies one metric group.
type Dimensions struct {
	Queue          *Reference `json:"Queue,omitempty"`
	RoutingProfile *Reference `json:"RoutingProfile,omitempty"`
	AgentStatus    *Reference `json:"AgentStatus,omitempty"`
	Channel        string     `json:"Channel,omitempty"`
}

// CurrentMetricData is one real-time metric value.
type CurrentMetricData struct {
	Metric CurrentMetric `json:"Metric"`
	Value  float64       `json:"Value"`
}

// CurrentMetricResult is the metrics of one group.
type CurrentMetricResult struct {
	Dimensions  *Dimensions         `json:"Dimensions,omitempty"`
	Collections []CurrentMetricData `json:"Collections"`
}

var currentMetricUnits = map[string]string{ //nolint:gochecknoglobals // immutable metric catalogue
	metricInQueue: unitCount, metricOldest: unitSeconds, metricScheduled: unitCount,
	"AGENTS_ONLINE": unitCount, "AGENTS_AVAILABLE": unitCount, "AGENTS_ON_CALL": unitCount, "AGENTS_STAFFED": unitCount,
	"AGENTS_ON_CONTACT": unitCount, "SLOTS_ACTIVE": unitCount, "SLOTS_AVAILABLE": unitCount,
}

func isContactMetric(name string) bool {
	return name == metricInQueue || name == metricOldest || name == metricScheduled
}

func (f *MetricFilters) validate(requireScope bool) error {
	if f == nil {
		return invalidParamErr("Filters is required")
	}

	if len(f.Unsupported)+len(f.Subtypes)+len(f.ValidationTests) > 0 {
		return invalidParamErr("RoutingStepExpressions, Subtypes and ValidationTestTypes filters are not supported")
	}

	if requireScope && len(f.Queues) == 0 && len(f.RoutingProfiles) == 0 {
		return invalidParamErr("Filters needs Queues or RoutingProfiles")
	}

	for _, c := range f.Channels {
		if err := oneOf("Filters.Channels", c, channels()...); err != nil {
			return err
		}
	}

	return nil
}

// agentRow is a user together with the data metrics need.
type agentRow struct {
	user    *userRec
	profile *routingProfileRec
	status  *agentStatusRec
	active  []*contactRec
}

func (b *Backend) agentRows(inst string) []agentRow {
	var offline *agentStatusRec

	for _, s := range b.agentStatuses.in(inst) {
		if s.Status.Type == "OFFLINE" {
			offline = s
		}
	}

	users := b.users.in(inst)
	rows := make([]agentRow, 0, len(users))

	for _, u := range users {
		row := agentRow{user: u}
		row.profile, _ = b.routingProfiles.table.Get(inst + "/" + u.User.RoutingProfileID)

		row.status = offline
		if s, ok := b.agentStatuses.table.Get(inst + "/" + u.AgentStatusID); ok {
			row.status = s
		}

		for _, c := range b.contacts.in(inst) {
			if c.active() && c.agentID() == u.User.ID {
				row.active = append(row.active, c)
			}
		}

		rows = append(rows, row)
	}

	return rows
}

func (a agentRow) queueIDs() []string {
	if a.profile == nil {
		return nil
	}

	return queueIDsOf(a.profile.Queues)
}

func (a agentRow) inAnyQueue(want []string) bool {
	return slices.ContainsFunc(a.queueIDs(), func(q string) bool { return slices.Contains(want, q) })
}

func (a agentRow) maxSlots(channel string) int32 {
	if a.profile == nil {
		return 0
	}

	for _, m := range a.profile.Profile.MediaConcurrencies {
		if m.Channel == channel {
			return m.Concurrency
		}
	}

	return 0
}

func (a agentRow) activeIn(channel string) int32 {
	var n int32

	for _, c := range a.active {
		if c.Contact.Channel == channel {
			n++
		}
	}

	return n
}

func (a agentRow) routable() bool { return a.status != nil && a.status.Status.Type == "ROUTABLE" }

func (a agentRow) online() bool { return a.status != nil && a.status.Status.Type != "OFFLINE" }

// group is one combination of requested dimensions.
type group struct {
	queue, channel, profile, status string
}

func (b *Backend) metricGroups(inst string, f *MetricFilters, dims []string) []group {
	groups := []group{{}}

	for _, d := range dims {
		values, set := b.dimensionValues(inst, f, d)

		next := make([]group, 0, len(groups)*len(values))

		for _, g := range groups {
			for _, v := range values {
				ng := g
				set(&ng, v)
				next = append(next, ng)
			}
		}

		groups = next
	}

	return groups
}

func (b *Backend) dimensionValues(inst string, f *MetricFilters, dim string) ([]string, func(*group, string)) {
	switch dim {
	case dimQueue:
		return b.scopeQueues(inst, f), func(g *group, v string) { g.queue = v }
	case dimChannel:
		if len(f.Channels) > 0 {
			return f.Channels, func(g *group, v string) { g.channel = v }
		}

		return channels()[:3], func(g *group, v string) { g.channel = v }
	case dimProfile:
		vals := f.RoutingProfiles
		if len(vals) == 0 {
			for _, p := range b.routingProfiles.in(inst) {
				vals = append(vals, p.Profile.RoutingProfileID)
			}
		}

		return vals, func(g *group, v string) { g.profile = v }
	default:
		vals := f.AgentStatuses
		if len(vals) == 0 {
			for _, s := range b.agentStatuses.in(inst) {
				vals = append(vals, s.Status.AgentStatusID)
			}
		}

		return vals, func(g *group, v string) { g.status = v }
	}
}

func (b *Backend) scopeQueues(inst string, f *MetricFilters) []string {
	queues := slices.Clone(f.Queues)

	for _, p := range f.RoutingProfiles {
		if rp, ok := b.routingProfiles.table.Get(inst + "/" + p); ok {
			for _, q := range queueIDsOf(rp.Queues) {
				if !slices.Contains(queues, q) {
					queues = append(queues, q)
				}
			}
		}
	}

	if len(queues) == 0 {
		for _, q := range b.queues.in(inst) {
			queues = append(queues, q.Queue.QueueID)
		}
	}

	return queues
}

func (g group) dimensions(b *Backend, inst string) *Dimensions {
	if g == (group{}) {
		return nil
	}

	d := &Dimensions{Channel: g.channel}

	if q, ok := b.queues.table.Get(inst + "/" + g.queue); ok && g.queue != "" {
		d.Queue = &Reference{ID: g.queue, Arn: q.Queue.QueueArn}
	}

	if p, ok := b.routingProfiles.table.Get(inst + "/" + g.profile); ok && g.profile != "" {
		d.RoutingProfile = &Reference{ID: g.profile, Arn: p.Profile.RoutingProfileArn}
	}

	if s, ok := b.agentStatuses.table.Get(inst + "/" + g.status); ok && g.status != "" {
		d.AgentStatus = &Reference{ID: g.status, Arn: s.Status.AgentStatusARN}
	}

	return d
}

func (b *Backend) currentValue(
	inst, metric string,
	g group,
	f *MetricFilters,
	contacts []*contactRec,
	agents []agentRow,
) float64 {
	if isContactMetric(metric) {
		return b.contactMetric(inst, metric, g, f, contacts)
	}

	return agentValue(metric, g, f, agents)
}

func (b *Backend) contactMetric(inst, metric string, g group, f *MetricFilters, contacts []*contactRec) float64 {
	now, scope := b.now(), b.scopeQueues(inst, f)

	var n float64

	for _, c := range contacts {
		if contactInGroup(c, g, f) {
			n = accumulateContact(metric, n, c, scope, now)
		}
	}

	return n
}

func contactInGroup(c *contactRec, g group, f *MetricFilters) bool {
	switch {
	case !c.active():
		return false
	case g.queue != "" && c.queueID() != g.queue:
		return false
	case g.channel != "" && c.Contact.Channel != g.channel:
		return false
	case len(f.Channels) > 0 && !slices.Contains(f.Channels, c.Contact.Channel):
		return false
	}

	return true
}

func accumulateContact(metric string, n float64, c *contactRec, scope []string, now time.Time) float64 {
	waiting := c.queueID() != "" && c.agentID() == "" && slices.Contains(scope, c.queueID())

	switch metric {
	case metricInQueue:
		if waiting {
			return n + 1
		}
	case metricScheduled:
		if t := c.Contact.ScheduledTimestamp; t != nil && t.After(now) {
			return n + 1
		}
	case metricOldest:
		if q := c.Contact.QueueInfo; waiting && q != nil && q.EnqueueTimestamp != nil {
			return max(n, now.Sub(q.EnqueueTimestamp.Time).Seconds())
		}
	}

	return n
}

func agentValue(metric string, g group, f *MetricFilters, agents []agentRow) float64 {
	var n float64

	for _, a := range agents {
		if agentMatches(a, g, f) {
			n += agentContribution(metric, g.channel, a)
		}
	}

	return n
}

func agentMatches(a agentRow, g group, f *MetricFilters) bool {
	queues, statusID := a.queueIDs(), ""
	if a.status != nil {
		statusID = a.status.Status.AgentStatusID
	}

	switch {
	case g.queue != "" && !slices.Contains(queues, g.queue):
		return false
	case g.profile != "" && a.user.User.RoutingProfileID != g.profile:
		return false
	case g.status != "" && statusID != g.status:
		return false
	case len(f.RoutingProfiles) > 0 && !slices.Contains(f.RoutingProfiles, a.user.User.RoutingProfileID):
		return false
	case len(f.Queues) > 0 && !a.inAnyQueue(f.Queues):
		return false
	case len(f.AgentStatuses) > 0 && !slices.Contains(f.AgentStatuses, statusID):
		return false
	}

	return true
}

func agentContribution(metric, channel string, a agentRow) float64 {
	chans := channels()[:3]
	if channel != "" {
		chans = []string{channel}
	}

	switch metric {
	case "AGENTS_ONLINE", "AGENTS_STAFFED":
		return boolFloat(a.online() && hasChannel(a, chans))
	case "AGENTS_AVAILABLE":
		return boolFloat(a.routable() && hasChannel(a, chans) && totalActive(a, chans) == 0)
	case "AGENTS_ON_CALL":
		return boolFloat(contains(chans, "VOICE") && a.activeIn("VOICE") > 0)
	case "AGENTS_ON_CONTACT":
		return boolFloat(totalActive(a, chans) > 0)
	case "SLOTS_ACTIVE":
		return float64(totalActive(a, chans))
	case "SLOTS_AVAILABLE":
		if !a.routable() {
			return 0
		}

		var free int32

		for _, c := range chans {
			free += max(a.maxSlots(c)-a.activeIn(c), 0)
		}

		return float64(free)
	}

	return 0
}

func hasChannel(a agentRow, chans []string) bool {
	return slices.ContainsFunc(chans, func(c string) bool { return a.maxSlots(c) > 0 })
}

func totalActive(a agentRow, chans []string) int32 {
	var n int32
	for _, c := range chans {
		n += a.activeIn(c)
	}

	return n
}

func contains(s []string, v string) bool { return slices.Contains(s, v) }

func boolFloat(v bool) float64 {
	if v {
		return 1
	}

	return 0
}

// GetCurrentMetricData derives real-time contact and agent metrics from stored state.
func (b *Backend) GetCurrentMetricData(in *GetCurrentMetricDataInput) (*currentMetricsOut, error) {
	if err := validateCurrentMetrics(in); err != nil {
		return nil, err
	}

	b.mu.RLock("GetCurrentMetricData")
	defer b.mu.RUnlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	inst := r.Instance.ID
	contacts, agents := b.contacts.in(inst), b.agentRows(inst)

	var results []CurrentMetricResult

	for _, g := range b.metricGroups(inst, in.Filters, in.Groupings) {
		res := CurrentMetricResult{Dimensions: g.dimensions(b, inst), Collections: []CurrentMetricData{}}
		for _, m := range in.CurrentMetrics {
			res.Collections = append(
				res.Collections,
				CurrentMetricData{Metric: m, Value: b.currentValue(inst, m.Name, g, in.Filters, contacts, agents)},
			)
		}

		results = append(results, res)
	}

	sortMetricResults(results, in.SortCriteria)

	items, next, err := paged(results, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &currentMetricsOut{
		MetricResults: orEmpty(items), NextToken: next, ApproximateTotalCount: int64(len(results)),
		DataSnapshotTime: awstime.NewEpochTime(b.now()),
	}, nil
}

func validateCurrentMetrics(in *GetCurrentMetricDataInput) error {
	if err := in.Filters.validate(true); err != nil {
		return err
	}

	if len(in.CurrentMetrics) == 0 {
		return invalidParamErr("CurrentMetrics is required")
	}

	for _, d := range in.Groupings {
		if err := oneOf("Groupings", d, dimQueue, dimChannel, dimProfile, dimStatus); err != nil {
			return err
		}
	}

	for _, m := range in.CurrentMetrics {
		unit, ok := currentMetricUnits[m.Name]
		if !ok {
			return invalidParamErr("metric %q is not supported", m.Name)
		}

		if m.Unit != unit {
			return invalidParamErr("metric %q has unit %s", m.Name, unit)
		}

		if isContactMetric(m.Name) &&
			(slices.Contains(in.Groupings, dimProfile) || slices.Contains(in.Groupings, dimStatus)) {
			return invalidParamErr("metric %q cannot be grouped by routing profile or agent status", m.Name)
		}
	}

	return nil
}

func sortMetricResults(results []CurrentMetricResult, criteria []CurrentMetricSort) {
	if len(criteria) == 0 {
		return
	}

	c := criteria[0]
	val := func(r CurrentMetricResult) float64 {
		for _, col := range r.Collections {
			if col.Metric.Name == c.SortByMetric {
				return col.Value
			}
		}

		return 0
	}

	slices.SortStableFunc(results, func(a, b CurrentMetricResult) int {
		d := 0

		switch {
		case val(a) < val(b):
			d = -1
		case val(a) > val(b):
			d = 1
		}

		if strings.EqualFold(c.SortOrder, "DESCENDING") {
			return -d
		}

		return d
	})
}

// HistoricalMetric names one historical metric and statistic.
type HistoricalMetric struct {
	Name      string          `json:"Name"`
	Statistic string          `json:"Statistic"`
	Unit      string          `json:"Unit"`
	Threshold json.RawMessage `json:"Threshold,omitempty"`
}

// GetMetricDataInput is the GetMetricData request.
type GetMetricDataInput struct {
	StartTime *awstime.EpochTime
	EndTime   *awstime.EpochTime
	Filters   *MetricFilters
	NextToken string
	instPath
	HistoricalMetrics []HistoricalMetric
	Groupings         []string
	MaxResults        int32
}

// HistoricalMetricData is one historical metric value.
type HistoricalMetricData struct {
	Metric HistoricalMetric `json:"Metric"`
	Value  float64          `json:"Value"`
}

// HistoricalMetricResult is the metrics of one group.
type HistoricalMetricResult struct {
	Dimensions  *Dimensions            `json:"Dimensions,omitempty"`
	Collections []HistoricalMetricData `json:"Collections"`
}

type historicalSpec struct {
	unit       string
	statistics []string
}

var historicalMetrics = map[string]historicalSpec{ //nolint:gochecknoglobals // immutable metric catalogue
	metricQueued: {unitCount, []string{statSum}}, metricHandled: {unitCount, []string{statSum}},
	"CONTACTS_ABANDONED": {unitCount, []string{statSum}}, "CONTACTS_HANDLED_OUTBOUND": {unitCount, []string{statSum}},
	"API_CONTACTS_HANDLED": {unitCount, []string{statSum}}, "QUEUED_TIME": {unitSeconds, []string{"AVG", "MAX"}},
}

func (b *Backend) historicalValue(
	m HistoricalMetric,
	g group,
	f *MetricFilters,
	contacts []*contactRec,
	from, to time.Time,
) float64 {
	var vals []float64

	for _, c := range contacts {
		if !historicalInGroup(c, g, f) {
			continue
		}

		if v, ok := historicalSample(m.Name, c, from, to); ok {
			vals = append(vals, v)
		}
	}

	return aggregate(m.Statistic, vals)
}

func historicalInGroup(c *contactRec, g group, f *MetricFilters) bool {
	switch {
	case c.queueID() == "":
		return false
	case g.queue != "" && c.queueID() != g.queue, g.channel != "" && c.Contact.Channel != g.channel:
		return false
	case len(f.Queues) > 0 && !slices.Contains(f.Queues, c.queueID()):
		return false
	case len(f.Channels) > 0 && !slices.Contains(f.Channels, c.Contact.Channel):
		return false
	}

	return true
}

// historicalSample returns the contribution of one contact to a metric within [from, to).
func historicalSample(name string, c *contactRec, from, to time.Time) (float64, bool) {
	in := func(t *awstime.EpochTime) bool { return t != nil && !t.Before(from) && t.Before(to) }

	enq := c.Contact.QueueInfo.EnqueueTimestamp

	var conn *awstime.EpochTime
	if c.Contact.AgentInfo != nil {
		conn = c.Contact.AgentInfo.ConnectedToAgentTimestamp
	}

	switch name {
	case metricQueued:
		return 1, in(enq)
	case metricHandled:
		return 1, in(conn)
	case "CONTACTS_HANDLED_OUTBOUND":
		return 1, in(conn) && c.Contact.InitiationMethod == "OUTBOUND"
	case "API_CONTACTS_HANDLED":
		return 1, in(conn) && c.Contact.InitiationMethod == "API"
	case "CONTACTS_ABANDONED":
		return 1, conn == nil && in(c.Contact.DisconnectTimestamp)
	case "QUEUED_TIME":
		if end := firstNonNil(conn, c.Contact.DisconnectTimestamp); in(enq) && end != nil {
			return end.Sub(enq.Time).Seconds(), true
		}
	}

	return 0, false
}

func firstNonNil(ts ...*awstime.EpochTime) *awstime.EpochTime {
	for _, t := range ts {
		if t != nil {
			return t
		}
	}

	return nil
}

func aggregate(stat string, vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}

	var sum, maxV float64

	for _, v := range vals {
		sum += v
		maxV = max(maxV, v)
	}

	switch stat {
	case "AVG":
		return sum / float64(len(vals))
	case "MAX":
		return maxV
	default:
		return sum
	}
}

// GetMetricData derives historical contact metrics from stored contacts.
func (b *Backend) GetMetricData(in *GetMetricDataInput) (*historicalMetricsOut, error) {
	if err := validateHistoricalMetrics(in); err != nil {
		return nil, err
	}

	b.mu.RLock("GetMetricData")
	defer b.mu.RUnlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	inst := r.Instance.ID
	contacts := b.contacts.in(inst)

	var results []HistoricalMetricResult

	for _, g := range b.metricGroups(inst, in.Filters, in.Groupings) {
		res := HistoricalMetricResult{Dimensions: g.dimensions(b, inst), Collections: []HistoricalMetricData{}}
		for _, m := range in.HistoricalMetrics {
			res.Collections = append(res.Collections, HistoricalMetricData{
				Metric: m, Value: b.historicalValue(m, g, in.Filters, contacts, in.StartTime.Time, in.EndTime.Time),
			})
		}

		results = append(results, res)
	}

	items, next, err := paged(results, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &historicalMetricsOut{MetricResults: orEmpty(items), NextToken: next}, nil
}

func validateHistoricalMetrics(in *GetMetricDataInput) error {
	if err := in.Filters.validate(true); err != nil {
		return err
	}

	if in.StartTime == nil || in.EndTime == nil || !in.StartTime.Before(in.EndTime.Time) {
		return invalidParamErr("StartTime must be before EndTime")
	}

	if len(in.HistoricalMetrics) == 0 {
		return invalidParamErr("HistoricalMetrics is required")
	}

	for _, d := range in.Groupings {
		if err := oneOf("Groupings", d, dimQueue, dimChannel); err != nil {
			return err
		}
	}

	for _, m := range in.HistoricalMetrics {
		spec, ok := historicalMetrics[m.Name]
		if !ok {
			return invalidParamErr("metric %q is not supported", m.Name)
		}

		if m.Unit != spec.unit || !slices.Contains(spec.statistics, m.Statistic) {
			return invalidParamErr(
				"metric %q needs unit %s and statistic %s",
				m.Name,
				spec.unit,
				strings.Join(spec.statistics, " or "),
			)
		}
	}

	return nil
}

// UserDataFilters selects the agents of GetCurrentUserData.
type UserDataFilters struct {
	ContactFilter *struct {
		ContactStates []string `json:"ContactStates"`
	} `json:"ContactFilter"`
	Agents              []string `json:"Agents"`
	Queues              []string `json:"Queues"`
	RoutingProfiles     []string `json:"RoutingProfiles"`
	UserHierarchyGroups []string `json:"UserHierarchyGroups"`
}

// GetCurrentUserDataInput is the GetCurrentUserData request.
type GetCurrentUserDataInput struct {
	Filters   *UserDataFilters
	NextToken string
	instPath
	MaxResults int32
}

// AgentContactReference is a contact an agent is handling.
type AgentContactReference struct {
	ConnectedToAgentTimestamp *awstime.EpochTime `json:"ConnectedToAgentTimestamp,omitempty"`
	StateStartTimestamp       *awstime.EpochTime `json:"StateStartTimestamp,omitempty"`
	Queue                     *Reference         `json:"Queue,omitempty"`
	AgentContactState         string             `json:"AgentContactState"`
	Channel                   string             `json:"Channel"`
	ContactID                 string             `json:"ContactId"`
	InitiationMethod          string             `json:"InitiationMethod"`
}

// AgentStatusReference is an agent's current status.
type AgentStatusReference struct {
	StatusStartTimestamp *awstime.EpochTime `json:"StatusStartTimestamp,omitempty"`
	StatusArn            string             `json:"StatusArn"`
	StatusName           string             `json:"StatusName"`
}

// UserData is the real-time view of one agent.
type UserData struct {
	User                    *Reference
	RoutingProfile          *Reference
	Status                  *AgentStatusReference
	HierarchyPath           *hierarchyPathRef
	ActiveSlotsByChannel    map[string]int32
	AvailableSlotsByChannel map[string]int32
	MaxSlotsByChannel       map[string]int32
	Contacts                []AgentContactReference
}

type hierarchyPathRef struct {
	LevelOne   *Reference `json:"LevelOne,omitempty"`
	LevelTwo   *Reference `json:"LevelTwo,omitempty"`
	LevelThree *Reference `json:"LevelThree,omitempty"`
	LevelFour  *Reference `json:"LevelFour,omitempty"`
	LevelFive  *Reference `json:"LevelFive,omitempty"`
}

func (b *Backend) userSelected(inst string, f *UserDataFilters, a agentRow) bool {
	u := a.user.User

	switch {
	case len(f.Agents) > 0 && !slices.Contains(f.Agents, u.ID):
		return false
	case len(f.RoutingProfiles) > 0 && !slices.Contains(f.RoutingProfiles, u.RoutingProfileID):
		return false
	case len(f.Queues) > 0 && !a.inAnyQueue(f.Queues):
		return false
	case len(f.UserHierarchyGroups) > 0 && !b.inGroups(inst, u.HierarchyGroupID, f.UserHierarchyGroups):
		return false
	}

	return true
}

func (b *Backend) inGroups(inst, groupID string, want []string) bool {
	for id := groupID; id != ""; {
		if slices.Contains(want, id) {
			return true
		}

		g, ok := b.hierarchyGroups.table.Get(inst + "/" + id)
		if !ok {
			return false
		}

		id = g.ParentID
	}

	return false
}

// GetCurrentUserData returns the real-time state of agents: status, slots and active contacts.
func (b *Backend) GetCurrentUserData(in *GetCurrentUserDataInput) (*userDataOut, error) {
	f := in.Filters
	if f == nil || len(f.Agents)+len(f.Queues)+len(f.RoutingProfiles)+len(f.UserHierarchyGroups) == 0 {
		return nil, invalidParamErr(
			"Filters needs at least one of Agents, Queues, RoutingProfiles or UserHierarchyGroups",
		)
	}

	showContacts := true

	if f.ContactFilter != nil && len(f.ContactFilter.ContactStates) > 0 {
		showContacts = slices.Contains(f.ContactFilter.ContactStates, "CONNECTED")
	}

	b.mu.RLock("GetCurrentUserData")

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		b.mu.RUnlock()

		return nil, err
	}

	inst := r.Instance.ID

	var all []UserData

	for _, a := range b.agentRows(inst) {
		if b.userSelected(inst, f, a) {
			all = append(all, b.userData(inst, a, showContacts))
		}
	}

	b.mu.RUnlock()

	items, next, err := paged(all, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &userDataOut{UserDataList: orEmpty(items), NextToken: next, ApproximateTotalCount: int64(len(all))}, nil
}

func (b *Backend) userData(inst string, a agentRow, showContacts bool) UserData {
	u := a.user.User
	d := UserData{
		User:                    &Reference{ID: u.ID, Arn: u.Arn},
		RoutingProfile:          &Reference{ID: u.RoutingProfileID},
		ActiveSlotsByChannel:    map[string]int32{},
		AvailableSlotsByChannel: map[string]int32{},
		MaxSlotsByChannel:       map[string]int32{},
		Contacts:                []AgentContactReference{},
	}

	if a.profile != nil {
		d.RoutingProfile.Arn = a.profile.Profile.RoutingProfileArn

		for _, m := range a.profile.Profile.MediaConcurrencies {
			active := a.activeIn(m.Channel)
			d.MaxSlotsByChannel[m.Channel], d.ActiveSlotsByChannel[m.Channel] = m.Concurrency, active
			d.AvailableSlotsByChannel[m.Channel] = max(m.Concurrency-active, 0)
		}
	}

	if a.status != nil {
		start := a.user.StatusStart
		if start == nil {
			start = u.LastModifiedTime
		}

		d.Status = &AgentStatusReference{
			StatusArn:            a.status.Status.AgentStatusARN,
			StatusName:           a.status.Status.Name,
			StatusStartTimestamp: start,
		}
	}

	if u.HierarchyGroupID != "" {
		if g, ok := b.hierarchyGroups.table.Get(inst + "/" + u.HierarchyGroupID); ok {
			d.HierarchyPath = refPath(b.groupPathLocked(inst, g))
		}
	}

	if showContacts {
		for _, c := range a.active {
			ref := AgentContactReference{
				ConnectedToAgentTimestamp: c.Contact.AgentInfo.ConnectedToAgentTimestamp,
				StateStartTimestamp:       c.Contact.AgentInfo.ConnectedToAgentTimestamp,
				AgentContactState:         "CONNECTED", Channel: c.Contact.Channel, ContactID: c.Contact.ID,
				InitiationMethod: c.Contact.InitiationMethod,
			}

			if q, ok := b.queues.table.Get(inst + "/" + c.queueID()); ok {
				ref.Queue = &Reference{ID: q.Queue.QueueID, Arn: q.Queue.QueueArn}
			}

			d.Contacts = append(d.Contacts, ref)
		}
	}

	return d
}

func refPath(p *HierarchyPath) *hierarchyPathRef {
	conv := func(s *HierarchyGroupSummary) *Reference {
		if s == nil {
			return nil
		}

		return &Reference{ID: s.ID, Arn: s.Arn}
	}

	return &hierarchyPathRef{
		LevelOne: conv(p.LevelOne), LevelTwo: conv(p.LevelTwo), LevelThree: conv(p.LevelThree),
		LevelFour: conv(p.LevelFour), LevelFive: conv(p.LevelFive),
	}
}
