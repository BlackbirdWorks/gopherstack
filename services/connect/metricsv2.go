package connect

import (
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

const (
	dimAgent       = "AGENT"
	dimInitiation  = "INITIATION_METHOD"
	dimDisconnect  = "DISCONNECT_REASON"
	periodTotal    = "TOTAL"
	maxV2Filters   = 5
	maxV2Values    = 100
	maxV2Groupings = 4
	maxV2Results   = 100
	maxThreshold   = 604800
	maxServiceLvl  = 20
	maxShortSpan   = 3 * 24 * time.Hour
	maxLongSpan    = 35 * 24 * time.Hour
	cmpLT          = "LT"
	cmpLTE         = "LTE"
	notExists      = "NOT_EXISTS"
)

// FilterV2 narrows GetMetricDataV2 contacts by one key.
type FilterV2 struct {
	StringCondition *struct {
		Comparison string `json:"Comparison"`
	} `json:"StringCondition,omitempty"`
	FilterKey    string   `json:"FilterKey,omitempty"`
	FilterValues []string `json:"FilterValues,omitempty"`
}

// ThresholdV2 is the comparison of a threshold metric.
type ThresholdV2 struct {
	Comparison     string  `json:"Comparison,omitempty"`
	ThresholdValue float64 `json:"ThresholdValue"`
}

// MetricFilterV2 narrows the contacts of one metric.
type MetricFilterV2 struct {
	MetricFilterKey    string   `json:"MetricFilterKey,omitempty"`
	MetricFilterValues []string `json:"MetricFilterValues,omitempty"`
	Negate             bool     `json:"Negate,omitempty"`
}

// MetricV2 names one metric of GetMetricDataV2.
type MetricV2 struct {
	Name          string           `json:"Name,omitempty"`
	MetricID      string           `json:"MetricId,omitempty"`
	Threshold     []ThresholdV2    `json:"Threshold,omitempty"`
	MetricFilters []MetricFilterV2 `json:"MetricFilters,omitempty"`
}

// IntervalDetails selects the aggregation period.
type IntervalDetails struct {
	TimeZone       string `json:"TimeZone,omitempty"`
	IntervalPeriod string `json:"IntervalPeriod,omitempty"`
}

// GetMetricDataV2Input is the GetMetricDataV2 request.
type GetMetricDataV2Input struct {
	StartTime   *awstime.EpochTime `json:"StartTime"`
	EndTime     *awstime.EpochTime `json:"EndTime"`
	Interval    *IntervalDetails   `json:"Interval"`
	ResourceArn string             `json:"ResourceArn"`
	NextToken   string             `json:"NextToken"`
	Filters     []FilterV2         `json:"Filters"`
	Groupings   []string           `json:"Groupings"`
	Metrics     []MetricV2         `json:"Metrics"`
	MaxResults  int32              `json:"MaxResults"`
}

// MetricIntervalV2 is the window of one result.
type MetricIntervalV2 struct {
	StartTime *awstime.EpochTime `json:"StartTime"`
	EndTime   *awstime.EpochTime `json:"EndTime"`
	Interval  string             `json:"Interval"`
}

// MetricDataV2 is one metric value.
type MetricDataV2 struct {
	Metric MetricV2 `json:"Metric"`
	Value  float64  `json:"Value"`
}

// MetricResultV2 is the metrics of one group in one interval.
type MetricResultV2 struct {
	Dimensions     map[string]string `json:"Dimensions,omitempty"`
	MetricInterval *MetricIntervalV2 `json:"MetricInterval"`
	Collections    []MetricDataV2    `json:"Collections"`
}

type metricDataV2Out struct {
	NextToken     string           `json:"NextToken,omitempty"`
	MetricResults []MetricResultV2 `json:"MetricResults"`
}

// window is [from, to); the last window of a request also includes its end instant.
type window struct {
	from, to time.Time
	closed   bool
}

func (w window) has(t *awstime.EpochTime) bool {
	return t != nil && !t.Before(w.from) && (t.Before(w.to) || (w.closed && t.Equal(w.to)))
}

type v2Spec struct {
	eval          func(cs []*contactRec, w window, th *ThresholdV2) float64
	metricFilters []string
	threshold     bool
}

func v2Specs() map[string]v2Spec {
	return map[string]v2Spec{
		"CONTACTS_CREATED": {metricFilters: []string{dimInitiation}, eval: func(cs []*contactRec, w window, _ *ThresholdV2) float64 {
			return countWhere(cs, func(c *contactRec) bool { return w.has(c.Contact.InitiationTimestamp) })
		}},
		"CONTACTS_QUEUED": {eval: func(cs []*contactRec, w window, _ *ThresholdV2) float64 {
			return countWhere(cs, func(c *contactRec) bool { return w.has(enqueued(c)) })
		}},
		"CONTACTS_HANDLED": {
			metricFilters: []string{dimInitiation, dimDisconnect},
			eval: func(cs []*contactRec, w window, _ *ThresholdV2) float64 {
				return countWhere(cs, func(c *contactRec) bool { return w.has(connected(c)) })
			},
		},
		"CONTACTS_ABANDONED": {
			metricFilters: []string{dimInitiation},
			eval: func(cs []*contactRec, w window, _ *ThresholdV2) float64 {
				return countWhere(cs, func(c *contactRec) bool { return abandoned(c) && w.has(c.Contact.DisconnectTimestamp) })
			},
		},
		"ABANDONMENT_RATE": {eval: func(cs []*contactRec, w window, _ *ThresholdV2) float64 {
			queued := countWhere(cs, func(c *contactRec) bool { return w.has(enqueued(c)) })

			return percent(countWhere(cs, func(c *contactRec) bool { return w.has(enqueued(c)) && abandoned(c) }), queued)
		}},
		"AVG_HANDLE_TIME": {eval: func(cs []*contactRec, w window, _ *ThresholdV2) float64 {
			return meanOf(cs, func(c *contactRec) (float64, bool) {
				return span(connected(c), c.Contact.DisconnectTimestamp, w.has(c.Contact.DisconnectTimestamp))
			})
		}},
		"AVG_CONTACT_DURATION": {eval: func(cs []*contactRec, w window, _ *ThresholdV2) float64 {
			return meanOf(cs, func(c *contactRec) (float64, bool) {
				return span(c.Contact.InitiationTimestamp, c.Contact.DisconnectTimestamp, w.has(c.Contact.DisconnectTimestamp))
			})
		}},
		"AVG_QUEUE_ANSWER_TIME": {eval: func(cs []*contactRec, w window, _ *ThresholdV2) float64 {
			return meanOf(cs, func(c *contactRec) (float64, bool) {
				return span(enqueued(c), connected(c), w.has(connected(c)))
			})
		}},
		"AVG_ABANDON_TIME": {
			metricFilters: []string{dimInitiation},
			eval: func(cs []*contactRec, w window, _ *ThresholdV2) float64 {
				return meanOf(cs, func(c *contactRec) (float64, bool) {
					return span(enqueued(c), c.Contact.DisconnectTimestamp, abandoned(c) && w.has(c.Contact.DisconnectTimestamp))
				})
			},
		},
		"MAX_QUEUED_TIME": {eval: func(cs []*contactRec, w window, _ *ThresholdV2) float64 {
			var most float64

			for _, c := range cs {
				if v, ok := span(enqueued(c), firstNonNil(connected(c), c.Contact.DisconnectTimestamp), w.has(enqueued(c))); ok {
					most = max(most, v)
				}
			}

			return most
		}},
		"SERVICE_LEVEL": {threshold: true, eval: serviceLevel},
	}
}

func serviceLevel(cs []*contactRec, w window, th *ThresholdV2) float64 {
	queued := countWhere(cs, func(c *contactRec) bool { return w.has(enqueued(c)) })
	within := countWhere(cs, func(c *contactRec) bool {
		d, ok := span(enqueued(c), connected(c), w.has(enqueued(c)))
		if !ok {
			return false
		}

		return d < th.ThresholdValue || (th.Comparison == cmpLTE && d == th.ThresholdValue)
	})

	return percent(within, queued)
}

func enqueued(c *contactRec) *awstime.EpochTime {
	if c.Contact.QueueInfo == nil {
		return nil
	}

	return c.Contact.QueueInfo.EnqueueTimestamp
}

func connected(c *contactRec) *awstime.EpochTime {
	if c.Contact.AgentInfo == nil {
		return nil
	}

	return c.Contact.AgentInfo.ConnectedToAgentTimestamp
}

func abandoned(c *contactRec) bool {
	return enqueued(c) != nil && connected(c) == nil && c.Contact.DisconnectTimestamp != nil
}

func span(from, to *awstime.EpochTime, include bool) (float64, bool) {
	if !include || from == nil || to == nil {
		return 0, false
	}

	return to.Sub(from.Time).Seconds(), true
}

func countWhere(cs []*contactRec, pred func(*contactRec) bool) float64 {
	var n float64

	for _, c := range cs {
		if pred(c) {
			n++
		}
	}

	return n
}

func meanOf(cs []*contactRec, sample func(*contactRec) (float64, bool)) float64 {
	var sum, n float64

	for _, c := range cs {
		if v, ok := sample(c); ok {
			sum += v
			n++
		}
	}

	if n == 0 {
		return 0
	}

	return sum / n
}

func percent(part, whole float64) float64 {
	if whole == 0 {
		return 0
	}

	return part / whole * 100
}

// v2Dims are the filter keys and groupings the emulator computes from stored contacts.
var v2Dims = []string{dimQueue, dimProfile, dimAgent, dimChannel, dimInitiation, dimDisconnect} //nolint:gochecknoglobals // immutable catalogue

func idTail(v string) string {
	if _, after, ok := strings.CutLast(v, "/"); ok {
		return after
	}

	return v
}

func (b *Backend) contactDim(inst string, c *contactRec, dim string) string {
	switch dim {
	case dimQueue:
		return c.queueID()
	case dimAgent:
		return c.agentID()
	case dimProfile:
		if u, ok := b.users.table.Get(inst + "/" + c.agentID()); ok && c.agentID() != "" {
			return u.User.RoutingProfileID
		}

		return ""
	case dimChannel:
		return c.Contact.Channel
	case dimInitiation:
		return c.Contact.InitiationMethod
	default:
		return c.Contact.DisconnectReason
	}
}

func (b *Backend) dimArn(inst, dim, id string) string {
	switch dim {
	case dimQueue:
		if q, ok := b.queues.table.Get(inst + "/" + id); ok {
			return q.Queue.QueueArn
		}
	case dimProfile:
		if p, ok := b.routingProfiles.table.Get(inst + "/" + id); ok {
			return p.Profile.RoutingProfileArn
		}
	case dimAgent:
		if u, ok := b.users.table.Get(inst + "/" + id); ok {
			return u.User.Arn
		}
	}

	return ""
}

func (b *Backend) matchesFilter(inst string, c *contactRec, f FilterV2) bool {
	got := b.contactDim(inst, c, f.FilterKey)
	if f.StringCondition != nil {
		return got == ""
	}

	if f.FilterKey == dimChannel || f.FilterKey == dimInitiation || f.FilterKey == dimDisconnect {
		return slices.Contains(f.FilterValues, got)
	}

	return slices.ContainsFunc(f.FilterValues, func(v string) bool { return idTail(v) == got })
}

func (b *Backend) matchesAll(inst string, c *contactRec, fs []FilterV2) bool {
	return !slices.ContainsFunc(fs, func(f FilterV2) bool { return !b.matchesFilter(inst, c, f) })
}

func (b *Backend) matchesMetric(inst string, c *contactRec, fs []MetricFilterV2) bool {
	for _, f := range fs {
		if slices.Contains(f.MetricFilterValues, b.contactDim(inst, c, f.MetricFilterKey)) == f.Negate {
			return false
		}
	}

	return true
}

func relevant(c *contactRec, w window) bool {
	return w.has(c.Contact.InitiationTimestamp) || w.has(enqueued(c)) || w.has(connected(c)) ||
		w.has(c.Contact.DisconnectTimestamp)
}

func validatePeriod(in *GetMetricDataV2Input) (string, *time.Location, error) {
	period, zone := periodTotal, "UTC"

	if iv := in.Interval; iv != nil {
		if iv.IntervalPeriod != "" {
			period = iv.IntervalPeriod
		}

		if iv.TimeZone != "" {
			zone = iv.TimeZone
		}
	}

	if err := oneOf("Interval.IntervalPeriod", period,
		"FIFTEEN_MIN", "THIRTY_MIN", "HOUR", "DAY", "WEEK", periodTotal); err != nil {
		return "", nil, err
	}

	loc, err := time.LoadLocation(zone)
	if err != nil {
		return "", nil, invalidParamErr("Interval.TimeZone %q is not a valid time zone", zone)
	}

	limit := maxLongSpan
	if slices.Contains([]string{"FIFTEEN_MIN", "THIRTY_MIN", "HOUR"}, period) {
		limit = maxShortSpan
	}

	if in.EndTime.Sub(in.StartTime.Time) >= limit {
		return "", nil, invalidParamErr("StartTime and EndTime must differ by less than %s for %s", limit, period)
	}

	return period, loc, nil
}

func validateV2Filters(fs []FilterV2) error {
	if len(fs) == 0 || len(fs) > maxV2Filters {
		return invalidParamErr("Filters must hold 1-%d items", maxV2Filters)
	}

	scoped, values := false, 0
	seen := map[string]bool{}

	for _, f := range fs {
		if !slices.Contains(v2Dims, f.FilterKey) {
			return invalidParamErr("FilterKey %q is not supported; supported keys: %s", f.FilterKey, strings.Join(v2Dims, ", "))
		}

		if seen[f.FilterKey] {
			return invalidParamErr("FilterKey %q is repeated", f.FilterKey)
		}

		seen[f.FilterKey] = true

		if f.StringCondition != nil {
			if f.StringCondition.Comparison != notExists || len(f.FilterValues) > 0 {
				return invalidParamErr("StringCondition NOT_EXISTS needs empty FilterValues")
			}
		} else if len(f.FilterValues) == 0 {
			return invalidParamErr("FilterValues is required for %q", f.FilterKey)
		}

		if f.FilterKey != dimChannel {
			values += len(f.FilterValues)
		}

		scoped = scoped || f.FilterKey == dimQueue || f.FilterKey == dimProfile || f.FilterKey == dimAgent
	}

	if !scoped {
		return invalidParamErr("Filters needs at least one of QUEUE, ROUTING_PROFILE or AGENT")
	}

	if values > maxV2Values {
		return invalidParamErr("Filters can hold at most %d values", maxV2Values)
	}

	return nil
}

func (b *Backend) validateV2Metric(inst string, m MetricV2, specs map[string]v2Spec) error {
	if (m.Name == "") == (m.MetricID == "") {
		return invalidParamErr("a metric needs exactly one of Name and MetricId")
	}

	if m.MetricID != "" {
		if !b.metricDefs.has(inst, idTail(m.MetricID)) {
			return notFoundErr(labelMetric, m.MetricID)
		}

		return invalidParamErr("custom metric %q cannot be computed: only named AWS metrics are supported", m.MetricID)
	}

	spec, ok := specs[m.Name]
	if !ok {
		return invalidParamErr("metric %q is not supported", m.Name)
	}

	for _, f := range m.MetricFilters {
		if !slices.Contains(spec.metricFilters, f.MetricFilterKey) || len(f.MetricFilterValues) == 0 {
			return invalidParamErr("metric %q does not accept the metric filter %q", m.Name, f.MetricFilterKey)
		}
	}

	return checkThreshold(m, spec)
}

func checkThreshold(m MetricV2, spec v2Spec) error {
	if !spec.threshold {
		if len(m.Threshold) > 0 {
			return invalidParamErr("metric %q does not take a Threshold", m.Name)
		}

		return nil
	}

	if len(m.Threshold) != 1 {
		return invalidParamErr("metric %q needs exactly one Threshold", m.Name)
	}

	th := m.Threshold[0]
	if th.Comparison != cmpLT && th.Comparison != cmpLTE {
		return invalidParamErr("Threshold.Comparison must be LT or LTE")
	}

	if th.ThresholdValue < 1 || th.ThresholdValue > maxThreshold || th.ThresholdValue != float64(int64(th.ThresholdValue)) {
		return invalidParamErr("Threshold.ThresholdValue must be a whole number from 1 to %d", maxThreshold)
	}

	return nil
}

func (b *Backend) validateMetricDataV2(inst string, in *GetMetricDataV2Input) error {
	if in.StartTime == nil || in.EndTime == nil || !in.StartTime.Before(in.EndTime.Time) {
		return invalidParamErr("StartTime must be before EndTime")
	}

	if in.EndTime.After(b.now()) {
		return invalidParamErr("EndTime cannot be later than the current time")
	}

	if err := validateV2Filters(in.Filters); err != nil {
		return err
	}

	if len(in.Groupings) > maxV2Groupings {
		return invalidParamErr("Groupings can hold at most %d items", maxV2Groupings)
	}

	for _, g := range in.Groupings {
		if !slices.Contains(v2Dims, g) {
			return invalidParamErr("grouping %q is not supported; supported groupings: %s", g, strings.Join(v2Dims, ", "))
		}
	}

	if len(in.Metrics) == 0 {
		return invalidParamErr("Metrics is required")
	}

	if in.MaxResults < 0 || in.MaxResults > maxV2Results {
		return invalidParamErr("MaxResults must be 1-%d", maxV2Results)
	}

	specs, levels := v2Specs(), 0

	for _, m := range in.Metrics {
		if err := b.validateV2Metric(inst, m, specs); err != nil {
			return err
		}

		if m.Name == "SERVICE_LEVEL" {
			levels++
		}
	}

	if levels > maxServiceLvl {
		return invalidParamErr("a request can hold at most %d SERVICE_LEVEL metrics", maxServiceLvl)
	}

	return nil
}

func intervalWindows(start, end time.Time, period string, loc *time.Location) []window {
	if period == periodTotal {
		return []window{{from: start, to: end, closed: true}}
	}

	var out []window

	step := func(t time.Time) time.Time {
		switch period {
		case "FIFTEEN_MIN":
			return t.Add(15 * time.Minute)
		case "THIRTY_MIN":
			return t.Add(30 * time.Minute)
		case "HOUR":
			return t.Add(time.Hour)
		case "DAY":
			return t.In(loc).AddDate(0, 0, 1)
		default:
			return t.In(loc).AddDate(0, 0, 7)
		}
	}

	for from := start; from.Before(end); {
		to := step(from)
		if to.After(end) {
			to = end
		}

		out = append(out, window{from: from, to: to, closed: !to.Before(end)})
		from = to
	}

	return out
}

// GetMetricDataV2 derives historical contact metrics from stored contacts.
func (b *Backend) GetMetricDataV2(in *GetMetricDataV2Input) (*metricDataV2Out, error) {
	b.mu.RLock("GetMetricDataV2")
	defer b.mu.RUnlock()

	if !strings.Contains(in.ResourceArn, ":instance/") {
		return nil, invalidParamErr("ResourceArn must be an instance ARN")
	}

	r, err := b.instanceLocked(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	inst := r.Instance.ID
	if err = b.validateMetricDataV2(inst, in); err != nil {
		return nil, err
	}

	period, loc, err := validatePeriod(in)
	if err != nil {
		return nil, err
	}

	var contacts []*contactRec

	whole := window{from: in.StartTime.Time, to: in.EndTime.Time, closed: true}

	for _, c := range b.contacts.in(inst) {
		if relevant(c, whole) && b.matchesAll(inst, c, in.Filters) {
			contacts = append(contacts, c)
		}
	}

	var results []MetricResultV2

	for _, w := range intervalWindows(in.StartTime.Time, in.EndTime.Time, period, loc) {
		for _, g := range b.v2Groups(inst, contacts, in.Groupings) {
			results = append(results, b.v2Result(inst, in, w, period, g, contacts))
		}
	}

	items, next, err := paged(results, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &metricDataV2Out{MetricResults: orEmpty(items), NextToken: next}, nil
}

func (b *Backend) v2Groups(inst string, contacts []*contactRec, dims []string) [][]string {
	if len(dims) == 0 {
		return [][]string{nil}
	}

	var groups [][]string

	for _, c := range contacts {
		g := make([]string, len(dims))
		for i, d := range dims {
			g[i] = b.contactDim(inst, c, d)
		}

		if !slices.Contains(g, "") && !slices.ContainsFunc(groups, func(o []string) bool { return slices.Equal(o, g) }) {
			groups = append(groups, g)
		}
	}

	slices.SortFunc(groups, slices.Compare)

	return groups
}

func (b *Backend) v2Result(
	inst string, in *GetMetricDataV2Input, w window, period string, group []string, contacts []*contactRec,
) MetricResultV2 {
	res := MetricResultV2{
		MetricInterval: &MetricIntervalV2{
			StartTime: &awstime.EpochTime{Time: w.from}, EndTime: &awstime.EpochTime{Time: w.to}, Interval: period,
		},
		Collections: make([]MetricDataV2, 0, len(in.Metrics)),
	}

	if len(group) > 0 {
		res.Dimensions = map[string]string{}

		for i, d := range in.Groupings {
			res.Dimensions[d] = group[i]

			if arn := b.dimArn(inst, d, group[i]); arn != "" {
				res.Dimensions[d+"_ARN"] = arn
			}
		}
	}

	var inGroup []*contactRec

	for _, c := range contacts {
		match := true

		for i, d := range in.Groupings {
			match = match && group[i] == b.contactDim(inst, c, d)
		}

		if match {
			inGroup = append(inGroup, c)
		}
	}

	specs := v2Specs()

	for _, m := range in.Metrics {
		spec := specs[m.Name]

		var th *ThresholdV2
		if len(m.Threshold) > 0 {
			th = &m.Threshold[0]
		}

		sel := make([]*contactRec, 0, len(inGroup))

		for _, c := range inGroup {
			if b.matchesMetric(inst, c, m.MetricFilters) {
				sel = append(sel, c)
			}
		}

		res.Collections = append(res.Collections, MetricDataV2{Metric: m, Value: spec.eval(sel, w, th)})
	}

	return res
}
