package gcpmonitoring

import (
	"regexp"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"google.golang.org/genproto/googleapis/type/calendarperiod"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	collSLOs       = "serviceLevelObjectives"
	sloMaxGoal     = 0.9999
	maxRollingDays = 30
	day            = 24 * time.Hour
	minSLIWindow   = time.Minute
	wildcardID     = "-"
	fldGoal        = "goal"
	fldSLI         = "service_level_indicator"
	fldRolling     = "rolling_period"
	fldCalendar    = "calendar_period"
)

var (
	sloIDRe   = regexp.MustCompile(`^[a-zA-Z0-9\-_:.]+$`)
	sloNameRe = regexp.MustCompile(`^(projects/[^/]+/services/[^/]+)/serviceLevelObjectives/([^/]+)$`)
)

func cloneSLO(s *monitoringpb.ServiceLevelObjective) *monitoringpb.ServiceLevelObjective {
	out, _ := proto.Clone(s).(*monitoringpb.ServiceLevelObjective)

	return out
}

func validateGoodFilter(path, src string) error {
	if src == "" {
		return errInvalid("Field %s had an invalid value: a filter is required.", path)
	}

	if _, err := parseFilter(src); err != nil {
		return errInvalid("Field %s had an invalid value: %s", path, err.Error())
	}

	return nil
}

func validateRequestBased(path string, r *monitoringpb.RequestBasedSli) error {
	switch m := r.GetMethod().(type) {
	case *monitoringpb.RequestBasedSli_GoodTotalRatio:
		set := 0

		for _, f := range []string{
			m.GoodTotalRatio.GetGoodServiceFilter(), m.GoodTotalRatio.GetBadServiceFilter(),
			m.GoodTotalRatio.GetTotalServiceFilter(),
		} {
			if f == "" {
				continue
			}

			set++

			if err := validateGoodFilter(path+".good_total_ratio", f); err != nil {
				return err
			}
		}

		if set != 2 { //nolint:mnd // exactly two of good, bad, total
			return errInvalid("Field %s.good_total_ratio had an invalid value: exactly two of "+
				"good_service_filter, bad_service_filter and total_service_filter must be set.", path)
		}
	case *monitoringpb.RequestBasedSli_DistributionCut:
		if err := validateGoodFilter(path+".distribution_cut.distribution_filter",
			m.DistributionCut.GetDistributionFilter()); err != nil {
			return err
		}

		if m.DistributionCut.GetRange() == nil {
			return errInvalid("Field %s.distribution_cut.range had an invalid value: a range is required.", path)
		}
	default:
		return errInvalid("Field %s had an invalid value: good_total_ratio or distribution_cut is required.", path)
	}

	return nil
}

func validateBasicSli(path string, b *monitoringpb.BasicSli) error {
	switch c := b.GetSliCriteria().(type) {
	case *monitoringpb.BasicSli_Availability:
		return nil
	case *monitoringpb.BasicSli_Latency:
		if c.Latency.GetThreshold().AsDuration() <= 0 {
			return errInvalid(
				"Field %s.latency.threshold had an invalid value: a positive threshold is required.",
				path,
			)
		}

		return nil
	}

	return errInvalid("Field %s had an invalid value: availability or latency is required.", path)
}

func validateWindowsBased(path string, w *monitoringpb.WindowsBasedSli) error {
	period := w.GetWindowPeriod().AsDuration()
	if period < minSLIWindow || period > day || day%period != 0 {
		return errInvalid("Field %s.window_period had an invalid value: it must be an integer fraction of a day "+
			"and at least 60s.", path)
	}

	switch c := w.GetWindowCriterion().(type) {
	case *monitoringpb.WindowsBasedSli_GoodBadMetricFilter:
		return validateGoodFilter(path+".good_bad_metric_filter", c.GoodBadMetricFilter)
	case *monitoringpb.WindowsBasedSli_GoodTotalRatioThreshold:
		switch p := c.GoodTotalRatioThreshold.GetType().(type) {
		case *monitoringpb.WindowsBasedSli_PerformanceThreshold_Performance:
			return validateRequestBased(path+".good_total_ratio_threshold.performance", p.Performance)
		case *monitoringpb.WindowsBasedSli_PerformanceThreshold_BasicSliPerformance:
			return validateBasicSli(path+".good_total_ratio_threshold.basic_sli_performance", p.BasicSliPerformance)
		}

		return errInvalid("Field %s.good_total_ratio_threshold had an invalid value: performance or "+
			"basic_sli_performance is required.", path)
	case *monitoringpb.WindowsBasedSli_MetricMeanInRange:
		return validateMetricRange(path+".metric_mean_in_range", c.MetricMeanInRange)
	case *monitoringpb.WindowsBasedSli_MetricSumInRange:
		return validateMetricRange(path+".metric_sum_in_range", c.MetricSumInRange)
	}

	return errInvalid("Field %s had an invalid value: a window criterion is required.", path)
}

func validateMetricRange(path string, r *monitoringpb.WindowsBasedSli_MetricRange) error {
	if err := validateGoodFilter(path+".time_series", r.GetTimeSeries()); err != nil {
		return err
	}

	if r.GetRange() == nil {
		return errInvalid("Field %s.range had an invalid value: a range is required.", path)
	}

	return nil
}

func validateSLI(sli *monitoringpb.ServiceLevelIndicator) error {
	const path = "service_level_objective.service_level_indicator"

	switch t := sli.GetType().(type) {
	case *monitoringpb.ServiceLevelIndicator_BasicSli:
		return validateBasicSli(path+".basic_sli", t.BasicSli)
	case *monitoringpb.ServiceLevelIndicator_RequestBased:
		return validateRequestBased(path+".request_based", t.RequestBased)
	case *monitoringpb.ServiceLevelIndicator_WindowsBased:
		return validateWindowsBased(path+".windows_based", t.WindowsBased)
	}

	return errInvalid("Field %s had an invalid value: basic_sli, request_based or windows_based is required.", path)
}

func validateSLOPeriod(s *monitoringpb.ServiceLevelObjective) error {
	switch p := s.GetPeriod().(type) {
	case *monitoringpb.ServiceLevelObjective_RollingPeriod:
		d := p.RollingPeriod.AsDuration()
		if d <= 0 || d%day != 0 || d > maxRollingDays*day {
			return errInvalid("Field service_level_objective.rolling_period had an invalid value: it must be a " +
				"whole number of days no larger than 30.")
		}
	case *monitoringpb.ServiceLevelObjective_CalendarPeriod:
		switch p.CalendarPeriod {
		case calendarperiod.CalendarPeriod_DAY, calendarperiod.CalendarPeriod_WEEK,
			calendarperiod.CalendarPeriod_FORTNIGHT, calendarperiod.CalendarPeriod_MONTH:
		case calendarperiod.CalendarPeriod_CALENDAR_PERIOD_UNSPECIFIED, calendarperiod.CalendarPeriod_QUARTER,
			calendarperiod.CalendarPeriod_HALF, calendarperiod.CalendarPeriod_YEAR:
			return errInvalid("Field service_level_objective.calendar_period had an invalid value: "+
				"%s is not supported.", p.CalendarPeriod)
		}
	default:
		return errInvalid("Field service_level_objective.period had an invalid value: rolling_period or " +
			"calendar_period is required.")
	}

	return nil
}

func validateSLO(s *monitoringpb.ServiceLevelObjective) error {
	if s == nil {
		return errInvalid("service_level_objective is required.")
	}

	if len(s.GetDisplayName()) > maxDisplayName {
		return errInvalid("Field service_level_objective.display_name had an invalid value: at most %d characters.",
			maxDisplayName)
	}

	if s.GetGoal() <= 0 || s.GetGoal() > sloMaxGoal {
		return errInvalid("Field service_level_objective.goal had an invalid value of %v: it must satisfy "+
			"0 < goal <= 0.9999.", s.GetGoal())
	}

	if err := validateSLOPeriod(s); err != nil {
		return err
	}

	if err := validateUserLabels(s.GetUserLabels()); err != nil {
		return err
	}

	return validateSLI(s.GetServiceLevelIndicator())
}

func sloParts(name string) (string, string, error) {
	m := sloNameRe.FindStringSubmatch(name)
	if m == nil {
		return "", "", errInvalid("Invalid %s name [%s].", collSLOs, name)
	}

	if _, _, err := splitName(m[1], collServices); err != nil {
		return "", "", err
	}

	return m[1], m[2], nil
}

// CreateServiceLevelObjective stores an objective under an existing service.
func (b *InMemoryBackend) CreateServiceLevelObjective(
	req *monitoringpb.CreateServiceLevelObjectiveRequest,
) (*monitoringpb.ServiceLevelObjective, error) {
	if _, _, err := splitName(req.GetParent(), collServices); err != nil {
		return nil, err
	}

	if err := validateSLO(req.GetServiceLevelObjective()); err != nil {
		return nil, err
	}

	id := req.GetServiceLevelObjectiveId()
	if id == "" {
		if parent, nameID, err := sloParts(req.GetServiceLevelObjective().GetName()); err == nil &&
			parent == req.GetParent() {
			id = nameID
		}
	}

	if id == "" {
		id = newHexSuffix() + newHexSuffix()
	} else if !sloIDRe.MatchString(id) {
		return nil, errInvalid("Field service_level_objective_id had an invalid value of %q: it must match "+
			"^[a-zA-Z0-9-_:.]+$.", id)
	}

	s := cloneSLO(req.GetServiceLevelObjective())
	s.Name = req.GetParent() + "/" + collSLOs + "/" + id

	b.mu.Lock("CreateServiceLevelObjective")
	defer b.mu.Unlock()

	if _, ok := b.services[req.GetParent()]; !ok {
		return nil, errNotFound("Service %s does not exist.", req.GetParent())
	}

	if _, dup := b.slos[s.GetName()]; dup {
		return nil, errExists("Service level objective %s already exists.", s.GetName())
	}

	b.slos[s.GetName()] = s

	return cloneSLO(s), nil
}

// GetServiceLevelObjective returns one objective.
func (b *InMemoryBackend) GetServiceLevelObjective(name string) (*monitoringpb.ServiceLevelObjective, error) {
	if _, _, err := sloParts(name); err != nil {
		return nil, err
	}

	b.mu.RLock("GetServiceLevelObjective")
	defer b.mu.RUnlock()

	s, ok := b.slos[name]
	if !ok {
		return nil, errNotFound("Service level objective %s does not exist.", name)
	}

	return cloneSLO(s), nil
}

type sloEnv struct {
	s *monitoringpb.ServiceLevelObjective
}

func (e sloEnv) values(path string) []string {
	switch {
	case path == fldDisplayName:
		return []string{e.s.GetDisplayName()}
	case path == fldName:
		return []string{e.s.GetName()}
	case strings.HasPrefix(path, fldUserLabels+"."):
		return one(e.s.GetUserLabels(), strings.TrimPrefix(path, fldUserLabels+"."))
	}

	return nil
}

func validateSLOFilterField(field string) error {
	if field == fldDisplayName || field == fldName || strings.HasPrefix(field, fldUserLabels+".") {
		return nil
	}

	return errInvalid("Field filter had an invalid value of %q: unknown field.", field)
}

// ListServiceLevelObjectives pages the objectives of a service, or of every service of the project for "-".
func (b *InMemoryBackend) ListServiceLevelObjectives(
	req *monitoringpb.ListServiceLevelObjectivesRequest,
) ([]*monitoringpb.ServiceLevelObjective, string, error) {
	project, svc, err := splitName(req.GetParent(), collServices)
	if err != nil {
		return nil, "", err
	}

	if err = pageTokenOK(req.GetPageToken()); err != nil {
		return nil, "", err
	}

	f, err := parseResourceFilter(req.GetFilter(), validateSLOFilterField)
	if err != nil {
		return nil, "", err
	}

	b.mu.RLock("ListServiceLevelObjectives")

	if svc != wildcardID {
		if _, ok := b.services[req.GetParent()]; !ok {
			b.mu.RUnlock()

			return nil, "", errNotFound("Service %s does not exist.", req.GetParent())
		}
	}

	var all []*monitoringpb.ServiceLevelObjective

	for name, s := range b.slos {
		if sloInScope(name, project, svc, req.GetParent()) && f.matches(sloEnv{s: s}) {
			all = append(all, cloneSLO(s))
		}
	}

	b.mu.RUnlock()

	sort.Slice(all, func(i, j int) bool { return all[i].GetName() < all[j].GetName() })
	pg := page.New(all, req.GetPageToken(), clampPage(req.GetPageSize()), defaultPage)

	return pg.Data, pg.Next, nil
}

func sloInScope(name, project, svc, parent string) bool {
	if svc == wildcardID {
		return strings.HasPrefix(name, projectScope+project+"/"+collServices+"/")
	}

	return strings.HasPrefix(name, parent+"/"+collSLOs+"/")
}

// UpdateServiceLevelObjective replaces the fields in the mask (every mutable field when empty).
func (b *InMemoryBackend) UpdateServiceLevelObjective(
	req *monitoringpb.UpdateServiceLevelObjectiveRequest,
) (*monitoringpb.ServiceLevelObjective, error) {
	in := req.GetServiceLevelObjective()

	if _, _, err := sloParts(in.GetName()); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateServiceLevelObjective")
	defer b.mu.Unlock()

	cur, ok := b.slos[in.GetName()]
	if !ok {
		return nil, errNotFound("Service level objective %s does not exist.", in.GetName())
	}

	next := cloneSLO(cur)

	paths := req.GetUpdateMask().GetPaths()
	if len(paths) == 0 {
		*next = *cloneSLO(in)
		next.Name = cur.GetName()
	} else if err := applySLOMask(next, cloneSLO(in), paths); err != nil {
		return nil, err
	}

	if err := validateSLO(next); err != nil {
		return nil, err
	}

	b.slos[next.GetName()] = next

	return cloneSLO(next), nil
}

func applySLOMask(next, in *monitoringpb.ServiceLevelObjective, paths []string) error {
	for _, p := range paths {
		switch p {
		case fldDisplayName:
			next.DisplayName = in.GetDisplayName()
		case fldGoal:
			next.Goal = in.GetGoal()
		case fldSLI:
			next.ServiceLevelIndicator = in.GetServiceLevelIndicator()
		case fldRolling, fldCalendar:
			next.Period = in.GetPeriod()
		case fldUserLabels:
			next.UserLabels = in.GetUserLabels()
		default:
			return errInvalid("Field update_mask had an invalid value of %q: the field cannot be updated.", p)
		}
	}

	return nil
}

// DeleteServiceLevelObjective removes an objective.
func (b *InMemoryBackend) DeleteServiceLevelObjective(name string) error {
	if _, _, err := sloParts(name); err != nil {
		return err
	}

	b.mu.Lock("DeleteServiceLevelObjective")
	defer b.mu.Unlock()

	if _, ok := b.slos[name]; !ok {
		return errNotFound("Service level objective %s does not exist.", name)
	}

	delete(b.slos, name)

	return nil
}
