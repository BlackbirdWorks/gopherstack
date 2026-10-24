package gcpmonitoring

import (
	"regexp"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	collSnoozes        = "snoozes"
	maxSnoozePolicies  = 16
	snoozeStartSkew    = 15 * time.Second
	pathIntervalStart  = "interval.start_time"
	pathIntervalEnd    = "interval.end_time"
	pathInterval       = "interval"
	snoozeFilterLayout = "2006-01-02T15:04:05Z"
)

var rfc3339Literal = regexp.MustCompile(`"(\d{4}-\d{2}-\d{2}T[0-9:.]+(?:Z|[+-]\d{2}:\d{2}))"`)

func cloneSnooze(s *monitoringpb.Snooze) *monitoringpb.Snooze {
	out, _ := proto.Clone(s).(*monitoringpb.Snooze)

	return out
}

func validateSnoozeInterval(iv *monitoringpb.TimeInterval, now time.Time, checkStart bool) error {
	if iv.GetStartTime() == nil || iv.GetEndTime() == nil {
		return errInvalid("Field snooze.interval had an invalid value: start_time and end_time are required.")
	}

	start, end := iv.GetStartTime().AsTime(), iv.GetEndTime().AsTime()
	if !end.After(start) {
		return errInvalid("Field snooze.interval had an invalid value: end_time must be after start_time.")
	}

	if checkStart && start.Before(now.Add(-snoozeStartSkew)) {
		return errInvalid("Field snooze.interval.start_time had an invalid value: it cannot be in the past.")
	}

	if !end.After(now) {
		return errInvalid("Field snooze.interval.end_time had an invalid value: it cannot be in the past.")
	}

	return nil
}

func validateSnoozeFilter(src string) error {
	f, err := parseFilter(src)
	if err != nil {
		return err
	}

	for field := range f.referenced() {
		if !strings.HasPrefix(field, prefixResLbl) && !strings.HasPrefix(field, prefixMetricLbl) {
			return errInvalid("Field snooze.criteria.filter had an invalid value of %q: "+
				"only resource.labels.* and metric.labels.* can be referenced.", field)
		}
	}

	return nil
}

func (b *InMemoryBackend) validateSnoozeLocked(project string, s *monitoringpb.Snooze, now time.Time) error {
	if s == nil {
		return errInvalid("snooze is required.")
	}

	if s.GetDisplayName() == "" || len(s.GetDisplayName()) > maxDisplayName {
		return errInvalid("Field snooze.display_name had an invalid value: a display name of 1-%d characters "+
			"is required.", maxDisplayName)
	}

	pols := s.GetCriteria().GetPolicies()
	if len(pols) == 0 || len(pols) > maxSnoozePolicies {
		return errInvalid("Field snooze.criteria.policies had an invalid value: 1-%d policies are required.",
			maxSnoozePolicies)
	}

	for _, p := range pols {
		pj, _, err := splitName(p, collAlertPolicies)
		if err != nil {
			return err
		}

		if _, ok := b.policies[p]; !ok || pj != project {
			return errNotFound("Alert policy %s does not exist.", p)
		}
	}

	if fs := s.GetCriteria().GetFilter(); fs != "" {
		if len(pols) != 1 {
			return errInvalid("Field snooze.criteria.filter had an invalid value: exactly one alert policy " +
				"is required when a filter is specified.")
		}

		if err := validateSnoozeFilter(fs); err != nil {
			return err
		}
	}

	return validateSnoozeInterval(s.GetInterval(), now, true)
}

// CreateSnooze stores a snooze with a generated id.
func (b *InMemoryBackend) CreateSnooze(req *monitoringpb.CreateSnoozeRequest) (*monitoringpb.Snooze, error) {
	project, err := projectOf(req.GetParent())
	if err != nil {
		return nil, err
	}

	b.mu.Lock("CreateSnooze")
	defer b.mu.Unlock()

	if err = b.validateSnoozeLocked(project, req.GetSnooze(), b.now()); err != nil {
		return nil, err
	}

	s := cloneSnooze(req.GetSnooze())
	s.Name = projectScope + project + "/" + collSnoozes + "/" + newNumericID()
	b.snoozes[s.GetName()] = s

	return cloneSnooze(s), nil
}

// GetSnooze returns one snooze.
func (b *InMemoryBackend) GetSnooze(name string) (*monitoringpb.Snooze, error) {
	if _, _, err := splitName(name, collSnoozes); err != nil {
		return nil, err
	}

	b.mu.RLock("GetSnooze")
	defer b.mu.RUnlock()

	s, ok := b.snoozes[name]
	if !ok {
		return nil, errNotFound("Snooze %s does not exist.", name)
	}

	return cloneSnooze(s), nil
}

type snoozeEnv struct{ s *monitoringpb.Snooze }

func (e snoozeEnv) values(path string) []string {
	switch path {
	case pathIntervalStart:
		return []string{e.s.GetInterval().GetStartTime().AsTime().UTC().Format(snoozeFilterLayout)}
	case pathIntervalEnd:
		return []string{e.s.GetInterval().GetEndTime().AsTime().UTC().Format(snoozeFilterLayout)}
	}

	return nil
}

// normalizeSnoozeFilter rewrites quoted RFC 3339 literals to UTC so string comparison orders them.
func normalizeSnoozeFilter(src string) string {
	return rfc3339Literal.ReplaceAllStringFunc(src, func(m string) string {
		t, err := time.Parse(time.RFC3339Nano, strings.Trim(m, `"`))
		if err != nil {
			return m
		}

		return `"` + t.UTC().Format(snoozeFilterLayout) + `"`
	})
}

func parseSnoozeListFilter(src string) (*filter, error) {
	f, err := parseFilter(normalizeSnoozeFilter(src))
	if err != nil {
		return nil, err
	}

	for field := range f.referenced() {
		if field != pathIntervalStart && field != pathIntervalEnd {
			return nil, errInvalid("Field filter had an invalid value of %q: "+
				"only interval.start_time and interval.end_time are supported.", field)
		}
	}

	return f, nil
}

// ListSnoozes pages the project's snoozes ordered by name.
func (b *InMemoryBackend) ListSnoozes(req *monitoringpb.ListSnoozesRequest) ([]*monitoringpb.Snooze, string, error) {
	project, err := projectOf(req.GetParent())
	if err != nil {
		return nil, "", err
	}

	if err = pageTokenOK(req.GetPageToken()); err != nil {
		return nil, "", err
	}

	f, err := parseSnoozeListFilter(req.GetFilter())
	if err != nil {
		return nil, "", err
	}

	prefix := projectScope + project + "/" + collSnoozes + "/"

	b.mu.RLock("ListSnoozes")

	var all []*monitoringpb.Snooze

	for name, s := range b.snoozes {
		if strings.HasPrefix(name, prefix) && f.matches(snoozeEnv{s: s}) {
			all = append(all, cloneSnooze(s))
		}
	}

	b.mu.RUnlock()

	sort.Slice(all, func(i, j int) bool { return all[i].GetName() < all[j].GetName() })
	pg := page.New(all, req.GetPageToken(), clampPage(req.GetPageSize()), defaultPage)

	return pg.Data, pg.Next, nil
}

// UpdateSnooze changes the display name or interval; an ended snooze is immutable and a started one keeps its start.
func (b *InMemoryBackend) UpdateSnooze(req *monitoringpb.UpdateSnoozeRequest) (*monitoringpb.Snooze, error) {
	in := req.GetSnooze()

	if _, _, err := splitName(in.GetName(), collSnoozes); err != nil {
		return nil, err
	}

	paths := req.GetUpdateMask().GetPaths()
	if len(paths) == 0 {
		return nil, errInvalid("Field update_mask had an invalid value: at least one path is required.")
	}

	b.mu.Lock("UpdateSnooze")
	defer b.mu.Unlock()

	cur, ok := b.snoozes[in.GetName()]
	if !ok {
		return nil, errNotFound("Snooze %s does not exist.", in.GetName())
	}

	now := b.now()
	if !cur.GetInterval().GetEndTime().AsTime().After(now) {
		return nil, errPrecondition("Snooze %s has ended and cannot be updated.", in.GetName())
	}

	next := cloneSnooze(cur)
	if err := applySnoozeMask(next, in, paths); err != nil {
		return nil, err
	}

	started := !cur.GetInterval().GetStartTime().AsTime().After(now)
	if started && !next.GetInterval().GetStartTime().AsTime().Equal(cur.GetInterval().GetStartTime().AsTime()) {
		return nil, errInvalid("Field snooze.interval.start_time had an invalid value: " +
			"the start of a snooze that has begun cannot be changed.")
	}

	if err := validateSnoozeInterval(next.GetInterval(), now, !started); err != nil {
		return nil, err
	}

	b.snoozes[next.GetName()] = next

	return cloneSnooze(next), nil
}

func applySnoozeMask(next, in *monitoringpb.Snooze, paths []string) error {
	for _, p := range paths {
		switch p {
		case fldDisplayName:
			if in.GetDisplayName() == "" || len(in.GetDisplayName()) > maxDisplayName {
				return errInvalid("Field snooze.display_name had an invalid value: a display name of 1-%d "+
					"characters is required.", maxDisplayName)
			}

			next.DisplayName = in.GetDisplayName()
		case pathIntervalStart:
			next.Interval.StartTime = timestamppb.New(in.GetInterval().GetStartTime().AsTime())
		case pathIntervalEnd:
			next.Interval.EndTime = timestamppb.New(in.GetInterval().GetEndTime().AsTime())
		case pathInterval:
			next.Interval = &monitoringpb.TimeInterval{
				StartTime: timestamppb.New(in.GetInterval().GetStartTime().AsTime()),
				EndTime:   timestamppb.New(in.GetInterval().GetEndTime().AsTime()),
			}
		default:
			return errInvalid("Field update_mask had an invalid value of %q: the field cannot be updated.", p)
		}
	}

	return nil
}

type snoozeEvalEnv struct{ o observation }

func (e snoozeEvalEnv) values(path string) []string {
	if k, ok := strings.CutPrefix(path, prefixResLbl); ok {
		return one(e.o.resourceLabels, k)
	}

	if k, ok := strings.CutPrefix(path, prefixMetricLbl); ok {
		return one(e.o.metricLabels, k)
	}

	return nil
}

// snoozedLocked reports whether an active snooze covers an incident of the policy with the observation's labels.
func (b *InMemoryBackend) snoozedLocked(policy string, o observation, now time.Time) bool {
	for _, s := range b.snoozes {
		iv := s.GetInterval()
		if now.Before(iv.GetStartTime().AsTime()) || !now.Before(iv.GetEndTime().AsTime()) {
			continue
		}

		if !containsStr(s.GetCriteria().GetPolicies(), policy) {
			continue
		}

		if s.GetCriteria().GetFilter() == "" {
			return true
		}

		if f, err := parseFilter(s.GetCriteria().GetFilter()); err == nil && f.matches(snoozeEvalEnv{o: o}) {
			return true
		}
	}

	return false
}
