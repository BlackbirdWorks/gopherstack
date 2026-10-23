package gcpmonitoring

import (
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxConditions   = 6
	durationStep    = time.Minute
	maxDuration     = 24 * time.Hour
	maxAggregations = 2
)

type (
	policyCondition = monitoringpb.AlertPolicy_Condition
)

const (
	combineOr       = monitoringpb.AlertPolicy_OR
	combineAnd      = monitoringpb.AlertPolicy_AND
	combineAndMatch = monitoringpb.AlertPolicy_AND_WITH_MATCHING_RESOURCE
)

func clonePolicy(p *monitoringpb.AlertPolicy) *monitoringpb.AlertPolicy {
	out, _ := proto.Clone(p).(*monitoringpb.AlertPolicy)

	return out
}

func validateTiming(path string, d, lowest time.Duration) error {
	if d < lowest || d > maxDuration || d%durationStep != 0 {
		return errInvalid("Field %s had an invalid value of %q: the duration must be a multiple of 60 seconds "+
			"between %s and 24h.", path, d, lowest)
	}

	return nil
}

func validateQuery(path, filterSrc string, aggs []*monitoringpb.Aggregation) error {
	f, err := parseFilter(filterSrc)
	if err != nil {
		return err
	}

	if _, ok := f.referenced()[pathMetricType]; !ok {
		return errInvalid("Field %s.filter had an invalid value of %q: the filter must specify metric.type.",
			path, filterSrc)
	}

	if len(aggs) > maxAggregations {
		return errInvalid("Field %s.aggregations had an invalid value: at most %d aggregations are allowed.",
			path, maxAggregations)
	}

	for _, a := range aggs {
		if err = validateAggregation(a, path+".aggregations"); err != nil {
			return err
		}
	}

	return nil
}

func validateCondition(i int, c *policyCondition) error {
	path := "alert_policy.conditions[" + strconv.Itoa(i) + "]"

	if c.GetDisplayName() == "" || len(c.GetDisplayName()) > maxDisplayName {
		return errInvalid("Field %s.display_name had an invalid value: a display name is required.", path)
	}

	switch cond := c.GetCondition().(type) {
	case *monitoringpb.AlertPolicy_Condition_ConditionThreshold:
		return validateThreshold(path+".condition_threshold", cond.ConditionThreshold)
	case *monitoringpb.AlertPolicy_Condition_ConditionAbsent:
		a := cond.ConditionAbsent
		if err := validateQuery(path+".condition_absent", a.GetFilter(), a.GetAggregations()); err != nil {
			return err
		}

		return validateTiming(path+".condition_absent.duration", a.GetDuration().AsDuration(), durationStep)
	case *monitoringpb.AlertPolicy_Condition_ConditionMatchedLog:
		if cond.ConditionMatchedLog.GetFilter() == "" {
			return errInvalid("Field %s.condition_matched_log.filter had an invalid value: a filter is required.", path)
		}

		return nil
	case *monitoringpb.AlertPolicy_Condition_ConditionMonitoringQueryLanguage:
		if cond.ConditionMonitoringQueryLanguage.GetQuery() == "" {
			return errInvalid("Field %s.condition_monitoring_query_language.query had an invalid value: "+
				"a query is required.", path)
		}

		return nil
	case *monitoringpb.AlertPolicy_Condition_ConditionPrometheusQueryLanguage:
		if cond.ConditionPrometheusQueryLanguage.GetQuery() == "" {
			return errInvalid("Field %s.condition_prometheus_query_language.query had an invalid value: "+
				"a query is required.", path)
		}

		return nil
	}

	return errInvalid("Field %s had an invalid value: a condition type must be set.", path)
}

func validateThreshold(path string, t *monitoringpb.AlertPolicy_Condition_MetricThreshold) error {
	if err := validateQuery(path, t.GetFilter(), t.GetAggregations()); err != nil {
		return err
	}

	if t.GetComparison() == monitoringpb.ComparisonType_COMPARISON_UNSPECIFIED {
		return errInvalid("Field %s.comparison had an invalid value: a comparison is required.", path)
	}

	if t.GetDenominatorFilter() != "" {
		return errInvalid("Field %s.denominator_filter had an invalid value: ratio conditions are not supported.", path)
	}

	return validateTiming(path+".duration", t.GetDuration().AsDuration(), 0)
}

func (b *InMemoryBackend) validatePolicyLocked(project string, p *monitoringpb.AlertPolicy) error {
	if p == nil {
		return errInvalid("alert_policy is required.")
	}

	if p.GetDisplayName() == "" || len(p.GetDisplayName()) > maxDisplayName {
		return errInvalid("Field alert_policy.display_name had an invalid value: a display name is required.")
	}

	if n := len(p.GetConditions()); n == 0 || n > maxConditions {
		return errInvalid("Field alert_policy.conditions had an invalid value: between 1 and %d conditions "+
			"are required.", maxConditions)
	}

	if p.GetCombiner() == monitoringpb.AlertPolicy_COMBINE_UNSPECIFIED {
		return errInvalid("Field alert_policy.combiner had an invalid value: a combiner is required.")
	}

	for i, c := range p.GetConditions() {
		if err := validateCondition(i, c); err != nil {
			return err
		}
	}

	for _, ch := range p.GetNotificationChannels() {
		if _, ok := b.channels[ch]; !ok || !strings.HasPrefix(ch, projectScope+project+"/") {
			return errInvalid("Field alert_policy.notification_channels had an invalid value of %q: "+
				"the notification channel does not exist.", ch)
		}
	}

	return nil
}

func nameConditions(policyName string, p *monitoringpb.AlertPolicy, existing map[string]string) {
	for _, c := range p.GetConditions() {
		if keep, ok := existing[c.GetName()]; ok && c.GetName() != "" {
			c.Name = keep

			continue
		}

		c.Name = policyName + "/" + collConditions + "/" + newNumericID()
	}
}

// CreateAlertPolicy stores a policy and names its conditions.
func (b *InMemoryBackend) CreateAlertPolicy(
	req *monitoringpb.CreateAlertPolicyRequest,
) (*monitoringpb.AlertPolicy, error) {
	project, err := projectOf(req.GetName())
	if err != nil {
		return nil, err
	}

	b.mu.Lock("CreateAlertPolicy")
	defer b.mu.Unlock()

	if err = b.validatePolicyLocked(project, req.GetAlertPolicy()); err != nil {
		return nil, err
	}

	p := clonePolicy(req.GetAlertPolicy())
	p.Name = projectScope + project + "/" + collAlertPolicies + "/" + newNumericID()
	nameConditions(p.GetName(), p, nil)

	if p.GetEnabled() == nil {
		p.Enabled = wrapperspb.Bool(true)
	}

	p.CreationRecord, p.MutationRecord = b.mutation(), b.mutation()
	b.policies[p.GetName()] = p

	return clonePolicy(p), nil
}

// GetAlertPolicy returns one policy.
func (b *InMemoryBackend) GetAlertPolicy(name string) (*monitoringpb.AlertPolicy, error) {
	if _, _, err := splitName(name, collAlertPolicies); err != nil {
		return nil, err
	}

	b.mu.RLock("GetAlertPolicy")
	defer b.mu.RUnlock()

	p, ok := b.policies[name]
	if !ok {
		return nil, errNotFound("Alert policy %s does not exist.", name)
	}

	return clonePolicy(p), nil
}

type policyEnv struct{ p *monitoringpb.AlertPolicy }

func (e policyEnv) values(path string) []string {
	switch path {
	case fldDisplayName:
		return []string{e.p.GetDisplayName()}
	case fldName:
		return []string{e.p.GetName()}
	case fldChannels:
		return e.p.GetNotificationChannels()
	}

	if key, ok := cutPrefix(path, "user_labels."); ok {
		return one(e.p.GetUserLabels(), key)
	}

	return nil
}

// ListAlertPolicies pages the project's policies ordered by name, or by display_name when requested.
func (b *InMemoryBackend) ListAlertPolicies(
	req *monitoringpb.ListAlertPoliciesRequest,
) ([]*monitoringpb.AlertPolicy, string, int32, error) {
	project, err := projectOf(req.GetName())
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

	prefix := projectScope + project + "/" + collAlertPolicies + "/"

	b.mu.RLock("ListAlertPolicies")

	var all []*monitoringpb.AlertPolicy

	for name, p := range b.policies {
		if strings.HasPrefix(name, prefix) && f.matches(policyEnv{p: p}) {
			all = append(all, clonePolicy(p))
		}
	}

	b.mu.RUnlock()

	sortPolicies(all, req.GetOrderBy())
	pg := page.New(all, req.GetPageToken(), clampPage(req.GetPageSize()), defaultPage)

	return pg.Data, pg.Next, int32(len(all)), nil //nolint:gosec // bounded by the policy count
}

func sortPolicies(all []*monitoringpb.AlertPolicy, orderBy string) {
	sort.Slice(all, func(i, j int) bool {
		switch orderBy {
		case fldDisplayName:
			return all[i].GetDisplayName() < all[j].GetDisplayName()
		case "-display_name", "display_name desc":
			return all[i].GetDisplayName() > all[j].GetDisplayName()
		}

		return all[i].GetName() < all[j].GetName()
	})
}

// UpdateAlertPolicy replaces the fields named in the mask (every mutable field when empty).
func (b *InMemoryBackend) UpdateAlertPolicy(
	req *monitoringpb.UpdateAlertPolicyRequest,
) (*monitoringpb.AlertPolicy, error) {
	in := req.GetAlertPolicy()

	project, _, err := splitName(in.GetName(), collAlertPolicies)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateAlertPolicy")
	defer b.mu.Unlock()

	cur, ok := b.policies[in.GetName()]
	if !ok {
		return nil, errNotFound("Alert policy %s does not exist.", in.GetName())
	}

	next := clonePolicy(cur)
	if err = applyPolicyMask(next, in, req.GetUpdateMask().GetPaths()); err != nil {
		return nil, err
	}

	if err = b.validatePolicyLocked(project, next); err != nil {
		return nil, err
	}

	existing := map[string]string{}
	for _, c := range cur.GetConditions() {
		existing[c.GetName()] = c.GetName()
	}

	nameConditions(next.GetName(), next, existing)
	next.MutationRecord = b.mutation()
	b.policies[next.GetName()] = next

	return clonePolicy(next), nil
}

func applyPolicyMask(next, in *monitoringpb.AlertPolicy, paths []string) error {
	if len(paths) == 0 {
		paths = []string{
			fldDisplayName, fldDocumentation, fldUserLabels, "conditions", "combiner", fldEnabled,
			fldChannels, "alert_strategy", fldSeverity,
		}
	}

	src := clonePolicy(in)

	for _, p := range paths {
		switch p {
		case fldDisplayName:
			next.DisplayName = src.GetDisplayName()
		case fldDocumentation:
			next.Documentation = src.GetDocumentation()
		case fldUserLabels:
			next.UserLabels = src.GetUserLabels()
		case "conditions":
			next.Conditions = src.GetConditions()
		case "combiner":
			next.Combiner = src.GetCombiner()
		case fldEnabled:
			next.Enabled = src.GetEnabled()
		case fldChannels:
			next.NotificationChannels = src.GetNotificationChannels()
		case "alert_strategy":
			next.AlertStrategy = src.GetAlertStrategy()
		case fldSeverity:
			next.Severity = src.GetSeverity()
		default:
			return errInvalid("Field update_mask had an invalid value of %q: the field cannot be updated.", p)
		}
	}

	return nil
}

// DeleteAlertPolicy removes a policy; its open incidents close on the next evaluation.
func (b *InMemoryBackend) DeleteAlertPolicy(name string) error {
	if _, _, err := splitName(name, collAlertPolicies); err != nil {
		return err
	}

	b.mu.Lock("DeleteAlertPolicy")
	defer b.mu.Unlock()

	if _, ok := b.policies[name]; !ok {
		return errNotFound("Alert policy %s does not exist.", name)
	}

	delete(b.policies, name)

	return nil
}

func policyEnabled(p *monitoringpb.AlertPolicy) bool {
	return p.GetEnabled() == nil || p.GetEnabled().GetValue()
}

func policyPrompts(p *monitoringpb.AlertPolicy) (bool, bool) {
	prompts := p.GetAlertStrategy().GetNotificationPrompts()
	if len(prompts) == 0 {
		return true, true
	}

	return slices.Contains(prompts, monitoringpb.AlertPolicy_AlertStrategy_OPENED),
		slices.Contains(prompts, monitoringpb.AlertPolicy_AlertStrategy_CLOSED)
}
