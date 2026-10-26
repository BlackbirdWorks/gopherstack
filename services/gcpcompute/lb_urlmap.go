package gcpcompute

import (
	"regexp"
	"slices"
	"strconv"
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	maxRoutePriority = 2147483647
	maxWeight        = 1000
	maxPathRuleLB    = 1000
)

type urlTarget struct {
	svc   *string
	ra    *computeapi.HttpRouteAction
	redir *computeapi.HttpRedirectAction
	field string
}

func urlMapSpec() lbSpec[computeapi.UrlMap] {
	return lbSpec[computeapi.UrlMap]{
		coll: collURLMaps, kind: "compute#urlMap", noun: "url_map",
		m: func(b *InMemoryBackend) map[string]*computeapi.UrlMap { return b.urlMaps },
		meta: func(v *computeapi.UrlMap) lbMeta {
			return lbMeta{
				name: &v.Name, kind: &v.Kind, created: &v.CreationTimestamp, self: &v.SelfLink,
				region: &v.Region, fp: &v.Fingerprint, id: &v.Id,
			}
		},
		prep: prepURLMap,
		refs: urlMapServices,
	}
}

func urlMapTargets(m *computeapi.UrlMap) []urlTarget {
	out := []urlTarget{{
		svc: &m.DefaultService, ra: m.DefaultRouteAction, redir: m.DefaultUrlRedirect,
		field: "resource.defaultService",
	}}

	for _, pm := range m.PathMatchers {
		f := "resource.pathMatchers[" + pm.Name + "]"
		out = append(out, urlTarget{svc: &pm.DefaultService, ra: pm.DefaultRouteAction, redir: pm.DefaultUrlRedirect, field: f + ".defaultService"})

		for _, pr := range pm.PathRules {
			out = append(out, urlTarget{svc: &pr.Service, ra: pr.RouteAction, redir: pr.UrlRedirect, field: f + ".pathRules.service"})
		}

		for _, rr := range pm.RouteRules {
			out = append(out, urlTarget{svc: &rr.Service, ra: rr.RouteAction, redir: rr.UrlRedirect, field: f + ".routeRules.service"})
		}
	}

	return out
}

func urlMapServices(m *computeapi.UrlMap) []string {
	var out []string

	for _, t := range urlMapTargets(m) {
		if *t.svc != "" {
			out = append(out, *t.svc)
		}

		if t.ra != nil {
			for _, w := range t.ra.WeightedBackendServices {
				out = append(out, w.BackendService)
			}
		}
	}

	return out
}

func (t urlTarget) count() int {
	n := 0
	if *t.svc != "" {
		n++
	}

	if t.ra != nil && len(t.ra.WeightedBackendServices) > 0 {
		n++
	}

	if t.redir != nil {
		n++
	}

	return n
}

func (b *InMemoryBackend) resolveServiceLocked(c lbCtx, field, raw string) (string, *APIError) {
	parts := strings.Split(trimURL(raw), "/")
	if len(parts) >= nameInRegionOrZone && parts[len(parts)-2] == collBackendBucket {
		if c.scope != scopeGlobal {
			return "", errField(field, raw, "Backend buckets are global.")
		}

		return lbResolve(b, bucketSpec(), c, field, raw)
	}

	return lbResolve(b, bsSpec(), c, field, raw)
}

func (b *InMemoryBackend) resolveTargetLocked(c lbCtx, t urlTarget, required bool) *APIError {
	n := t.count()
	if n == 0 && required {
		return errInvalid("Invalid value for field '%s': exactly one of service, routeAction.weightedBackendServices "+
			"or urlRedirect must be specified.", t.field)
	}

	if n > 1 {
		return errInvalid("Invalid value for field '%s': only one of service, routeAction.weightedBackendServices "+
			"or urlRedirect may be specified.", t.field)
	}

	if *t.svc != "" {
		l, err := b.resolveServiceLocked(c, t.field, *t.svc)
		if err != nil {
			return err
		}

		*t.svc = l
	}

	if t.ra == nil {
		return nil
	}

	total := int64(0)

	for _, w := range t.ra.WeightedBackendServices {
		l, err := b.resolveServiceLocked(c, t.field, w.BackendService)
		if err != nil {
			return err
		}

		if w.Weight < 0 || w.Weight > maxWeight {
			return errField(t.field+".weight", strconv.FormatInt(w.Weight, 10), "Must be between 0 and 1000.")
		}

		w.BackendService, total = l, total+w.Weight
	}

	if len(t.ra.WeightedBackendServices) > 0 && total == 0 {
		return errInvalid("Invalid value for field '%s': the total weight must be greater than 0.", t.field)
	}

	return nil
}

var hostRE = regexp.MustCompile(`^(\*|\*?[A-Za-z0-9.-]+)$`)

func validateHostRules(m *computeapi.UrlMap) *APIError {
	matchers := map[string]bool{}
	for _, pm := range m.PathMatchers {
		matchers[pm.Name] = true
	}

	seen := map[string]bool{}

	for _, hr := range m.HostRules {
		if len(hr.Hosts) == 0 {
			return errRequired("resource.hostRules.hosts")
		}

		if !matchers[hr.PathMatcher] {
			return errField("resource.hostRules.pathMatcher", hr.PathMatcher, "No path matcher with this name exists.")
		}

		for _, h := range hr.Hosts {
			if !hostRE.MatchString(h) || seen[strings.ToLower(h)] {
				return errField("resource.hostRules.hosts", h, "Invalid or duplicate host pattern.")
			}

			seen[strings.ToLower(h)] = true
		}
	}

	return nil
}

func validatePathMatcher(pm *computeapi.PathMatcher) *APIError {
	if len(pm.PathRules) > 0 && len(pm.RouteRules) > 0 {
		return errInvalid("Invalid value for field 'resource.pathMatchers[%s]': pathRules and routeRules "+
			"cannot both be specified.", pm.Name)
	}

	if len(pm.PathRules) > maxPathRuleLB || len(pm.RouteRules) > maxPathRuleLB {
		return errField("resource.pathMatchers", pm.Name, "Too many rules.")
	}

	seen := map[string]bool{}

	for _, pr := range pm.PathRules {
		if len(pr.Paths) == 0 {
			return errRequired("resource.pathMatchers.pathRules.paths")
		}

		for _, p := range pr.Paths {
			if !validRulePath(p) || seen[p] {
				return errField("resource.pathMatchers.pathRules.paths", p, "Invalid or duplicate path.")
			}

			seen[p] = true
		}
	}

	prios := map[int64]bool{}

	for _, rr := range pm.RouteRules {
		if rr.Priority < 0 || rr.Priority > maxRoutePriority || prios[rr.Priority] {
			return errField("resource.pathMatchers.routeRules.priority", strconv.FormatInt(rr.Priority, 10),
				"Must be a unique value between 0 and 2147483647.")
		}

		prios[rr.Priority] = true
	}

	return nil
}

func validRulePath(p string) bool {
	if !strings.HasPrefix(p, "/") {
		return false
	}

	i := strings.IndexByte(p, '*')

	return i < 0 || i == len(p)-1 && strings.HasSuffix(p, "/*")
}

func (b *InMemoryBackend) normalizeURLMapLocked(c lbCtx, in *computeapi.UrlMap) *APIError {
	names := map[string]bool{}

	for _, pm := range in.PathMatchers {
		if err := validName("resource.pathMatchers.name", pm.Name); err != nil {
			return err
		}

		if names[pm.Name] {
			return errField("resource.pathMatchers.name", pm.Name, "Duplicate path matcher.")
		}

		names[pm.Name] = true

		if err := validatePathMatcher(pm); err != nil {
			return err
		}
	}

	if err := validateHostRules(in); err != nil {
		return err
	}

	for _, t := range urlMapTargets(in) {
		if err := b.resolveTargetLocked(c, t, true); err != nil {
			return err
		}
	}

	return nil
}

func prepURLMap(b *InMemoryBackend, c lbCtx, in *computeapi.UrlMap, _ map[string]any) *APIError {
	if err := b.normalizeURLMapLocked(c, in); err != nil {
		return err
	}

	if fails := evalURLMapTests(in); len(fails) > 0 {
		f := fails[0]

		return errInvalid("Invalid value for field 'resource.tests': test for host '%s' path '%s' expected service '%s' "+
			"but got '%s'.", f.Host, f.Path, f.ExpectedService, f.ActualService)
	}

	return nil
}

func svcID(raw string) string {
	parts := strings.Split(trimURL(raw), "/")
	if len(parts) < nameInRegionOrZone {
		return raw
	}

	return strings.Join(parts[len(parts)-2:], "/")
}

func firstService(svc string, ra *computeapi.HttpRouteAction) string {
	if svc != "" {
		return svc
	}

	if ra != nil && len(ra.WeightedBackendServices) > 0 {
		return ra.WeightedBackendServices[0].BackendService
	}

	return ""
}

func hostMatches(host, pattern string) (bool, int) {
	host, pattern = strings.ToLower(host), strings.ToLower(pattern)

	switch {
	case pattern == "*":
		return true, 1
	case strings.HasPrefix(pattern, "*"):
		suffix := pattern[1:]

		return strings.HasSuffix(host, suffix) && len(host) > len(suffix), len(suffix) + 1
	}

	return host == pattern, maxRoutePriority
}

func matcherFor(m *computeapi.UrlMap, host string) *computeapi.PathMatcher {
	best, bestScore := "", 0

	for _, hr := range m.HostRules {
		for _, h := range hr.Hosts {
			if ok, score := hostMatches(host, h); ok && score > bestScore {
				best, bestScore = hr.PathMatcher, score
			}
		}
	}

	for _, pm := range m.PathMatchers {
		if pm.Name == best && best != "" {
			return pm
		}
	}

	return nil
}

func pathRuleMatches(p, path string) (bool, int) {
	if strings.HasSuffix(p, "/*") {
		prefix := strings.TrimSuffix(p, "*")

		return strings.HasPrefix(path, prefix) || path+"/" == prefix, len(prefix)
	}

	return p == path, maxRoutePriority
}

func routeRuleMatches(rr *computeapi.HttpRouteRule, path string) bool {
	if len(rr.MatchRules) == 0 {
		return true
	}

	for _, mr := range rr.MatchRules {
		if len(mr.HeaderMatches) > 0 || len(mr.QueryParameterMatches) > 0 || len(mr.MetadataFilters) > 0 {
			continue
		}

		p, full, prefix := path, mr.FullPathMatch, mr.PrefixMatch

		if mr.IgnoreCase {
			p, full, prefix = strings.ToLower(p), strings.ToLower(full), strings.ToLower(prefix)
		}

		switch {
		case full != "" && p == full, prefix != "" && strings.HasPrefix(p, prefix):
			return true
		case mr.RegexMatch != "":
			if re, err := regexp.Compile("^(?:" + mr.RegexMatch + ")$"); err == nil && re.MatchString(path) {
				return true
			}
		case full == "" && prefix == "" && mr.PathTemplateMatch == "":
			return true
		}
	}

	return false
}

// routeURLMap returns the service a request for host and path is routed to ("" for a redirect or no match).
func routeURLMap(m *computeapi.UrlMap, host, path string) string {
	pm := matcherFor(m, host)
	if pm == nil {
		return firstService(m.DefaultService, m.DefaultRouteAction)
	}

	best, bestLen := "", -1

	for _, pr := range pm.PathRules {
		for _, p := range pr.Paths {
			if ok, l := pathRuleMatches(p, path); ok && l > bestLen {
				best, bestLen = firstService(pr.Service, pr.RouteAction), l
			}
		}
	}

	if bestLen >= 0 {
		return best
	}

	rules := slices.Clone(pm.RouteRules)
	slices.SortFunc(rules, func(a, b *computeapi.HttpRouteRule) int { return int(a.Priority - b.Priority) })

	for _, rr := range rules {
		if routeRuleMatches(rr, path) {
			return firstService(rr.Service, rr.RouteAction)
		}
	}

	return firstService(pm.DefaultService, pm.DefaultRouteAction)
}

func evalURLMapTests(m *computeapi.UrlMap) []*computeapi.TestFailure {
	var out []*computeapi.TestFailure

	for _, t := range m.Tests {
		if t.Service == "" {
			continue
		}

		actual := routeURLMap(m, t.Host, t.Path)
		if actual == "" || svcID(actual) != svcID(t.Service) {
			out = append(out, &computeapi.TestFailure{
				Host: t.Host, Path: t.Path, ExpectedService: t.Service, ActualService: actual,
			})
		}
	}

	return out
}

func (b *InMemoryBackend) validateURLMap(c lbCtx, req *computeapi.UrlMapsValidateRequest) (*computeapi.UrlMapsValidateResponse, error) {
	if req.Resource == nil {
		return nil, errRequired("resource")
	}

	b.mu.RLock("ValidateUrlMap")
	defer b.mu.RUnlock()

	m := clone(req.Resource)
	res := &computeapi.UrlMapValidationResult{LoadSucceeded: true, TestPassed: true}

	for _, t := range m.Tests {
		if t.Host == "" || t.Path == "" {
			return nil, errRequired("resource.tests.host")
		}
	}

	if err := b.normalizeURLMapLocked(c, m); err != nil {
		res.LoadSucceeded, res.TestPassed = false, false
		res.LoadErrors = []string{err.Message}
	} else if msg := b.schemeMismatchLocked(m, req.LoadBalancingSchemes); msg != "" {
		res.LoadSucceeded, res.TestPassed = false, false
		res.LoadErrors = []string{msg}
	} else if fails := evalURLMapTests(m); len(fails) > 0 {
		res.TestPassed, res.TestFailures = false, fails
	}

	res.ForceSendFields = []string{"LoadSucceeded", "TestPassed"}

	return &computeapi.UrlMapsValidateResponse{Result: res}, nil
}

func (b *InMemoryBackend) schemeMismatchLocked(m *computeapi.UrlMap, schemes []string) string {
	if len(schemes) == 0 {
		return ""
	}

	for _, l := range urlMapServices(m) {
		for _, bs := range b.backendSvcs {
			if bs.SelfLink == l && !slices.Contains(schemes, bs.LoadBalancingScheme) {
				return "Backend service '" + bs.Name + "' has load balancing scheme " + bs.LoadBalancingScheme +
					", which is not in the requested schemes."
			}
		}
	}

	return ""
}
