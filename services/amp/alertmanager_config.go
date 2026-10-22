package amp

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/blackbirdworks/gopherstack/services/amp/promql"
)

const (
	defaultGroupWait      = 30 * time.Second
	defaultGroupInterval  = 5 * time.Minute
	defaultRepeatInterval = 4 * time.Hour
	maxRouteDepth         = 16
	maxRoutes             = 1000
	maxReceivers          = 1000
)

var (
	errAMNoRoute     = errors.New("alertmanager_config must define a route")
	errAMRouteDepth  = errors.New("route tree is too deep")
	errAMTooMany     = errors.New("too many routes or receivers")
	errAMReceiver    = errors.New("route references an undefined receiver")
	errAMMatcher     = errors.New("invalid matcher")
	errAMDuration    = errors.New("invalid duration")
	errAMNoTopic     = errors.New("sns_configs entry needs topic_arn, target_arn or phone_number")
	matcherSyntaxRe  = regexp.MustCompile(`^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*(=~|!=|!~|=)\s*(.*?)\s*$`)
	errAMGroupByMix  = errors.New("group_by cannot combine '...' with other labels")
	errAMRootMatcher = errors.New("the root route must not have matchers")
)

type amConfigDef struct {
	Route        *amRouteDef     `yaml:"route"`
	InhibitRules []amInhibitDef  `yaml:"inhibit_rules"`
	Receivers    []amReceiverDef `yaml:"receivers"`
}

type amRouteDef struct {
	Match          map[string]string `yaml:"match"`
	MatchRE        map[string]string `yaml:"match_re"`
	Receiver       string            `yaml:"receiver"`
	GroupWait      string            `yaml:"group_wait"`
	GroupInterval  string            `yaml:"group_interval"`
	RepeatInterval string            `yaml:"repeat_interval"`
	GroupBy        []string          `yaml:"group_by"`
	Matchers       []string          `yaml:"matchers"`
	Routes         []*amRouteDef     `yaml:"routes"`
	Continue       bool              `yaml:"continue"`
}

type amInhibitDef struct {
	SourceMatch    map[string]string `yaml:"source_match"`
	SourceMatchRE  map[string]string `yaml:"source_match_re"`
	TargetMatch    map[string]string `yaml:"target_match"`
	TargetMatchRE  map[string]string `yaml:"target_match_re"`
	SourceMatchers []string          `yaml:"source_matchers"`
	TargetMatchers []string          `yaml:"target_matchers"`
	Equal          []string          `yaml:"equal"`
}

type amReceiverDef struct {
	Name       string     `yaml:"name"`
	SNSConfigs []amSNSDef `yaml:"sns_configs"`
}

type amSNSDef struct {
	SendResolved *bool             `yaml:"send_resolved"`
	Attributes   map[string]string `yaml:"attributes"`
	TopicARN     string            `yaml:"topic_arn"`
	TargetARN    string            `yaml:"target_arn"`
	PhoneNumber  string            `yaml:"phone_number"`
	Subject      string            `yaml:"subject"`
	Message      string            `yaml:"message"`
}

type amRoute struct {
	receiver string
	groupBy  []string
	matchers []*promql.Matcher
	children []*amRoute
	wait     time.Duration
	interval time.Duration
	repeat   time.Duration
	id       int
	groupAll bool
	cont     bool
}

type amInhibit struct {
	source []*promql.Matcher
	target []*promql.Matcher
	equal  []string
}

type amSNS struct {
	attrs        map[string]string
	topic        string
	target       string
	phone        string
	subject      string
	message      string
	sendResolved bool
}

type amReceiver struct {
	name string
	sns  []amSNS
}

type amConfig struct {
	root      *amRoute
	receivers map[string]*amReceiver
	inhibit   []amInhibit
}

// alertManagerConfigYAML extracts the inner alertmanager_config document from an AMP definition.
func alertManagerConfigYAML(def []byte) ([]byte, error) {
	var doc struct {
		Config string `yaml:"alertmanager_config"`
	}

	if err := yaml.Unmarshal(def, &doc); err != nil {
		return nil, err
	}

	return []byte(doc.Config), nil
}

func parseMatcher(raw string) (*promql.Matcher, error) {
	m := matcherSyntaxRe.FindStringSubmatch(strings.Trim(strings.TrimSpace(raw), "{}"))
	if m == nil {
		return nil, fmt.Errorf("%w: %q", errAMMatcher, raw)
	}

	val := m[3]
	if len(val) >= 2 && val[0] == '"' && val[len(val)-1] == '"' {
		val = strings.ReplaceAll(val[1:len(val)-1], `\"`, `"`)
	}

	types := map[string]promql.MatchType{
		"=": promql.MatchEqual, "!=": promql.MatchNotEqual, "=~": promql.MatchRegexp, "!~": promql.MatchNotRegexp,
	}

	return promql.NewMatcher(types[m[2]], m[1], val)
}

func buildMatchers(match, matchRE map[string]string, list []string) ([]*promql.Matcher, error) {
	var out []*promql.Matcher

	for k, v := range match {
		m, err := promql.NewMatcher(promql.MatchEqual, k, v)
		if err != nil {
			return nil, err
		}

		out = append(out, m)
	}

	for k, v := range matchRE {
		m, err := promql.NewMatcher(promql.MatchRegexp, k, v)
		if err != nil {
			return nil, err
		}

		out = append(out, m)
	}

	for _, raw := range list {
		m, err := parseMatcher(raw)
		if err != nil {
			return nil, err
		}

		out = append(out, m)
	}

	return out, nil
}

func durOr(raw string, def time.Duration) (time.Duration, error) {
	if raw == "" {
		return def, nil
	}

	d, err := promql.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%w: %q", errAMDuration, raw)
	}

	return d, nil
}

type routeBuilder struct {
	count int
}

func (rb *routeBuilder) build(def *amRouteDef, parent *amRoute, depth int) (*amRoute, error) {
	if depth > maxRouteDepth {
		return nil, errAMRouteDepth
	}

	if rb.count++; rb.count > maxRoutes {
		return nil, errAMTooMany
	}

	r := &amRoute{id: rb.count, cont: def.Continue}
	if parent != nil {
		r.receiver, r.groupBy, r.groupAll = parent.receiver, parent.groupBy, parent.groupAll
		r.wait, r.interval, r.repeat = parent.wait, parent.interval, parent.repeat
	} else {
		r.wait, r.interval, r.repeat = defaultGroupWait, defaultGroupInterval, defaultRepeatInterval
	}

	if err := r.apply(def); err != nil {
		return nil, err
	}

	for _, cd := range def.Routes {
		c, err := rb.build(cd, r, depth+1)
		if err != nil {
			return nil, err
		}

		r.children = append(r.children, c)
	}

	return r, nil
}

func (r *amRoute) apply(def *amRouteDef) error {
	var err error

	if def.Receiver != "" {
		r.receiver = def.Receiver
	}

	if def.GroupBy != nil {
		r.groupBy, r.groupAll = nil, false

		for _, g := range def.GroupBy {
			if g == "..." {
				r.groupAll = true
			} else {
				r.groupBy = append(r.groupBy, g)
			}
		}

		if r.groupAll && len(r.groupBy) > 0 {
			return errAMGroupByMix
		}
	}

	if r.matchers, err = buildMatchers(def.Match, def.MatchRE, def.Matchers); err != nil {
		return err
	}

	if r.wait, err = durOr(def.GroupWait, r.wait); err != nil {
		return err
	}

	if r.interval, err = durOr(def.GroupInterval, r.interval); err != nil {
		return err
	}

	r.repeat, err = durOr(def.RepeatInterval, r.repeat)

	return err
}

func buildReceivers(defs []amReceiverDef) (map[string]*amReceiver, error) {
	if len(defs) > maxReceivers {
		return nil, errAMTooMany
	}

	out := make(map[string]*amReceiver, len(defs))

	for _, d := range defs {
		rc := &amReceiver{name: d.Name}

		for _, s := range d.SNSConfigs {
			if s.TopicARN == "" && s.TargetARN == "" && s.PhoneNumber == "" {
				return nil, errAMNoTopic
			}

			rc.sns = append(rc.sns, amSNS{
				topic: s.TopicARN, target: s.TargetARN, phone: s.PhoneNumber, subject: s.Subject,
				message: s.Message, attrs: s.Attributes, sendResolved: s.SendResolved == nil || *s.SendResolved,
			})
		}

		out[d.Name] = rc
	}

	return out, nil
}

func buildInhibit(defs []amInhibitDef) ([]amInhibit, error) {
	out := make([]amInhibit, 0, len(defs))

	for _, d := range defs {
		src, err := buildMatchers(d.SourceMatch, d.SourceMatchRE, d.SourceMatchers)
		if err != nil {
			return nil, err
		}

		tgt, err := buildMatchers(d.TargetMatch, d.TargetMatchRE, d.TargetMatchers)
		if err != nil {
			return nil, err
		}

		out = append(out, amInhibit{source: src, target: tgt, equal: d.Equal})
	}

	return out, nil
}

func checkReceivers(r *amRoute, recv map[string]*amReceiver) error {
	if _, ok := recv[r.receiver]; !ok {
		return fmt.Errorf("%w: %q", errAMReceiver, r.receiver)
	}

	for _, c := range r.children {
		if err := checkReceivers(c, recv); err != nil {
			return err
		}
	}

	return nil
}

// parseAlertManagerConfig builds the routing tree, inhibition rules and receivers of an alertmanager_config document.
func parseAlertManagerConfig(raw []byte) (*amConfig, error) {
	var def amConfigDef
	if err := yaml.Unmarshal(raw, &def); err != nil {
		return nil, err
	}

	if def.Route == nil {
		return nil, errAMNoRoute
	}

	if len(def.Route.Match)+len(def.Route.MatchRE)+len(def.Route.Matchers) > 0 {
		return nil, errAMRootMatcher
	}

	recv, err := buildReceivers(def.Receivers)
	if err != nil {
		return nil, err
	}

	root, err := (&routeBuilder{}).build(def.Route, nil, 0)
	if err != nil {
		return nil, err
	}

	if cerr := checkReceivers(root, recv); cerr != nil {
		return nil, cerr
	}

	inh, err := buildInhibit(def.InhibitRules)
	if err != nil {
		return nil, err
	}

	return &amConfig{root: root, receivers: recv, inhibit: inh}, nil
}
