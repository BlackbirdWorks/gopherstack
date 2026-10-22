package amp

import (
	"time"

	"gopkg.in/yaml.v3"

	"github.com/blackbirdworks/gopherstack/services/amp/promql"
)

const (
	defaultRuleInterval = time.Minute
	maxRuleGroups       = 1000
	maxRulesPerGroup    = 10000
)

type ruleFileDef struct {
	Groups []ruleGroupDef `yaml:"groups"`
}

type ruleGroupDef struct {
	Name        string    `yaml:"name"`
	Interval    string    `yaml:"interval"`
	QueryOffset string    `yaml:"query_offset"`
	Rules       []ruleDef `yaml:"rules"`
	Limit       int       `yaml:"limit"`
}

type ruleDef struct {
	Labels        map[string]string `yaml:"labels"`
	Annotations   map[string]string `yaml:"annotations"`
	Record        string            `yaml:"record"`
	Alert         string            `yaml:"alert"`
	Expr          string            `yaml:"expr"`
	For           string            `yaml:"for"`
	KeepFiringFor string            `yaml:"keep_firing_for"`
}

// parsedRule is a rule with its expression parsed and durations resolved.
type parsedRule struct {
	expr          promql.Expr
	def           ruleDef
	forDur        time.Duration
	keepFiringFor time.Duration
}

type parsedGroup struct {
	name        string
	rules       []parsedRule
	interval    time.Duration
	queryOffset time.Duration
	limit       int
}

func parseDur(raw string, def time.Duration) (time.Duration, error) {
	if raw == "" {
		return def, nil
	}

	return promql.ParseDuration(raw)
}

func parseRule(def ruleDef) (parsedRule, error) {
	expr, err := promql.Parse(def.Expr)
	if err != nil {
		return parsedRule{}, err
	}

	forDur, err := parseDur(def.For, 0)
	if err != nil {
		return parsedRule{}, err
	}

	kff, err := parseDur(def.KeepFiringFor, 0)
	if err != nil {
		return parsedRule{}, err
	}

	return parsedRule{def: def, expr: expr, forDur: forDur, keepFiringFor: kff}, nil
}

func parseGroup(def ruleGroupDef) (parsedGroup, error) {
	interval, err := parseDur(def.Interval, defaultRuleInterval)
	if err != nil || interval <= 0 {
		return parsedGroup{}, errBadRuleDuration
	}

	offset, err := parseDur(def.QueryOffset, 0)
	if err != nil {
		return parsedGroup{}, errBadRuleDuration
	}

	g := parsedGroup{name: def.Name, interval: interval, queryOffset: offset, limit: def.Limit}

	for _, rd := range def.Rules {
		r, perr := parseRule(rd)
		if perr != nil {
			return parsedGroup{}, perr
		}

		g.rules = append(g.rules, r)
	}

	return g, nil
}

// parseRuleFile decodes a rules file into parsed groups; every expression must parse as PromQL.
func parseRuleFile(data []byte) ([]parsedGroup, error) {
	var f ruleFileDef
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, err
	}

	if len(f.Groups) > maxRuleGroups {
		return nil, errTooManyRules
	}

	out := make([]parsedGroup, 0, len(f.Groups))

	for _, gd := range f.Groups {
		if len(gd.Rules) > maxRulesPerGroup {
			return nil, errTooManyRules
		}

		g, err := parseGroup(gd)
		if err != nil {
			return nil, err
		}

		out = append(out, g)
	}

	return out, nil
}
