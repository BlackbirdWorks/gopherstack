package cloudwatch

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

const (
	maxInsightFilters      = 4
	maxInsightFilterValues = 10

	insightFormatJSON = "JSON"
	insightFormatCLF  = "CLF"
	aggregateSum      = "Sum"
	aggregateCount    = "Count"
)

var jsonPathRe = regexp.MustCompile(`^\$(\.[A-Za-z][A-Za-z0-9_-]*(\[[0-9]+\])*)+$`)

type insightFilter struct {
	IsPresent     *bool    `json:"IsPresent"`
	GreaterThan   *float64 `json:"GreaterThan"`
	LessThan      *float64 `json:"LessThan"`
	EqualTo       *float64 `json:"EqualTo"`
	NotEqualTo    *float64 `json:"NotEqualTo"`
	Match         string   `json:"Match"`
	In            []string `json:"In"`
	NotIn         []string `json:"NotIn"`
	StartsWith    []string `json:"StartsWith"`
	NotStartsWith []string `json:"NotStartsWith"`
}

type insightContribution struct {
	ValueOf string          `json:"ValueOf"`
	Keys    []string        `json:"Keys"`
	Filters []insightFilter `json:"Filters"`
}

type insightRuleSpec struct {
	Fields        map[string]string   `json:"Fields"`
	LogFormat     string              `json:"LogFormat"`
	AggregateOn   string              `json:"AggregateOn"`
	LogGroupNames []string            `json:"LogGroupNames"`
	Contribution  insightContribution `json:"Contribution"`
}

func parseInsightRuleSpec(definition string) (*insightRuleSpec, error) {
	var spec insightRuleSpec
	if err := json.Unmarshal([]byte(definition), &spec); err != nil {
		return nil, fmt.Errorf("%w: RuleDefinition has an invalid field type: %s", ErrValidation, err.Error())
	}

	return &spec, nil
}

// aggregatesSum reports whether the rule ranks contributors by ValueOf rather than occurrences.
func (s *insightRuleSpec) aggregatesSum() bool {
	return s.AggregateOn == aggregateSum || (s.AggregateOn == "" && s.Contribution.ValueOf != "")
}

func validateInsightRuleSpec(definition string) error {
	spec, err := parseInsightRuleSpec(definition)
	if err != nil {
		return err
	}

	if err = spec.validateFields(); err != nil {
		return err
	}

	paths := append([]string{}, spec.Contribution.Keys...)
	if spec.Contribution.ValueOf != "" {
		paths = append(paths, spec.Contribution.ValueOf)
	}

	for _, f := range spec.Contribution.Filters {
		paths = append(paths, f.Match)
	}

	for _, p := range paths {
		if err = spec.validatePath(p); err != nil {
			return err
		}
	}

	return spec.validateFilters()
}

func (s *insightRuleSpec) validateFields() error {
	if len(s.Fields) > 0 && s.LogFormat != insightFormatCLF {
		return fmt.Errorf("%w: RuleDefinition.Fields is only valid with LogFormat CLF", ErrValidation)
	}

	for idx, alias := range s.Fields {
		if n, err := strconv.Atoi(idx); err != nil || n < 1 || alias == "" {
			return fmt.Errorf("%w: RuleDefinition.Fields maps positions from 1 to alias names", ErrValidation)
		}
	}

	return nil
}

func (s *insightRuleSpec) validatePath(p string) error {
	if s.LogFormat == insightFormatJSON {
		if !jsonPathRe.MatchString(p) {
			return fmt.Errorf("%w: %q is not a valid JSON property path like $.name", ErrValidation, p)
		}

		return nil
	}

	if _, ok := s.clfIndex(p); !ok {
		return fmt.Errorf("%w: %q is not a CLF field position or a Fields alias", ErrValidation, p)
	}

	return nil
}

func (s *insightRuleSpec) clfIndex(name string) (int, bool) {
	for idx, alias := range s.Fields {
		if alias == name {
			n, err := strconv.Atoi(idx)

			return n, err == nil
		}
	}

	n, err := strconv.Atoi(name)

	return n, err == nil && n >= 1
}

func (s *insightRuleSpec) validateFilters() error {
	if len(s.Contribution.Filters) > maxInsightFilters {
		return fmt.Errorf("%w: Contribution.Filters allows at most %d entries", ErrInsightRuleLimit, maxInsightFilters)
	}

	for _, f := range s.Contribution.Filters {
		if f.Match == "" {
			return fmt.Errorf("%w: every filter requires Match", ErrValidation)
		}

		if f.operatorCount() != 1 {
			return fmt.Errorf("%w: filter on %s needs exactly one matching operator", ErrValidation, f.Match)
		}

		for _, list := range [][]string{f.In, f.NotIn, f.StartsWith, f.NotStartsWith} {
			if len(list) > maxInsightFilterValues {
				return fmt.Errorf(
					"%w: filter value lists allow at most %d entries", ErrInsightRuleLimit, maxInsightFilterValues,
				)
			}
		}
	}

	return nil
}

func (f insightFilter) operatorCount() int {
	n := 0

	for _, set := range []bool{
		f.In != nil, f.NotIn != nil, f.StartsWith != nil, f.NotStartsWith != nil,
		f.GreaterThan != nil, f.LessThan != nil, f.EqualTo != nil, f.NotEqualTo != nil, f.IsPresent != nil,
	} {
		if set {
			n++
		}
	}

	return n
}

// keyLabels returns the rule's Contribution.Keys as written.
func (s *insightRuleSpec) keyLabels() []string {
	return append([]string{}, s.Contribution.Keys...)
}

func trimQuotes(s string) string {
	if len(s) >= 2 && ((s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '[' && s[len(s)-1] == ']')) {
		return s[1 : len(s)-1]
	}

	return strings.TrimSpace(s)
}
