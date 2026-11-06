package connect

import (
	"context"
	"encoding/json"
	"regexp"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

const (
	metricCustomer  = "CUSTOMER_MANAGED"
	metricAWS       = "AWS_MANAGED"
	metricSaved     = "SAVED"
	metricPublished = "PUBLISHED"
	metricCategory  = "CUSTOM"
	labelMetric     = "Metric"
	maxMetricName   = 128
	maxMetricDesc   = 500
	maxCalcLen      = 1024
	maxCalcParts    = 5
)

var (
	identPattern = regexp.MustCompile(`[A-Za-z_][A-Za-z0-9_]*`)
	callPattern  = regexp.MustCompile(`[A-Za-z_][A-Za-z0-9_]*\s*\(`)
)

// CalculationComponent is one metric a custom calculation refers to by alias.
type CalculationComponent struct {
	Alias         string          `json:"Alias"`
	MetricFilters json.RawMessage `json:"MetricFilters,omitempty"`
	MetricID      string          `json:"MetricId,omitempty"`
	MetricName    string          `json:"MetricName,omitempty"`
}

// MetricCalculation is the formula of a custom metric.
type MetricCalculation struct {
	Calculation           string                 `json:"Calculation"`
	CalculationComponents []CalculationComponent `json:"CalculationComponents"`
}

// MetricDefinition is a metric definition.
type MetricDefinition struct {
	Stamp
	CreatedTime            *awstime.EpochTime `json:"CreatedTime,omitempty"`
	MetricCalculation      *MetricCalculation `json:"MetricCalculation,omitempty"`
	Tags                   map[string]string  `json:"Tags,omitempty"`
	Filters                []any              `json:"Filters"`
	Groupings              []string           `json:"Groupings"`
	Arn                    string             `json:"Arn"`
	Category               string             `json:"Category"`
	Description            string             `json:"Description,omitempty"`
	ID                     string             `json:"Id"`
	Name                   string             `json:"Name"`
	PositiveTrendIndicator string             `json:"PositiveTrendIndicator,omitempty"`
	Status                 string             `json:"Status,omitempty"`
	Type                   string             `json:"Type"`
	Unit                   string             `json:"Unit"`
	SupportsCustomCalc     bool               `json:"SupportsCustomCalculation"`
	SupportsPreaggregate   bool               `json:"SupportsPreaggregateCalculation"`
}

// MetricSummary is the list shape of a metric definition.
type MetricSummary struct {
	Stamp
	Arn    string `json:"Arn"`
	ID     string `json:"Id"`
	Name   string `json:"Name"`
	Status string `json:"Status"`
	Type   string `json:"Type"`
}

type metricRec struct {
	InstanceID string           `json:"instanceId"`
	Metric     MetricDefinition `json:"metric"`
}

type metricRef struct {
	MetricID string `json:"-" path:"MetricId"`
	instPath
}

// CreateMetricInput is the CreateMetric request.
type CreateMetricInput struct {
	MetricCalculation      *MetricCalculation
	Tags                   map[string]string
	ClientToken            string
	Description            string
	Name                   string
	PositiveTrendIndicator string
	Status                 string
	Unit                   string
	instPath
}

type metricIDsOut struct {
	MetricArn string `json:"MetricArn"`
	MetricID  string `json:"MetricId"`
}

// UpdateMetricContentInput is the UpdateMetricContent request.
type UpdateMetricContentInput struct {
	MetricCalculation      *MetricCalculation
	PositiveTrendIndicator string
	Unit                   string
	metricRef
}

// UpdateMetricMetadataInput is the UpdateMetricMetadata request.
type UpdateMetricMetadataInput struct {
	Description *string
	Name        *string
	metricRef
}

func checkMetricCalculation(c *MetricCalculation) error {
	if c == nil {
		return invalidParamErr("MetricCalculation is required")
	}

	if c.Calculation == "" || len(c.Calculation) > maxCalcLen {
		return invalidParamErr("Calculation must be 1-%d characters", maxCalcLen)
	}

	if n := len(c.CalculationComponents); n < 1 || n > maxCalcParts {
		return invalidParamErr("CalculationComponents must hold 1-%d items", maxCalcParts)
	}

	aliases := map[string]bool{}

	for _, comp := range c.CalculationComponents {
		if !identPattern.MatchString(comp.Alias) || identPattern.FindString(comp.Alias) != comp.Alias {
			return invalidParamErr("component alias %q is not an identifier", comp.Alias)
		}

		if aliases[comp.Alias] {
			return invalidParamErr("component alias %q is repeated", comp.Alias)
		}

		if (comp.MetricID == "") == (comp.MetricName == "") {
			return invalidParamErr("component %q needs exactly one of MetricId and MetricName", comp.Alias)
		}

		aliases[comp.Alias] = true
	}

	return checkFormula(c.Calculation, aliases)
}

func checkFormula(formula string, aliases map[string]bool) error {
	if strings.Count(formula, "(") != strings.Count(formula, ")") {
		return invalidParamErr("Calculation has unbalanced parentheses")
	}

	calls := map[string]bool{}
	for _, m := range callPattern.FindAllString(formula, -1) {
		calls[strings.TrimSpace(strings.TrimSuffix(m, "("))] = true
	}

	used := map[string]bool{}

	for _, id := range identPattern.FindAllString(formula, -1) {
		switch {
		case aliases[id]:
			used[id] = true
		case calls[id]:
		default:
			return invalidParamErr("Calculation refers to %q, which is not a component alias", id)
		}
	}

	for a := range aliases {
		if !used[a] {
			return invalidParamErr("component alias %q is not used in the Calculation", a)
		}
	}

	return nil
}

func checkMetricFields(name, desc, unit, trend, status string) error {
	if name != "" && len(name) > maxMetricName {
		return invalidParamErr("Name must be at most %d characters", maxMetricName)
	}

	if len(desc) > maxMetricDesc {
		return invalidParamErr("Description must be at most %d characters", maxMetricDesc)
	}

	if unit != "" {
		if err := oneOf("Unit", unit, "INTEGER", "DOUBLE", "PERCENT", "SECONDS"); err != nil {
			return err
		}
	}

	if trend != "" {
		if err := oneOf("PositiveTrendIndicator", trend, "POSITIVE", "NEGATIVE", "NEUTRAL"); err != nil {
			return err
		}
	}

	if status != "" {
		return oneOf("Status", status, metricPublished, metricSaved)
	}

	return nil
}

func (b *Backend) touchMetric(ctx context.Context, m *metricRec) {
	m.Metric.Stamp = b.stamp(ctx)
}

func (m *metricRec) view() MetricDefinition {
	out := m.Metric
	out.Tags = cloneTags(out.Tags)

	return out
}

// CreateMetric creates a customer-managed metric definition.
func (b *Backend) CreateMetric(ctx context.Context, in *CreateMetricInput) (*metricIDsOut, error) {
	if strings.TrimSpace(in.Name) == "" || in.Unit == "" {
		return nil, invalidParamErr("Name and Unit are required")
	}

	if err := firstError(
		checkMetricFields(in.Name, in.Description, in.Unit, in.PositiveTrendIndicator, in.Status),
		checkMetricCalculation(in.MetricCalculation), checkTags(in.Tags),
	); err != nil {
		return nil, err
	}

	return writeInstance(b, "CreateMetric", in.InstanceID, func(inst string) (*metricIDsOut, error) {
		if nameExists(b.metricDefs, inst, func(m *metricRec) string { return m.Metric.Name }, in.Name, "") {
			return nil, duplicateErr(labelMetric, in.Name)
		}

		status := in.Status
		if status == "" {
			status = metricSaved
		}

		id := newID()
		rec := &metricRec{InstanceID: inst, Metric: MetricDefinition{
			Arn: b.buildARN(ctx, "instance/"+inst+"/metric/"+id), ID: id, Name: in.Name, Category: metricCategory,
			Description: in.Description, Unit: in.Unit, Type: metricCustomer, Status: status,
			PositiveTrendIndicator: in.PositiveTrendIndicator, MetricCalculation: in.MetricCalculation,
			Tags: cloneTags(in.Tags), Filters: []any{}, Groupings: []string{},
		}}
		t := awstime.NewEpochTime(b.now())
		rec.Metric.CreatedTime = &t
		b.touchMetric(ctx, rec)
		b.metricDefs.put(rec)

		return &metricIDsOut{MetricArn: rec.Metric.Arn, MetricID: id}, nil
	})
}

// DescribeMetric returns a customer-managed metric definition.
func (b *Backend) DescribeMetric(inst, id string) (*MetricDefinition, error) {
	return readInstance(b, "DescribeMetric", inst, func(inst string) (*MetricDefinition, error) {
		m, err := b.metricDefs.get(inst, id)
		if err != nil {
			return nil, err
		}

		out := m.view()

		return &out, nil
	})
}

// ListMetrics pages through the metric definitions; AWS-managed metrics are not modeled.
func (b *Backend) ListMetrics(inst, typ, token string, maxResults int32) ([]MetricSummary, string, error) {
	if typ != "" {
		if err := oneOf("type", typ, metricAWS, metricCustomer); err != nil {
			return nil, "", err
		}
	}

	all, err := readInstance(b, "ListMetrics", inst, func(inst string) ([]MetricSummary, error) {
		var out []MetricSummary

		if typ == metricAWS {
			return out, nil
		}

		for _, m := range b.metricDefs.in(inst) {
			out = append(out, MetricSummary{
				Stamp: m.Metric.Stamp, Arn: m.Metric.Arn, ID: m.Metric.ID, Name: m.Metric.Name,
				Status: m.Metric.Status, Type: m.Metric.Type,
			})
		}

		return out, nil
	})
	if err != nil {
		return nil, "", err
	}

	slices.SortFunc(all, func(a, c MetricSummary) int {
		return strings.Compare(strings.ToLower(a.Name), strings.ToLower(c.Name))
	})

	return paged(all, token, maxResults)
}

// UpdateMetricContent changes the calculation, unit or trend of a metric.
func (b *Backend) UpdateMetricContent(ctx context.Context, in *UpdateMetricContentInput) (*empty, error) {
	if err := checkMetricFields("", "", in.Unit, in.PositiveTrendIndicator, ""); err != nil {
		return nil, err
	}

	if in.MetricCalculation != nil {
		if err := checkMetricCalculation(in.MetricCalculation); err != nil {
			return nil, err
		}
	}

	return writeInstance(b, "UpdateMetricContent", in.InstanceID, func(inst string) (*empty, error) {
		m, err := b.metricDefs.get(inst, in.MetricID)
		if err != nil {
			return nil, err
		}

		if in.MetricCalculation != nil {
			m.Metric.MetricCalculation = in.MetricCalculation
		}

		if in.Unit != "" {
			m.Metric.Unit = in.Unit
		}

		if in.PositiveTrendIndicator != "" {
			m.Metric.PositiveTrendIndicator = in.PositiveTrendIndicator
		}

		b.touchMetric(ctx, m)

		return &empty{}, nil
	})
}

// UpdateMetricMetadata renames a metric or changes its description.
func (b *Backend) UpdateMetricMetadata(ctx context.Context, in *UpdateMetricMetadataInput) (*empty, error) {
	name, desc := "", ""
	if in.Name != nil {
		name = *in.Name
		if strings.TrimSpace(name) == "" {
			return nil, invalidParamErr("Name must not be empty")
		}
	}

	if in.Description != nil {
		desc = *in.Description
	}

	if err := checkMetricFields(name, desc, "", "", ""); err != nil {
		return nil, err
	}

	return writeInstance(b, "UpdateMetricMetadata", in.InstanceID, func(inst string) (*empty, error) {
		m, err := b.metricDefs.get(inst, in.MetricID)
		if err != nil {
			return nil, err
		}

		if in.Name != nil {
			if nameExists(b.metricDefs, inst, func(r *metricRec) string { return r.Metric.Name }, name, m.Metric.ID) {
				return nil, duplicateErr(labelMetric, name)
			}

			m.Metric.Name = name
		}

		if in.Description != nil {
			m.Metric.Description = desc
		}

		b.touchMetric(ctx, m)

		return &empty{}, nil
	})
}

// DeleteMetric removes a metric definition.
func (b *Backend) DeleteMetric(inst, id string) (*empty, error) {
	return writeInstance(b, "DeleteMetric", inst, func(inst string) (*empty, error) {
		return nil, b.metricDefs.del(inst, id)
	})
}

// SearchMetrics searches the customer-managed metric definitions.
func (b *Backend) SearchMetrics(in *SearchRequest) (*SearchResult, error) {
	return runSearch(b, "SearchMetrics", "Metrics", in, b.metricDefs, nameDescSpec(),
		func(m *metricRec) searchSubject {
			return searchSubject{fields: nameDesc(m.Metric.Name, m.Metric.Description), tags: m.Metric.Tags}
		},
		func(m *metricRec) MetricDefinition { return m.view() })
}
