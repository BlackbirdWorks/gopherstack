package ce

import (
	"fmt"
	"net/mail"
	"slices"
	"strings"
	"time"
)

const (
	dateLayout     = "2006-01-02"
	dateTimeLayout = "2006-01-02T15:04:05Z"
	maxGroupBy     = 2
)

const (
	groupTypeDimension = "DIMENSION"
	metricBlendedCost  = "BlendedCost"
)

func isCostMetric(m string) bool {
	return slices.Contains([]string{
		"AmortizedCost", metricBlendedCost, "NetAmortizedCost", "NetUnblendedCost",
		"NormalizedUsageAmount", "UnblendedCost", "UsageQuantity",
	}, m)
}

func isDimensionKey(k string) bool {
	return slices.Contains([]string{
		"AZ", "INSTANCE_TYPE", dimKeyLinkedAccount, "PAYER_ACCOUNT", "LINKED_ACCOUNT_NAME", "OPERATION",
		"PURCHASE_TYPE", "REGION", dimKeyService, "SERVICE_CODE", "USAGE_TYPE", "USAGE_TYPE_GROUP",
		"RECORD_TYPE", "OPERATING_SYSTEM", "TENANCY", "SCOPE", "PLATFORM", "SUBSCRIPTION_ID",
		"LEGAL_ENTITY_NAME", "DEPLOYMENT_OPTION", "DATABASE_ENGINE", "CACHE_ENGINE", "INSTANCE_TYPE_FAMILY",
		"BILLING_ENTITY", "RESERVATION_ID", "RESOURCE_ID", "RIGHTSIZING_TYPE", "SAVINGS_PLANS_TYPE",
		"SAVINGS_PLAN_ARN", "PAYMENT_OPTION", "INVOICING_ENTITY", "AGREEMENT_END_DATE_TIME_AFTER",
		"AGREEMENT_END_DATE_TIME_BEFORE", "ANOMALY_TOTAL_IMPACT_ABSOLUTE", "ANOMALY_TOTAL_IMPACT_PERCENTAGE",
	}, k)
}

// parseTimePeriod parses Start/End; HOURLY also accepts YYYY-MM-DDThh:mm:ssZ.
func parseTimePeriod(start, end, granularity string) (time.Time, time.Time, error) {
	parse := func(field, v string) (time.Time, error) {
		if t, err := time.Parse(dateLayout, v); err == nil {
			return t, nil
		}

		if granularity == granularityHourly {
			if t, err := time.Parse(dateTimeLayout, v); err == nil {
				return t, nil
			}
		}

		return time.Time{}, fmt.Errorf("%w: TimePeriod.%s must be a date in YYYY-MM-DD format", ErrValidation, field)
	}

	s, err := parse("Start", start)
	if err != nil {
		return s, s, err
	}

	e, err := parse("End", end)
	if err != nil {
		return s, e, err
	}

	if !s.Before(e) {
		return s, e, fmt.Errorf("%w: Start date (and hour) should be before end date (and hour)", ErrValidation)
	}

	return s, e, nil
}

func validateCostUsageMetrics(metrics []string) error {
	for _, m := range metrics {
		if !isCostMetric(m) {
			return fmt.Errorf("%w: Metrics value %q is not valid", ErrValidation, m)
		}
	}

	return nil
}

func validateGroupBy(groupBy []groupBySpec) error {
	if len(groupBy) > maxGroupBy {
		return fmt.Errorf("%w: GroupBy supports at most %d group definitions", ErrValidation, maxGroupBy)
	}

	for _, g := range groupBy {
		switch g.Type {
		case groupTypeDimension:
			if !isDimensionKey(g.Key) {
				return fmt.Errorf("%w: GroupBy dimension %q is not valid", ErrValidation, g.Key)
			}
		case "TAG", "COST_CATEGORY":
			if g.Key == "" {
				return fmt.Errorf("%w: GroupBy.Key is required for type %s", ErrValidation, g.Type)
			}
		default:
			return fmt.Errorf("%w: GroupBy.Type %q is not valid", ErrValidation, g.Type)
		}
	}

	return nil
}

func isForecastMetric(m string) bool {
	return slices.Contains([]string{
		"BLENDED_COST", "UNBLENDED_COST", "AMORTIZED_COST", "NET_UNBLENDED_COST",
		"NET_AMORTIZED_COST", "USAGE_QUANTITY", "NORMALIZED_USAGE_AMOUNT",
	}, m)
}

func validateContext(ctx string) error {
	if ctx != "" && !slices.Contains([]string{"COST_AND_USAGE", "RESERVATIONS", "SAVINGS_PLANS"}, ctx) {
		return fmt.Errorf("%w: Context must be one of COST_AND_USAGE, RESERVATIONS, SAVINGS_PLANS", ErrValidation)
	}

	return nil
}

func validateForecastPeriod(start, end, metric string) error {
	if !isForecastMetric(metric) {
		return fmt.Errorf("%w: Metric %q is not valid", ErrValidation, metric)
	}

	_, _, err := parseTimePeriod(start, end, granularityDaily)

	return err
}

func validateDimensionName(dim string) error {
	if dim == "" {
		return fmt.Errorf("%w: Dimension is required", ErrValidation)
	}

	if !isDimensionKey(dim) {
		return fmt.Errorf("%w: Dimension %q is not valid", ErrValidation, dim)
	}

	return nil
}

func validateMonitorDimension(dim string) error {
	if dim != "" && dim != "SERVICE" && dim != "LINKED_ACCOUNT" {
		return fmt.Errorf("%w: MonitorDimension must be SERVICE or LINKED_ACCOUNT", ErrValidation)
	}

	return nil
}

func validateSubscribers(subs []subscriberInput) error {
	for _, s := range subs {
		switch s.Type {
		case "EMAIL":
			if _, err := mail.ParseAddress(s.Address); err != nil {
				return fmt.Errorf("%w: subscriber address %q is not a valid email address", ErrValidation, s.Address)
			}
		case "SNS":
			if !strings.HasPrefix(s.Address, "arn:") || strings.Count(s.Address, ":") < 5 {
				return fmt.Errorf("%w: subscriber address %q is not a valid SNS topic ARN", ErrValidation, s.Address)
			}
		default:
			return fmt.Errorf("%w: subscriber Type must be EMAIL or SNS", ErrValidation)
		}
	}

	return nil
}
