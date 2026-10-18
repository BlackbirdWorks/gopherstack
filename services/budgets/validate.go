package budgets

import (
	"net/mail"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxNameLen            = 100
	maxNotifications      = 5
	maxSubscribers        = 11
	maxActionsPerBudget   = 10
	maxTags               = 50
	maxTagKeyLen          = 128
	maxTagValueLen        = 256
	defaultPageSize       = 100
	maxPageSize           = 100
	maxBudgetsPageSize    = 1000
	maxAutoAdjustPeriods  = 60
	unitPercentage        = "PERCENTAGE"
	defaultEndYear        = 2087
	defaultEndMonth       = time.June
	defaultEndDay         = 15
	budgetTypeCost        = "COST"
	budgetTypeUsage       = "USAGE"
	timeUnitDaily         = "DAILY"
	timeUnitMonthly       = "MONTHLY"
	timeUnitQuarterly     = "QUARTERLY"
	timeUnitAnnually      = "ANNUALLY"
	timeUnitCustom        = "CUSTOM"
	notifActual           = "ACTUAL"
	notifForecasted       = "FORECASTED"
	subEmail              = "EMAIL"
	subSNS                = "SNS"
	thresholdPercentage   = "PERCENTAGE"
	thresholdAbsolute     = "ABSOLUTE_VALUE"
	stateOK               = "OK"
	stateAlarm            = "ALARM"
	autoAdjustHistorical  = "HISTORICAL"
	autoAdjustForecast    = "FORECAST"
	approvalAutomatic     = "AUTOMATIC"
	approvalManual        = "MANUAL"
	actionIAM             = "APPLY_IAM_POLICY"
	actionSCP             = "APPLY_SCP_POLICY"
	actionSSM             = "RUN_SSM_DOCUMENTS"
	healthyStatus         = "HEALTHY"
	opGreater             = "GREATER_THAN"
	opLess                = "LESS_THAN"
	opEqual               = "EQUAL_TO"
	maxHistoricalMonthly  = 60
	maxHistoricalQuarter  = 20
	maxHistoricalAnnually = 5
)

var (
	accountRE = regexp.MustCompile(`^\d{12}$`)
	amountRE  = regexp.MustCompile(`^\d+(\.\d+)?$`)
	topicRE   = regexp.MustCompile(`^arn:aws[a-z-]*:sns:[a-z0-9-]+:\d{12}:[A-Za-z0-9_-]{1,256}$`)
	roleRE    = regexp.MustCompile(`^arn:aws[a-z-]*:iam::(\d{12}):role/.+$`)
	policyRE  = regexp.MustCompile(`^arn:aws[a-z-]*:iam::(\d{12}|aws):policy/.+$`)
	viewRE    = regexp.MustCompile(`^arn:aws[a-z-]*:billing::\d{12}:billingview/.+$`)
)

func validateAccount(id string) error {
	if !accountRE.MatchString(id) {
		return fail(ErrInvalidParameter, "AccountId must be a 12-digit account ID")
	}

	return nil
}

func validateName(name string) error {
	if name == "" || len(name) > maxNameLen || strings.ContainsAny(name, ":\\") || strings.Contains(name, "/action/") {
		return fail(ErrInvalidParameter,
			"BudgetName must be 1-%d characters without ':' or '\\' or the substring '/action/'", maxNameLen)
	}

	return nil
}

func validateAmount(s *Spend, what string) error {
	if s == nil {
		return nil
	}

	if !amountRE.MatchString(s.Amount) {
		return fail(ErrInvalidParameter, "%s amount %q is not a non-negative decimal", what, s.Amount)
	}

	if s.Unit == "" {
		return fail(ErrInvalidParameter, "%s unit is required", what)
	}

	return nil
}

func validateBudget(bg *Budget) error {
	if err := validateName(bg.BudgetName); err != nil {
		return err
	}

	if !validBudgetType(bg.BudgetType) {
		return fail(ErrInvalidParameter, "BudgetType %q is not valid", bg.BudgetType)
	}

	if !validTimeUnit(bg.TimeUnit) {
		return fail(ErrInvalidParameter, "TimeUnit %q is not valid", bg.TimeUnit)
	}

	if err := validateLimits(bg); err != nil {
		return err
	}

	if err := validateFilters(bg); err != nil {
		return err
	}

	if bg.TimePeriod != nil && bg.TimePeriod.Start != nil && bg.TimePeriod.End != nil &&
		!bg.TimePeriod.Start.Before(bg.TimePeriod.End.Time) {
		return fail(ErrInvalidParameter, "TimePeriod start must be before its end")
	}

	if bg.BillingViewArn != "" && !viewRE.MatchString(bg.BillingViewArn) {
		return fail(ErrInvalidParameter, "BillingViewArn %q is not a billing view ARN", bg.BillingViewArn)
	}

	for _, m := range bg.Metrics {
		if !validMetric(m) {
			return fail(ErrInvalidParameter, "Metric %q is not valid", m)
		}
	}

	return validateAutoAdjust(bg)
}

func validateLimits(bg *Budget) error {
	if err := validateAmount(bg.BudgetLimit, "BudgetLimit"); err != nil {
		return err
	}

	for k, v := range bg.PlannedBudgetLimits {
		if _, err := strconv.Atoi(k); err != nil {
			return fail(ErrInvalidParameter, "PlannedBudgetLimits key %q must be a period offset", k)
		}

		if err := validateAmount(&v, "PlannedBudgetLimits"); err != nil {
			return err
		}
	}

	if len(bg.PlannedBudgetLimits) > 0 {
		costOrUsage := bg.BudgetType == budgetTypeCost || bg.BudgetType == budgetTypeUsage
		if !costOrUsage || bg.TimeUnit == timeUnitDaily || bg.TimeUnit == timeUnitCustom || bg.AutoAdjustData != nil {
			return fail(ErrInvalidParameter,
				"PlannedBudgetLimits needs a monthly, quarterly or annual cost or usage budget without AutoAdjustData")
		}
	}

	if bg.BudgetLimit == nil && len(bg.PlannedBudgetLimits) == 0 && bg.AutoAdjustData == nil {
		return fail(ErrInvalidParameter, "BudgetLimit is required")
	}

	return nil
}

func validateFilters(bg *Budget) error {
	if len(bg.CostFilters) > 0 && bg.FilterExpression != nil {
		return fail(ErrInvalidParameter, "CostFilters and FilterExpression cannot both be set")
	}

	for k := range bg.CostFilters {
		if !validCostKey(k) {
			return fail(ErrInvalidParameter, "CostFilters key %q is not valid", k)
		}
	}

	if bg.FilterExpression != nil {
		return validateExpression(bg.FilterExpression)
	}

	return nil
}

func validateExpression(e *Expression) error {
	clauses := 0

	for _, f := range []*ValuesFilter{e.CostCategories, e.Dimensions, e.Tags, e.ProductAttributes} {
		if f != nil {
			clauses++
		}
	}

	for _, set := range []bool{e.Not != nil, len(e.And) > 0, len(e.Or) > 0} {
		if set {
			clauses++
		}
	}

	if clauses != 1 {
		return fail(ErrInvalidParameter, "each FilterExpression node must set exactly one clause")
	}

	children := slices.Concat(e.And, e.Or)
	if e.Not != nil {
		children = append(children, *e.Not)
	}

	for i := range children {
		if err := validateExpression(&children[i]); err != nil {
			return err
		}
	}

	return nil
}

func validateAutoAdjust(bg *Budget) error {
	a := bg.AutoAdjustData
	if a == nil {
		return nil
	}

	switch a.AutoAdjustType {
	case autoAdjustForecast:
		if a.HistoricalOptions != nil {
			return fail(ErrInvalidParameter, "HistoricalOptions is only valid for HISTORICAL auto-adjust")
		}
	case autoAdjustHistorical:
		if a.HistoricalOptions == nil {
			return fail(ErrInvalidParameter, "HistoricalOptions is required for HISTORICAL auto-adjust")
		}

		limit := map[string]int32{
			timeUnitMonthly: maxHistoricalMonthly, timeUnitQuarterly: maxHistoricalQuarter,
			timeUnitAnnually: maxHistoricalAnnually, timeUnitDaily: maxAutoAdjustPeriods,
		}[bg.TimeUnit]
		if p := a.HistoricalOptions.BudgetAdjustmentPeriod; p < 1 || p > limit {
			return fail(ErrInvalidParameter, "BudgetAdjustmentPeriod must be between 1 and %d", limit)
		}
	default:
		return fail(ErrInvalidParameter, "AutoAdjustType %q is not valid", a.AutoAdjustType)
	}

	return nil
}

func validateNotification(n *Notification) error {
	if n.NotificationType != notifActual && n.NotificationType != notifForecasted {
		return fail(ErrInvalidParameter, "NotificationType %q is not valid", n.NotificationType)
	}

	if !slices.Contains([]string{opGreater, opLess, opEqual}, n.ComparisonOperator) {
		return fail(ErrInvalidParameter, "ComparisonOperator %q is not valid", n.ComparisonOperator)
	}

	if n.ThresholdType == "" {
		n.ThresholdType = thresholdPercentage
	}

	if n.ThresholdType != thresholdPercentage && n.ThresholdType != thresholdAbsolute {
		return fail(ErrInvalidParameter, "ThresholdType %q is not valid", n.ThresholdType)
	}

	if n.Threshold < 0 {
		return fail(ErrInvalidParameter, "Threshold must not be negative")
	}

	return nil
}

func validateSubscribers(subs []Subscriber) error {
	if len(subs) == 0 || len(subs) > maxSubscribers {
		return fail(ErrInvalidParameter, "a notification needs between 1 and %d subscribers", maxSubscribers)
	}

	seen := map[Subscriber]bool{}

	for _, s := range subs {
		if err := validateSubscriber(s); err != nil {
			return err
		}

		if seen[s] {
			return fail(ErrDuplicateRecord, "duplicate subscriber %s", s.Address)
		}

		seen[s] = true
	}

	return nil
}

func validateSubscriber(s Subscriber) error {
	switch s.SubscriptionType {
	case subEmail:
		if _, err := mail.ParseAddress(s.Address); err != nil {
			return fail(ErrInvalidParameter, "subscriber address %q is not an email address", s.Address)
		}
	case subSNS:
		if !topicRE.MatchString(s.Address) {
			return fail(ErrInvalidParameter, "subscriber address %q is not an SNS topic ARN", s.Address)
		}
	default:
		return fail(ErrInvalidParameter, "SubscriptionType %q is not valid", s.SubscriptionType)
	}

	return nil
}

func validateTags(in []ResourceTag) (map[string]string, error) {
	out := make(map[string]string, len(in))

	for _, t := range in {
		if t.Key == "" || len(t.Key) > maxTagKeyLen || len(t.Value) > maxTagValueLen {
			return nil, fail(ErrInvalidParameter, "tag %q is invalid", t.Key)
		}

		if _, dup := out[t.Key]; dup {
			return nil, fail(ErrInvalidParameter, "duplicate tag key %q", t.Key)
		}

		out[t.Key] = t.Value
	}

	if len(out) > maxTags {
		return nil, fail(ErrCreationLimitExceeded, "a resource can have at most %d tags", maxTags)
	}

	return out, nil
}

func pageSize(limit *int32, upper int) (int, error) {
	if limit == nil {
		return defaultPageSize, nil
	}

	if *limit < 1 || int(*limit) > upper {
		return 0, fail(ErrInvalidParameter, "MaxResults must be between 1 and %d", upper)
	}

	return int(*limit), nil
}

func paginate[T any](all []T, token string, limit *int32, upper int) (page.Page[T], error) {
	if err := page.ValidateToken(token); err != nil {
		return page.Page[T]{}, fail(ErrInvalidNextToken, "invalid NextToken")
	}

	size, err := pageSize(limit, upper)
	if err != nil {
		return page.Page[T]{}, err
	}

	if page.DecodeToken(token) > len(all) {
		return page.Page[T]{}, fail(ErrInvalidNextToken, "invalid NextToken")
	}

	return page.New(all, token, size, defaultPageSize), nil
}

func oneOf(v string, set ...string) bool { return slices.Contains(set, v) }

func validBudgetType(v string) bool {
	return oneOf(v, budgetTypeUsage, budgetTypeCost, "RI_UTILIZATION", "RI_COVERAGE",
		"SAVINGS_PLANS_UTILIZATION", "SAVINGS_PLANS_COVERAGE")
}

func validTimeUnit(v string) bool {
	return oneOf(v, timeUnitDaily, timeUnitMonthly, timeUnitQuarterly, timeUnitAnnually, timeUnitCustom)
}

func validCostKey(v string) bool {
	return oneOf(v, "AZ", "BillingEntity", "CostCategory", "InstanceType", "InvoicingEntity", "LegalEntityName",
		"LinkedAccount", "Operation", "PurchaseType", "Region", "Service", "SubscriptionId", "TagKeyValue",
		"UsageType", "UsageTypeGroup")
}

func validMetric(v string) bool {
	return oneOf(v, "BlendedCost", "UnblendedCost", "AmortizedCost", "NetUnblendedCost", "NetAmortizedCost",
		"UsageQuantity", "NormalizedUsageAmount", "Hours")
}
