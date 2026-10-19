package budgets

import (
	"encoding/json"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// Wire shapes follow the pinned aws-sdk-go-v2/service/budgets v1.53.1 awsjson1.1 schemas;
// timestamps are epoch-seconds numbers. The same structs are the persisted model.

// Timestamp marshals as epoch seconds.
type Timestamp struct{ time.Time }

// MarshalJSON renders the epoch-seconds number the SDK expects.
func (t Timestamp) MarshalJSON() ([]byte, error) { return json.Marshal(awstime.Epoch(t.Time)) }

// UnmarshalJSON reads an epoch-seconds number.
func (t *Timestamp) UnmarshalJSON(data []byte) error {
	var f float64
	if err := json.Unmarshal(data, &f); err != nil {
		return err
	}

	sec := int64(f)
	t.Time = time.Unix(sec, int64((f-float64(sec))*float64(time.Second))).UTC()

	return nil
}

func ts(t time.Time) *Timestamp { return &Timestamp{Time: t.UTC()} }

// Spend is an amount with a unit.
type Spend struct {
	Amount string `json:"Amount"`
	Unit   string `json:"Unit"`
}

// TimePeriod is a half-open budget window.
type TimePeriod struct {
	Start *Timestamp `json:"Start,omitempty"`
	End   *Timestamp `json:"End,omitempty"`
}

// HistoricalOptions configures historical auto-adjust.
type HistoricalOptions struct {
	BudgetAdjustmentPeriod   int32 `json:"BudgetAdjustmentPeriod"`
	LookBackAvailablePeriods int32 `json:"LookBackAvailablePeriods,omitempty"`
}

// AutoAdjustData configures automatic limit adjustment.
type AutoAdjustData struct {
	HistoricalOptions  *HistoricalOptions `json:"HistoricalOptions,omitempty"`
	LastAutoAdjustTime *Timestamp         `json:"LastAutoAdjustTime,omitempty"`
	AutoAdjustType     string             `json:"AutoAdjustType"`
}

// CostTypes selects which charges a budget includes.
type CostTypes struct {
	IncludeCredit            *bool `json:"IncludeCredit,omitempty"`
	IncludeDiscount          *bool `json:"IncludeDiscount,omitempty"`
	IncludeOtherSubscription *bool `json:"IncludeOtherSubscription,omitempty"`
	IncludeRecurring         *bool `json:"IncludeRecurring,omitempty"`
	IncludeRefund            *bool `json:"IncludeRefund,omitempty"`
	IncludeSubscription      *bool `json:"IncludeSubscription,omitempty"`
	IncludeSupport           *bool `json:"IncludeSupport,omitempty"`
	IncludeTax               *bool `json:"IncludeTax,omitempty"`
	IncludeUpfront           *bool `json:"IncludeUpfront,omitempty"`
	UseAmortized             *bool `json:"UseAmortized,omitempty"`
	UseBlended               *bool `json:"UseBlended,omitempty"`
}

// ValuesFilter is the shared shape of CostCategories, Dimensions, Tags and ProductAttributes clauses.
type ValuesFilter struct {
	Key          string   `json:"Key,omitempty"`
	Values       []string `json:"Values,omitempty"`
	MatchOptions []string `json:"MatchOptions,omitempty"`
}

// Expression is a budget filter expression tree.
type Expression struct {
	CostCategories    *ValuesFilter `json:"CostCategories,omitempty"`
	Dimensions        *ValuesFilter `json:"Dimensions,omitempty"`
	Tags              *ValuesFilter `json:"Tags,omitempty"`
	ProductAttributes *ValuesFilter `json:"ProductAttributes,omitempty"`
	Not               *Expression   `json:"Not,omitempty"`
	And               []Expression  `json:"And,omitempty"`
	Or                []Expression  `json:"Or,omitempty"`
}

// HealthStatus reports billing-view health.
type HealthStatus struct {
	LastUpdatedTime *Timestamp `json:"LastUpdatedTime,omitempty"`
	Status          string     `json:"Status,omitempty"`
	StatusReason    string     `json:"StatusReason,omitempty"`
}

// CalculatedSpend is the derived spend of the current period.
type CalculatedSpend struct {
	ForecastedSpend *Spend `json:"ForecastedSpend,omitempty"`
	ActualSpend     Spend  `json:"ActualSpend"`
}

// Budget is a budget as stored and returned.
type Budget struct {
	AutoAdjustData      *AutoAdjustData     `json:"AutoAdjustData,omitempty"`
	BudgetLimit         *Spend              `json:"BudgetLimit,omitempty"`
	CalculatedSpend     *CalculatedSpend    `json:"CalculatedSpend,omitempty"`
	CostFilters         map[string][]string `json:"CostFilters,omitempty"`
	CostTypes           *CostTypes          `json:"CostTypes,omitempty"`
	FilterExpression    *Expression         `json:"FilterExpression,omitempty"`
	HealthStatus        *HealthStatus       `json:"HealthStatus,omitempty"`
	LastUpdatedTime     *Timestamp          `json:"LastUpdatedTime,omitempty"`
	PlannedBudgetLimits map[string]Spend    `json:"PlannedBudgetLimits,omitempty"`
	TimePeriod          *TimePeriod         `json:"TimePeriod,omitempty"`
	BillingViewArn      string              `json:"BillingViewArn,omitempty"`
	BudgetName          string              `json:"BudgetName"`
	BudgetType          string              `json:"BudgetType"`
	TimeUnit            string              `json:"TimeUnit"`
	Metrics             []string            `json:"Metrics,omitempty"`
}

// Notification is a budget alert rule.
type Notification struct {
	ComparisonOperator string  `json:"ComparisonOperator"`
	NotificationState  string  `json:"NotificationState,omitempty"`
	NotificationType   string  `json:"NotificationType"`
	ThresholdType      string  `json:"ThresholdType,omitempty"`
	Threshold          float64 `json:"Threshold"`
}

// Subscriber is an alert recipient.
type Subscriber struct {
	Address          string `json:"Address"`
	SubscriptionType string `json:"SubscriptionType"`
}

// NotificationWithSubscribers pairs a notification with its recipients.
type NotificationWithSubscribers struct {
	Notification Notification `json:"Notification"`
	Subscribers  []Subscriber `json:"Subscribers"`
}

// ResourceTag is a key/value tag.
type ResourceTag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// ActionThreshold triggers an action.
type ActionThreshold struct {
	ActionThresholdType  string  `json:"ActionThresholdType"`
	ActionThresholdValue float64 `json:"ActionThresholdValue"`
}

// IamActionDefinition attaches a policy to principals.
type IamActionDefinition struct {
	PolicyArn string   `json:"PolicyArn"`
	Groups    []string `json:"Groups,omitempty"`
	Roles     []string `json:"Roles,omitempty"`
	Users     []string `json:"Users,omitempty"`
}

// ScpActionDefinition attaches a service control policy to targets.
type ScpActionDefinition struct {
	PolicyID  string   `json:"PolicyId"`
	TargetIDs []string `json:"TargetIds"`
}

// SsmActionDefinition runs a Systems Manager document against instances.
type SsmActionDefinition struct {
	ActionSubType string   `json:"ActionSubType"`
	Region        string   `json:"Region"`
	InstanceIDs   []string `json:"InstanceIds"`
}

// Definition holds the type-specific action parameters.
type Definition struct {
	IamActionDefinition *IamActionDefinition `json:"IamActionDefinition,omitempty"`
	ScpActionDefinition *ScpActionDefinition `json:"ScpActionDefinition,omitempty"`
	SsmActionDefinition *SsmActionDefinition `json:"SsmActionDefinition,omitempty"`
}

// Action is a budget action.
type Action struct {
	ActionThreshold  *ActionThreshold `json:"ActionThreshold,omitempty"`
	Definition       *Definition      `json:"Definition,omitempty"`
	ActionID         string           `json:"ActionId"`
	ActionType       string           `json:"ActionType"`
	ApprovalModel    string           `json:"ApprovalModel"`
	BudgetName       string           `json:"BudgetName"`
	ExecutionRoleArn string           `json:"ExecutionRoleArn"`
	NotificationType string           `json:"NotificationType"`
	Status           string           `json:"Status"`
	Subscribers      []Subscriber     `json:"Subscribers"`
}

// ActionHistoryDetails is the payload of a history entry.
type ActionHistoryDetails struct {
	Message string `json:"Message"`
	Action  Action `json:"Action"`
}

// ActionHistory is one action event.
type ActionHistory struct {
	Timestamp            Timestamp            `json:"Timestamp"`
	EventType            string               `json:"EventType"`
	Status               string               `json:"Status"`
	ActionHistoryDetails ActionHistoryDetails `json:"ActionHistoryDetails"`
}

// BudgetedAndActualAmounts is one performance-history period.
type BudgetedAndActualAmounts struct {
	ActualAmount   *Spend      `json:"ActualAmount,omitempty"`
	BudgetedAmount *Spend      `json:"BudgetedAmount,omitempty"`
	TimePeriod     *TimePeriod `json:"TimePeriod,omitempty"`
}

// PerformanceHistory is the DescribeBudgetPerformanceHistory payload.
type PerformanceHistory struct {
	CostFilters                  map[string][]string        `json:"CostFilters,omitempty"`
	CostTypes                    *CostTypes                 `json:"CostTypes,omitempty"`
	FilterExpression             *Expression                `json:"FilterExpression,omitempty"`
	BillingViewArn               string                     `json:"BillingViewArn,omitempty"`
	BudgetName                   string                     `json:"BudgetName"`
	BudgetType                   string                     `json:"BudgetType"`
	TimeUnit                     string                     `json:"TimeUnit"`
	Metrics                      []string                   `json:"Metrics,omitempty"`
	BudgetedAndActualAmountsList []BudgetedAndActualAmounts `json:"BudgetedAndActualAmountsList"`
}

// BudgetNotificationsForAccount groups notifications by budget.
type BudgetNotificationsForAccount struct {
	BudgetName    string         `json:"BudgetName"`
	Notifications []Notification `json:"Notifications"`
}
