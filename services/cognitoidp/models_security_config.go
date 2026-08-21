package cognitoidp

// CompromisedCredentialsActions defines what action to take for compromised credentials.
type CompromisedCredentialsActions struct {
	EventAction string `json:"EventAction,omitempty"` // "BLOCK" | "NO_ACTION"
}

// CompromisedCredentialsRiskConfig holds the compromised credentials risk configuration.
type CompromisedCredentialsRiskConfig struct {
	Actions     *CompromisedCredentialsActions `json:"Actions,omitempty"`
	EventFilter []string                       `json:"EventFilter,omitempty"`
}

// AccountTakeoverActionType defines an action for a specific risk level.
type AccountTakeoverActionType struct {
	EventAction string `json:"EventAction,omitempty"`
	Notify      bool   `json:"Notify,omitempty"`
}

// AccountTakeoverActions defines actions per risk level.
type AccountTakeoverActions struct {
	LowAction    *AccountTakeoverActionType `json:"LowAction,omitempty"`
	MediumAction *AccountTakeoverActionType `json:"MediumAction,omitempty"`
	HighAction   *AccountTakeoverActionType `json:"HighAction,omitempty"`
}

// NotifyEmailType holds an email template for notifications.
type NotifyEmailType struct {
	HTMLBody string `json:"HtmlBody,omitempty"`
	Subject  string `json:"Subject,omitempty"`
	TextBody string `json:"TextBody,omitempty"`
}

// NotifyConfigurationType holds notification settings for account takeover.
type NotifyConfigurationType struct {
	BlockEmail    *NotifyEmailType `json:"BlockEmail,omitempty"`
	MfaEmail      *NotifyEmailType `json:"MfaEmail,omitempty"`
	NoActionEmail *NotifyEmailType `json:"NoActionEmail,omitempty"`
	From          string           `json:"From,omitempty"`
	ReplyTo       string           `json:"ReplyTo,omitempty"`
	SourceArn     string           `json:"SourceArn,omitempty"`
}

// AccountTakeoverRiskConfig holds account takeover risk configuration.
type AccountTakeoverRiskConfig struct {
	Actions             *AccountTakeoverActions  `json:"Actions,omitempty"`
	NotifyConfiguration *NotifyConfigurationType `json:"NotifyConfiguration,omitempty"`
}

// RiskExceptionConfig holds IP range exception configuration for adaptive authentication.
type RiskExceptionConfig struct {
	BlockedIPRangeList []string `json:"blockedIPRangeList,omitempty"`
	SkippedIPRangeList []string `json:"skippedIPRangeList,omitempty"`
}

// TypedRiskConfiguration holds fully typed risk config fields.
type TypedRiskConfiguration struct {
	CompromisedCredentialsRiskConfig *CompromisedCredentialsRiskConfig `json:"compromisedCredentialsRiskConfig,omitempty"`
	AccountTakeoverRiskConfig        *AccountTakeoverRiskConfig        `json:"accountTakeoverRiskConfig,omitempty"`
	RiskExceptionConfiguration       *RiskExceptionConfig              `json:"riskExceptionConfiguration,omitempty"`
	UserPoolID                       string                            `json:"userPoolID,omitempty"`
	ClientID                         string                            `json:"clientID,omitempty"`
}

// RiskConfiguration stores adaptive authentication settings for a pool or client.
type RiskConfiguration struct {
	// Stored as an opaque blob; individual fields not needed for emulation.
	Raw map[string]any `json:"raw,omitempty"`
}

// LogDeliveryConfig holds log delivery destination configuration for a pool.
type LogDeliveryConfig struct {
	Raw map[string]any `json:"raw,omitempty"`
}

type compromisedCredActionsJSON struct {
	EventAction string `json:"EventAction,omitempty"`
}

type compromisedCredRiskConfigJSON struct {
	Actions     *compromisedCredActionsJSON `json:"Actions,omitempty"`
	EventFilter []string                    `json:"EventFilter,omitempty"`
}

// Notify is required and real for a false value (e.g. "don't notify" for a
// low-risk assessment) -- omitempty on a bool drops the key precisely when
// the value is false, indistinguishable from Go's zero value to gofmt but
// wrong on AWS's own wire contract. Not provable via a real SDK client round
// trip (an omitted key and an explicit false both decode to false), so this
// is fixed but excluded from the r80d bug tally.
type accountTakeoverActionTypeJSON struct {
	EventAction string `json:"EventAction,omitempty"`
	Notify      bool   `json:"Notify"`
}

type accountTakeoverActionsJSON struct {
	LowAction    *accountTakeoverActionTypeJSON `json:"LowAction,omitempty"`
	MediumAction *accountTakeoverActionTypeJSON `json:"MediumAction,omitempty"`
	HighAction   *accountTakeoverActionTypeJSON `json:"HighAction,omitempty"`
}

type notifyEmailTypeJSON struct {
	HTMLBody string `json:"HtmlBody,omitempty"`
	Subject  string `json:"Subject,omitempty"`
	TextBody string `json:"TextBody,omitempty"`
}

// SourceArn is required whenever NotifyConfiguration is present. The real
// SDK's client-side validator only null-checks the *string pointer, not its
// content, so a real client can send an empty-string SourceArn -- it must
// still round-trip, never be omitted.
type notifyConfigJSON struct {
	BlockEmail    *notifyEmailTypeJSON `json:"BlockEmail,omitempty"`
	MfaEmail      *notifyEmailTypeJSON `json:"MfaEmail,omitempty"`
	NoActionEmail *notifyEmailTypeJSON `json:"NoActionEmail,omitempty"`
	From          string               `json:"From,omitempty"`
	ReplyTo       string               `json:"ReplyTo,omitempty"`
	SourceArn     string               `json:"SourceArn"`
}

type accountTakeoverRiskConfigJSON struct {
	Actions             *accountTakeoverActionsJSON `json:"Actions,omitempty"`
	NotifyConfiguration *notifyConfigJSON           `json:"NotifyConfiguration,omitempty"`
}

type riskExceptionConfigJSON struct {
	BlockedIPRangeList []string `json:"BlockedIPRangeList,omitempty"`
	SkippedIPRangeList []string `json:"SkippedIPRangeList,omitempty"`
}

type describeRiskConfigFullInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	ClientID   string `json:"ClientId,omitempty"`
}

type riskConfigurationJSON struct {
	CompromisedCredentialsRiskConfiguration *compromisedCredRiskConfigJSON `json:"CompromisedCredentialsRiskConfiguration,omitempty"` //nolint:lll // AWS field name
	AccountTakeoverRiskConfiguration        *accountTakeoverRiskConfigJSON `json:"AccountTakeoverRiskConfiguration,omitempty"`        //nolint:lll // AWS field name
	RiskExceptionConfiguration              *riskExceptionConfigJSON       `json:"RiskExceptionConfiguration,omitempty"`
	UserPoolID                              string                         `json:"UserPoolId,omitempty"`
	ClientID                                string                         `json:"ClientId,omitempty"`
}

type describeRiskConfigFullOutput struct {
	RiskConfiguration *riskConfigurationJSON `json:"RiskConfiguration,omitempty"`
}

type setRiskConfigFullInput struct {
	CompromisedCredentialsRiskConfiguration *compromisedCredRiskConfigJSON `json:"CompromisedCredentialsRiskConfiguration,omitempty"` //nolint:lll // AWS field name
	AccountTakeoverRiskConfiguration        *accountTakeoverRiskConfigJSON `json:"AccountTakeoverRiskConfiguration,omitempty"`        //nolint:lll // AWS field name
	RiskExceptionConfiguration              *riskExceptionConfigJSON       `json:"RiskExceptionConfiguration,omitempty"`
	UserPoolID                              string                         `json:"UserPoolId,omitempty"`
	ClientID                                string                         `json:"ClientId,omitempty"`
}

type setRiskConfigFullOutput struct {
	RiskConfiguration *riskConfigurationJSON `json:"RiskConfiguration,omitempty"`
}

type describeRiskConfigurationInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	ClientID   string `json:"ClientId,omitempty"`
}

type riskConfigurationType struct{}

type describeRiskConfigurationOutput struct {
	RiskConfiguration *riskConfigurationType `json:"RiskConfiguration,omitempty"`
}

type setRiskConfigurationInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	ClientID   string `json:"ClientId,omitempty"`
}

type setRiskConfigurationOutput struct {
	RiskConfiguration *riskConfigurationType `json:"RiskConfiguration,omitempty"`
}

type getLogDeliveryConfigurationInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
}

type getLogDeliveryConfigurationOutput struct {
	LogDeliveryConfiguration map[string]any `json:"LogDeliveryConfiguration,omitempty"`
}

type setLogDeliveryConfigurationInput struct {
	UserPoolID        string           `json:"UserPoolId,omitempty"`
	LogConfigurations []map[string]any `json:"LogConfigurations,omitempty"`
}

type setLogDeliveryConfigurationOutput struct {
	LogDeliveryConfiguration map[string]any `json:"LogDeliveryConfiguration,omitempty"`
}
