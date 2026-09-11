package ses

import "fmt"

// Hard-coded resource caps enforced at the same Create*/Clone*/Update*
// operations where the real AWS SES SDK declares LimitExceededException --
// confirmed per-op by reading aws-sdk-go-v2/service/ses@v1.37.4/
// deserializers.go's own awsAwsquery_deserializeOpError<Op> switch:
// CloneReceiptRuleSet, CreateConfigurationSet,
// CreateConfigurationSetEventDestination, CreateCustomVerificationEmailTemplate,
// CreateReceiptFilter, CreateReceiptRule, CreateReceiptRuleSet, CreateTemplate,
// UpdateReceiptRule. Every other Create*/Put* op's own error switch has no
// LimitExceeded case (e.g. VerifyEmailIdentity/VerifyDomainIdentity do not
// declare it, despite the real 10,000-identity cap existing) and is
// therefore NOT enforced here, per the no-invented-errors rule.
//
// Values are the real, non-adjustable SES v1 defaults from
// https://docs.aws.amazon.com/ses/latest/dg/quotas.html, except
// defaultMaxCustomVerificationTemplates, which that page omits entirely --
// sourced instead from the SES Developer Guide FAQ
// (https://docs.aws.amazon.com/ses/latest/dg/creating-identities.html
// #send-email-verify-address-custom-faq-q1: "You can create up to 50 custom
// verification email templates per Amazon SES account."). That value
// coincides numerically with the unrelated customVerifTemplateMaxResults
// pagination cap (custom_verification.go) -- the two are not the same quota.
const (
	defaultMaxReceiptRuleSets             = 40    // "Maximum number of receipt rule sets per AWS account"
	defaultMaxRulesPerRuleSet             = 200   // "Maximum number of rules per receipt rule set"
	defaultMaxActionsPerRule              = 10    // "Maximum number of actions per receipt rule"
	defaultMaxRecipientsPerRule           = 500   // "Maximum number of recipients per receipt rule"
	defaultMaxReceiptFilters              = 100   // "Maximum number of IP address filters per AWS account"
	defaultMaxEventDestinationsPerSet     = 10    // "Maximum number of event destinations per configuration set"
	defaultMaxConfigurationSets           = 10000 // "Maximum number of configuration sets"
	defaultMaxEmailTemplates              = 20000 // "Maximum number of email templates in each AWS Region"
	defaultMaxCustomVerificationTemplates = 50    // dev guide FAQ Q1, see above
)

// resourceLimits holds the caps InMemoryBackend enforces, defaulted to the
// real SES values above (defaultResourceLimits) and overridable via
// WithResourceLimits (store.go) -- the same constructor-option pattern as
// WithEmailTTL -- so tests exercising LimitExceededException on a
// 10,000/20,000-sized cap don't have to create that many real resources.
type resourceLimits struct {
	receiptRuleSets             int
	rulesPerRuleSet             int
	actionsPerRule              int
	recipientsPerRule           int
	receiptFilters              int
	eventDestinationsPerSet     int
	configurationSets           int
	emailTemplates              int
	customVerificationTemplates int
}

func defaultResourceLimits() resourceLimits {
	return resourceLimits{
		receiptRuleSets:             defaultMaxReceiptRuleSets,
		rulesPerRuleSet:             defaultMaxRulesPerRuleSet,
		actionsPerRule:              defaultMaxActionsPerRule,
		recipientsPerRule:           defaultMaxRecipientsPerRule,
		receiptFilters:              defaultMaxReceiptFilters,
		eventDestinationsPerSet:     defaultMaxEventDestinationsPerSet,
		configurationSets:           defaultMaxConfigurationSets,
		emailTemplates:              defaultMaxEmailTemplates,
		customVerificationTemplates: defaultMaxCustomVerificationTemplates,
	}
}

// ResourceLimits overrides the resource caps a *InMemoryBackend enforces
// with LimitExceededException. A zero field keeps its real-SES default (see
// WithResourceLimits) -- used by tests that need to trip a large cap (e.g.
// the 20,000-template or 10,000-configuration-set limit) without actually
// creating that many resources.
type ResourceLimits struct {
	ReceiptRuleSets             int
	RulesPerRuleSet             int
	ActionsPerRule              int
	RecipientsPerRule           int
	ReceiptFilters              int
	EventDestinationsPerSet     int
	ConfigurationSets           int
	EmailTemplates              int
	CustomVerificationTemplates int
}

// applyResourceLimitOverrides copies every positive field of l onto rl,
// leaving fields left at zero in l unchanged.
func applyResourceLimitOverrides(rl *resourceLimits, l ResourceLimits) {
	if l.ReceiptRuleSets > 0 {
		rl.receiptRuleSets = l.ReceiptRuleSets
	}

	if l.RulesPerRuleSet > 0 {
		rl.rulesPerRuleSet = l.RulesPerRuleSet
	}

	if l.ActionsPerRule > 0 {
		rl.actionsPerRule = l.ActionsPerRule
	}

	if l.RecipientsPerRule > 0 {
		rl.recipientsPerRule = l.RecipientsPerRule
	}

	if l.ReceiptFilters > 0 {
		rl.receiptFilters = l.ReceiptFilters
	}

	if l.EventDestinationsPerSet > 0 {
		rl.eventDestinationsPerSet = l.EventDestinationsPerSet
	}

	if l.ConfigurationSets > 0 {
		rl.configurationSets = l.ConfigurationSets
	}

	if l.EmailTemplates > 0 {
		rl.emailTemplates = l.EmailTemplates
	}

	if l.CustomVerificationTemplates > 0 {
		rl.customVerificationTemplates = l.CustomVerificationTemplates
	}
}

// limitExceeded builds the LimitExceededException wire error for resource.
func limitExceeded(resource string) error {
	return fmt.Errorf("%w: %s limit exceeded", ErrLimitExceeded, resource)
}
