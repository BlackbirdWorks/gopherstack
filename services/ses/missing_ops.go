package ses

import (
	"fmt"
	"sort"
	"strings"
)

// ---- identity policy operations (no-op stubs) ----

// PutIdentityPolicy is a no-op stub: gopherstack does not enforce IAM sending policies.
func (b *InMemoryBackend) PutIdentityPolicy(identity, policyName, _ string) error {
	if strings.TrimSpace(identity) == "" {
		return fmt.Errorf("%w: Identity is required", ErrInvalidParameter)
	}

	if strings.TrimSpace(policyName) == "" {
		return fmt.Errorf("%w: PolicyName is required", ErrInvalidParameter)
	}

	return nil
}

// DeleteIdentityPolicy is a no-op stub.
func (b *InMemoryBackend) DeleteIdentityPolicy(identity, policyName string) error {
	if strings.TrimSpace(identity) == "" {
		return fmt.Errorf("%w: Identity is required", ErrInvalidParameter)
	}

	if strings.TrimSpace(policyName) == "" {
		return fmt.Errorf("%w: PolicyName is required", ErrInvalidParameter)
	}

	return nil
}

// GetIdentityPolicies is a no-op stub; returns empty policies.
func (b *InMemoryBackend) GetIdentityPolicies(identity string, _ []string) (map[string]string, error) {
	if strings.TrimSpace(identity) == "" {
		return nil, fmt.Errorf("%w: Identity is required", ErrInvalidParameter)
	}

	return map[string]string{}, nil
}

// ListIdentityPolicies is a no-op stub; returns empty list.
func (b *InMemoryBackend) ListIdentityPolicies(identity string) ([]string, error) {
	if strings.TrimSpace(identity) == "" {
		return nil, fmt.Errorf("%w: Identity is required", ErrInvalidParameter)
	}

	return []string{}, nil
}

// ---- identity attribute operations (no-op stubs) ----

// GetIdentityDkimAttributes returns stub DKIM attributes (disabled) for each identity.
func (b *InMemoryBackend) GetIdentityDkimAttributes(identities []string) map[string]DkimAttributes {
	out := make(map[string]DkimAttributes, len(identities))
	for _, id := range identities {
		out[id] = DkimAttributes{DkimEnabled: false, DkimVerificationStatus: "NotStarted"}
	}

	return out
}

// DkimAttributes holds stub DKIM verification attributes.
type DkimAttributes struct {
	DkimVerificationStatus string
	DkimTokens             []string
	DkimEnabled            bool
}

// GetIdentityMailFromDomainAttributes returns stub MailFrom attributes for each identity.
func (b *InMemoryBackend) GetIdentityMailFromDomainAttributes(identities []string) map[string]MailFromDomainAttributes {
	out := make(map[string]MailFromDomainAttributes, len(identities))
	for _, id := range identities {
		out[id] = MailFromDomainAttributes{MailFromDomainStatus: identityStatusSuccess}
	}

	return out
}

// MailFromDomainAttributes holds stub MailFrom domain attributes.
type MailFromDomainAttributes struct {
	MailFromDomain       string
	MailFromDomainStatus string
	BehaviorOnMXFailure  string
}

// GetIdentityNotificationAttributes returns stub notification attributes for each identity.
func (b *InMemoryBackend) GetIdentityNotificationAttributes(identities []string) map[string]NotificationAttributes {
	out := make(map[string]NotificationAttributes, len(identities))
	for _, id := range identities {
		out[id] = NotificationAttributes{ForwardingEnabled: true}
	}

	return out
}

// NotificationAttributes holds stub notification attributes.
type NotificationAttributes struct {
	BounceTopic        string
	ComplaintTopic     string
	DeliveryTopic      string
	ForwardingEnabled  bool
	HeadersInBounce    bool
	HeadersInComplaint bool
	HeadersInDelivery  bool
}

// SetIdentityDkimEnabled is a no-op stub.
func (b *InMemoryBackend) SetIdentityDkimEnabled(identity string, _ bool) error {
	if strings.TrimSpace(identity) == "" {
		return fmt.Errorf("%w: Identity is required", ErrInvalidParameter)
	}

	return nil
}

// SetIdentityFeedbackForwardingEnabled is a no-op stub.
func (b *InMemoryBackend) SetIdentityFeedbackForwardingEnabled(identity string, _ bool) error {
	if strings.TrimSpace(identity) == "" {
		return fmt.Errorf("%w: Identity is required", ErrInvalidParameter)
	}

	return nil
}

// SetIdentityHeadersInNotificationsEnabled is a no-op stub.
func (b *InMemoryBackend) SetIdentityHeadersInNotificationsEnabled(identity, notificationType string, _ bool) error {
	if strings.TrimSpace(identity) == "" {
		return fmt.Errorf("%w: Identity is required", ErrInvalidParameter)
	}

	if strings.TrimSpace(notificationType) == "" {
		return fmt.Errorf("%w: NotificationType is required", ErrInvalidParameter)
	}

	return nil
}

// SetIdentityMailFromDomain is a no-op stub.
func (b *InMemoryBackend) SetIdentityMailFromDomain(identity, _ string) error {
	if strings.TrimSpace(identity) == "" {
		return fmt.Errorf("%w: Identity is required", ErrInvalidParameter)
	}

	return nil
}

// SetIdentityNotificationTopic is a no-op stub.
func (b *InMemoryBackend) SetIdentityNotificationTopic(identity, notificationType, _ string) error {
	if strings.TrimSpace(identity) == "" {
		return fmt.Errorf("%w: Identity is required", ErrInvalidParameter)
	}

	if strings.TrimSpace(notificationType) == "" {
		return fmt.Errorf("%w: NotificationType is required", ErrInvalidParameter)
	}

	return nil
}

// ---- domain verification (stubs that auto-verify) ----

// VerifyDomainIdentity adds a domain as a verified identity, returning a stub token.
func (b *InMemoryBackend) VerifyDomainIdentity(domain string) (string, error) {
	if strings.TrimSpace(domain) == "" {
		return "", fmt.Errorf("%w: Domain is required", ErrInvalidParameter)
	}

	b.mu.Lock("VerifyDomainIdentity")
	defer b.mu.Unlock()

	b.identities[domain] = true

	return "gopherstack-domain-token-" + domain, nil
}

// VerifyDomainDkim adds a domain as a verified identity and returns stub DKIM tokens.
func (b *InMemoryBackend) VerifyDomainDkim(domain string) ([]string, error) {
	if strings.TrimSpace(domain) == "" {
		return nil, fmt.Errorf("%w: Domain is required", ErrInvalidParameter)
	}

	b.mu.Lock("VerifyDomainDkim")
	defer b.mu.Unlock()

	b.identities[domain] = true

	return []string{"token1", "token2", "token3"}, nil
}

// VerifyEmailAddress is an alias for VerifyEmailIdentity (legacy API).
func (b *InMemoryBackend) VerifyEmailAddress(email string) error {
	return b.VerifyEmailIdentity(email)
}

// DeleteVerifiedEmailAddress removes a verified email address (legacy API).
func (b *InMemoryBackend) DeleteVerifiedEmailAddress(email string) {
	b.DeleteIdentity(email)
}

// ListVerifiedEmailAddresses returns all verified identities that are email addresses (contain @).
func (b *InMemoryBackend) ListVerifiedEmailAddresses() []string {
	b.mu.RLock("ListVerifiedEmailAddresses")
	defer b.mu.RUnlock()

	var out []string

	for id := range b.identities {
		if strings.Contains(id, "@") {
			out = append(out, id)
		}
	}

	sort.Strings(out)

	return out
}

// ---- account-level controls (stubs) ----

// UpdateAccountSendingEnabled is a no-op stub (sending is always enabled).
func (b *InMemoryBackend) UpdateAccountSendingEnabled(_ bool) {}

// ---- send operations (stubs) ----

// SendBounce is a no-op stub that returns a synthetic bounce message ID.
func (b *InMemoryBackend) SendBounce(originalMsgID string) (string, error) {
	if strings.TrimSpace(originalMsgID) == "" {
		return "", fmt.Errorf("%w: OriginalMessageId is required", ErrInvalidParameter)
	}

	return "bounce-" + originalMsgID, nil
}

// SendBulkTemplatedEmail is a stub that returns a synthetic destination status list.
func (b *InMemoryBackend) SendBulkTemplatedEmail(source, templateName string, destinations []string) ([]string, error) {
	if strings.TrimSpace(source) == "" {
		return nil, fmt.Errorf("%w: Source is required", ErrInvalidParameter)
	}

	if strings.TrimSpace(templateName) == "" {
		return nil, fmt.Errorf("%w: Template is required", ErrInvalidParameter)
	}

	msgIDs := make([]string, len(destinations))
	for i, d := range destinations {
		msgIDs[i] = "bulk-" + d
	}

	return msgIDs, nil
}

// SendCustomVerificationEmail is a no-op stub.
func (b *InMemoryBackend) SendCustomVerificationEmail(email, templateName string) (string, error) {
	if strings.TrimSpace(email) == "" {
		return "", fmt.Errorf("%w: EmailAddress is required", ErrInvalidParameter)
	}

	if strings.TrimSpace(templateName) == "" {
		return "", fmt.Errorf("%w: TemplateName is required", ErrInvalidParameter)
	}

	return "custom-verif-" + email, nil
}

// TestRenderTemplate is a stub that returns the template subject unchanged.
func (b *InMemoryBackend) TestRenderTemplate(templateName, _ string) (string, error) {
	tmpl, err := b.GetTemplate(templateName)
	if err != nil {
		return "", err
	}

	return tmpl.SubjectPart, nil
}

// UpdateCustomVerificationEmailTemplate updates an existing custom verification email template.
func (b *InMemoryBackend) UpdateCustomVerificationEmailTemplate(tmpl CustomVerificationEmailTemplate) error {
	if strings.TrimSpace(tmpl.TemplateName) == "" {
		return fmt.Errorf("%w: TemplateName is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateCustomVerificationEmailTemplate")
	defer b.mu.Unlock()

	if _, exists := b.customVerifTemplates[tmpl.TemplateName]; !exists {
		return fmt.Errorf("%w: %s", ErrCustomVerifTemplateNotFound, tmpl.TemplateName)
	}

	t := tmpl
	b.customVerifTemplates[tmpl.TemplateName] = &t

	return nil
}

// ---- receipt rule operations ----

// DescribeReceiptRule returns a named rule from a rule set.
func (b *InMemoryBackend) DescribeReceiptRule(ruleSetName, ruleName string) (ReceiptRule, error) {
	if strings.TrimSpace(ruleSetName) == "" {
		return ReceiptRule{}, fmt.Errorf("%w: RuleSetName is required", ErrInvalidParameter)
	}

	if strings.TrimSpace(ruleName) == "" {
		return ReceiptRule{}, fmt.Errorf("%w: RuleName is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeReceiptRule")
	defer b.mu.RUnlock()

	rs, exists := b.receiptRuleSets[ruleSetName]
	if !exists {
		return ReceiptRule{}, fmt.Errorf("%w: %s", ErrReceiptRuleSetNotFound, ruleSetName)
	}

	idx := findRuleIndex(rs.Rules, ruleName)
	if idx < 0 {
		return ReceiptRule{}, fmt.Errorf("%w: %s", ErrReceiptRuleNotFound, ruleName)
	}

	r := rs.Rules[idx]
	recipients := make([]string, len(r.Recipients))
	copy(recipients, r.Recipients)
	r.Recipients = recipients

	return r, nil
}

// UpdateReceiptRule replaces an existing rule in a rule set.
func (b *InMemoryBackend) UpdateReceiptRule(ruleSetName string, rule ReceiptRule) error {
	if strings.TrimSpace(ruleSetName) == "" {
		return fmt.Errorf("%w: RuleSetName is required", ErrInvalidParameter)
	}

	if strings.TrimSpace(rule.Name) == "" {
		return fmt.Errorf("%w: Rule.Name is required", ErrInvalidParameter)
	}

	if rule.TLSPolicy != "" && rule.TLSPolicy != TLSPolicyOptional && rule.TLSPolicy != TLSPolicyRequire {
		return fmt.Errorf("%w: TlsPolicy must be Optional or Require", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateReceiptRule")
	defer b.mu.Unlock()

	rs, exists := b.receiptRuleSets[ruleSetName]
	if !exists {
		return fmt.Errorf("%w: %s", ErrReceiptRuleSetNotFound, ruleSetName)
	}

	idx := findRuleIndex(rs.Rules, rule.Name)
	if idx < 0 {
		return fmt.Errorf("%w: %s", ErrReceiptRuleNotFound, rule.Name)
	}

	rs.Rules[idx] = rule

	return nil
}

// ReorderReceiptRuleSet reorders the rules in a rule set according to the given ordered name list.
func (b *InMemoryBackend) ReorderReceiptRuleSet(ruleSetName string, ruleNames []string) error {
	if strings.TrimSpace(ruleSetName) == "" {
		return fmt.Errorf("%w: RuleSetName is required", ErrInvalidParameter)
	}

	b.mu.Lock("ReorderReceiptRuleSet")
	defer b.mu.Unlock()

	rs, exists := b.receiptRuleSets[ruleSetName]
	if !exists {
		return fmt.Errorf("%w: %s", ErrReceiptRuleSetNotFound, ruleSetName)
	}

	if len(ruleNames) != len(rs.Rules) {
		return fmt.Errorf(
			"%w: ruleNames length (%d) must match rule set size (%d)",
			ErrInvalidParameter,
			len(ruleNames),
			len(rs.Rules),
		)
	}

	index := make(map[string]ReceiptRule, len(rs.Rules))
	for _, r := range rs.Rules {
		index[r.Name] = r
	}

	reordered := make([]ReceiptRule, 0, len(ruleNames))

	for _, name := range ruleNames {
		r, ok := index[name]
		if !ok {
			return fmt.Errorf("%w: rule %s not found", ErrReceiptRuleNotFound, name)
		}

		reordered = append(reordered, r)
	}

	rs.Rules = reordered

	return nil
}

// SetReceiptRulePosition moves a rule to the given zero-based position in a rule set.
func (b *InMemoryBackend) SetReceiptRulePosition(ruleSetName, ruleName string, position int) error {
	if strings.TrimSpace(ruleSetName) == "" {
		return fmt.Errorf("%w: RuleSetName is required", ErrInvalidParameter)
	}

	if strings.TrimSpace(ruleName) == "" {
		return fmt.Errorf("%w: RuleName is required", ErrInvalidParameter)
	}

	b.mu.Lock("SetReceiptRulePosition")
	defer b.mu.Unlock()

	rs, exists := b.receiptRuleSets[ruleSetName]
	if !exists {
		return fmt.Errorf("%w: %s", ErrReceiptRuleSetNotFound, ruleSetName)
	}

	idx := findRuleIndex(rs.Rules, ruleName)
	if idx < 0 {
		return fmt.Errorf("%w: %s", ErrReceiptRuleNotFound, ruleName)
	}

	if position < 0 || position >= len(rs.Rules) {
		return fmt.Errorf("%w: position %d out of range [0, %d)", ErrInvalidParameter, position, len(rs.Rules))
	}

	rule := rs.Rules[idx]
	// Build a slice without the rule at idx, then re-insert at position.
	withoutRule := make([]ReceiptRule, 0, len(rs.Rules)-1)
	withoutRule = append(withoutRule, rs.Rules[:idx]...)
	withoutRule = append(withoutRule, rs.Rules[idx+1:]...)
	rules := withoutRule
	newRules := make([]ReceiptRule, 0, len(rules)+1)
	newRules = append(newRules, rules[:position]...)
	newRules = append(newRules, rule)
	newRules = append(newRules, rules[position:]...)
	rs.Rules = newRules

	return nil
}

// ---- configuration set operations ----

// DescribeConfigurationSet returns the named configuration set metadata plus event destinations and tracking options.
func (b *InMemoryBackend) DescribeConfigurationSet(name string) (ConfigurationSetDescription, error) {
	if strings.TrimSpace(name) == "" {
		return ConfigurationSetDescription{}, fmt.Errorf("%w: ConfigurationSetName is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeConfigurationSet")
	defer b.mu.RUnlock()

	if _, exists := b.configSets[name]; !exists {
		return ConfigurationSetDescription{}, fmt.Errorf("%w: %s", ErrConfigSetNotFound, name)
	}

	desc := ConfigurationSetDescription{Name: name}

	if dests := b.eventDestinations[name]; dests != nil {
		for _, d := range dests {
			dc := *d
			desc.EventDestinations = append(desc.EventDestinations, dc)
		}

		sort.Slice(desc.EventDestinations, func(i, j int) bool {
			return desc.EventDestinations[i].Name < desc.EventDestinations[j].Name
		})
	}

	if to := b.trackingOptions[name]; to != nil {
		tc := *to
		desc.TrackingOptions = &tc
	}

	return desc, nil
}

// ConfigurationSetDescription holds full details of a configuration set.
type ConfigurationSetDescription struct {
	TrackingOptions   *TrackingOptions
	Name              string
	EventDestinations []EventDestination
}

// PutConfigurationSetDeliveryOptions is a no-op stub.
func (b *InMemoryBackend) PutConfigurationSetDeliveryOptions(configSetName, _ string) error {
	if strings.TrimSpace(configSetName) == "" {
		return fmt.Errorf("%w: ConfigurationSetName is required", ErrInvalidParameter)
	}

	b.mu.RLock("PutConfigurationSetDeliveryOptions")
	defer b.mu.RUnlock()

	if _, exists := b.configSets[configSetName]; !exists {
		return fmt.Errorf("%w: %s", ErrConfigSetNotFound, configSetName)
	}

	return nil
}

// UpdateConfigurationSetEventDestination updates an existing event destination on a configuration set.
func (b *InMemoryBackend) UpdateConfigurationSetEventDestination(configSetName string, dest EventDestination) error {
	if strings.TrimSpace(configSetName) == "" {
		return fmt.Errorf("%w: ConfigurationSetName is required", ErrInvalidParameter)
	}

	if strings.TrimSpace(dest.Name) == "" {
		return fmt.Errorf("%w: EventDestination.Name is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateConfigurationSetEventDestination")
	defer b.mu.Unlock()

	if _, exists := b.configSets[configSetName]; !exists {
		return fmt.Errorf("%w: %s", ErrConfigSetNotFound, configSetName)
	}

	dests := b.eventDestinations[configSetName]
	if dests == nil {
		return fmt.Errorf("%w: %s", ErrEventDestinationNotFound, dest.Name)
	}

	if _, exists := dests[dest.Name]; !exists {
		return fmt.Errorf("%w: %s", ErrEventDestinationNotFound, dest.Name)
	}

	d := dest
	dests[dest.Name] = &d

	return nil
}

// UpdateConfigurationSetReputationMetricsEnabled is a no-op stub.
func (b *InMemoryBackend) UpdateConfigurationSetReputationMetricsEnabled(configSetName string, _ bool) error {
	if strings.TrimSpace(configSetName) == "" {
		return fmt.Errorf("%w: ConfigurationSetName is required", ErrInvalidParameter)
	}

	b.mu.RLock("UpdateConfigurationSetReputationMetricsEnabled")
	defer b.mu.RUnlock()

	if _, exists := b.configSets[configSetName]; !exists {
		return fmt.Errorf("%w: %s", ErrConfigSetNotFound, configSetName)
	}

	return nil
}

// UpdateConfigurationSetSendingEnabled is a no-op stub.
func (b *InMemoryBackend) UpdateConfigurationSetSendingEnabled(configSetName string, _ bool) error {
	if strings.TrimSpace(configSetName) == "" {
		return fmt.Errorf("%w: ConfigurationSetName is required", ErrInvalidParameter)
	}

	b.mu.RLock("UpdateConfigurationSetSendingEnabled")
	defer b.mu.RUnlock()

	if _, exists := b.configSets[configSetName]; !exists {
		return fmt.Errorf("%w: %s", ErrConfigSetNotFound, configSetName)
	}

	return nil
}

// UpdateConfigurationSetTrackingOptions updates the tracking options for a configuration set.
func (b *InMemoryBackend) UpdateConfigurationSetTrackingOptions(configSetName, customRedirectDomain string) error {
	if strings.TrimSpace(configSetName) == "" {
		return fmt.Errorf("%w: ConfigurationSetName is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateConfigurationSetTrackingOptions")
	defer b.mu.Unlock()

	if _, exists := b.configSets[configSetName]; !exists {
		return fmt.Errorf("%w: %s", ErrConfigSetNotFound, configSetName)
	}

	if _, exists := b.trackingOptions[configSetName]; !exists {
		return fmt.Errorf(
			"%w: tracking options do not exist for configuration set %s",
			ErrTrackingOptionsNotFound,
			configSetName,
		)
	}

	b.trackingOptions[configSetName] = &TrackingOptions{CustomRedirectDomain: customRedirectDomain}

	return nil
}

// ---- email search index ----

// SearchEmails returns emails whose From, Subject, or To fields contain the given query string (case-insensitive).
// This provides O(n) filtered access while leveraging the existing email slice.
func (b *InMemoryBackend) SearchEmails(query string) []Email {
	b.mu.RLock("SearchEmails")
	defer b.mu.RUnlock()

	if query == "" {
		out := make([]Email, len(b.emails))
		copy(out, b.emails)

		return out
	}

	q := strings.ToLower(query)
	var out []Email

	for _, e := range b.emails {
		if strings.Contains(strings.ToLower(e.From), q) ||
			strings.Contains(strings.ToLower(e.Subject), q) ||
			containsAny(e.To, q) {
			out = append(out, e)
		}
	}

	return out
}

// containsAny reports whether any element of ss contains substr (case-insensitive).
func containsAny(ss []string, substr string) bool {
	for _, s := range ss {
		if strings.Contains(strings.ToLower(s), substr) {
			return true
		}
	}

	return false
}
