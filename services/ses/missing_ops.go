package ses

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

const (
	notifTypeBounce    = "Bounce"
	notifTypeComplaint = "Complaint"
	notifTypeDelivery  = "Delivery"
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

// ---- identity attribute operations ----

// dkimTokenCount is the number of DKIM tokens issued per domain, matching AWS SES behavior.
const dkimTokenCount = 3

// dkimTokensForIdentity generates deterministic DKIM tokens for an identity.
// Tokens are stable across calls for the same identity, matching AWS SES Pending→Success flow.
func dkimTokensForIdentity(identity string) []string {
	tokens := make([]string, dkimTokenCount)
	for i := range dkimTokenCount {
		key := fmt.Appendf(nil, "%s:dkim:%d", identity, i)
		h := sha256.Sum256(key)
		tokens[i] = hex.EncodeToString(h[:])[:32]
	}

	return tokens
}

// DkimAttributes holds DKIM verification attributes for an identity.
type DkimAttributes struct {
	DkimVerificationStatus string
	DkimTokens             []string
	DkimEnabled            bool
}

// GetIdentityDkimAttributes returns DKIM attributes for each identity.
// Known identities return their persisted DKIM state; unknown identities return NotStarted.
func (b *InMemoryBackend) GetIdentityDkimAttributes(identities []string) map[string]DkimAttributes {
	b.mu.RLock("GetIdentityDkimAttributes")
	defer b.mu.RUnlock()

	out := make(map[string]DkimAttributes, len(identities))

	for _, id := range identities {
		rec, ok := b.identities[id]
		if !ok {
			out[id] = DkimAttributes{DkimVerificationStatus: identityStatusNotStarted}

			continue
		}

		tokens := rec.DkimTokens
		status := identityStatusNotStarted

		if len(tokens) > 0 {
			status = identityStatusSuccess
		}

		out[id] = DkimAttributes{
			DkimEnabled:            rec.DkimEnabled,
			DkimVerificationStatus: status,
			DkimTokens:             tokens,
		}
	}

	return out
}

// MailFromDomainAttributes holds MailFrom domain attributes for an identity.
type MailFromDomainAttributes struct {
	MailFromDomain       string
	MailFromDomainStatus string
	BehaviorOnMXFailure  string
}

// GetIdentityMailFromDomainAttributes returns MailFrom attributes for each identity.
// Identities with a configured MailFromDomain return Success; others return an empty status.
func (b *InMemoryBackend) GetIdentityMailFromDomainAttributes(identities []string) map[string]MailFromDomainAttributes {
	b.mu.RLock("GetIdentityMailFromDomainAttributes")
	defer b.mu.RUnlock()

	out := make(map[string]MailFromDomainAttributes, len(identities))

	for _, id := range identities {
		rec, ok := b.identities[id]
		if !ok {
			out[id] = MailFromDomainAttributes{}

			continue
		}

		status := rec.MailFromStatus
		if status == "" && rec.MailFromDomain != "" {
			status = identityStatusSuccess
		}

		out[id] = MailFromDomainAttributes{
			MailFromDomain:       rec.MailFromDomain,
			MailFromDomainStatus: status,
			BehaviorOnMXFailure:  rec.BehaviorOnMXFail,
		}
	}

	return out
}

// NotificationAttributes holds notification topic attributes for an identity.
type NotificationAttributes struct {
	BounceTopic        string
	ComplaintTopic     string
	DeliveryTopic      string
	ForwardingEnabled  bool
	HeadersInBounce    bool
	HeadersInComplaint bool
	HeadersInDelivery  bool
}

// GetIdentityNotificationAttributes returns notification attributes for each identity.
func (b *InMemoryBackend) GetIdentityNotificationAttributes(identities []string) map[string]NotificationAttributes {
	b.mu.RLock("GetIdentityNotificationAttributes")
	defer b.mu.RUnlock()

	out := make(map[string]NotificationAttributes, len(identities))

	for _, id := range identities {
		rec, ok := b.identities[id]
		if !ok {
			out[id] = NotificationAttributes{ForwardingEnabled: true}

			continue
		}

		out[id] = NotificationAttributes{
			BounceTopic:        rec.BounceTopic,
			ComplaintTopic:     rec.ComplaintTopic,
			DeliveryTopic:      rec.DeliveryTopic,
			ForwardingEnabled:  rec.ForwardingEnabled,
			HeadersInBounce:    rec.HeadersInBounce,
			HeadersInComplaint: rec.HeadersInComplaint,
			HeadersInDelivery:  rec.HeadersInDelivery,
		}
	}

	return out
}

// SetIdentityDkimEnabled persists the DKIM-enabled flag for an identity.
func (b *InMemoryBackend) SetIdentityDkimEnabled(identity string, enabled bool) error {
	if strings.TrimSpace(identity) == "" {
		return fmt.Errorf("%w: Identity is required", ErrInvalidParameter)
	}

	b.mu.Lock("SetIdentityDkimEnabled")
	defer b.mu.Unlock()

	b.getOrCreateIdentityLocked(identity).DkimEnabled = enabled

	return nil
}

// SetIdentityFeedbackForwardingEnabled persists the forwarding-enabled flag for an identity.
func (b *InMemoryBackend) SetIdentityFeedbackForwardingEnabled(identity string, enabled bool) error {
	if strings.TrimSpace(identity) == "" {
		return fmt.Errorf("%w: Identity is required", ErrInvalidParameter)
	}

	b.mu.Lock("SetIdentityFeedbackForwardingEnabled")
	defer b.mu.Unlock()

	b.getOrCreateIdentityLocked(identity).ForwardingEnabled = enabled

	return nil
}

// SetIdentityHeadersInNotificationsEnabled persists the header-inclusion flag for an identity
// and notification type (Bounce, Complaint, or Delivery).
func (b *InMemoryBackend) SetIdentityHeadersInNotificationsEnabled(
	identity, notificationType string, enabled bool,
) error {
	if strings.TrimSpace(identity) == "" {
		return fmt.Errorf("%w: Identity is required", ErrInvalidParameter)
	}

	if strings.TrimSpace(notificationType) == "" {
		return fmt.Errorf("%w: NotificationType is required", ErrInvalidParameter)
	}

	b.mu.Lock("SetIdentityHeadersInNotificationsEnabled")
	defer b.mu.Unlock()

	rec := b.getOrCreateIdentityLocked(identity)

	switch notificationType {
	case notifTypeBounce:
		rec.HeadersInBounce = enabled
	case notifTypeComplaint:
		rec.HeadersInComplaint = enabled
	case notifTypeDelivery:
		rec.HeadersInDelivery = enabled
	}

	return nil
}

// SetIdentityMailFromDomain persists the custom MAIL FROM domain for an identity.
// An empty mailFromDomain clears the setting.
func (b *InMemoryBackend) SetIdentityMailFromDomain(identity, mailFromDomain string) error {
	if strings.TrimSpace(identity) == "" {
		return fmt.Errorf("%w: Identity is required", ErrInvalidParameter)
	}

	b.mu.Lock("SetIdentityMailFromDomain")
	defer b.mu.Unlock()

	rec := b.getOrCreateIdentityLocked(identity)
	rec.MailFromDomain = mailFromDomain

	if mailFromDomain == "" {
		rec.MailFromStatus = ""
	} else {
		rec.MailFromStatus = identityStatusSuccess
	}

	return nil
}

// SetIdentityNotificationTopic persists the SNS notification topic for an identity
// and notification type (Bounce, Complaint, or Delivery).
func (b *InMemoryBackend) SetIdentityNotificationTopic(identity, notificationType, snsTopic string) error {
	if strings.TrimSpace(identity) == "" {
		return fmt.Errorf("%w: Identity is required", ErrInvalidParameter)
	}

	if strings.TrimSpace(notificationType) == "" {
		return fmt.Errorf("%w: NotificationType is required", ErrInvalidParameter)
	}

	b.mu.Lock("SetIdentityNotificationTopic")
	defer b.mu.Unlock()

	rec := b.getOrCreateIdentityLocked(identity)

	switch notificationType {
	case notifTypeBounce:
		rec.BounceTopic = snsTopic
	case notifTypeComplaint:
		rec.ComplaintTopic = snsTopic
	case notifTypeDelivery:
		rec.DeliveryTopic = snsTopic
	}

	return nil
}

// ---- domain verification ----

// VerifyDomainIdentity adds a domain as a verified identity, returning a deterministic verification token.
func (b *InMemoryBackend) VerifyDomainIdentity(domain string) (string, error) {
	if strings.TrimSpace(domain) == "" {
		return "", fmt.Errorf("%w: Domain is required", ErrInvalidParameter)
	}

	b.mu.Lock("VerifyDomainIdentity")
	defer b.mu.Unlock()

	if rec, ok := b.identities[domain]; ok {
		rec.Verified = true
	} else {
		b.identities[domain] = &IdentityRecord{Verified: true, ForwardingEnabled: true}
	}

	h := sha256.Sum256([]byte("domain-token:" + domain))

	return hex.EncodeToString(h[:])[:32], nil
}

// VerifyDomainDkim adds a domain as a verified identity and returns deterministic DKIM tokens.
func (b *InMemoryBackend) VerifyDomainDkim(domain string) ([]string, error) {
	if strings.TrimSpace(domain) == "" {
		return nil, fmt.Errorf("%w: Domain is required", ErrInvalidParameter)
	}

	b.mu.Lock("VerifyDomainDkim")
	defer b.mu.Unlock()

	tokens := dkimTokensForIdentity(domain)

	if rec, ok := b.identities[domain]; ok {
		rec.Verified = true
		rec.DkimTokens = tokens
	} else {
		b.identities[domain] = &IdentityRecord{Verified: true, ForwardingEnabled: true, DkimTokens: tokens}
	}

	return tokens, nil
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

	for id, rec := range b.identities {
		if rec.Verified && strings.Contains(id, "@") {
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

// SendBulkTemplatedEmail sends one email per destination and returns a message ID for each.
func (b *InMemoryBackend) SendBulkTemplatedEmail(
	source, templateName string,
	destinations []BulkEmailDestination,
) ([]string, error) {
	if strings.TrimSpace(source) == "" {
		return nil, fmt.Errorf("%w: Source is required", ErrInvalidParameter)
	}

	if strings.TrimSpace(templateName) == "" {
		return nil, fmt.Errorf("%w: Template is required", ErrInvalidParameter)
	}

	msgIDs := make([]string, 0, len(destinations))

	for _, d := range destinations {
		msgID, err := b.SendTemplatedEmail(SendTemplatedEmailInput{
			From:         source,
			To:           d.To,
			Cc:           d.Cc,
			Bcc:          d.Bcc,
			TemplateName: templateName,
		})
		if err != nil {
			return nil, err
		}

		msgIDs = append(msgIDs, msgID)
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

// ConfigurationSetDescription holds full details of a configuration set.
type ConfigurationSetDescription struct {
	TrackingOptions          *TrackingOptions
	DeliveryOptions          *DeliveryOptions
	Name                     string
	EventDestinations        []EventDestination
	SendingEnabled           bool
	ReputationMetricsEnabled bool
}

// DeliveryOptions holds the delivery options for a configuration set.
type DeliveryOptions struct {
	TLSPolicy string `json:"tlsPolicy,omitempty"`
}

// DescribeConfigurationSet returns the named configuration set metadata plus event destinations and tracking options.
func (b *InMemoryBackend) DescribeConfigurationSet(name string) (ConfigurationSetDescription, error) {
	if strings.TrimSpace(name) == "" {
		return ConfigurationSetDescription{}, fmt.Errorf("%w: ConfigurationSetName is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeConfigurationSet")
	defer b.mu.RUnlock()

	cs, exists := b.configSets[name]
	if !exists {
		return ConfigurationSetDescription{}, fmt.Errorf("%w: %s", ErrConfigSetNotFound, name)
	}

	desc := ConfigurationSetDescription{
		Name:                     name,
		SendingEnabled:           cs.SendingEnabled,
		ReputationMetricsEnabled: cs.ReputationMetrics,
	}

	if cs.TLSPolicy != "" {
		desc.DeliveryOptions = &DeliveryOptions{TLSPolicy: cs.TLSPolicy}
	}

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

// PutConfigurationSetDeliveryOptions persists the TLS policy for a configuration set.
func (b *InMemoryBackend) PutConfigurationSetDeliveryOptions(configSetName, tlsPolicy string) error {
	if strings.TrimSpace(configSetName) == "" {
		return fmt.Errorf("%w: ConfigurationSetName is required", ErrInvalidParameter)
	}

	b.mu.Lock("PutConfigurationSetDeliveryOptions")
	defer b.mu.Unlock()

	cs, exists := b.configSets[configSetName]
	if !exists {
		return fmt.Errorf("%w: %s", ErrConfigSetNotFound, configSetName)
	}

	cs.TLSPolicy = tlsPolicy

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

// UpdateConfigurationSetReputationMetricsEnabled persists the reputation metrics flag.
func (b *InMemoryBackend) UpdateConfigurationSetReputationMetricsEnabled(configSetName string, enabled bool) error {
	if strings.TrimSpace(configSetName) == "" {
		return fmt.Errorf("%w: ConfigurationSetName is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateConfigurationSetReputationMetricsEnabled")
	defer b.mu.Unlock()

	cs, exists := b.configSets[configSetName]
	if !exists {
		return fmt.Errorf("%w: %s", ErrConfigSetNotFound, configSetName)
	}

	cs.ReputationMetrics = enabled

	return nil
}

// UpdateConfigurationSetSendingEnabled persists the sending-enabled flag.
func (b *InMemoryBackend) UpdateConfigurationSetSendingEnabled(configSetName string, enabled bool) error {
	if strings.TrimSpace(configSetName) == "" {
		return fmt.Errorf("%w: ConfigurationSetName is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateConfigurationSetSendingEnabled")
	defer b.mu.Unlock()

	cs, exists := b.configSets[configSetName]
	if !exists {
		return fmt.Errorf("%w: %s", ErrConfigSetNotFound, configSetName)
	}

	cs.SendingEnabled = enabled

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
