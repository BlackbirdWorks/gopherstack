package quicksight

import "maps"

const (
	editionStandard = "STANDARD"

	authTypeIAMAndQuickSight = "IAM_AND_QUICKSIGHT"

	subscriptionStatusCreated = "ACCOUNT_CREATED"

	purchaseModeManual       = "MANUAL"
	purchaseModeAutoPurchase = "AUTO_PURCHASE"

	// personalizationModeEnabled/personalizationModeDisabled reuse the same
	// string values as iamAssignmentStatusEnabled/iamAssignmentStatusDisabled
	// (ENABLED/DISABLED) but are also used for QSearch and DashboardsQA status.
)

// ---- Account Settings ----

// storedAccountSettings is the persisted representation of a QuickSight
// account's account-wide settings.
type storedAccountSettings struct {
	AccountName                  string `json:"accountName"`
	Edition                      string `json:"edition"`
	DefaultNamespace             string `json:"defaultNamespace"`
	NotificationEmail            string `json:"notificationEmail"`
	TerminationProtectionEnabled bool   `json:"terminationProtectionEnabled"`
}

func (s *storedAccountSettings) toAccountSettings(publicSharingEnabled bool) *AccountSettings {
	return &AccountSettings{
		AccountName:                  s.AccountName,
		Edition:                      s.Edition,
		DefaultNamespace:             s.DefaultNamespace,
		NotificationEmail:            s.NotificationEmail,
		PublicSharingEnabled:         publicSharingEnabled,
		TerminationProtectionEnabled: s.TerminationProtectionEnabled,
	}
}

func defaultStoredAccountSettings(accountID string) *storedAccountSettings {
	return &storedAccountSettings{
		AccountName:      accountID,
		Edition:          editionStandard,
		DefaultNamespace: defaultNamespace,
	}
}

// DescribeAccountSettings returns the account's settings, defaulting to the
// AWS out-of-the-box values (STANDARD edition, "default" namespace, no
// notification email, sharing/termination-protection disabled) when the
// account has never called UpdateAccountSettings.
func (b *InMemoryBackend) DescribeAccountSettings(accountID string) (*AccountSettings, error) {
	b.mu.RLock("DescribeAccountSettings")
	defer b.mu.RUnlock()

	s, ok := b.accountSettings[accountID]
	if !ok {
		s = defaultStoredAccountSettings(accountID)
	}

	return s.toAccountSettings(b.publicSharing[accountID]), nil
}

// UpdateAccountSettings mutates (creating if necessary) the account's settings.
func (b *InMemoryBackend) UpdateAccountSettings(
	accountID, defaultNS, notificationEmail string,
	terminationProtectionEnabled *bool,
) (*AccountSettings, error) {
	b.mu.Lock("UpdateAccountSettings")
	defer b.mu.Unlock()

	s, ok := b.accountSettings[accountID]
	if !ok {
		s = defaultStoredAccountSettings(accountID)
		b.accountSettings[accountID] = s
	}

	if defaultNS != "" {
		s.DefaultNamespace = defaultNS
	}
	if notificationEmail != "" {
		s.NotificationEmail = notificationEmail
	}
	if terminationProtectionEnabled != nil {
		s.TerminationProtectionEnabled = *terminationProtectionEnabled
	}

	return s.toAccountSettings(b.publicSharing[accountID]), nil
}

// ---- Account Subscription ----

// storedAccountSubscription is the persisted representation of a QuickSight
// account subscription.
type storedAccountSubscription struct {
	AccountName               string `json:"accountName"`
	Edition                   string `json:"edition"`
	NotificationEmail         string `json:"notificationEmail"`
	AuthenticationType        string `json:"authenticationType"`
	AccountSubscriptionStatus string `json:"accountSubscriptionStatus"`
}

func (s *storedAccountSubscription) toAccountSubscription() *AccountSubscription {
	return &AccountSubscription{
		AccountName:               s.AccountName,
		Edition:                   s.Edition,
		NotificationEmail:         s.NotificationEmail,
		AuthenticationType:        s.AuthenticationType,
		AccountSubscriptionStatus: s.AccountSubscriptionStatus,
	}
}

// CreateAccountSubscription subscribes accountID to QuickSight.
func (b *InMemoryBackend) CreateAccountSubscription(
	accountID, accountName, edition, authenticationMethod, notificationEmail string,
) (*AccountSubscription, error) {
	b.mu.Lock("CreateAccountSubscription")
	defer b.mu.Unlock()

	if _, exists := b.accountSubscriptions[accountID]; exists {
		return nil, ErrAccountSubscriptionAlreadyExists
	}

	if accountName == "" {
		accountName = accountID
	}
	if edition == "" {
		edition = editionStandard
	}
	if authenticationMethod == "" {
		authenticationMethod = authTypeIAMAndQuickSight
	}

	s := &storedAccountSubscription{
		AccountName:               accountName,
		Edition:                   edition,
		NotificationEmail:         notificationEmail,
		AuthenticationType:        authenticationMethod,
		AccountSubscriptionStatus: subscriptionStatusCreated,
	}
	b.accountSubscriptions[accountID] = s

	return s.toAccountSubscription(), nil
}

// DescribeAccountSubscription returns accountID's subscription info.
func (b *InMemoryBackend) DescribeAccountSubscription(accountID string) (*AccountSubscription, error) {
	b.mu.RLock("DescribeAccountSubscription")
	defer b.mu.RUnlock()

	s, ok := b.accountSubscriptions[accountID]
	if !ok {
		return nil, ErrAccountSubscriptionNotFound
	}

	return s.toAccountSubscription(), nil
}

// DeleteAccountSubscription unsubscribes accountID from QuickSight.
func (b *InMemoryBackend) DeleteAccountSubscription(accountID string) error {
	b.mu.Lock("DeleteAccountSubscription")
	defer b.mu.Unlock()

	if _, ok := b.accountSubscriptions[accountID]; !ok {
		return ErrAccountSubscriptionNotFound
	}

	if s, ok := b.accountSettings[accountID]; ok && s.TerminationProtectionEnabled {
		return ErrAccountTerminationProtectionEnabled
	}

	delete(b.accountSubscriptions, accountID)

	return nil
}

// ---- Account Customization ----

// storedAccountCustomization is the persisted representation of a QuickSight
// account (or namespace) branding customization.
type storedAccountCustomization struct {
	Namespace                         string `json:"namespace"`
	DefaultTheme                      string `json:"defaultTheme,omitempty"`
	DefaultEmailCustomizationTemplate string `json:"defaultEmailCustomizationTemplate,omitempty"`
}

func (c *storedAccountCustomization) toAccountCustomization() *AccountCustomization {
	return &AccountCustomization{
		Namespace:                         c.Namespace,
		DefaultTheme:                      c.DefaultTheme,
		DefaultEmailCustomizationTemplate: c.DefaultEmailCustomizationTemplate,
	}
}

func accountCustomizationKey(accountID, namespace string) string {
	return accountID + "/" + namespace
}

// CreateAccountCustomization creates the branding customization for accountID
// (scoped to namespace, or account-wide when namespace is "").
func (b *InMemoryBackend) CreateAccountCustomization(
	accountID, namespace, defaultTheme, defaultEmailCustomizationTemplate string,
) (*AccountCustomization, error) {
	b.mu.Lock("CreateAccountCustomization")
	defer b.mu.Unlock()

	key := accountCustomizationKey(accountID, namespace)
	if b.accountCustomizations.Has(key) {
		return nil, ErrAccountCustomizationAlreadyExists
	}

	c := &storedAccountCustomization{
		Namespace:                         namespace,
		DefaultTheme:                      defaultTheme,
		DefaultEmailCustomizationTemplate: defaultEmailCustomizationTemplate,
	}
	b.accountCustomizations.Put(c)

	return c.toAccountCustomization(), nil
}

// DescribeAccountCustomization returns accountID's (namespace-scoped) branding customization.
func (b *InMemoryBackend) DescribeAccountCustomization(accountID, namespace string) (*AccountCustomization, error) {
	b.mu.RLock("DescribeAccountCustomization")
	defer b.mu.RUnlock()

	c, ok := b.accountCustomizations.Get(accountCustomizationKey(accountID, namespace))
	if !ok {
		return nil, ErrAccountCustomizationNotFound
	}

	return c.toAccountCustomization(), nil
}

// UpdateAccountCustomization mutates an existing branding customization.
func (b *InMemoryBackend) UpdateAccountCustomization(
	accountID, namespace, defaultTheme, defaultEmailCustomizationTemplate string,
) (*AccountCustomization, error) {
	b.mu.Lock("UpdateAccountCustomization")
	defer b.mu.Unlock()

	key := accountCustomizationKey(accountID, namespace)
	c, ok := b.accountCustomizations.Get(key)
	if !ok {
		return nil, ErrAccountCustomizationNotFound
	}

	if defaultTheme != "" {
		c.DefaultTheme = defaultTheme
	}
	if defaultEmailCustomizationTemplate != "" {
		c.DefaultEmailCustomizationTemplate = defaultEmailCustomizationTemplate
	}

	return c.toAccountCustomization(), nil
}

// DeleteAccountCustomization removes accountID's (namespace-scoped) branding customization.
func (b *InMemoryBackend) DeleteAccountCustomization(accountID, namespace string) error {
	b.mu.Lock("DeleteAccountCustomization")
	defer b.mu.Unlock()

	key := accountCustomizationKey(accountID, namespace)
	if !b.accountCustomizations.Delete(key) {
		return ErrAccountCustomizationNotFound
	}

	return nil
}

// ---- Account Custom Permission ----

// DescribeAccountCustomPermission returns the name of the custom permissions
// profile applied to accountID, if any.
func (b *InMemoryBackend) DescribeAccountCustomPermission(accountID string) (string, error) {
	b.mu.RLock("DescribeAccountCustomPermission")
	defer b.mu.RUnlock()

	name, ok := b.accountCustomPermissions[accountID]
	if !ok {
		return "", ErrAccountCustomPermissionNotFound
	}

	return name, nil
}

// UpdateAccountCustomPermission applies (creating or replacing) a custom
// permissions profile to accountID.
func (b *InMemoryBackend) UpdateAccountCustomPermission(accountID, customPermissionsName string) error {
	b.mu.Lock("UpdateAccountCustomPermission")
	defer b.mu.Unlock()

	b.accountCustomPermissions[accountID] = customPermissionsName

	return nil
}

// DeleteAccountCustomPermission removes accountID's applied custom
// permissions profile.
func (b *InMemoryBackend) DeleteAccountCustomPermission(accountID string) error {
	b.mu.Lock("DeleteAccountCustomPermission")
	defer b.mu.Unlock()

	if _, ok := b.accountCustomPermissions[accountID]; !ok {
		return ErrAccountCustomPermissionNotFound
	}
	delete(b.accountCustomPermissions, accountID)

	return nil
}

// ---- SPICE Capacity ----

func isValidPurchaseMode(mode string) bool {
	switch mode {
	case purchaseModeManual, purchaseModeAutoPurchase:
		return true
	}

	return false
}

// UpdateSPICECapacityConfiguration sets accountID's SPICE capacity purchase
// mode. The real AWS operation's response carries no data beyond
// RequestId/Status, so there is nothing to return here beyond validation. An
// empty purchaseMode is a no-op (PurchaseMode is required by the real API,
// but callers that omit it here leave the stored configuration untouched
// rather than erroring); a non-empty, unrecognized value is rejected.
func (b *InMemoryBackend) UpdateSPICECapacityConfiguration(accountID, purchaseMode string) error {
	if purchaseMode == "" {
		return nil
	}

	if !isValidPurchaseMode(purchaseMode) {
		return ErrValidation
	}

	b.mu.Lock("UpdateSPICECapacityConfiguration")
	defer b.mu.Unlock()

	b.spiceCapacity[accountID] = purchaseMode

	return nil
}

// ---- IP Restriction ----

// storedIPRestriction is the persisted representation of a QuickSight
// account's IP/VPC access restriction rules.
type storedIPRestriction struct {
	RuleMap              map[string]string `json:"ruleMap,omitempty"`
	VPCIDRuleMap         map[string]string `json:"vpcIdRuleMap,omitempty"`
	VPCEndpointIDRuleMap map[string]string `json:"vpcEndpointIdRuleMap,omitempty"`
	Enabled              bool              `json:"enabled"`
}

func (r *storedIPRestriction) toIPRestriction() *IPRestriction {
	return &IPRestriction{
		RuleMap:              cloneStringMap(r.RuleMap),
		VPCIDRuleMap:         cloneStringMap(r.VPCIDRuleMap),
		VPCEndpointIDRuleMap: cloneStringMap(r.VPCEndpointIDRuleMap),
		Enabled:              r.Enabled,
	}
}

func cloneStringMap(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}

	out := make(map[string]string, len(m))
	maps.Copy(out, m)

	return out
}

// DescribeIPRestriction returns accountID's IP restriction rules, defaulting
// to an empty, disabled rule set when never configured.
func (b *InMemoryBackend) DescribeIPRestriction(accountID string) (*IPRestriction, error) {
	b.mu.RLock("DescribeIPRestriction")
	defer b.mu.RUnlock()

	r, ok := b.ipRestrictions[accountID]
	if !ok {
		return &IPRestriction{}, nil
	}

	return r.toIPRestriction(), nil
}

// UpdateIPRestriction replaces (creating if necessary) accountID's IP
// restriction rules. Nil maps leave the corresponding rule map unchanged;
// a nil enabled leaves Enabled unchanged.
func (b *InMemoryBackend) UpdateIPRestriction(
	accountID string,
	ruleMap, vpcIDRuleMap, vpcEndpointIDRuleMap map[string]string,
	enabled *bool,
) (*IPRestriction, error) {
	b.mu.Lock("UpdateIPRestriction")
	defer b.mu.Unlock()

	r, ok := b.ipRestrictions[accountID]
	if !ok {
		r = &storedIPRestriction{}
		b.ipRestrictions[accountID] = r
	}

	if ruleMap != nil {
		r.RuleMap = cloneStringMap(ruleMap)
	}
	if vpcIDRuleMap != nil {
		r.VPCIDRuleMap = cloneStringMap(vpcIDRuleMap)
	}
	if vpcEndpointIDRuleMap != nil {
		r.VPCEndpointIDRuleMap = cloneStringMap(vpcEndpointIDRuleMap)
	}
	if enabled != nil {
		r.Enabled = *enabled
	}

	return r.toIPRestriction(), nil
}

// ---- Public Sharing ----

// UpdatePublicSharingSettings sets whether public sharing is enabled for accountID.
// The value is surfaced back through DescribeAccountSettings.PublicSharingEnabled.
func (b *InMemoryBackend) UpdatePublicSharingSettings(accountID string, enabled bool) error {
	b.mu.Lock("UpdatePublicSharingSettings")
	defer b.mu.Unlock()

	b.publicSharing[accountID] = enabled

	return nil
}

// ---- Key Registration ----

// storedRegisteredKey is the persisted representation of a single registered
// customer-managed KMS key.
type storedRegisteredKey struct {
	KeyArn     string `json:"keyArn"`
	DefaultKey bool   `json:"defaultKey"`
}

func toStoredRegisteredKeys(keys []RegisteredCustomerManagedKey) []storedRegisteredKey {
	out := make([]storedRegisteredKey, 0, len(keys))
	for _, k := range keys {
		out = append(out, storedRegisteredKey(k))
	}

	return out
}

func fromStoredRegisteredKeys(keys []storedRegisteredKey) []RegisteredCustomerManagedKey {
	out := make([]RegisteredCustomerManagedKey, 0, len(keys))
	for _, k := range keys {
		out = append(out, RegisteredCustomerManagedKey(k))
	}

	return out
}

// DescribeKeyRegistration returns accountID's registered customer-managed keys.
func (b *InMemoryBackend) DescribeKeyRegistration(accountID string) ([]RegisteredCustomerManagedKey, error) {
	b.mu.RLock("DescribeKeyRegistration")
	defer b.mu.RUnlock()

	return fromStoredRegisteredKeys(b.keyRegistrations[accountID]), nil
}

// UpdateKeyRegistration replaces accountID's set of registered customer-managed keys.
func (b *InMemoryBackend) UpdateKeyRegistration(
	accountID string,
	keys []RegisteredCustomerManagedKey,
) ([]RegisteredCustomerManagedKey, error) {
	b.mu.Lock("UpdateKeyRegistration")
	defer b.mu.Unlock()

	stored := toStoredRegisteredKeys(keys)
	b.keyRegistrations[accountID] = stored

	return fromStoredRegisteredKeys(stored), nil
}

// ---- Default Q Business Application ----

// storedDefaultQBusinessApplication is the persisted representation of the
// default Amazon Q Business application linked to a QuickSight account.
type storedDefaultQBusinessApplication struct {
	ApplicationID string `json:"applicationId"`
	Namespace     string `json:"namespace"`
}

func (a *storedDefaultQBusinessApplication) toDefaultQBusinessApplication() *DefaultQBusinessApplication {
	return &DefaultQBusinessApplication{
		ApplicationID: a.ApplicationID,
		Namespace:     a.Namespace,
	}
}

// DescribeDefaultQBusinessApplication returns accountID's default Q Business
// application, defaulting to an empty (unconfigured) application when never set.
func (b *InMemoryBackend) DescribeDefaultQBusinessApplication(
	accountID, _ string,
) (*DefaultQBusinessApplication, error) {
	b.mu.RLock("DescribeDefaultQBusinessApplication")
	defer b.mu.RUnlock()

	a, ok := b.defaultQBusinessApps[accountID]
	if !ok {
		return &DefaultQBusinessApplication{}, nil
	}

	return a.toDefaultQBusinessApplication(), nil
}

// UpdateDefaultQBusinessApplication sets (creating if necessary) accountID's
// default Q Business application.
func (b *InMemoryBackend) UpdateDefaultQBusinessApplication(
	accountID, applicationID, namespace string,
) (*DefaultQBusinessApplication, error) {
	if applicationID == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("UpdateDefaultQBusinessApplication")
	defer b.mu.Unlock()

	a := &storedDefaultQBusinessApplication{ApplicationID: applicationID, Namespace: namespace}
	b.defaultQBusinessApps[accountID] = a

	return a.toDefaultQBusinessApplication(), nil
}

// DeleteDefaultQBusinessApplication removes accountID's default Q Business application.
func (b *InMemoryBackend) DeleteDefaultQBusinessApplication(accountID, _ string) error {
	b.mu.Lock("DeleteDefaultQBusinessApplication")
	defer b.mu.Unlock()

	if _, ok := b.defaultQBusinessApps[accountID]; !ok {
		return ErrDefaultQBusinessApplicationNotFound
	}
	delete(b.defaultQBusinessApps, accountID)

	return nil
}

// ---- Q Personalization / Q Search / Dashboards Q&A (ENABLED|DISABLED flags) ----

func isValidToggleStatus(status string) bool {
	switch status {
	case iamAssignmentStatusEnabled, iamAssignmentStatusDisabled:
		return true
	}

	return false
}

// DescribeQPersonalizationConfiguration returns accountID's Q personalization
// mode, defaulting to DISABLED when never configured.
func (b *InMemoryBackend) DescribeQPersonalizationConfiguration(accountID string) (string, error) {
	b.mu.RLock("DescribeQPersonalizationConfiguration")
	defer b.mu.RUnlock()

	if mode, ok := b.qPersonalization[accountID]; ok {
		return mode, nil
	}

	return iamAssignmentStatusDisabled, nil
}

// UpdateQPersonalizationConfiguration sets accountID's Q personalization mode.
func (b *InMemoryBackend) UpdateQPersonalizationConfiguration(accountID, mode string) (string, error) {
	if !isValidToggleStatus(mode) {
		return "", ErrValidation
	}

	b.mu.Lock("UpdateQPersonalizationConfiguration")
	defer b.mu.Unlock()

	b.qPersonalization[accountID] = mode

	return mode, nil
}

// DescribeQuickSightQSearchConfiguration returns accountID's Q Search status,
// defaulting to DISABLED when never configured.
func (b *InMemoryBackend) DescribeQuickSightQSearchConfiguration(accountID string) (string, error) {
	b.mu.RLock("DescribeQuickSightQSearchConfiguration")
	defer b.mu.RUnlock()

	if status, ok := b.qSearchConfig[accountID]; ok {
		return status, nil
	}

	return iamAssignmentStatusDisabled, nil
}

// UpdateQuickSightQSearchConfiguration sets accountID's Q Search status.
func (b *InMemoryBackend) UpdateQuickSightQSearchConfiguration(accountID, status string) (string, error) {
	if !isValidToggleStatus(status) {
		return "", ErrValidation
	}

	b.mu.Lock("UpdateQuickSightQSearchConfiguration")
	defer b.mu.Unlock()

	b.qSearchConfig[accountID] = status

	return status, nil
}

// DescribeDashboardsQAConfiguration returns accountID's Dashboards Q&A status,
// defaulting to DISABLED when never configured.
func (b *InMemoryBackend) DescribeDashboardsQAConfiguration(accountID string) (string, error) {
	b.mu.RLock("DescribeDashboardsQAConfiguration")
	defer b.mu.RUnlock()

	if status, ok := b.dashboardsQAConfig[accountID]; ok {
		return status, nil
	}

	return iamAssignmentStatusDisabled, nil
}

// UpdateDashboardsQAConfiguration sets accountID's Dashboards Q&A status.
func (b *InMemoryBackend) UpdateDashboardsQAConfiguration(accountID, status string) (string, error) {
	if !isValidToggleStatus(status) {
		return "", ErrValidation
	}

	b.mu.Lock("UpdateDashboardsQAConfiguration")
	defer b.mu.Unlock()

	b.dashboardsQAConfig[accountID] = status

	return status, nil
}
