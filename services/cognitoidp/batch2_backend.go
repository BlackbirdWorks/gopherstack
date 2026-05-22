package cognitoidp

import (
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
)

// defaultTempPasswordSuffix is appended to ensure temp password meets basic complexity.
const defaultTempPasswordSuffix = "Aa1!"

// defaultTempPasswordPrefixLen is the length of the random prefix in generated temp passwords.
const defaultTempPasswordPrefixLen = 12

// ---------------------------------------------------------------------------
// MFA Configuration Types
// ---------------------------------------------------------------------------

// SmsConfiguration holds the SNS topic ARN for SMS delivery.
type SmsConfiguration struct {
	SnsCallerArn string `json:"SnsCallerArn,omitempty"`
	SnsRegion    string `json:"SnsRegion,omitempty"`
	ExternalID   string `json:"ExternalId,omitempty"`
}

// SmsMfaConfiguration holds SMS MFA settings for a user pool.
type SmsMfaConfiguration struct {
	SmsConfiguration         *SmsConfiguration `json:"SmsConfiguration,omitempty"`
	SmsAuthenticationMessage string            `json:"SmsAuthenticationMessage,omitempty"`
}

// SoftwareTokenMfaConfiguration holds TOTP MFA settings for a user pool.
type SoftwareTokenMfaConfiguration struct {
	Enabled bool `json:"Enabled"`
}

// EmailMfaConfiguration holds email OTP MFA settings for a user pool.
type EmailMfaConfiguration struct {
	Message string `json:"Message,omitempty"`
	Subject string `json:"Subject,omitempty"`
}

// UserPoolMfaFullConfig holds the complete MFA configuration for a pool.
type UserPoolMfaFullConfig struct {
	SmsMfaConfiguration   *SmsMfaConfiguration
	SoftwareTokenMfa      *SoftwareTokenMfaConfiguration
	EmailMfaConfiguration *EmailMfaConfiguration
	MfaConfiguration      string
}

// ---------------------------------------------------------------------------
// Attribute Verification Types
// ---------------------------------------------------------------------------

// attrVerificationEntry stores a pending attribute verification code.
type attrVerificationEntry struct {
	ExpiresAt time.Time
	Code      string
}

// attrVerificationTTL is how long attribute verification codes remain valid.
const attrVerificationTTL = 24 * time.Hour

// attrVerificationCodeLen is the length of attribute verification codes.
const attrVerificationCodeLen = 6

// mfaConfigOFF is the default MFA configuration value when none is set.
const mfaConfigOFF = "OFF"

// attrPhoneNumber is the standard Cognito phone_number attribute name.
const attrPhoneNumber = "phone_number"

// attrVerifiedTrue is the string value for a verified attribute.
const attrVerifiedTrue = "true"

// maskEmailPrefixLen is the number of chars shown unmasked in an email prefix.
const maskEmailPrefixLen = 2

// maskPhoneVisibleSuffix is the number of phone digits shown unmasked at the end.
const maskPhoneVisibleSuffix = 4

// cloudFrontDistIDLen is the length of the random part in generated CloudFront distribution IDs.
const cloudFrontDistIDLen = 13

// ---------------------------------------------------------------------------
// Typed Risk Configuration
// ---------------------------------------------------------------------------

// CompromisedCredentialsActions defines what action to take for compromised credentials.
type CompromisedCredentialsActions struct {
	EventAction string `json:"EventAction"` // "BLOCK" | "NO_ACTION"
}

// CompromisedCredentialsRiskConfig holds the compromised credentials risk configuration.
type CompromisedCredentialsRiskConfig struct {
	Actions     *CompromisedCredentialsActions `json:"Actions,omitempty"`
	EventFilter []string                       `json:"EventFilter,omitempty"`
}

// AccountTakeoverActionType defines an action for a specific risk level.
type AccountTakeoverActionType struct {
	EventAction string `json:"EventAction"`
	Notify      bool   `json:"Notify"`
}

// AccountTakeoverActions defines actions per risk level.
type AccountTakeoverActions struct {
	LowAction    *AccountTakeoverActionType `json:"LowAction,omitempty"`
	MediumAction *AccountTakeoverActionType `json:"MediumAction,omitempty"`
	HighAction   *AccountTakeoverActionType `json:"HighAction,omitempty"`
}

// NotifyEmailType holds an email template for notifications.
type NotifyEmailType struct {
	HtmlBody string `json:"HtmlBody,omitempty"`
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

// TypedRiskConfiguration holds fully typed risk config fields.
type TypedRiskConfiguration struct {
	CompromisedCredentialsRiskConfig *CompromisedCredentialsRiskConfig
	AccountTakeoverRiskConfig        *AccountTakeoverRiskConfig
	RiskExceptionConfiguration       map[string]any
	UserPoolID                       string
	ClientID                         string
}

// ---------------------------------------------------------------------------
// UserPool extended MFA fields — added to UserPool struct via separate backing map
// ---------------------------------------------------------------------------
// We store MFA sub-configs in a separate map on the backend keyed by poolID
// to avoid modifying the frozen UserPool struct in backend.go.

// SetUserPoolMfaConfigFull stores the full MFA configuration for a pool.
func (b *InMemoryBackend) SetUserPoolMfaConfigFull(userPoolID string, cfg UserPoolMfaFullConfig) error {
	b.mu.Lock("SetUserPoolMfaConfigFull")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	pool := b.pools[userPoolID]
	if cfg.MfaConfiguration != "" {
		pool.MfaConfiguration = cfg.MfaConfiguration
	}

	b.poolMfaConfigs[userPoolID] = &cfg

	return nil
}

// GetUserPoolMfaConfigFull returns the full MFA configuration for a pool.
func (b *InMemoryBackend) GetUserPoolMfaConfigFull(userPoolID string) (*UserPoolMfaFullConfig, error) {
	b.mu.RLock("GetUserPoolMfaConfigFull")
	defer b.mu.RUnlock()

	pool, ok := b.pools[userPoolID]
	if !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	cfg, ok := b.poolMfaConfigs[userPoolID]
	if !ok {
		mfaVal := pool.MfaConfiguration
		if mfaVal == "" {
			mfaVal = mfaConfigOFF
		}

		return &UserPoolMfaFullConfig{MfaConfiguration: mfaVal}, nil
	}

	cp := *cfg

	return &cp, nil
}

// ---------------------------------------------------------------------------
// Attribute Verification — stateful
// ---------------------------------------------------------------------------

// GetUserAttributeVerificationCode generates a verification code for a user attribute
// and stores it for later validation via VerifyUserAttributeWithCode.
func (b *InMemoryBackend) GetUserAttributeVerificationCode(
	accessToken, attributeName string,
) (code, deliveryDest, deliveryMedium string, err error) {
	b.mu.Lock("GetUserAttributeVerificationCode")
	defer b.mu.Unlock()

	user, verr := b.findUserByAccessTokenLocked(accessToken)
	if verr != nil {
		return "", "", "", verr
	}

	switch attributeName {
	case attrEmail, attrPhoneNumber:
		// valid verifiable attributes
	default:
		return "", "", "", fmt.Errorf("%w: attribute %q is not verifiable", ErrInvalidParameter, attributeName)
	}

	code = randomAlphanumeric(attrVerificationCodeLen)
	key := user.UserPoolID + ":" + user.Username + ":" + attributeName

	b.attrVerificationCodes[key] = &attrVerificationEntry{
		Code:      code,
		ExpiresAt: time.Now().Add(attrVerificationTTL),
	}

	// Determine delivery destination and medium.
	if attributeName == attrEmail {
		dest := user.Attributes[attrEmail]
		if dest == "" {
			dest = "unknown@example.com"
		}
		// Mask email: show first 2 chars + *** + domain.
		dest = maskEmail(dest)
		return code, dest, "EMAIL", nil
	}

	// phone_number
	dest := user.Attributes[attrPhoneNumber]
	if dest == "" {
		dest = "+*******1234"
	}

	dest = maskPhone(dest)

	return code, dest, "SMS", nil
}

// VerifyUserAttributeWithCode validates the code stored by GetUserAttributeVerificationCode
// and marks the attribute as verified in the user record.
func (b *InMemoryBackend) VerifyUserAttributeWithCode(accessToken, attributeName, code string) error {
	b.mu.Lock("VerifyUserAttributeWithCode")
	defer b.mu.Unlock()

	user, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return err
	}

	switch attributeName {
	case attrEmail, attrPhoneNumber:
	default:
		return fmt.Errorf("%w: attribute %q is not verifiable", ErrInvalidParameter, attributeName)
	}

	key := user.UserPoolID + ":" + user.Username + ":" + attributeName

	entry, ok := b.attrVerificationCodes[key]
	if !ok {
		return fmt.Errorf("%w: no verification code found for attribute %q", ErrCodeMismatch, attributeName)
	}

	if time.Now().After(entry.ExpiresAt) {
		delete(b.attrVerificationCodes, key)

		return fmt.Errorf("%w: verification code for attribute %q has expired", ErrExpiredCode, attributeName)
	}

	if entry.Code != code {
		return fmt.Errorf("%w: incorrect verification code for attribute %q", ErrCodeMismatch, attributeName)
	}

	delete(b.attrVerificationCodes, key)

	// Mark attribute as verified.
	user.Attributes[attributeName+"_verified"] = attrVerifiedTrue
	user.UpdatedAt = time.Now()

	return nil
}

// maskEmail masks most of an email address for privacy.
func maskEmail(email string) string {
	at := -1
	for i, c := range email {
		if c == '@' {
			at = i
			break
		}
	}

	if at <= 0 {
		return "***@***"
	}

	prefix := email[:at]
	domain := email[at:]

	if len(prefix) <= maskEmailPrefixLen {
		return prefix + "***" + domain
	}

	return prefix[:maskEmailPrefixLen] + "***" + domain
}

// maskPhone masks most of a phone number for privacy.
func maskPhone(phone string) string {
	if len(phone) <= maskPhoneVisibleSuffix {
		return "+*******"
	}

	return "+*******" + phone[len(phone)-maskPhoneVisibleSuffix:]
}

// ---------------------------------------------------------------------------
// Typed RiskConfiguration
// ---------------------------------------------------------------------------

// SetTypedRiskConfiguration stores a fully typed risk configuration for a pool or client.
func (b *InMemoryBackend) SetTypedRiskConfiguration(cfg *TypedRiskConfiguration) error {
	b.mu.Lock("SetTypedRiskConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.pools[cfg.UserPoolID]; !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, cfg.UserPoolID)
	}

	key := cfg.UserPoolID + ":" + cfg.ClientID
	b.typedRiskConfigurations[key] = cfg

	return nil
}

// GetTypedRiskConfiguration returns the typed risk configuration for a pool or client.
func (b *InMemoryBackend) GetTypedRiskConfiguration(poolID, clientID string) (*TypedRiskConfiguration, error) {
	b.mu.RLock("GetTypedRiskConfiguration")
	defer b.mu.RUnlock()

	if _, ok := b.pools[poolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, poolID)
	}

	key := poolID + ":" + clientID

	cfg, ok := b.typedRiskConfigurations[key]
	if !ok {
		return &TypedRiskConfiguration{UserPoolID: poolID, ClientID: clientID}, nil
	}

	cp := *cfg

	return &cp, nil
}

// ---------------------------------------------------------------------------
// UICustomization extended
// ---------------------------------------------------------------------------

// SetUICustomizationFull stores extended UI customization including image URL.
func (b *InMemoryBackend) SetUICustomizationFull(poolID, clientID, css, imageURL string) (*UICustomization, error) {
	b.mu.Lock("SetUICustomizationFull")
	defer b.mu.Unlock()

	if _, ok := b.pools[poolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, poolID)
	}

	key := poolID + ":" + clientID
	now := time.Now()

	existing, ok := b.uiCustomizations[key]
	if !ok {
		existing = &UICustomization{
			UserPoolID: poolID,
			ClientID:   clientID,
			CreatedAt:  now,
		}
		b.uiCustomizations[key] = existing
	}

	existing.CSS = css
	existing.ImageUrl = imageURL
	existing.LastModifiedAt = now

	cp := *existing

	return &cp, nil
}

// GetUICustomizationFull returns extended UI customization.
func (b *InMemoryBackend) GetUICustomizationFull(poolID, clientID string) (*UICustomization, error) {
	b.mu.RLock("GetUICustomizationFull")
	defer b.mu.RUnlock()

	if _, ok := b.pools[poolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, poolID)
	}

	key := poolID + ":" + clientID

	existing, ok := b.uiCustomizations[key]
	if !ok {
		return &UICustomization{UserPoolID: poolID, ClientID: clientID}, nil
	}

	cp := *existing

	return &cp, nil
}

// ---------------------------------------------------------------------------
// IdentityProvider extended — AttributeMapping and IdpIdentifiers
// ---------------------------------------------------------------------------

// CreateIdentityProviderFull creates an identity provider with AttributeMapping and IdpIdentifiers.
func (b *InMemoryBackend) CreateIdentityProviderFull(
	userPoolID, providerName, providerType string,
	providerDetails map[string]string,
	attributeMapping map[string]string,
	idpIdentifiers []string,
) (*IdentityProvider, error) {
	b.mu.Lock("CreateIdentityProviderFull")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if b.identityProviders[userPoolID] == nil {
		b.identityProviders[userPoolID] = make(map[string]*IdentityProvider)
	}

	if _, exists := b.identityProviders[userPoolID][providerName]; exists {
		return nil, fmt.Errorf("%w: identity provider %q already exists in pool %q",
			ErrAlreadyExists, providerName, userPoolID)
	}

	now := time.Now()

	details := make(map[string]string, len(providerDetails))
	maps.Copy(details, providerDetails)

	attrMap := make(map[string]string, len(attributeMapping))
	maps.Copy(attrMap, attributeMapping)

	ids := make([]string, len(idpIdentifiers))
	copy(ids, idpIdentifiers)

	idp := &IdentityProvider{
		UserPoolID:       userPoolID,
		ProviderName:     providerName,
		ProviderType:     providerType,
		ProviderDetails:  details,
		AttributeMapping: attrMap,
		IdpIdentifiers:   ids,
		CreatedAt:        now,
		LastModifiedAt:   now,
	}

	b.identityProviders[userPoolID][providerName] = idp

	cp := *idp

	return &cp, nil
}

// UpdateIdentityProviderFull updates an identity provider with AttributeMapping and IdpIdentifiers.
func (b *InMemoryBackend) UpdateIdentityProviderFull(
	userPoolID, providerName string,
	providerDetails map[string]string,
	attributeMapping map[string]string,
	idpIdentifiers []string,
) (*IdentityProvider, error) {
	b.mu.Lock("UpdateIdentityProviderFull")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	idp, ok := b.identityProviders[userPoolID][providerName]
	if !ok {
		return nil, fmt.Errorf("%w: identity provider %q not found in pool %q",
			ErrUserPoolNotFound, providerName, userPoolID)
	}

	if providerDetails != nil {
		maps.Copy(idp.ProviderDetails, providerDetails)
	}

	if attributeMapping != nil {
		if idp.AttributeMapping == nil {
			idp.AttributeMapping = make(map[string]string)
		}

		maps.Copy(idp.AttributeMapping, attributeMapping)
	}

	if idpIdentifiers != nil {
		ids := make([]string, len(idpIdentifiers))
		copy(ids, idpIdentifiers)
		idp.IdpIdentifiers = ids
	}

	idp.LastModifiedAt = time.Now()

	cp := *idp

	return &cp, nil
}

// ---------------------------------------------------------------------------
// UserPoolDomain extended — CertificateArn for custom domains
// ---------------------------------------------------------------------------

// CreateUserPoolDomainFull creates a user pool domain with optional custom domain cert.
func (b *InMemoryBackend) CreateUserPoolDomainFull(userPoolID, domain, certificateArn string) (*UserPoolDomain, error) {
	b.mu.Lock("CreateUserPoolDomainFull")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, exists := b.domains[domain]; exists {
		return nil, fmt.Errorf("%w: domain %q already exists", ErrAlreadyExists, domain)
	}

	cfDomain := domain + ".auth." + b.region + ".amazoncognito.com"
	if certificateArn != "" {
		// Custom domain: generate a CloudFront distribution domain.
		cfDomain = "d" + randomAlphanumeric(cloudFrontDistIDLen) + ".cloudfront.net"
	}

	d := &UserPoolDomain{
		Domain:                 domain,
		UserPoolID:             userPoolID,
		CloudFrontDistribution: cfDomain,
		CertificateArn:         certificateArn,
		Status:                 "ACTIVE",
	}
	b.domains[domain] = d

	cp := *d

	return &cp, nil
}

// UpdateUserPoolDomainFull updates a domain's certificate ARN and returns the CloudFront domain.
func (b *InMemoryBackend) UpdateUserPoolDomainFull(userPoolID, domain, certificateArn string) (string, error) {
	b.mu.Lock("UpdateUserPoolDomainFull")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return "", fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	d, ok := b.domains[domain]
	if !ok {
		return "", fmt.Errorf("%w: domain %q not found", ErrUserPoolNotFound, domain)
	}

	if certificateArn != "" {
		d.CertificateArn = certificateArn
		d.CloudFrontDistribution = "d" + randomAlphanumeric(cloudFrontDistIDLen) + ".cloudfront.net"
	}

	return d.CloudFrontDistribution, nil
}

// ---------------------------------------------------------------------------
// Group with RoleArn
// ---------------------------------------------------------------------------

// CreateGroupFull creates a group with a RoleArn.
func (b *InMemoryBackend) CreateGroupFull(
	userPoolID, groupName, description, roleArn string,
	precedence int32,
) (*Group, error) {
	b.mu.Lock("CreateGroupFull")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if b.groups[userPoolID] == nil {
		b.groups[userPoolID] = make(map[string]*Group)
	}

	if _, exists := b.groups[userPoolID][groupName]; exists {
		return nil, fmt.Errorf("%w: group %q already exists in pool %q", ErrAlreadyExists, groupName, userPoolID)
	}

	g := &Group{
		GroupName:   groupName,
		UserPoolID:  userPoolID,
		Description: description,
		RoleArn:     roleArn,
		Precedence:  precedence,
		CreatedAt:   time.Now(),
	}

	b.groups[userPoolID][groupName] = g

	if b.groupMembers[userPoolID] == nil {
		b.groupMembers[userPoolID] = make(map[string]map[string]struct{})
	}

	b.groupMembers[userPoolID][groupName] = make(map[string]struct{})

	cp := *g

	return &cp, nil
}

// UpdateGroupFull updates group with description, roleArn, and precedence.
func (b *InMemoryBackend) UpdateGroupFull(
	userPoolID, groupName, description, roleArn string,
	precedence int32,
) (*Group, error) {
	b.mu.Lock("UpdateGroupFull")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	g, ok := b.groups[userPoolID][groupName]
	if !ok {
		return nil, fmt.Errorf("%w: group %q not found in pool %q", ErrGroupNotFound, groupName, userPoolID)
	}

	if description != "" {
		g.Description = description
	}

	if roleArn != "" {
		g.RoleArn = roleArn
	}

	g.Precedence = precedence

	cp := *g

	return &cp, nil
}

// ---------------------------------------------------------------------------
// AdminCreateUser with MessageAction
// ---------------------------------------------------------------------------

// AdminCreateUserFull creates a user with optional MessageAction (SUPPRESS/RESEND) and delivery mediums.
func (b *InMemoryBackend) AdminCreateUserFull(
	userPoolID, username, tempPassword string,
	userAttributes map[string]string,
	messageAction string,
	desiredDeliveryMediums []string,
	forceAliasCreation bool,
) (*User, error) {
	b.mu.Lock("AdminCreateUserFull")
	defer b.mu.Unlock()

	pool, ok := b.pools[userPoolID]
	if !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	poolUsers, ok := b.users[userPoolID]
	if !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if existing, exists := poolUsers[username]; exists {
		if messageAction == "RESEND" {
			// Re-send temp password to existing FORCE_CHANGE_PASSWORD user.
			if existing.Status != UserStatusForceChangePassword {
				return nil, fmt.Errorf(
					"%w: user %q is not in FORCE_CHANGE_PASSWORD state",
					ErrInvalidParameter,
					username,
				)
			}

			cp := *existing

			return &cp, nil
		}

		return nil, fmt.Errorf("%w: user %q already exists", ErrUserAlreadyExists, username)
	}

	if tempPassword != "" {
		if err := validatePassword(pool.PasswordPolicy, tempPassword); err != nil {
			return nil, err
		}
	} else {
		// Generate a temporary password.
		tempPassword = randomAlphanumeric(defaultTempPasswordPrefixLen) + defaultTempPasswordSuffix
	}

	importHash, err := bcryptHashPassword(tempPassword)
	if err != nil {
		return nil, err
	}

	attrs := make(map[string]string, len(userAttributes))
	maps.Copy(attrs, userAttributes)

	if messageAction != "SUPPRESS" && tempPassword != "" {
		attrs["custom:temporaryPassword"] = tempPassword
	}

	// Auto-verify attributes configured on the pool.
	for _, attr := range pool.AutoVerifiedAttributes {
		if _, hasAttr := attrs[attr]; hasAttr {
			attrs[attr+"_verified"] = attrVerifiedTrue
		}
	}

	_ = desiredDeliveryMediums
	_ = forceAliasCreation

	user := &User{
		Sub:          uuid.New().String(),
		Username:     username,
		UserPoolID:   userPoolID,
		PasswordHash: importHash,
		Status:       UserStatusForceChangePassword,
		Attributes:   attrs,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
		Enabled:      true,
	}

	poolUsers[username] = user
	b.usersBySub[userPoolID+":"+user.Sub] = username

	cp := *user

	return &cp, nil
}

// ---------------------------------------------------------------------------
// AdminSetUserPassword — accurate permanent/temporary
// ---------------------------------------------------------------------------

// AdminSetUserPasswordFull sets password with proper status handling.
// If permanent=true, status becomes CONFIRMED. If permanent=false, status remains FORCE_CHANGE_PASSWORD.
func (b *InMemoryBackend) AdminSetUserPasswordFull(userPoolID, username, password string, permanent bool) error {
	b.mu.Lock("AdminSetUserPasswordFull")
	defer b.mu.Unlock()

	pool, ok := b.pools[userPoolID]
	if !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if err := validatePassword(pool.PasswordPolicy, password); err != nil {
		return err
	}

	user, ok := b.users[userPoolID][username]
	if !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	hash, err := bcryptHashPassword(password)
	if err != nil {
		return err
	}

	user.PasswordHash = hash
	user.UpdatedAt = time.Now()

	if permanent {
		user.Status = UserStatusConfirmed
	} else {
		user.Status = UserStatusForceChangePassword
		user.Attributes["custom:temporaryPassword"] = password
	}

	return nil
}

// ---------------------------------------------------------------------------
// ListGroups with pagination
// ---------------------------------------------------------------------------

// ListGroupsPage returns a page of groups with optional NextToken pagination.
func (b *InMemoryBackend) ListGroupsPage(userPoolID string, limit int, nextToken string) ([]*Group, string, error) {
	b.mu.RLock("ListGroupsPage")
	defer b.mu.RUnlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, "", fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	poolGroups := b.groups[userPoolID]
	all := make([]*Group, 0, len(poolGroups))

	for _, g := range poolGroups {
		cp := *g
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].GroupName < all[j].GroupName })

	// Apply token-based pagination.
	startIdx := 0

	if nextToken != "" {
		for i, g := range all {
			if g.GroupName == nextToken {
				startIdx = i

				break
			}
		}
	}

	all = all[startIdx:]

	if limit <= 0 || limit > len(all) {
		return all, "", nil
	}

	page := all[:limit]
	newToken := ""

	if limit < len(all) {
		newToken = all[limit].GroupName
	}

	return page, newToken, nil
}

// ListUsersInGroupPage returns a page of users in a group with optional NextToken pagination.
func (b *InMemoryBackend) ListUsersInGroupPage(
	userPoolID, groupName string,
	limit int,
	nextToken string,
) ([]*User, string, error) {
	b.mu.RLock("ListUsersInGroupPage")
	defer b.mu.RUnlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, "", fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.groups[userPoolID][groupName]; !ok {
		return nil, "", fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	members := b.groupMembers[userPoolID][groupName]
	all := make([]*User, 0, len(members))

	for username := range members {
		if u, ok := b.users[userPoolID][username]; ok {
			cp := *u
			all = append(all, &cp)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Username < all[j].Username })

	startIdx := 0

	if nextToken != "" {
		for i, u := range all {
			if u.Username == nextToken {
				startIdx = i

				break
			}
		}
	}

	all = all[startIdx:]

	if limit <= 0 || limit > len(all) {
		return all, "", nil
	}

	page := all[:limit]
	newToken := ""

	if limit < len(all) {
		newToken = all[limit].Username
	}

	return page, newToken, nil
}

// ---------------------------------------------------------------------------
// EvictExpiredAttrVerificationCodes removes expired attribute verification codes.
// Called by the janitor.
// ---------------------------------------------------------------------------

func (b *InMemoryBackend) EvictExpiredAttrVerificationCodes() {
	b.mu.Lock("EvictExpiredAttrVerificationCodes")
	defer b.mu.Unlock()

	now := time.Now()

	for key, entry := range b.attrVerificationCodes {
		if now.After(entry.ExpiresAt) {
			delete(b.attrVerificationCodes, key)
		}
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// bcryptHashPassword hashes a password with bcrypt.
func bcryptHashPassword(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
	if err != nil {
		return "", fmt.Errorf("hashing password: %w", err)
	}

	return string(hash), nil
}
