package cognitoidp

import (
	"fmt"
	"slices"
	"strings"
	"time"
)

const (
	deliveryMediumEmail = "EMAIL"
	deliveryMediumSMS   = "SMS"
	adminClientID       = "CLIENT_ID_NOT_APPLICABLE"
	settingRequireVerif = "AttributesRequireVerificationBeforeUpdate"
	verifiedSuffix      = "_verified"
)

// AttributeDelivery is one verification message sent for an email/phone attribute change.
type AttributeDelivery struct {
	AttributeName  string
	DeliveryMedium string
	Destination    string
	Code           string
}

// AttributeUpdateResult carries the deliveries of an attribute update plus the identity
// needed to fire CustomMessage after the backend lock is released.
type AttributeUpdateResult struct {
	PoolID     string
	Username   string
	ClientID   string
	Deliveries []AttributeDelivery
}

func verifiableAttribute(name string) bool { return name == attrEmail || name == attrPhoneNumber }

func requireVerificationBeforeUpdate(pool *UserPool, name string) bool {
	raw, ok := pool.Settings.UserAttributeUpdateSettings[settingRequireVerif].([]any)
	if !ok {
		return false
	}

	return slices.ContainsFunc(raw, func(v any) bool {
		s, isStr := v.(string)

		return isStr && s == name
	})
}

// accessTokenClientID returns the client_id claim of a token already validated for pool.
func accessTokenClientID(pool *UserPool, accessToken string) string {
	claims, err := pool.issuer.ParseAccessToken(accessToken)
	if err != nil {
		return ""
	}

	id, _ := claims["client_id"].(string)

	return id
}

func deliveryFor(name, value, code string) AttributeDelivery {
	if name == attrEmail {
		if value == "" {
			value = "unknown@example.com"
		}

		return AttributeDelivery{
			AttributeName:  name,
			DeliveryMedium: deliveryMediumEmail,
			Destination:    maskEmail(value),
			Code:           code,
		}
	}

	return AttributeDelivery{
		AttributeName:  name,
		DeliveryMedium: deliveryMediumSMS,
		Destination:    maskPhone(value),
		Code:           code,
	}
}

// applyUserAttributeUpdateLocked writes attrs onto u, holding changed email/phone values
// behind a verification code when the pool requires it and flagging them unverified when
// the pool auto-verifies them. Returns the verification messages to send.
func (b *InMemoryBackend) applyUserAttributeUpdateLocked(
	pool *UserPool, u *User, attrs map[string]string,
) []AttributeDelivery {
	if u.Attributes == nil {
		u.Attributes = make(map[string]string)
	}

	names := make([]string, 0, len(attrs))
	for name := range attrs {
		names = append(names, name)
	}

	slices.Sort(names)

	var deliveries []AttributeDelivery

	held := make(map[string]bool)

	for _, name := range names {
		if !verifiableAttribute(name) {
			if !isVerifiedFlag(name) {
				u.Attributes[name] = attrs[name]
			}

			continue
		}

		if d, ok := b.applyVerifiableLocked(pool, u, name, attrs); ok {
			held[name] = true
			deliveries = append(deliveries, d)
		}
	}

	for _, name := range names {
		if isVerifiedFlag(name) && !held[strings.TrimSuffix(name, verifiedSuffix)] {
			u.Attributes[name] = attrs[name]
		}
	}

	u.UpdatedAt = time.Now()

	return deliveries
}

func isVerifiedFlag(name string) bool {
	return strings.HasSuffix(name, verifiedSuffix) && verifiableAttribute(strings.TrimSuffix(name, verifiedSuffix))
}

// applyVerifiableLocked applies one email/phone change; ok reports a verification message was issued.
func (b *InMemoryBackend) applyVerifiableLocked(
	pool *UserPool, u *User, name string, attrs map[string]string,
) (AttributeDelivery, bool) {
	val := attrs[name]
	needsCode := u.Attributes[name] != val && attrs[name+verifiedSuffix] != attrVerifiedTrue

	switch {
	case needsCode && requireVerificationBeforeUpdate(pool, name):
		return b.issueAttributeCodeLocked(u, name, val, val), true
	case needsCode && slices.Contains(pool.AutoVerifiedAttributes, name):
		u.Attributes[name] = val
		u.Attributes[name+verifiedSuffix] = "false"

		return b.issueAttributeCodeLocked(u, name, val, ""), true
	default:
		u.Attributes[name] = val

		return AttributeDelivery{}, false
	}
}

func (b *InMemoryBackend) issueAttributeCodeLocked(u *User, name, destValue, pending string) AttributeDelivery {
	code := randomAlphanumeric(attrVerificationCodeLen)
	b.attrVerificationCodes[u.UserPoolID+":"+u.Username+":"+name] = &attrVerificationEntry{
		Code: code, ExpiresAt: time.Now().Add(attrVerificationTTL), PendingValue: pending,
	}

	return deliveryFor(name, destValue, code)
}

// UpdateUserAttributesWithDelivery is UpdateUserAttributes that also reports verification messages.
func (b *InMemoryBackend) UpdateUserAttributesWithDelivery(
	accessToken string, attributes map[string]string,
) (*AttributeUpdateResult, error) {
	b.mu.Lock("UpdateUserAttributes")
	defer b.mu.Unlock()

	u, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return nil, err
	}

	pool, ok := b.pools.Get(u.UserPoolID)
	if !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, u.UserPoolID)
	}

	return &AttributeUpdateResult{
		PoolID: pool.ID, Username: u.Username, ClientID: accessTokenClientID(pool, accessToken),
		Deliveries: b.applyUserAttributeUpdateLocked(pool, u, attributes),
	}, nil
}

// AdminUpdateUserAttributesWithDelivery is AdminUpdateUserAttributes that also reports verification messages.
func (b *InMemoryBackend) AdminUpdateUserAttributesWithDelivery(
	userPoolID, username string, attributes map[string]string,
) (*AttributeUpdateResult, error) {
	b.mu.Lock("AdminUpdateUserAttributes")
	defer b.mu.Unlock()

	pool, ok := b.pools.Get(userPoolID)
	if !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	u, ok := b.users.Get(userKey(userPoolID, username))
	if !ok {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	return &AttributeUpdateResult{
		PoolID: pool.ID, Username: u.Username, ClientID: adminClientID,
		Deliveries: b.applyUserAttributeUpdateLocked(pool, u, attributes),
	}, nil
}

// AccessTokenIdentity resolves the pool, username and app client behind an access token.
func (b *InMemoryBackend) AccessTokenIdentity(accessToken string) (string, string, string, error) {
	b.mu.RLock("AccessTokenIdentity")
	defer b.mu.RUnlock()

	u, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return "", "", "", err
	}

	pool, ok := b.pools.Get(u.UserPoolID)
	if !ok {
		return "", "", "", fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, u.UserPoolID)
	}

	return pool.ID, u.Username, accessTokenClientID(pool, accessToken), nil
}
