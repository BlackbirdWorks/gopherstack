package marketplacemetering

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	ent "github.com/blackbirdworks/gopherstack/services/marketplaceentitlementservice"
)

// emulatorMACKey signs registration tokens and RegisterUsage signatures; the emulator is its own AWS.
var emulatorMACKey = []byte("gopherstack-marketplace-metering-v1") //nolint:gochecknoglobals // immutable key material

// ProductSpec declares a product; a declared product enforces its dimensions and customer subscriptions.
type ProductSpec struct {
	Code             string
	Dimensions       []Dimension
	PublicKeyVersion int32
}

// ResolvedCustomer is the ResolveCustomer result.
type ResolvedCustomer struct {
	CustomerAWSAccountID string `json:"CustomerAWSAccountId"`
	CustomerIdentifier   string `json:"CustomerIdentifier"`
	ProductCode          string `json:"ProductCode"`
}

type tokenClaims struct {
	P string `json:"p"`
	A string `json:"a"`
	I int64  `json:"i"`
}

func customerID(product, account string) string {
	sum := sha256.Sum256([]byte(product + "|" + account))

	return hex.EncodeToString(sum[:])[:customerIDLen]
}

func validProductCode(code string) bool {
	return code != "" && len(code) <= maxProductCodeLen && !strings.ContainsAny(code, " \t\r\n")
}

func validAccountID(id string) bool {
	if len(id) != accountIDLen {
		return false
	}

	for _, r := range id {
		if r < '0' || r > '9' {
			return false
		}
	}

	return true
}

func mac(msg string) string {
	h := hmac.New(sha256.New, emulatorMACKey)
	h.Write([]byte(msg))

	return hex.EncodeToString(h.Sum(nil))
}

func validateSpec(spec *ProductSpec) error {
	if !validProductCode(spec.Code) {
		return fail(ErrInvalidProductCode, "ProductCode is not valid")
	}

	seen := map[string]bool{}

	for i := range spec.Dimensions {
		d := &spec.Dimensions[i]
		if d.Name == "" || len(d.Name) > maxDimensionLen || seen[d.Name] {
			return fail(ErrInvalidUsageDimension, "dimension %q is empty, too long or repeated", d.Name)
		}

		seen[d.Name] = true
	}

	return nil
}

// RegisterProduct declares a product. Customers that subscribed earlier get the new dimensions' entitlements.
func (b *InMemoryBackend) RegisterProduct(spec ProductSpec) error {
	if err := validateSpec(&spec); err != nil {
		return err
	}

	if spec.PublicKeyVersion <= 0 {
		spec.PublicKeyVersion = defaultPublicKeyVersion
	}

	b.mu.Lock("RegisterProduct")
	defer b.mu.Unlock()

	registeredAt := awstime.NewEpochTime(b.now())
	if old, ok := b.products.Get(spec.Code); ok {
		registeredAt = old.RegisteredAt
	}

	p := &Product{
		Code: spec.Code, Dimensions: spec.Dimensions, PublicKeyVersion: spec.PublicKeyVersion,
		Declared: true, RegisteredAt: registeredAt,
	}
	b.products.Put(p)

	for _, s := range b.subscriptions.All() {
		if s.ProductCode == p.Code {
			b.grantLocked(p, s)
		}
	}

	return nil
}

// Subscribe records account as a customer of the product and grants the product's entitlements.
func (b *InMemoryBackend) Subscribe(productCode, accountID string) (*Subscription, error) {
	if !validProductCode(productCode) {
		return nil, fail(ErrInvalidProductCode, "ProductCode is not valid")
	}

	if !validAccountID(accountID) {
		return nil, fail(ErrInvalidCustomerIdentifier, "customer account %q is not a 12-digit account ID", accountID)
	}

	b.mu.Lock("Subscribe")
	defer b.mu.Unlock()

	return b.subscribeLocked(productCode, accountID), nil
}

func (b *InMemoryBackend) subscribeLocked(productCode, accountID string) *Subscription {
	p := b.productLocked(productCode)

	s, ok := b.subscriptions.Get(productCode + "|" + accountID)
	if !ok {
		s = &Subscription{
			CreatedAt: awstime.NewEpochTime(b.now()), ProductCode: productCode,
			CustomerAWSAccountID: accountID, CustomerIdentifier: customerID(productCode, accountID),
		}
		b.subscriptions.Put(s)
	}

	b.grantLocked(p, s)

	cp := *s

	return &cp
}

// Unsubscribe removes a customer's subscription and the entitlements derived from it.
func (b *InMemoryBackend) Unsubscribe(productCode, accountID string) {
	b.mu.Lock("Unsubscribe")
	defer b.mu.Unlock()

	s, ok := b.subscriptions.Get(productCode + "|" + accountID)
	if !ok {
		return
	}

	b.subscriptions.Delete(productCode + "|" + accountID)

	if b.sink != nil {
		b.sink.RevokeEntitlements(productCode, s.CustomerIdentifier)
	}
}

// productLocked returns the product, creating an undeclared (open) one on first sight.
func (b *InMemoryBackend) productLocked(code string) *Product {
	if p, ok := b.products.Get(code); ok {
		return p
	}

	p := &Product{
		Code: code, PublicKeyVersion: defaultPublicKeyVersion, RegisteredAt: awstime.NewEpochTime(b.now()),
	}
	b.products.Put(p)

	return p
}

func (b *InMemoryBackend) grantLocked(p *Product, s *Subscription) {
	if b.sink == nil {
		return
	}

	for _, d := range p.Dimensions {
		if d.Value == nil {
			continue
		}

		_ = b.sink.GrantEntitlement(ent.Entitlement{
			ProductCode: p.Code, CustomerIdentifier: s.CustomerIdentifier,
			CustomerAWSAccountID: s.CustomerAWSAccountID, Dimension: d.Name,
			Value: d.Value, ExpirationDate: d.ExpiresAt,
		})
	}
}

func (b *InMemoryBackend) subscriptionLocked(productCode, account, identifier string) (*Subscription, bool) {
	if account != "" {
		if s, ok := b.subscriptions.Get(productCode + "|" + account); ok {
			return s, true
		}
	}

	if identifier != "" {
		for _, s := range b.subscriptions.All() {
			if s.ProductCode == productCode && s.CustomerIdentifier == identifier {
				return s, true
			}
		}
	}

	return nil, false
}

// IssueRegistrationToken mints the token the Marketplace console would redirect a new subscriber with.
// Tokens are HMAC-signed and valid for four hours.
func (b *InMemoryBackend) IssueRegistrationToken(productCode, accountID string) (string, error) {
	if !validProductCode(productCode) {
		return "", fail(ErrInvalidProductCode, "ProductCode is not valid")
	}

	if !validAccountID(accountID) {
		return "", fail(ErrInvalidCustomerIdentifier, "customer account %q is not a 12-digit account ID", accountID)
	}

	b.mu.RLock("IssueRegistrationToken")
	issued := b.now().Unix()
	b.mu.RUnlock()

	raw, _ := json.Marshal(tokenClaims{P: productCode, A: accountID, I: issued})
	payload := base64.RawURLEncoding.EncodeToString(raw)

	return payload + "." + mac(payload), nil
}

// ResolveCustomer verifies a registration token, subscribes the customer to the product and returns the identity.
func (b *InMemoryBackend) ResolveCustomer(token string) (*ResolvedCustomer, error) {
	if token == "" {
		return nil, fail(ErrValidation, "RegistrationToken is required")
	}

	payload, sig, ok := strings.Cut(token, ".")
	if !ok || !hmac.Equal([]byte(sig), []byte(mac(payload))) {
		return nil, fail(ErrInvalidToken, "the registration token is not valid")
	}

	raw, err := base64.RawURLEncoding.DecodeString(payload)

	var claims tokenClaims

	if err != nil || json.Unmarshal(raw, &claims) != nil || !validProductCode(claims.P) || !validAccountID(claims.A) {
		return nil, fail(ErrInvalidToken, "the registration token is not valid")
	}

	b.mu.Lock("ResolveCustomer")
	defer b.mu.Unlock()

	if b.now().Sub(time.Unix(claims.I, 0)) > tokenLifetime {
		return nil, fail(ErrExpiredToken, "the registration token has expired")
	}

	s := b.subscribeLocked(claims.P, claims.A)

	return &ResolvedCustomer{
		CustomerAWSAccountID: s.CustomerAWSAccountID,
		CustomerIdentifier:   s.CustomerIdentifier,
		ProductCode:          s.ProductCode,
	}, nil
}
