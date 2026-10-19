package marketplacemetering

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// RegisterUsageInput is the RegisterUsage request.
type RegisterUsageInput struct {
	PublicKeyVersion *int32 `json:"PublicKeyVersion,omitempty"`
	ProductCode      string `json:"ProductCode"`
	Nonce            string `json:"Nonce,omitempty"`
}

// RegisterUsageOutput is the RegisterUsage response.
type RegisterUsageOutput struct {
	PublicKeyRotationTimestamp *awstime.EpochTime `json:"PublicKeyRotationTimestamp,omitempty"`
	Signature                  string             `json:"Signature,omitempty"`
}

// RegisterUsage validates the product and public key version, records the registration and returns an
// HS256-signed token over the product, nonce and account (the emulator signs with its own key).
func (b *InMemoryBackend) RegisterUsage(ctx context.Context, in *RegisterUsageInput) (*RegisterUsageOutput, error) {
	if !validProductCode(in.ProductCode) {
		return nil, fail(ErrInvalidProductCode, "ProductCode is not valid")
	}

	if in.PublicKeyVersion == nil || *in.PublicKeyVersion < 1 {
		return nil, fail(ErrValidation, "PublicKeyVersion is required and must be at least 1")
	}

	account := b.callerAccount(ctx)

	b.mu.Lock("RegisterUsage")
	defer b.mu.Unlock()

	p := b.productLocked(in.ProductCode)
	if *in.PublicKeyVersion != p.PublicKeyVersion {
		return nil, fail(ErrInvalidPublicKeyVersion, "public key version %d is not current (%d)",
			*in.PublicKeyVersion, p.PublicKeyVersion)
	}

	if _, ok := b.subscriptionLocked(in.ProductCode, account, ""); p.Declared && !ok {
		return nil, fail(ErrCustomerNotEntitled, "account %s is not subscribed to the product", account)
	}

	now := b.now()
	b.registrations.Put(&Registration{
		ID: awsid.NewN("", recordIDLen), ProductCode: in.ProductCode, Nonce: in.Nonce, AccountID: account,
		PublicKeyVersion: *in.PublicKeyVersion, RegisteredAt: awstime.NewEpochTime(now),
	})

	rotation := awstime.NewEpochTime(p.RegisteredAt.Add(keyRotationPeriod))

	return &RegisterUsageOutput{
		PublicKeyRotationTimestamp: &rotation,
		Signature:                  signRegistration(p, in, account, now.Unix()),
	}, nil
}

func signRegistration(p *Product, in *RegisterUsageInput, account string, issued int64) string {
	enc := base64.RawURLEncoding
	header, _ := json.Marshal(map[string]string{
		"alg": "HS256", "typ": "JWT", "kid": "v" + strconv.Itoa(int(p.PublicKeyVersion)),
	})
	claims, _ := json.Marshal(map[string]any{
		"productCode": in.ProductCode, "nonce": in.Nonce, "customerAWSAccountId": account,
		"publicKeyVersion": p.PublicKeyVersion, "iat": issued,
	})
	signing := enc.EncodeToString(header) + "." + enc.EncodeToString(claims)

	return signing + "." + enc.EncodeToString([]byte(mac(signing)))
}

// Registrations returns the RegisterUsage calls recorded for a product.
func (b *InMemoryBackend) Registrations(productCode string) []Registration {
	b.mu.RLock("Registrations")
	defer b.mu.RUnlock()

	var out []Registration

	for _, r := range b.registrations.All() {
		if r.ProductCode == productCode {
			out = append(out, *r)
		}
	}

	return out
}
