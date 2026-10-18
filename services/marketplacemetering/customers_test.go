package marketplacemetering_test

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	entsdk "github.com/aws/aws-sdk-go-v2/service/marketplaceentitlementservice"
	enttypes "github.com/aws/aws-sdk-go-v2/service/marketplaceentitlementservice/types"
	sdk "github.com/aws/aws-sdk-go-v2/service/marketplacemetering"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mm "github.com/blackbirdworks/gopherstack/services/marketplacemetering"
)

func TestRegisterUsage(t *testing.T) {
	t.Parallel()

	declared := mm.ProductSpec{Code: "container-product", PublicKeyVersion: 2}

	tests := []struct {
		setup    func(t *testing.T, f *fixture)
		name     string
		product  string
		wantCode string
		version  int32
	}{
		{name: "open product", product: testProduct, version: 1},
		{
			name: "declared and subscribed", product: declared.Code, version: 2,
			setup: func(t *testing.T, f *fixture) {
				t.Helper()
				require.NoError(t, f.backend.RegisterProduct(declared))
				_, err := f.backend.Subscribe(declared.Code, testAccount)
				require.NoError(t, err)
			},
		},
		{
			name: "declared and not subscribed", product: declared.Code, version: 2,
			setup: func(t *testing.T, f *fixture) {
				t.Helper()
				require.NoError(t, f.backend.RegisterProduct(declared))
			},
			wantCode: "CustomerNotEntitledException",
		},
		{
			name: "stale key version", product: declared.Code, version: 1,
			setup: func(t *testing.T, f *fixture) {
				t.Helper()
				require.NoError(t, f.backend.RegisterProduct(declared))
				_, err := f.backend.Subscribe(declared.Code, testAccount)
				require.NoError(t, err)
			},
			wantCode: "InvalidPublicKeyVersionException",
		},
		{name: "bad product", product: "", version: 1, wantCode: "InvalidProductCodeException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			if tt.setup != nil {
				tt.setup(t, f)
			}

			out, err := f.client.RegisterUsage(t.Context(), &sdk.RegisterUsageInput{
				ProductCode: aws.String(tt.product), PublicKeyVersion: new(tt.version), Nonce: aws.String("n-1"),
			})
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, apiCode(err))

				return
			}

			require.NoError(t, err)

			parts := strings.Split(aws.ToString(out.Signature), ".")
			require.Len(t, parts, 3)

			raw, decErr := base64.RawURLEncoding.DecodeString(parts[1])
			require.NoError(t, decErr)

			var claims map[string]any
			require.NoError(t, json.Unmarshal(raw, &claims))
			assert.Equal(t, tt.product, claims["productCode"])
			assert.Equal(t, "n-1", claims["nonce"])
			assert.Equal(t, testAccount, claims["customerAWSAccountId"])
			assert.True(t, out.PublicKeyRotationTimestamp.After(f.now()))

			regs := f.backend.Registrations(tt.product)
			require.Len(t, regs, 1)
			assert.Equal(t, "n-1", regs[0].Nonce)
		})
	}
}

func TestResolveCustomerDerivesEntitlements(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	require.NoError(t, f.backend.RegisterProduct(mm.ProductSpec{Code: "saas-product", Dimensions: []mm.Dimension{
		{Name: "Seats", Value: intValue(10)},
		{Name: "Premium", Value: boolValue(true)},
	}}))

	token, err := f.backend.IssueRegistrationToken("saas-product", otherAccount)
	require.NoError(t, err)

	got, err := f.client.ResolveCustomer(t.Context(), &sdk.ResolveCustomerInput{RegistrationToken: aws.String(token)})
	require.NoError(t, err)
	assert.Equal(t, "saas-product", aws.ToString(got.ProductCode))
	assert.Equal(t, otherAccount, aws.ToString(got.CustomerAWSAccountId))
	assert.NotEmpty(t, aws.ToString(got.CustomerIdentifier))

	again, err := f.client.ResolveCustomer(t.Context(), &sdk.ResolveCustomerInput{RegistrationToken: aws.String(token)})
	require.NoError(t, err)
	assert.Equal(t, got.CustomerIdentifier, again.CustomerIdentifier)

	all, err := f.entitled.GetEntitlements(
		t.Context(),
		&entsdk.GetEntitlementsInput{ProductCode: aws.String("saas-product")},
	)
	require.NoError(t, err)
	require.Len(t, all.Entitlements, 2)

	byDim, err := f.entitled.GetEntitlements(t.Context(), &entsdk.GetEntitlementsInput{
		ProductCode: aws.String("saas-product"),
		Filter: map[string][]string{
			string(enttypes.GetEntitlementFilterNameCustomerIdentifier): {aws.ToString(got.CustomerIdentifier)},
			string(enttypes.GetEntitlementFilterNameDimension):          {"Seats"},
		},
	})
	require.NoError(t, err)
	require.Len(t, byDim.Entitlements, 1)
	assert.Equal(t, "Seats", aws.ToString(byDim.Entitlements[0].Dimension))
	assert.Equal(t, otherAccount, aws.ToString(byDim.Entitlements[0].CustomerAWSAccountId))

	require.NotNil(t, byDim.Entitlements[0].Value)
	assert.Equal(t, int32(10), aws.ToInt32(byDim.Entitlements[0].Value.IntegerValue))

	f.backend.Unsubscribe("saas-product", otherAccount)

	none, err := f.entitled.GetEntitlements(
		t.Context(),
		&entsdk.GetEntitlementsInput{ProductCode: aws.String("saas-product")},
	)
	require.NoError(t, err)
	assert.Empty(t, none.Entitlements)
}

func TestResolveCustomerErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		token    func(t *testing.T, f *fixture) string
		name     string
		wantCode string
	}{
		{
			name:     "garbage",
			token:    func(*testing.T, *fixture) string { return "nonsense" },
			wantCode: "InvalidTokenException",
		},
		{
			name: "tampered",
			token: func(t *testing.T, f *fixture) string {
				t.Helper()
				tok, err := f.backend.IssueRegistrationToken(testProduct, testAccount)
				require.NoError(t, err)

				return tok[:len(tok)-1] + "0"
			},
			wantCode: "InvalidTokenException",
		},
		{
			name: "expired",
			token: func(t *testing.T, f *fixture) string {
				t.Helper()
				tok, err := f.backend.IssueRegistrationToken(testProduct, testAccount)
				require.NoError(t, err)
				f.advance(5 * time.Hour)

				return tok
			},
			wantCode: "ExpiredTokenException",
		},
		{
			name: "still valid",
			token: func(t *testing.T, f *fixture) string {
				t.Helper()
				tok, err := f.backend.IssueRegistrationToken(testProduct, testAccount)
				require.NoError(t, err)
				f.advance(3 * time.Hour)

				return tok
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			_, err := f.client.ResolveCustomer(t.Context(), &sdk.ResolveCustomerInput{
				RegistrationToken: aws.String(tt.token(t, f)),
			})
			assert.Equal(t, tt.wantCode, apiCode(err))
		})
	}
}

func TestIssueRegistrationTokenValidation(t *testing.T) {
	t.Parallel()

	b := newFixture(t).backend

	_, err := b.IssueRegistrationToken("", testAccount)
	require.Error(t, err)

	_, err = b.IssueRegistrationToken(testProduct, "not-an-account")
	require.Error(t, err)
}
