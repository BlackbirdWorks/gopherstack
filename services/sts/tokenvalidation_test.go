package sts_test

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/sts"
)

// makeUnitJWT builds a header.payload.sig JWT with the given claims for tests.
func makeUnitJWT(t *testing.T, claims map[string]any) string {
	t.Helper()

	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))

	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal claims: %v", err)
	}

	return header + "." + base64.RawURLEncoding.EncodeToString(payload) + ".sig"
}

// TestValidateWebIdentityToken covers temporal and structural JWT validation.
func TestValidateWebIdentityToken(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	tests := []struct {
		wantErr error
		name    string
		token   string
	}{
		{
			name:    "opaque_non_jwt_permissive",
			token:   "some-opaque-token",
			wantErr: nil,
		},
		{
			name:    "two_segment_token_permissive",
			token:   "aaa.bbb",
			wantErr: nil,
		},
		{
			name:    "valid_future_exp",
			token:   makeUnitJWT(t, map[string]any{"sub": "u", "exp": now.Add(time.Hour).Unix()}),
			wantErr: nil,
		},
		{
			name:    "expired_exp",
			token:   makeUnitJWT(t, map[string]any{"sub": "u", "exp": now.Add(-time.Hour).Unix()}),
			wantErr: sts.ErrExpiredToken,
		},
		{
			name:    "not_yet_valid_nbf",
			token:   makeUnitJWT(t, map[string]any{"sub": "u", "nbf": now.Add(time.Hour).Unix()}),
			wantErr: sts.ErrInvalidIdentityToken,
		},
		{
			name: "nbf_within_skew_ok",
			token: makeUnitJWT(
				t,
				map[string]any{"sub": "u", "nbf": now.Add(-time.Second).Unix()},
			),
			wantErr: nil,
		},
		{
			name:    "issued_in_future_iat",
			token:   makeUnitJWT(t, map[string]any{"sub": "u", "iat": now.Add(time.Hour).Unix()}),
			wantErr: sts.ErrInvalidIdentityToken,
		},
		{
			name:    "iat_in_past_ok",
			token:   makeUnitJWT(t, map[string]any{"sub": "u", "iat": now.Add(-time.Hour).Unix()}),
			wantErr: nil,
		},
		{
			name:    "malformed_three_part_payload",
			token:   "header.notbase64json.sig",
			wantErr: sts.ErrInvalidIdentityToken,
		},
		{
			name: "no_temporal_claims_ok",
			token: makeUnitJWT(
				t,
				map[string]any{"sub": "u", "iss": "https://example.com", "aud": "app"},
			),
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := sts.ValidateWebIdentityToken(tt.token)
			switch {
			case tt.wantErr == nil && err != nil:
				t.Fatalf("want nil, got %v", err)
			case tt.wantErr != nil && !errors.Is(err, tt.wantErr):
				t.Fatalf("want %v, got %v", tt.wantErr, err)
			}
		})
	}
}

// TestValidateSAMLTemporalConditions covers the SAML NotBefore/NotOnOrAfter window.
func TestValidateSAMLTemporalConditions(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	assertion := func(notBefore, notOnOrAfter string) string {
		doc := `<samlp:Response xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol">` +
			`<saml:Assertion xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion">` +
			`<saml:Conditions NotBefore="` + notBefore + `" NotOnOrAfter="` + notOnOrAfter + `">` +
			`</saml:Conditions></saml:Assertion></samlp:Response>`

		return base64.StdEncoding.EncodeToString([]byte(doc))
	}

	rfc := func(d time.Duration) string {
		return now.Add(d).Format(time.RFC3339)
	}

	tests := []struct {
		name      string
		assertion string
		wantErr   bool
	}{
		{
			name:      "no_conditions_permissive",
			assertion: base64.StdEncoding.EncodeToString([]byte(`<samlp:Assertion/>`)),
			wantErr:   false,
		},
		{
			name:      "within_window_ok",
			assertion: assertion(rfc(-time.Hour), rfc(time.Hour)),
			wantErr:   false,
		},
		{
			name:      "before_notbefore_denied",
			assertion: assertion(rfc(time.Hour), rfc(2*time.Hour)),
			wantErr:   true,
		},
		{
			name:      "after_notonorafter_denied",
			assertion: assertion(rfc(-2*time.Hour), rfc(-time.Hour)),
			wantErr:   true,
		},
		{
			name:      "not_base64_permissive",
			assertion: "@@@not-base64@@@",
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := sts.ValidateSAMLTemporalConditions(tt.assertion)
			if tt.wantErr {
				if !errors.Is(err, sts.ErrInvalidSAMLAssertion) {
					t.Fatalf("want ErrInvalidSAMLAssertion, got %v", err)
				}

				return
			}

			if err != nil {
				t.Fatalf("want nil, got %v", err)
			}
		})
	}
}

// TestParseSAMLTime covers the accepted SAML timestamp layouts.
func TestParseSAMLTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{name: "rfc3339_z", value: "2026-01-02T15:04:05Z", wantErr: false},
		{name: "rfc3339_nano", value: "2026-01-02T15:04:05.123Z", wantErr: false},
		{name: "rfc3339_offset", value: "2026-01-02T15:04:05+02:00", wantErr: false},
		{name: "no_zone", value: "2026-01-02T15:04:05", wantErr: false},
		{name: "garbage", value: "not-a-time", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := sts.ParseSAMLTime(tt.value)
			if tt.wantErr != (err != nil) {
				t.Fatalf("wantErr=%v got err=%v", tt.wantErr, err)
			}
		})
	}
}

// TestJWTTimeClaim covers numeric-date extraction from claims.
func TestJWTTimeClaim(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		claims    map[string]any
		key       string
		wantOK    bool
		wantErr   bool
		wantEpoch int64
	}{
		{name: "absent", claims: map[string]any{}, key: "exp", wantOK: false},
		{
			name:      "float",
			claims:    map[string]any{"exp": float64(1000)},
			key:       "exp",
			wantOK:    true,
			wantEpoch: 1000,
		},
		{
			name:      "json_number",
			claims:    map[string]any{"exp": json.Number("2000")},
			key:       "exp",
			wantOK:    true,
			wantEpoch: 2000,
		},
		{name: "wrong_type", claims: map[string]any{"exp": "soon"}, key: "exp", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok, err := sts.JWTTimeClaim(tt.claims, tt.key)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("want error, got nil")
				}

				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if ok != tt.wantOK {
				t.Fatalf("ok=%v want %v", ok, tt.wantOK)
			}

			if ok && got.Unix() != tt.wantEpoch {
				t.Fatalf("epoch=%d want %d", got.Unix(), tt.wantEpoch)
			}
		})
	}
}
