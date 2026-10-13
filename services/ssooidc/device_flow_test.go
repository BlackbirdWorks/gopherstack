package ssooidc_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ssooidc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oidc "github.com/blackbirdworks/gopherstack/services/ssooidc"
)

const (
	grantDevice  = "urn:ietf:params:oauth:grant-type:device_code"
	grantRefresh = "refresh_token"
)

func (f fixture) startDevice(t *testing.T, id, secret string) *ssooidc.StartDeviceAuthorizationOutput {
	t.Helper()

	out, err := f.client.StartDeviceAuthorization(t.Context(), &ssooidc.StartDeviceAuthorizationInput{
		ClientId: aws.String(id), ClientSecret: aws.String(secret), StartUrl: aws.String(startURL),
	})
	require.NoError(t, err)

	return out
}

func (f fixture) deviceToken(t *testing.T, id, secret, deviceCode string) (*ssooidc.CreateTokenOutput, error) {
	t.Helper()

	return f.client.CreateToken(t.Context(), &ssooidc.CreateTokenInput{
		ClientId: aws.String(id), ClientSecret: aws.String(secret),
		GrantType: aws.String(grantDevice), DeviceCode: aws.String(deviceCode),
	})
}

func TestStartDeviceAuthorization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate   func(in *ssooidc.StartDeviceAuthorizationInput)
		name     string
		wantCode string
		grants   []string
	}{
		{name: "ok", grants: []string{grantDevice}},
		{name: "legacy_client_ok"},
		{
			name: "bad_secret", grants: []string{grantDevice}, wantCode: "InvalidClientException",
			mutate: func(in *ssooidc.StartDeviceAuthorizationInput) { in.ClientSecret = aws.String("wrong") },
		},
		{
			name: "unknown_client", grants: []string{grantDevice}, wantCode: "InvalidClientException",
			mutate: func(in *ssooidc.StartDeviceAuthorizationInput) { in.ClientId = aws.String("nope") },
		},
		{
			name: "blank_start_url", grants: []string{grantDevice}, wantCode: "InvalidRequestException",
			mutate: func(in *ssooidc.StartDeviceAuthorizationInput) { in.StartUrl = aws.String("") },
		},
		{name: "device_grant_not_registered", grants: []string{grantRefresh}, wantCode: "UnauthorizedClientException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			id, secret := f.register(t, tt.grants...)
			in := &ssooidc.StartDeviceAuthorizationInput{
				ClientId: aws.String(id), ClientSecret: aws.String(secret), StartUrl: aws.String(startURL),
			}

			if tt.mutate != nil {
				tt.mutate(in)
			}

			out, err := f.client.StartDeviceAuthorization(t.Context(), in)
			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, apiCode(err))

				return
			}

			require.NoError(t, err)
			assert.Regexp(t, `^[BCDFGHJKLMNPQRSTVWXZ]{4}-[BCDFGHJKLMNPQRSTVWXZ]{4}$`, aws.ToString(out.UserCode))
			assert.NotEmpty(t, aws.ToString(out.DeviceCode))
			assert.Equal(t, int32(600), out.ExpiresIn)
			assert.Equal(t, int32(1), out.Interval)
			assert.Equal(t, f.srv.URL+oidc.DevicePath, aws.ToString(out.VerificationUri))
			assert.Equal(t, f.srv.URL+oidc.DevicePath+"?user_code="+aws.ToString(out.UserCode),
				aws.ToString(out.VerificationUriComplete))
		})
	}
}

func TestDeviceFlowSDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		act       func(t *testing.T, f fixture, userCode string)
		name      string
		wantCode  string
		wantToken bool
	}{
		{name: "pending", act: func(*testing.T, fixture, string) {}, wantCode: "AuthorizationPendingException"},
		{
			name: "approved",
			act: func(t *testing.T, f fixture, code string) {
				t.Helper()
				require.NoError(t, f.handler.Backend.ApproveDevice(code, oidc.Subject{UserName: "alice"}))
			},
			wantToken: true,
		},
		{
			name: "denied",
			act: func(t *testing.T, f fixture, code string) {
				t.Helper()
				require.NoError(t, f.handler.Backend.DenyDevice(code))
			},
			wantCode: "AccessDeniedException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			id, secret := f.register(t, grantDevice, grantRefresh)
			dev := f.startDevice(t, id, secret)
			tt.act(t, f, aws.ToString(dev.UserCode))

			out, err := f.deviceToken(t, id, secret, aws.ToString(dev.DeviceCode))
			if !tt.wantToken {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, apiCode(err))

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "Bearer", aws.ToString(out.TokenType))
			assert.Equal(t, int32(28800), out.ExpiresIn)
			assert.NotEmpty(t, aws.ToString(out.RefreshToken))

			sess, serr := f.handler.Backend.SessionForAccessToken(aws.ToString(out.AccessToken))
			require.NoError(t, serr)
			assert.Equal(t, "alice", sess.Subject.UserName)

			_, err = f.deviceToken(t, id, secret, aws.ToString(dev.DeviceCode))
			assert.Equal(t, "InvalidGrantException", apiCode(err), "a device code is single use")
		})
	}
}

func TestCreateTokenErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate   func(in *ssooidc.CreateTokenInput)
		name     string
		wantCode string
		grants   []string
	}{
		{
			name: "bad_secret", grants: []string{grantDevice}, wantCode: "InvalidClientException",
			mutate: func(in *ssooidc.CreateTokenInput) { in.ClientSecret = aws.String("wrong") },
		},
		{
			name: "unsupported_grant", grants: []string{grantDevice}, wantCode: "UnsupportedGrantTypeException",
			mutate: func(in *ssooidc.CreateTokenInput) { in.GrantType = aws.String("password") },
		},
		{
			name: "grant_not_registered", grants: []string{grantDevice}, wantCode: "UnauthorizedClientException",
			mutate: func(in *ssooidc.CreateTokenInput) { in.GrantType = aws.String(grantRefresh) },
		},
		{
			name: "missing_device_code", grants: []string{grantDevice}, wantCode: "InvalidRequestException",
			mutate: func(in *ssooidc.CreateTokenInput) { in.DeviceCode = nil },
		},
		{
			name: "unknown_device_code", grants: []string{grantDevice}, wantCode: "InvalidGrantException",
			mutate: func(in *ssooidc.CreateTokenInput) { in.DeviceCode = aws.String("nope") },
		},
		{
			name:     "unknown_refresh_token",
			grants:   []string{grantDevice, grantRefresh},
			wantCode: "InvalidGrantException",
			mutate: func(in *ssooidc.CreateTokenInput) {
				in.GrantType = aws.String(grantRefresh)
				in.RefreshToken = aws.String("nope")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			id, secret := f.register(t, tt.grants...)
			dev := f.startDevice(t, id, secret)
			in := &ssooidc.CreateTokenInput{
				ClientId: aws.String(id), ClientSecret: aws.String(secret),
				GrantType: aws.String(grantDevice), DeviceCode: dev.DeviceCode,
			}
			tt.mutate(in)

			_, err := f.client.CreateToken(t.Context(), in)
			require.Error(t, err)
			assert.Equal(t, tt.wantCode, apiCode(err))
		})
	}
}

func TestDeviceCodeBelongsToClient(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	idA, secretA := f.register(t, grantDevice)
	idB, secretB := f.register(t, grantDevice)
	dev := f.startDevice(t, idA, secretA)
	require.NoError(t, f.handler.Backend.ApproveDevice(aws.ToString(dev.UserCode), oidc.Subject{UserName: "alice"}))

	_, err := f.deviceToken(t, idB, secretB, aws.ToString(dev.DeviceCode))
	assert.Equal(t, "InvalidGrantException", apiCode(err))
}

func TestRefreshTokenGrant(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	id, secret := f.register(t, grantDevice, grantRefresh)
	dev := f.startDevice(t, id, secret)
	require.NoError(t, f.handler.Backend.ApproveDevice(aws.ToString(dev.UserCode), oidc.Subject{UserName: "alice"}))

	first, err := f.deviceToken(t, id, secret, aws.ToString(dev.DeviceCode))
	require.NoError(t, err)

	second, err := f.client.CreateToken(t.Context(), &ssooidc.CreateTokenInput{
		ClientId: aws.String(id), ClientSecret: aws.String(secret),
		GrantType: aws.String(grantRefresh), RefreshToken: first.RefreshToken,
	})
	require.NoError(t, err)
	assert.NotEqual(t, aws.ToString(first.AccessToken), aws.ToString(second.AccessToken))
	assert.Equal(t, aws.ToString(first.RefreshToken), aws.ToString(second.RefreshToken))

	sess, err := f.handler.Backend.SessionForAccessToken(aws.ToString(second.AccessToken))
	require.NoError(t, err)
	assert.Equal(t, "alice", sess.Subject.UserName)

	require.NoError(t, f.handler.Backend.RevokeSession(aws.ToString(second.AccessToken)))

	_, err = f.client.CreateToken(t.Context(), &ssooidc.CreateTokenInput{
		ClientId: aws.String(id), ClientSecret: aws.String(secret),
		GrantType: aws.String(grantRefresh), RefreshToken: first.RefreshToken,
	})
	assert.Equal(t, "InvalidGrantException", apiCode(err), "logout revokes the refresh token")

	_, err = f.handler.Backend.SessionForAccessToken(aws.ToString(first.AccessToken))
	assert.ErrorIs(t, err, oidc.ErrTokenNotFound)
}
