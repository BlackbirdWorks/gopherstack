package ssooidc_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oidc "github.com/blackbirdworks/gopherstack/services/ssooidc"
)

const (
	stepPoll    = "poll"
	stepApprove = "approve"
	stepDeny    = "deny"
)

type deviceStep struct {
	action   string
	wantCode string
	wait     time.Duration
}

func TestDevicePollingStateMachine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		steps []deviceStep
	}{
		{
			name: "pending_then_slow_down",
			steps: []deviceStep{
				{action: stepPoll, wantCode: "AuthorizationPendingException"},
				{action: stepPoll, wantCode: "SlowDownException"},
			},
		},
		{
			name: "each_slow_down_raises_interval",
			steps: []deviceStep{
				{action: stepPoll, wantCode: "AuthorizationPendingException"},
				{action: stepPoll, wantCode: "SlowDownException"},
				{action: stepPoll, wait: 2 * time.Second, wantCode: "SlowDownException"},
				{action: stepPoll, wait: 10 * time.Second, wantCode: "SlowDownException"},
				{action: stepPoll, wait: 16 * time.Second, wantCode: "AuthorizationPendingException"},
			},
		},
		{
			name: "polling_after_interval_stays_pending",
			steps: []deviceStep{
				{action: stepPoll, wantCode: "AuthorizationPendingException"},
				{action: stepPoll, wait: time.Second, wantCode: "AuthorizationPendingException"},
				{action: stepPoll, wait: time.Second, wantCode: "AuthorizationPendingException"},
			},
		},
		{
			name: "approval_wins_over_slow_down",
			steps: []deviceStep{
				{action: stepPoll, wantCode: "AuthorizationPendingException"},
				{action: stepApprove},
				{action: stepPoll},
			},
		},
		{
			name: "denied",
			steps: []deviceStep{
				{action: stepPoll, wantCode: "AuthorizationPendingException"},
				{action: stepDeny},
				{action: stepPoll, wait: time.Second, wantCode: "AccessDeniedException"},
			},
		},
		{
			name: "expired",
			steps: []deviceStep{
				{action: stepPoll, wait: 601 * time.Second, wantCode: "ExpiredTokenException"},
			},
		},
		{
			name: "approved_but_expired",
			steps: []deviceStep{
				{action: stepApprove},
				{action: stepPoll, wait: 601 * time.Second, wantCode: "ExpiredTokenException"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := oidc.NewInMemoryBackend()
				reg, err := b.RegisterClient(&oidc.RegisterClientInput{
					ClientName: "c", ClientType: "public", GrantTypes: []string{grantDevice},
				})
				require.Nil(t, err)

				dev, err := b.StartDeviceAuthorization(reg.ClientID, reg.ClientSecret, startURL)
				require.Nil(t, err)

				for i, s := range tt.steps {
					time.Sleep(s.wait)
					runStep(t, b, reg, dev, s, i)
				}
			})
		})
	}
}

func runStep(
	t *testing.T, b *oidc.InMemoryBackend, reg *oidc.ClientRegistration, dev *oidc.DeviceAuthorization,
	s deviceStep, i int,
) {
	t.Helper()

	switch s.action {
	case stepApprove:
		require.NoError(t, b.ApproveDevice(dev.UserCode, oidc.Subject{UserName: "alice"}))
	case stepDeny:
		require.NoError(t, b.DenyDevice(dev.UserCode))
	default:
		res, err := b.CreateToken(&oidc.CreateTokenInput{
			ClientID: reg.ClientID, ClientSecret: reg.ClientSecret,
			GrantType: grantDevice, DeviceCode: dev.DeviceCode,
		}, false)

		if s.wantCode == "" {
			require.Nil(t, err, "step %d", i)
			assert.NotEmpty(t, res.AccessToken)

			return
		}

		require.NotNil(t, err, "step %d", i)
		assert.Equal(t, s.wantCode, err.Code, "step %d", i)
	}
}

func TestAccessTokenExpiry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		wait    time.Duration
	}{
		{name: "fresh", wait: time.Hour},
		{name: "just_before_expiry", wait: 8*time.Hour - time.Second},
		{name: "expired", wait: 8*time.Hour + time.Second, wantErr: oidc.ErrTokenNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := oidc.NewInMemoryBackend()
				reg, err := b.RegisterClient(&oidc.RegisterClientInput{
					ClientName: "c", ClientType: "public", GrantTypes: []string{grantDevice},
				})
				require.Nil(t, err)

				dev, err := b.StartDeviceAuthorization(reg.ClientID, reg.ClientSecret, startURL)
				require.Nil(t, err)
				require.NoError(t, b.ApproveDevice(dev.UserCode, oidc.Subject{UserName: "alice"}))

				tok, err := b.CreateToken(&oidc.CreateTokenInput{
					ClientID: reg.ClientID, ClientSecret: reg.ClientSecret,
					GrantType: grantDevice, DeviceCode: dev.DeviceCode,
				}, false)
				require.Nil(t, err)

				time.Sleep(tt.wait)

				_, serr := b.SessionForAccessToken(tok.AccessToken)
				if tt.wantErr == nil {
					require.NoError(t, serr)

					return
				}

				assert.Error(t, serr)
			})
		})
	}
}
