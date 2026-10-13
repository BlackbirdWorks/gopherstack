package ssooidc_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oidc "github.com/blackbirdworks/gopherstack/services/ssooidc"
)

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	src := oidc.NewInMemoryBackend()
	reg, err := src.RegisterClient(&oidc.RegisterClientInput{
		ClientName: "c", ClientType: "public", GrantTypes: []string{grantDevice, grantRefresh},
	})
	require.Nil(t, err)

	dev, err := src.StartDeviceAuthorization(reg.ClientID, reg.ClientSecret, startURL)
	require.Nil(t, err)
	require.NoError(t, src.ApproveDevice(dev.UserCode, oidc.Subject{UserName: "alice", UserID: "u1"}))

	tok, err := src.CreateToken(&oidc.CreateTokenInput{
		ClientID: reg.ClientID, ClientSecret: reg.ClientSecret, GrantType: grantDevice, DeviceCode: dev.DeviceCode,
	}, false)
	require.Nil(t, err)

	snap := src.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	dst := oidc.NewInMemoryBackend()
	require.NoError(t, dst.Restore(t.Context(), snap))

	sess, serr := dst.SessionForAccessToken(tok.AccessToken)
	require.NoError(t, serr)
	assert.Equal(t, "alice", sess.Subject.UserName)

	refreshed, rerr := dst.CreateToken(&oidc.CreateTokenInput{
		ClientID: reg.ClientID, ClientSecret: reg.ClientSecret, GrantType: grantRefresh, RefreshToken: tok.RefreshToken,
	}, false)
	require.Nil(t, rerr)
	assert.NotEmpty(t, refreshed.AccessToken)

	dst.Reset()

	_, serr = dst.SessionForAccessToken(tok.AccessToken)
	assert.ErrorIs(t, serr, oidc.ErrTokenNotFound)
}

func TestRestoreIgnoresIncompatibleVersion(t *testing.T) {
	t.Parallel()

	b := oidc.NewInMemoryBackend()
	require.NoError(t, b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`)))
}
