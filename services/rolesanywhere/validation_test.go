package rolesanywhere_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rolesanywhere"
)

func TestCreateProfile_Validation(t *testing.T) {
	t.Parallel()

	const role = "arn:aws:iam::123456789012:role/r"

	short, long, ok := int32(899), int32(43201), int32(900)

	tests := []struct {
		duration *int32
		name     string
		pname    string
		roles    []string
		wantErr  bool
	}{
		{name: "valid", pname: "my profile-1_x", roles: []string{role}, duration: &ok},
		{name: "bad name", pname: "bad!name", roles: []string{role}, wantErr: true},
		{name: "name too long", pname: strings.Repeat("a", 256), roles: []string{role}, wantErr: true},
		{name: "bad role arn", pname: "p", roles: []string{"notarn"}, wantErr: true},
		{name: "duration low", pname: "p", roles: []string{role}, duration: &short, wantErr: true},
		{name: "duration high", pname: "p", roles: []string{role}, duration: &long, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.CreateProfile(t.Context(), tt.pname, tt.roles, nil, tt.duration, nil, "", false, nil, nil)

			if tt.wantErr {
				require.ErrorIs(t, err, rolesanywhere.ErrValidation)
				assert.NotEqual(t, "ValidationException", err.Error())

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestPutNotificationSettings_Validation(t *testing.T) {
	t.Parallel()

	low, high, ok := int32(0), int32(361), int32(30)

	tests := []struct {
		name    string
		setting rolesanywhere.NotificationSetting
		wantErr bool
	}{
		{name: "valid", setting: rolesanywhere.NotificationSetting{Event: "CA_CERTIFICATE_EXPIRY", Threshold: &ok}},
		{name: "bad event", setting: rolesanywhere.NotificationSetting{Event: "NOPE"}, wantErr: true},
		{name: "bad channel", setting: rolesanywhere.NotificationSetting{
			Event: "CA_CERTIFICATE_EXPIRY", Channel: "SMS",
		}, wantErr: true},
		{name: "threshold low", setting: rolesanywhere.NotificationSetting{
			Event: "CA_CERTIFICATE_EXPIRY", Threshold: &low,
		}, wantErr: true},
		{name: "threshold high", setting: rolesanywhere.NotificationSetting{
			Event: "CA_CERTIFICATE_EXPIRY", Threshold: &high,
		}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			ta, err := b.CreateTrustAnchor(
				t.Context(), "ta", rolesanywhere.TrustAnchorSource{SourceType: "CERTIFICATE_BUNDLE"}, nil, nil, nil,
			)
			require.NoError(t, err)

			_, err = b.PutNotificationSettings(
				t.Context(),
				ta.TrustAnchorID,
				[]rolesanywhere.NotificationSetting{tt.setting},
			)
			if tt.wantErr {
				require.ErrorIs(t, err, rolesanywhere.ErrValidation)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestErrorMessages_NoCodePrefix(t *testing.T) {
	t.Parallel()

	rec := doREST(t, newTestHandler(t), "GET", "/profile/11111111-1111-1111-1111-111111111111", nil)
	require.Equal(t, 404, rec.Code)
	assert.NotContains(t, rec.Body.String(), "ResourceNotFoundException\"}")
	assert.NotContains(t, rec.Body.String(), "ResourceNotFoundException:")
	assert.Contains(t, rec.Body.String(), "does not exist")
}
