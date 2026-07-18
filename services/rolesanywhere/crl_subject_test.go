package rolesanywhere_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rolesanywhere"
)

// ---- CRL tests ----

func TestCrl_ImportGetListUpdateDeleteCycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		crlName        string
		trustAnchorArn string
		updateName     string
		crlData        []byte
		updateData     []byte
		enabled        bool
	}{
		{
			name:           "basic import and update",
			crlName:        "my-crl",
			trustAnchorArn: "arn:aws:rolesanywhere:us-east-1:123456789012:trust-anchor/abc",
			crlData:        []byte("fakecrldata"),
			enabled:        true,
			updateName:     "renamed-crl",
			updateData:     []byte("newcrldata"),
		},
		{
			name:           "disabled crl",
			crlName:        "disabled-crl",
			trustAnchorArn: "arn:aws:rolesanywhere:us-east-1:123456789012:trust-anchor/xyz",
			crlData:        []byte("crlbytes"),
			enabled:        false,
			updateName:     "still-disabled",
			updateData:     nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)

			// Import.
			crl, err := b.ImportCrl(context.Background(), tc.crlName, tc.crlData, tc.trustAnchorArn, tc.enabled, nil)
			require.NoError(t, err)
			assert.NotEmpty(t, crl.CrlID)
			assert.Equal(t, tc.crlName, crl.Name)
			assert.Equal(t, tc.enabled, crl.Enabled)
			assert.Contains(t, crl.CrlArn, "crl")

			// Get.
			got, err := b.GetCrl(context.Background(), crl.CrlID)
			require.NoError(t, err)
			assert.Equal(t, crl.CrlID, got.CrlID)
			assert.Equal(t, tc.crlName, got.Name)

			// List.
			all, _, err := b.ListCrls(context.Background(), "", 0)
			require.NoError(t, err)
			assert.Len(t, all, 1)

			// Update.
			updateData := tc.updateData
			updated, err := b.UpdateCrl(context.Background(), crl.CrlID, tc.updateName, updateData)
			require.NoError(t, err)
			assert.Equal(t, tc.updateName, updated.Name)

			// Delete.
			deleted, err := b.DeleteCrl(context.Background(), crl.CrlID)
			require.NoError(t, err)
			assert.Equal(t, crl.CrlID, deleted.CrlID)

			// Confirm gone.
			_, err = b.GetCrl(context.Background(), crl.CrlID)
			require.Error(t, err)
		})
	}
}

func TestCrl_EnableDisable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		startState bool
	}{
		{"starts enabled, disable then enable", true},
		{"starts disabled, enable then disable", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			crl, err := b.ImportCrl(context.Background(),
				"toggle-crl",
				[]byte("data"),
				"arn:aws:rolesanywhere:us-east-1:123:trust-anchor/t",
				tc.startState,
				nil,
			)
			require.NoError(t, err)
			assert.Equal(t, tc.startState, crl.Enabled)

			if tc.startState {
				disabled, err := b.DisableCrl(context.Background(), crl.CrlID) //nolint:govet // existing issue.
				require.NoError(t, err)
				assert.False(t, disabled.Enabled)

				enabled, err := b.EnableCrl(context.Background(), crl.CrlID)
				require.NoError(t, err)
				assert.True(t, enabled.Enabled)
			} else {
				enabled, err := b.EnableCrl(context.Background(), crl.CrlID) //nolint:govet // existing issue.
				require.NoError(t, err)
				assert.True(t, enabled.Enabled)

				disabled, err := b.DisableCrl(context.Background(), crl.CrlID)
				require.NoError(t, err)
				assert.False(t, disabled.Enabled)
			}
		})
	}
}

func TestCrl_NotFound(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	tests := []struct {
		run  func() error
		name string
	}{
		{name: "GetCrl", run: func() error {
			_, err := b.GetCrl(context.Background(), "no-such-id")

			return err
		}},
		{name: "UpdateCrl", run: func() error {
			_, err := b.UpdateCrl(context.Background(), "no-such-id", "name", nil)

			return err
		}},
		{name: "DeleteCrl", run: func() error {
			_, err := b.DeleteCrl(context.Background(), "no-such-id")

			return err
		}},
		{name: "EnableCrl", run: func() error {
			_, err := b.EnableCrl(context.Background(), "no-such-id")

			return err
		}},
		{name: "DisableCrl", run: func() error {
			_, err := b.DisableCrl(context.Background(), "no-such-id")

			return err
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Error(t, tc.run())
		})
	}
}

func TestCrl_DuplicateNameRejected(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.ImportCrl(context.Background(), "dup-crl", nil, "arn:ta", true, nil)
	require.NoError(t, err)

	_, err = b.ImportCrl(context.Background(), "dup-crl", nil, "arn:ta", true, nil)
	require.Error(t, err)
}

func TestImportCrl_EmptyName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		crlName string
		wantErr bool
	}{
		{"empty name returns validation error", "", true},
		{"non-empty name succeeds", "valid-crl", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.ImportCrl(context.Background(), tt.crlName, []byte("data"), "arn:ta", true, nil)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// ---- Subject tests ----

func TestSubject_GetNotFound(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.GetSubject(context.Background(), "nonexistent-subject")
	require.Error(t, err)
}

func TestSubject_ListEmpty(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	all, next, err := b.ListSubjects(context.Background(), "", 0)
	require.NoError(t, err)
	assert.Empty(t, all)
	assert.Empty(t, next)
}

// ---- Attribute mapping tests ----

func TestAttributeMapping_PutGetDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		certificateField  string
		rules             []rolesanywhere.MappingRule
		deleteSpecifiers  []string
		expectAfterDelete int
	}{
		{
			name:              "put and delete entire field",
			certificateField:  "x509Subject",
			rules:             []rolesanywhere.MappingRule{{Specifier: "CN"}, {Specifier: "OU"}},
			deleteSpecifiers:  nil,
			expectAfterDelete: 0,
		},
		{
			name:              "put and delete one specifier",
			certificateField:  "x509SAN",
			rules:             []rolesanywhere.MappingRule{{Specifier: "DNS"}, {Specifier: "URI"}},
			deleteSpecifiers:  []string{"DNS"},
			expectAfterDelete: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			p, _ := b.CreateProfile(context.Background(), "mapping-profile", nil, nil, nil, nil, "", false)

			_, err := b.PutAttributeMapping(context.Background(), p.ProfileID, tc.certificateField, tc.rules)
			require.NoError(t, err)

			mappings := b.GetAttributeMappings(context.Background(), p.ProfileID)
			require.Len(t, mappings, 1)
			assert.Equal(t, tc.certificateField, mappings[0].CertificateField)
			assert.Len(t, mappings[0].MappingRules, len(tc.rules))

			_, err = b.DeleteAttributeMapping(
				context.Background(),
				p.ProfileID,
				tc.certificateField,
				tc.deleteSpecifiers,
			)
			require.NoError(t, err)

			mappings = b.GetAttributeMappings(context.Background(), p.ProfileID)

			if tc.expectAfterDelete == 0 {
				assert.Empty(t, mappings)
			} else {
				require.Len(t, mappings, 1)
				assert.Len(t, mappings[0].MappingRules, tc.expectAfterDelete)
			}
		})
	}
}

func TestAttributeMapping_ReplacesExistingField(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	p, _ := b.CreateProfile(context.Background(), "replace-profile", nil, nil, nil, nil, "", false)

	_, err := b.PutAttributeMapping(
		context.Background(),
		p.ProfileID,
		"x509Subject",
		[]rolesanywhere.MappingRule{{Specifier: "CN"}},
	)
	require.NoError(t, err)

	_, err = b.PutAttributeMapping(context.Background(),
		p.ProfileID,
		"x509Subject",
		[]rolesanywhere.MappingRule{{Specifier: "OU"}, {Specifier: "O"}},
	)
	require.NoError(t, err)

	mappings := b.GetAttributeMappings(context.Background(), p.ProfileID)
	require.Len(t, mappings, 1)
	assert.Len(t, mappings[0].MappingRules, 2)
}

func TestAttributeMapping_ProfileNotFound(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.PutAttributeMapping(context.Background(), "no-such-profile", "x509Subject", nil)
	require.Error(t, err)

	_, err = b.DeleteAttributeMapping(context.Background(), "no-such-profile", "x509Subject", nil)
	require.Error(t, err)
}

// ---- Notification settings tests ----

func TestNotificationSettings_PutResetCycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		settings    []rolesanywhere.NotificationSetting
		resetKeys   []rolesanywhere.NotificationSettingKey
		expectAfter int
	}{
		{
			name: "put two, reset one",
			settings: []rolesanywhere.NotificationSetting{
				{Event: "CA_CERTIFICATE_EXPIRY", Enabled: true},
				{Event: "END_ENTITY_CERTIFICATE_EXPIRY", Enabled: false},
			},
			resetKeys:   []rolesanywhere.NotificationSettingKey{{Event: "CA_CERTIFICATE_EXPIRY"}},
			expectAfter: 1,
		},
		{
			name: "put one, reset all",
			settings: []rolesanywhere.NotificationSetting{
				{Event: "CA_CERTIFICATE_EXPIRY", Channel: "ALL", Enabled: true},
			},
			resetKeys:   []rolesanywhere.NotificationSettingKey{{Event: "CA_CERTIFICATE_EXPIRY", Channel: "ALL"}},
			expectAfter: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			src := rolesanywhere.TrustAnchorSource{SourceType: "CERTIFICATE_BUNDLE"}
			ta, err := b.CreateTrustAnchor(context.Background(), "notif-anchor", src, nil, nil)
			require.NoError(t, err)

			_, err = b.PutNotificationSettings(context.Background(), ta.TrustAnchorID, tc.settings)
			require.NoError(t, err)

			settings := b.GetNotificationSettings(context.Background(), ta.TrustAnchorID)
			assert.Len(t, settings, len(tc.settings))

			_, err = b.ResetNotificationSettings(context.Background(), ta.TrustAnchorID, tc.resetKeys)
			require.NoError(t, err)

			settings = b.GetNotificationSettings(context.Background(), ta.TrustAnchorID)
			assert.Len(t, settings, tc.expectAfter)
		})
	}
}

func TestNotificationSettings_TrustAnchorNotFound(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.PutNotificationSettings(context.Background(), "no-such-anchor", []rolesanywhere.NotificationSetting{})
	require.Error(t, err)

	_, err = b.ResetNotificationSettings(
		context.Background(),
		"no-such-anchor",
		[]rolesanywhere.NotificationSettingKey{},
	)
	require.Error(t, err)
}

func TestNotificationSettings_UpdateExisting(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	src := rolesanywhere.TrustAnchorSource{SourceType: "CERTIFICATE_BUNDLE"}
	ta, _ := b.CreateTrustAnchor(context.Background(), "update-notif-anchor", src, nil, nil)

	_, err := b.PutNotificationSettings(context.Background(), ta.TrustAnchorID, []rolesanywhere.NotificationSetting{
		{Event: "CA_CERTIFICATE_EXPIRY", Enabled: true},
	})
	require.NoError(t, err)

	_, err = b.PutNotificationSettings(context.Background(), ta.TrustAnchorID, []rolesanywhere.NotificationSetting{
		{Event: "CA_CERTIFICATE_EXPIRY", Enabled: false},
	})
	require.NoError(t, err)

	settings := b.GetNotificationSettings(context.Background(), ta.TrustAnchorID)
	require.Len(t, settings, 1)
	assert.False(t, settings[0].Enabled)
}
