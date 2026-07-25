package lakeformation_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/lakeformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetPutDataLakeSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		settings *lakeformation.DataLakeSettings
		name     string
	}{
		{
			name: "default_empty",
		},
		{
			name: "with_admins",
			settings: &lakeformation.DataLakeSettings{
				DataLakeAdmins: []lakeformation.DataLakePrincipal{
					{DataLakePrincipalIdentifier: "arn:aws:iam::123456789012:user/admin"},
				},
			},
		},
		{
			name: "with_trusted_owners",
			settings: &lakeformation.DataLakeSettings{
				TrustedResourceOwners: []string{"123456789012"},
			},
		},
		{
			name: "with_external_data_filtering_allow_list",
			settings: &lakeformation.DataLakeSettings{
				ExternalDataFilteringAllowList: []lakeformation.DataLakePrincipal{
					{DataLakePrincipalIdentifier: "arn:aws:iam::123456789012:user/filterer"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()

			got := b.GetDataLakeSettings()
			require.NotNil(t, got)

			if tt.settings != nil {
				b.PutDataLakeSettings(tt.settings)

				got = b.GetDataLakeSettings()
				assert.Equal(t, tt.settings, got)
			}
		})
	}
}

func TestGetDataLakeSettings_ReturnsCopy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "mutating returned settings does not affect backend state"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()

			b.PutDataLakeSettings(&lakeformation.DataLakeSettings{
				DataLakeAdmins: []lakeformation.DataLakePrincipal{
					{DataLakePrincipalIdentifier: "arn:aws:iam::123:user/admin"},
				},
			})

			s := b.GetDataLakeSettings()
			require.NotNil(t, s)

			// Mutate the returned copy.
			s.DataLakeAdmins = append(s.DataLakeAdmins, lakeformation.DataLakePrincipal{
				DataLakePrincipalIdentifier: "arn:aws:iam::123:user/evil",
			})

			// Backend state must be unchanged.
			s2 := b.GetDataLakeSettings()
			assert.Len(t, s2.DataLakeAdmins, 1, "mutating returned settings must not affect backend state")
		})
	}
}

func TestCopyDataLakeSettings_NilFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "nil settings fields are preserved as nil in copy"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()

			// Settings with all nil slices.
			b.PutDataLakeSettings(&lakeformation.DataLakeSettings{})

			s := b.GetDataLakeSettings()
			assert.Nil(t, s.DataLakeAdmins)
			assert.Nil(t, s.TrustedResourceOwners)
		})
	}
}

func TestGetDataLakeSettings_NilBackingStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "nil settings stored returns empty DataLakeSettings"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()

			// Put nil settings to test nil case handling in GetDataLakeSettings.
			b.PutDataLakeSettings(nil)

			s := b.GetDataLakeSettings()
			assert.NotNil(t, s)
		})
	}
}

func TestCopyDataLakeSettings_WithAllFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		admins     []lakeformation.DataLakePrincipal
		trusted    []string
		wantCopied bool
	}{
		{
			name: "deep copy preserves all fields",
			admins: []lakeformation.DataLakePrincipal{
				{DataLakePrincipalIdentifier: "arn:aws:iam::123:user/admin"},
			},
			trusted:    []string{"123456789012"},
			wantCopied: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()

			b.PutDataLakeSettings(&lakeformation.DataLakeSettings{
				DataLakeAdmins:        tt.admins,
				TrustedResourceOwners: tt.trusted,
				CreateDatabaseDefaultPermissions: []lakeformation.PrincipalPermissions{
					{Principal: &lakeformation.DataLakePrincipal{DataLakePrincipalIdentifier: "arn:iam::role/r"}},
				},
				CreateTableDefaultPermissions: []lakeformation.PrincipalPermissions{
					{Principal: &lakeformation.DataLakePrincipal{DataLakePrincipalIdentifier: "arn:iam::role/t"}},
				},
			})

			s := b.GetDataLakeSettings()
			assert.Len(t, s.DataLakeAdmins, len(tt.admins))
			assert.Len(t, s.TrustedResourceOwners, len(tt.trusted))
		})
	}
}

func TestPutDataLakeSettings_StoresCopy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "mutating settings pointer after Put does not affect backend state"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()

			settings := &lakeformation.DataLakeSettings{
				DataLakeAdmins: []lakeformation.DataLakePrincipal{
					{DataLakePrincipalIdentifier: "arn:aws:iam::123:user/admin"},
				},
				CreateDatabaseDefaultPermissions: []lakeformation.PrincipalPermissions{
					{
						Principal:   &lakeformation.DataLakePrincipal{DataLakePrincipalIdentifier: "arn:role/r"},
						Permissions: []string{"CREATE_TABLE"},
					},
				},
			}

			b.PutDataLakeSettings(settings)

			// Mutate the original settings pointer after Put.
			settings.DataLakeAdmins = append(settings.DataLakeAdmins, lakeformation.DataLakePrincipal{
				DataLakePrincipalIdentifier: "arn:aws:iam::123:user/evil",
			})
			settings.CreateDatabaseDefaultPermissions[0].Permissions = append(
				settings.CreateDatabaseDefaultPermissions[0].Permissions, "DROP",
			)
			settings.CreateDatabaseDefaultPermissions[0].Principal.DataLakePrincipalIdentifier = "MUTATED"

			// Backend state must be unchanged.
			s := b.GetDataLakeSettings()
			assert.Len(
				t,
				s.DataLakeAdmins,
				1,
				"mutating settings pointer after PutDataLakeSettings must not affect backend",
			)
			assert.Len(t, s.CreateDatabaseDefaultPermissions[0].Permissions, 1,
				"mutating Permissions slice after PutDataLakeSettings must not affect backend")
			assert.Equal(t, "arn:role/r",
				s.CreateDatabaseDefaultPermissions[0].Principal.DataLakePrincipalIdentifier,
				"mutating Principal after PutDataLakeSettings must not affect backend")
		})
	}
}
