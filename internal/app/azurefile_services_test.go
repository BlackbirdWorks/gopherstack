package app

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/aadauth"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
	"github.com/blackbirdworks/gopherstack/services/azurefile"
)

func TestAzureFileServices_AppliesSettingsToDataPlane(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		methods []string
		wantErr bool
	}{
		{name: "valid rule", methods: []string{"GET", "PUT"}},
		{name: "unknown method refused", methods: []string{"FETCH"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := azurefile.NewInMemoryBackend()
			fs := azureFileServices{b: b}

			err := fs.SetCORS("acct", []azurearm.FileCORSRule{{
				AllowedOrigins: []string{"https://a.example"}, AllowedMethods: tt.methods, MaxAgeInSeconds: 30,
			}})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "GET,PUT", b.CORS("acct")[0].AllowedMethods)
			assert.Equal(t, []string{"GET", "PUT"}, fs.CORS("acct")[0].AllowedMethods)

			fs.SetSMB("acct", azurearm.FileSMBSettings{Versions: "SMB3.0", MultichannelEnabled: true})
			assert.Equal(t, azurefile.SMBSetting{Versions: "SMB3.0", MultichannelEnabled: true}, b.SMB("acct"))
		})
	}
}

func TestAzureFileTokenValidator(t *testing.T) {
	t.Parallel()

	iss, err := aadauth.NewIssuer()
	require.NoError(t, err)

	good, err := iss.IssueClientCredentialsToken(aadauth.ClientCredentialsClaims{
		Issuer: "https://h/t/v2.0", Audience: "https://storage.azure.com", TenantID: "tenant-1", ClientID: "client-1",
		ObjectID: "object-1",
	}, 0)
	require.NoError(t, err)

	other, err := aadauth.NewIssuer()
	require.NoError(t, err)

	foreign, err := other.IssueClientCredentialsToken(aadauth.ClientCredentialsClaims{ClientID: "x"}, 0)
	require.NoError(t, err)

	tests := []struct {
		name    string
		token   string
		oid     string
		wantErr bool
	}{
		{name: "own token", token: good, oid: "object-1"},
		{name: "foreign issuer", token: foreign, wantErr: true},
		{name: "garbage", token: "tok", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			oid, tid, verr := azureFileTokenValidator(iss)(tt.token)
			if tt.wantErr {
				require.Error(t, verr)

				return
			}

			require.NoError(t, verr)
			assert.Equal(t, tt.oid, oid)
			assert.Equal(t, "tenant-1", tid)
		})
	}
}
