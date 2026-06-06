package directoryservice_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/directoryservice"
)

// mkDir creates a Simple AD directory and returns its ID.
func mkDir(t *testing.T, h *directoryservice.Handler) string {
	t.Helper()
	rec := doRequest(t, h, "CreateDirectory", map[string]any{
		"Name":     "corp.example.com",
		"Password": "Admin1234!",
		"Size":     "Small",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	id, ok := resp["DirectoryId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, id)
	return id
}

func TestAppendixA_IpRoutes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "add list remove cycle"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mkDir(t, h)

			// Add
			rec1 := doRequest(t, h, "AddIpRoutes", map[string]any{
				"DirectoryId": dirID,
				"IpRoutes":    []any{map[string]any{"CidrIp": "10.0.0.0/24", "Description": "test"}},
			})
			assert.Equal(t, http.StatusOK, rec1.Code)

			// List
			rec2 := doRequest(t, h, "ListIpRoutes", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec2.Code)
			var r2 map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
			routes, _ := r2["IpRoutesInfo"].([]any)
			assert.Len(t, routes, 1)

			// Remove
			rec3 := doRequest(t, h, "RemoveIpRoutes", map[string]any{
				"DirectoryId": dirID,
				"CidrIps":     []any{"10.0.0.0/24"},
			})
			assert.Equal(t, http.StatusOK, rec3.Code)

			// List after remove
			rec4 := doRequest(t, h, "ListIpRoutes", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec4.Code)
			var r4 map[string]any
			require.NoError(t, json.Unmarshal(rec4.Body.Bytes(), &r4))
			routes2, _ := r4["IpRoutesInfo"].([]any)
			assert.Empty(t, routes2)

			_ = tc
		})
	}
}

func TestAppendixA_Regions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "add describe remove cycle"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mkDir(t, h)

			// Add
			rec1 := doRequest(t, h, "AddRegion", map[string]any{
				"DirectoryId": dirID,
				"RegionName":  "us-west-2",
			})
			assert.Equal(t, http.StatusOK, rec1.Code)

			// Describe
			rec2 := doRequest(t, h, "DescribeRegions", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec2.Code)
			var r2 map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
			regions, _ := r2["RegionsDescription"].([]any)
			assert.Len(t, regions, 1)

			// Remove
			rec3 := doRequest(t, h, "RemoveRegion", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec3.Code)

			// Describe after remove
			rec4 := doRequest(t, h, "DescribeRegions", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec4.Code)
			var r4 map[string]any
			require.NoError(t, json.Unmarshal(rec4.Body.Bytes(), &r4))
			regions2, _ := r4["RegionsDescription"].([]any)
			assert.Empty(t, regions2)

			_ = tc
		})
	}
}

func TestAppendixA_SchemaExtensions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "start list cancel cycle"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mkDir(t, h)

			// Start
			rec1 := doRequest(t, h, "StartSchemaExtension", map[string]any{
				"DirectoryId":                         dirID,
				"Description":                         "test extension",
				"LdifContent":                         "dn: cn=test",
				"CreateSnapshotBeforeSchemaExtension": false,
			})
			assert.Equal(t, http.StatusOK, rec1.Code)
			var r1 map[string]any
			require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &r1))
			extID, _ := r1["SchemaExtensionId"].(string)
			assert.NotEmpty(t, extID)

			// List
			rec2 := doRequest(t, h, "ListSchemaExtensions", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec2.Code)
			var r2 map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
			exts, _ := r2["SchemaExtensionsInfo"].([]any)
			assert.Len(t, exts, 1)

			// Cancel
			rec3 := doRequest(t, h, "CancelSchemaExtension", map[string]any{
				"DirectoryId":       dirID,
				"SchemaExtensionId": extID,
			})
			assert.Equal(t, http.StatusOK, rec3.Code)

			_ = tc
		})
	}
}

func TestAppendixA_ConditionalForwarders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "create describe update delete cycle"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mkDir(t, h)

			// Create
			rec1 := doRequest(t, h, "CreateConditionalForwarder", map[string]any{
				"DirectoryId":      dirID,
				"RemoteDomainName": "partner.example.com",
				"DnsIpAddrs":       []any{"8.8.8.8"},
			})
			assert.Equal(t, http.StatusOK, rec1.Code)

			// Describe
			rec2 := doRequest(t, h, "DescribeConditionalForwarders", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec2.Code)
			var r2 map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
			fwds, _ := r2["ConditionalForwarders"].([]any)
			require.Len(t, fwds, 1)
			fwd := fwds[0].(map[string]any)
			assert.Equal(t, "partner.example.com", fwd["RemoteDomainName"])

			// Update
			rec3 := doRequest(t, h, "UpdateConditionalForwarder", map[string]any{
				"DirectoryId":      dirID,
				"RemoteDomainName": "partner.example.com",
				"DnsIpAddrs":       []any{"1.1.1.1"},
			})
			assert.Equal(t, http.StatusOK, rec3.Code)

			// Describe after update
			rec4 := doRequest(t, h, "DescribeConditionalForwarders", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec4.Code)
			var r4 map[string]any
			require.NoError(t, json.Unmarshal(rec4.Body.Bytes(), &r4))
			fwds2, _ := r4["ConditionalForwarders"].([]any)
			require.Len(t, fwds2, 1)
			fwd2 := fwds2[0].(map[string]any)
			dns, _ := fwd2["DnsIpAddrs"].([]any)
			assert.Equal(t, "1.1.1.1", dns[0].(string))

			// Delete
			rec5 := doRequest(t, h, "DeleteConditionalForwarder", map[string]any{
				"DirectoryId":      dirID,
				"RemoteDomainName": "partner.example.com",
			})
			assert.Equal(t, http.StatusOK, rec5.Code)

			// Describe after delete
			rec6 := doRequest(t, h, "DescribeConditionalForwarders", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec6.Code)
			var r6 map[string]any
			require.NoError(t, json.Unmarshal(rec6.Body.Bytes(), &r6))
			fwds3, _ := r6["ConditionalForwarders"].([]any)
			assert.Empty(t, fwds3)

			_ = tc
		})
	}
}

func TestAppendixA_LogSubscriptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "create list delete cycle"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mkDir(t, h)

			// Create
			rec1 := doRequest(t, h, "CreateLogSubscription", map[string]any{
				"DirectoryId":  dirID,
				"LogGroupName": "/aws/directoryservice/test",
			})
			assert.Equal(t, http.StatusOK, rec1.Code)

			// List
			rec2 := doRequest(t, h, "ListLogSubscriptions", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec2.Code)
			var r2 map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
			subs, _ := r2["LogSubscriptions"].([]any)
			assert.Len(t, subs, 1)

			// Delete
			rec3 := doRequest(t, h, "DeleteLogSubscription", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec3.Code)

			// List after delete
			rec4 := doRequest(t, h, "ListLogSubscriptions", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec4.Code)
			var r4 map[string]any
			require.NoError(t, json.Unmarshal(rec4.Body.Bytes(), &r4))
			subs2, _ := r4["LogSubscriptions"].([]any)
			assert.Empty(t, subs2)

			_ = tc
		})
	}
}

func TestAppendixA_EventTopics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "register describe deregister cycle"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mkDir(t, h)

			// Register
			rec1 := doRequest(t, h, "RegisterEventTopic", map[string]any{
				"DirectoryId": dirID,
				"TopicName":   "my-topic",
			})
			assert.Equal(t, http.StatusOK, rec1.Code)

			// Describe
			rec2 := doRequest(t, h, "DescribeEventTopics", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec2.Code)
			var r2 map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
			topics, _ := r2["EventTopics"].([]any)
			require.Len(t, topics, 1)
			topic := topics[0].(map[string]any)
			assert.Equal(t, "my-topic", topic["TopicName"])

			// Deregister
			rec3 := doRequest(t, h, "DeregisterEventTopic", map[string]any{
				"DirectoryId": dirID,
				"TopicName":   "my-topic",
			})
			assert.Equal(t, http.StatusOK, rec3.Code)

			// Describe after deregister
			rec4 := doRequest(t, h, "DescribeEventTopics", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec4.Code)
			var r4 map[string]any
			require.NoError(t, json.Unmarshal(rec4.Body.Bytes(), &r4))
			topics2, _ := r4["EventTopics"].([]any)
			assert.Empty(t, topics2)

			_ = tc
		})
	}
}

func TestAppendixA_DomainControllers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "update to 2 then describe"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mkDir(t, h)

			// Describe empty
			rec1 := doRequest(t, h, "DescribeDomainControllers", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec1.Code)
			var r1 map[string]any
			require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &r1))
			dcs, _ := r1["DomainControllers"].([]any)
			assert.Empty(t, dcs)

			// Update to 2
			rec2 := doRequest(t, h, "UpdateNumberOfDomainControllers", map[string]any{
				"DirectoryId":   dirID,
				"DesiredNumber": 2,
			})
			assert.Equal(t, http.StatusOK, rec2.Code)

			// Describe again
			rec3 := doRequest(t, h, "DescribeDomainControllers", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec3.Code)
			var r3 map[string]any
			require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &r3))
			dcs2, _ := r3["DomainControllers"].([]any)
			assert.Len(t, dcs2, 2)

			_ = tc
		})
	}
}

func TestAppendixA_Trusts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "create describe update verify delete cycle"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mkDir(t, h)

			// Create trust
			rec1 := doRequest(t, h, "CreateTrust", map[string]any{
				"DirectoryId":      dirID,
				"RemoteDomainName": "partner.example.com",
				"TrustPassword":    "TrustPw1!",
				"TrustDirection":   "Two-Way",
			})
			assert.Equal(t, http.StatusOK, rec1.Code)
			var r1 map[string]any
			require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &r1))
			trustID, _ := r1["TrustId"].(string)
			assert.NotEmpty(t, trustID)

			// Describe
			rec2 := doRequest(t, h, "DescribeTrusts", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec2.Code)
			var r2 map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
			trusts, _ := r2["Trusts"].([]any)
			require.Len(t, trusts, 1)
			trust := trusts[0].(map[string]any)
			assert.Equal(t, "partner.example.com", trust["RemoteDomainName"])

			// Update
			rec3 := doRequest(t, h, "UpdateTrust", map[string]any{
				"TrustId":       trustID,
				"SelectiveAuth": "Enabled",
			})
			assert.Equal(t, http.StatusOK, rec3.Code)
			var r3 map[string]any
			require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &r3))
			assert.Equal(t, trustID, r3["TrustId"])

			// Verify
			rec4 := doRequest(t, h, "VerifyTrust", map[string]any{"TrustId": trustID})
			assert.Equal(t, http.StatusOK, rec4.Code)
			var r4 map[string]any
			require.NoError(t, json.Unmarshal(rec4.Body.Bytes(), &r4))
			assert.Equal(t, trustID, r4["TrustId"])

			// Delete
			rec5 := doRequest(t, h, "DeleteTrust", map[string]any{"TrustId": trustID})
			assert.Equal(t, http.StatusOK, rec5.Code)
			var r5 map[string]any
			require.NoError(t, json.Unmarshal(rec5.Body.Bytes(), &r5))
			assert.Equal(t, trustID, r5["TrustId"])

			// Describe after delete
			rec6 := doRequest(t, h, "DescribeTrusts", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec6.Code)
			var r6 map[string]any
			require.NoError(t, json.Unmarshal(rec6.Body.Bytes(), &r6))
			trusts2, _ := r6["Trusts"].([]any)
			assert.Empty(t, trusts2)

			_ = tc
		})
	}
}

func TestAppendixA_SharedDirectories(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "share accept describe unshare"},
		{name: "share reject"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mkDir(t, h)

			// Share
			rec1 := doRequest(t, h, "ShareDirectory", map[string]any{
				"DirectoryId": dirID,
				"ShareMethod": "HANDSHAKE",
				"ShareTarget": map[string]any{"Id": "111111111111", "Type": "ACCOUNT"},
			})
			assert.Equal(t, http.StatusOK, rec1.Code)
			var r1 map[string]any
			require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &r1))
			sharedDirID, _ := r1["SharedDirectoryId"].(string)
			assert.NotEmpty(t, sharedDirID)

			if tc.name == "share accept describe unshare" {
				// Accept
				rec2 := doRequest(t, h, "AcceptSharedDirectory", map[string]any{
					"SharedDirectoryId": sharedDirID,
				})
				assert.Equal(t, http.StatusOK, rec2.Code)

				// Describe
				rec3 := doRequest(t, h, "DescribeSharedDirectories", map[string]any{
					"OwnerDirectoryId": dirID,
				})
				assert.Equal(t, http.StatusOK, rec3.Code)
				var r3 map[string]any
				require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &r3))
				dirs, _ := r3["SharedDirectories"].([]any)
				assert.Len(t, dirs, 1)

				// Unshare
				rec4 := doRequest(t, h, "UnshareDirectory", map[string]any{
					"DirectoryId":   dirID,
					"UnshareTarget": map[string]any{"Id": "111111111111", "Type": "ACCOUNT"},
				})
				assert.Equal(t, http.StatusOK, rec4.Code)
			} else {
				// Reject
				rec2 := doRequest(t, h, "RejectSharedDirectory", map[string]any{
					"SharedDirectoryId": sharedDirID,
				})
				assert.Equal(t, http.StatusOK, rec2.Code)
				var r2 map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
				assert.Equal(t, sharedDirID, r2["SharedDirectoryId"])
			}
		})
	}
}

func TestAppendixA_Certificates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "register list describe deregister cycle"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mkDir(t, h)

			// Register
			rec1 := doRequest(t, h, "RegisterCertificate", map[string]any{
				"DirectoryId":     dirID,
				"CertificateData": "-----BEGIN CERTIFICATE-----\nMIIA...",
				"Type":            "ClientLDAPS",
			})
			assert.Equal(t, http.StatusOK, rec1.Code)
			var r1 map[string]any
			require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &r1))
			certID, _ := r1["CertificateId"].(string)
			assert.NotEmpty(t, certID)

			// List
			rec2 := doRequest(t, h, "ListCertificates", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec2.Code)
			var r2 map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
			certs, _ := r2["CertificatesInfo"].([]any)
			assert.Len(t, certs, 1)

			// Describe
			rec3 := doRequest(t, h, "DescribeCertificate", map[string]any{
				"DirectoryId":   dirID,
				"CertificateId": certID,
			})
			assert.Equal(t, http.StatusOK, rec3.Code)
			var r3 map[string]any
			require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &r3))
			cert, _ := r3["Certificate"].(map[string]any)
			assert.Equal(t, certID, cert["CertificateId"])

			// Deregister
			rec4 := doRequest(t, h, "DeregisterCertificate", map[string]any{
				"DirectoryId":   dirID,
				"CertificateId": certID,
			})
			assert.Equal(t, http.StatusOK, rec4.Code)

			// List after deregister
			rec5 := doRequest(t, h, "ListCertificates", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec5.Code)
			var r5 map[string]any
			require.NoError(t, json.Unmarshal(rec5.Body.Bytes(), &r5))
			certs2, _ := r5["CertificatesInfo"].([]any)
			assert.Empty(t, certs2)

			_ = tc
		})
	}
}

func TestAppendixA_LDAPS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "enable describe disable cycle"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mkDir(t, h)

			// Enable
			rec1 := doRequest(t, h, "EnableLDAPS", map[string]any{
				"DirectoryId": dirID,
				"Type":        "Client",
			})
			assert.Equal(t, http.StatusOK, rec1.Code)

			// Describe
			rec2 := doRequest(t, h, "DescribeLDAPSSettings", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec2.Code)
			var r2 map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
			settings, _ := r2["LDAPSSettingsInfo"].([]any)
			require.Len(t, settings, 1)
			setting := settings[0].(map[string]any)
			assert.Equal(t, "Enabled", setting["LDAPSStatus"])

			// Disable
			rec3 := doRequest(t, h, "DisableLDAPS", map[string]any{
				"DirectoryId": dirID,
				"Type":        "Client",
			})
			assert.Equal(t, http.StatusOK, rec3.Code)

			_ = tc
		})
	}
}

func TestAppendixA_ClientAuthentication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "enable describe disable cycle"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mkDir(t, h)

			// Enable
			rec1 := doRequest(t, h, "EnableClientAuthentication", map[string]any{
				"DirectoryId": dirID,
				"Type":        "SmartCard",
			})
			assert.Equal(t, http.StatusOK, rec1.Code)

			// Describe
			rec2 := doRequest(t, h, "DescribeClientAuthenticationSettings", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec2.Code)
			var r2 map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
			settings, _ := r2["ClientAuthenticationSettingsInfo"].([]any)
			require.Len(t, settings, 1)
			setting := settings[0].(map[string]any)
			assert.Equal(t, "Enabled", setting["Status"])

			// Disable
			rec3 := doRequest(t, h, "DisableClientAuthentication", map[string]any{
				"DirectoryId": dirID,
				"Type":        "SmartCard",
			})
			assert.Equal(t, http.StatusOK, rec3.Code)

			_ = tc
		})
	}
}

func TestAppendixA_Radius(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "enable update disable cycle"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mkDir(t, h)

			radiusSettings := map[string]any{
				"RadiusServers":   []any{"10.0.0.1"},
				"RadiusPort":      1812,
				"RadiusRetries":   3,
				"RadiusTimeout":   30,
				"SharedSecret":    "secret",
				"UseSameUsername": false,
			}

			// Enable
			rec1 := doRequest(t, h, "EnableRadius", map[string]any{
				"DirectoryId":    dirID,
				"RadiusSettings": radiusSettings,
			})
			assert.Equal(t, http.StatusOK, rec1.Code)

			// Update
			rec2 := doRequest(t, h, "UpdateRadius", map[string]any{
				"DirectoryId":    dirID,
				"RadiusSettings": radiusSettings,
			})
			assert.Equal(t, http.StatusOK, rec2.Code)

			// Disable
			rec3 := doRequest(t, h, "DisableRadius", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec3.Code)

			_ = tc
		})
	}
}

func TestAppendixA_DirectoryDataAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "enable describe disable cycle"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mkDir(t, h)

			// Describe before enable
			rec1 := doRequest(t, h, "DescribeDirectoryDataAccess", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec1.Code)
			var r1 map[string]any
			require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &r1))
			assert.Equal(t, "Disabled", r1["DirectoryDataAccessStatus"])

			// Enable
			rec2 := doRequest(t, h, "EnableDirectoryDataAccess", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec2.Code)

			// Describe after enable
			rec3 := doRequest(t, h, "DescribeDirectoryDataAccess", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec3.Code)
			var r3 map[string]any
			require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &r3))
			assert.Equal(t, "Enabled", r3["DirectoryDataAccessStatus"])

			// Disable
			rec4 := doRequest(t, h, "DisableDirectoryDataAccess", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec4.Code)

			_ = tc
		})
	}
}

func TestAppendixA_CAEnrollmentPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "enable describe disable cycle"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mkDir(t, h)

			// Enable
			rec1 := doRequest(t, h, "EnableCAEnrollmentPolicy", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec1.Code)

			// Describe
			rec2 := doRequest(t, h, "DescribeCAEnrollmentPolicy", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec2.Code)
			var r2 map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
			policy, _ := r2["CAEnrollmentPolicy"].(map[string]any)
			assert.Equal(t, "Enabled", policy["EnrollmentStatus"])

			// Disable
			rec3 := doRequest(t, h, "DisableCAEnrollmentPolicy", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec3.Code)

			// Describe after disable
			rec4 := doRequest(t, h, "DescribeCAEnrollmentPolicy", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec4.Code)
			var r4 map[string]any
			require.NoError(t, json.Unmarshal(rec4.Body.Bytes(), &r4))
			policy2, _ := r4["CAEnrollmentPolicy"].(map[string]any)
			assert.Equal(t, "Disabled", policy2["EnrollmentStatus"])

			_ = tc
		})
	}
}

func TestAppendixA_ADAssessments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "start list describe delete cycle"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mkDir(t, h)

			// Start
			rec1 := doRequest(t, h, "StartADAssessment", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec1.Code)
			var r1 map[string]any
			require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &r1))
			assessID, _ := r1["AssessmentId"].(string)
			assert.NotEmpty(t, assessID)

			// List
			rec2 := doRequest(t, h, "ListADAssessments", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec2.Code)
			var r2 map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
			assessments, _ := r2["ADAssessments"].([]any)
			assert.Len(t, assessments, 1)

			// Describe
			rec3 := doRequest(t, h, "DescribeADAssessment", map[string]any{
				"DirectoryId":  dirID,
				"AssessmentId": assessID,
			})
			assert.Equal(t, http.StatusOK, rec3.Code)
			var r3 map[string]any
			require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &r3))
			assessment, _ := r3["ADAssessment"].(map[string]any)
			assert.Equal(t, assessID, assessment["AssessmentId"])

			// Delete
			rec4 := doRequest(t, h, "DeleteADAssessment", map[string]any{
				"DirectoryId":  dirID,
				"AssessmentId": assessID,
			})
			assert.Equal(t, http.StatusOK, rec4.Code)

			// List after delete
			rec5 := doRequest(t, h, "ListADAssessments", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec5.Code)
			var r5 map[string]any
			require.NoError(t, json.Unmarshal(rec5.Body.Bytes(), &r5))
			assessments2, _ := r5["ADAssessments"].([]any)
			assert.Empty(t, assessments2)

			_ = tc
		})
	}
}

func TestAppendixA_HybridAD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "create update describe cycle"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			// CreateHybridAD
			rec1 := doRequest(t, h, "CreateHybridAD", map[string]any{
				"Name":     "hybrid.example.com",
				"Password": "Admin1234!",
				"Edition":  "Enterprise",
			})
			assert.Equal(t, http.StatusOK, rec1.Code)
			var r1 map[string]any
			require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &r1))
			dirID, _ := r1["DirectoryId"].(string)
			assert.NotEmpty(t, dirID)
			assert.NotEmpty(t, r1["RequestId"])

			// UpdateHybridAD
			rec2 := doRequest(t, h, "UpdateHybridAD", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec2.Code)
			var r2 map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
			assert.NotEmpty(t, r2["RequestId"])

			// DescribeHybridADUpdate
			rec3 := doRequest(t, h, "DescribeHybridADUpdate", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec3.Code)
			var r3 map[string]any
			require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &r3))
			updates, _ := r3["HybridADUpdateInfo"].([]any)
			assert.NotEmpty(t, updates)

			_ = tc
		})
	}
}

func TestAppendixA_CreateComputer(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	dirID := mkDir(t, h)

	tests := []struct {
		name     string
		body     any
		wantCode int
		check    func(t *testing.T, resp map[string]any)
	}{
		{
			name: "creates computer returns ComputerName",
			body: map[string]any{
				"DirectoryId":  dirID,
				"ComputerName": "WORKSTATION1",
				"Password":     "Comp1234!",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()
				computer, _ := resp["Computer"].(map[string]any)
				assert.Equal(t, "WORKSTATION1", computer["ComputerName"])
				assert.NotEmpty(t, computer["ComputerId"])
			},
		},
		{
			name:     "missing ComputerName returns 400",
			body:     map[string]any{"DirectoryId": dirID, "Password": "Comp1234!"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, "CreateComputer", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				tc.check(t, resp)
			}
		})
	}
}

func TestAppendixA_Settings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "update then describe"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mkDir(t, h)

			// UpdateSettings
			rec1 := doRequest(t, h, "UpdateSettings", map[string]any{
				"DirectoryId": dirID,
				"Settings":    []any{map[string]any{"Name": "TLS_1_0", "Value": "Disable"}},
			})
			assert.Equal(t, http.StatusOK, rec1.Code)
			var r1 map[string]any
			require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &r1))
			assert.Equal(t, dirID, r1["DirectoryId"])

			// DescribeSettings
			rec2 := doRequest(t, h, "DescribeSettings", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec2.Code)
			var r2 map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
			settings, _ := r2["SettingEntries"].([]any)
			require.Len(t, settings, 1)
			setting := settings[0].(map[string]any)
			assert.Equal(t, "TLS_1_0", setting["Name"])

			_ = tc
		})
	}
}

func TestAppendixA_UpdateDirectorySetup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "update then describe"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mkDir(t, h)

			// UpdateDirectorySetup
			rec1 := doRequest(t, h, "UpdateDirectorySetup", map[string]any{
				"DirectoryId":                dirID,
				"UpdateType":                 "OS",
				"CreateSnapshotBeforeUpdate": true,
			})
			assert.Equal(t, http.StatusOK, rec1.Code)

			// DescribeUpdateDirectory
			rec2 := doRequest(t, h, "DescribeUpdateDirectory", map[string]any{
				"DirectoryId": dirID,
				"UpdateType":  "OS",
			})
			assert.Equal(t, http.StatusOK, rec2.Code)
			var r2 map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
			entries, _ := r2["UpdateDirectoryInfo"].([]any)
			assert.Len(t, entries, 1)

			_ = tc
		})
	}
}

func TestAppendixA_ResetUserPassword(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	dirID := mkDir(t, h)

	tests := []struct {
		name     string
		body     any
		wantCode int
	}{
		{
			name: "resets password returns ok",
			body: map[string]any{
				"DirectoryId": dirID,
				"UserName":    "jdoe",
				"NewPassword": "NewPw1234!",
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "missing UserName returns 400",
			body:     map[string]any{"DirectoryId": dirID, "NewPassword": "pw"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, "ResetUserPassword", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestAppendixA_ConnectDirectory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     any
		wantCode int
		check    func(t *testing.T, resp map[string]any)
	}{
		{
			name: "creates ADConnector and returns DirectoryId",
			body: map[string]any{
				"Name":     "corp.example.com",
				"Password": "Admin1234!",
				"Size":     "Small",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()
				id, ok := resp["DirectoryId"].(string)
				assert.True(t, ok)
				assert.NotEmpty(t, id)
			},
		},
		{
			name:     "missing Name returns 400",
			body:     map[string]any{"Password": "Admin1234!", "Size": "Small"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			rec := doRequest(t, h, "ConnectDirectory", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				tc.check(t, resp)
			}
		})
	}
}
