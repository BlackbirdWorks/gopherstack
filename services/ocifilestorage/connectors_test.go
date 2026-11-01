package ocifilestorage_test

import (
	"encoding/binary"
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/filestorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	svc "github.com/blackbirdworks/gopherstack/services/ocifilestorage"
)

func ldapConnector(mut func(*filestorage.CreateLdapBindAccountDetails)) filestorage.CreateLdapBindAccountDetails {
	d := filestorage.CreateLdapBindAccountDetails{
		AvailabilityDomain: new(testAD), CompartmentId: new(testCompartment), DisplayName: new("ldap"),
		BindDistinguishedName: new("cn=bind,dc=example,dc=com"), PasswordSecretId: new("ocid1.vaultsecret.oc1.iad.pw"),
		Endpoints: []filestorage.Endpoint{{Hostname: new("ldap.example.com"), Port: new(int64(389))}},
	}
	if mut != nil {
		mut(&d)
	}

	return d
}

func (e *env) createConnector(
	t *testing.T,
	mut func(*filestorage.CreateLdapBindAccountDetails),
) filestorage.LdapBindAccount {
	t.Helper()

	resp, err := e.client.CreateOutboundConnector(t.Context(), filestorage.CreateOutboundConnectorRequest{
		CreateOutboundConnectorDetails: ldapConnector(mut),
	})
	require.NoError(t, err)
	e.settle()

	got, err := e.client.GetOutboundConnector(
		t.Context(),
		filestorage.GetOutboundConnectorRequest{
			OutboundConnectorId: resp.OutboundConnector.(filestorage.LdapBindAccount).Id,
		},
	)
	require.NoError(t, err)

	return got.OutboundConnector.(filestorage.LdapBindAccount)
}

func TestConnectorLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	c := e.createConnector(t, nil)
	assert.Equal(t, filestorage.OutboundConnectorLifecycleStateActive, c.LifecycleState)
	assert.Equal(t, "cn=bind,dc=example,dc=com", *c.BindDistinguishedName)
	require.Len(t, c.Endpoints, 1)

	upd, err := e.client.UpdateOutboundConnector(t.Context(), filestorage.UpdateOutboundConnectorRequest{
		OutboundConnectorId: c.Id, DisplayName: new("renamed"),
	})
	require.NoError(t, err)
	assert.Equal(t, "renamed", *upd.OutboundConnector.(filestorage.LdapBindAccount).DisplayName)

	list, err := e.client.ListOutboundConnectors(t.Context(), filestorage.ListOutboundConnectorsRequest{
		CompartmentId: new(testCompartment), AvailabilityDomain: new(testAD),
	})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, "renamed", *list.Items[0].(filestorage.LdapBindAccountSummary).DisplayName)

	_, err = e.client.ChangeOutboundConnectorCompartment(
		t.Context(),
		filestorage.ChangeOutboundConnectorCompartmentRequest{
			OutboundConnectorId: c.Id,
			CompartmentId:       new(otherCompart),
		},
	)
	require.NoError(t, err)

	_, err = e.client.AddOutboundConnectorLock(t.Context(), filestorage.AddOutboundConnectorLockRequest{
		OutboundConnectorId:             c.Id,
		AddOutboundConnectorLockDetails: filestorage.ResourceLock{Type: filestorage.ResourceLockTypeDelete},
	})
	require.NoError(t, err)

	_, err = e.client.DeleteOutboundConnector(
		t.Context(),
		filestorage.DeleteOutboundConnectorRequest{OutboundConnectorId: c.Id},
	)
	assertStatus(t, err, http.StatusConflict, "Conflict")

	_, err = e.client.RemoveOutboundConnectorLock(t.Context(), filestorage.RemoveOutboundConnectorLockRequest{
		OutboundConnectorId:                c.Id,
		RemoveOutboundConnectorLockDetails: filestorage.ResourceLock{Type: filestorage.ResourceLockTypeDelete},
	})
	require.NoError(t, err)

	_, err = e.client.DeleteOutboundConnector(
		t.Context(),
		filestorage.DeleteOutboundConnectorRequest{OutboundConnectorId: c.Id},
	)
	require.NoError(t, err)
}

func TestConnectorRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mut  func(*filestorage.CreateLdapBindAccountDetails)
		name string
		code string
		st   int
	}{
		{
			name: "no-bind",
			mut:  func(d *filestorage.CreateLdapBindAccountDetails) { d.BindDistinguishedName = new("") },
			st:   400,
			code: "MissingParameter",
		},
		{
			name: "no-secret",
			mut:  func(d *filestorage.CreateLdapBindAccountDetails) { d.PasswordSecretId = new("") },
			st:   400,
			code: "MissingParameter",
		},
		{
			name: "no-endpoints",
			mut:  func(d *filestorage.CreateLdapBindAccountDetails) { d.Endpoints = []filestorage.Endpoint{} },
			st:   400,
			code: "InvalidParameter",
		},
		{name: "bad-port", mut: func(d *filestorage.CreateLdapBindAccountDetails) {
			d.Endpoints = []filestorage.Endpoint{{Hostname: new("h"), Port: new(int64(70000))}}
		}, st: 400, code: "InvalidParameter"},
		{name: "no-host", mut: func(d *filestorage.CreateLdapBindAccountDetails) {
			d.Endpoints = []filestorage.Endpoint{{Port: new(int64(389))}}
		}, st: 400, code: "InvalidParameter"},
		{name: "unknown-secret", mut: func(d *filestorage.CreateLdapBindAccountDetails) {
			d.PasswordSecretId = new("ocid1.vaultsecret.oc1.iad.nope")
		}, st: 404, code: "NotAuthorizedOrNotFound"},
		{name: "unknown-compartment", mut: func(d *filestorage.CreateLdapBindAccountDetails) {
			d.CompartmentId = new("ocid1.compartment.oc1..nope")
		}, st: 404, code: "NotAuthorizedOrNotFound"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{configure: func(b *svc.InMemoryBackend) {
				b.SetSecretLookup(
					func(id string, _ *int) ([]byte, bool) { return []byte("x"), id == "ocid1.vaultsecret.oc1.iad.pw" },
				)
			}})

			_, err := e.client.CreateOutboundConnector(
				t.Context(),
				filestorage.CreateOutboundConnectorRequest{CreateOutboundConnectorDetails: ldapConnector(tc.mut)},
			)
			assertStatus(t, err, tc.st, tc.code)
		})
	}
}

func TestMountTargetLdapIdmap(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	c := e.createConnector(t, nil)

	mt := e.createMT(t, func(d *filestorage.CreateMountTargetDetails) {
		d.IdmapType = filestorage.MountTargetIdmapTypeLdap
		d.LdapIdmap = &filestorage.CreateLdapIdmapDetails{
			OutboundConnector1Id: c.Id, UserSearchBase: new("ou=people"), GroupSearchBase: new("ou=groups"),
		}
	})
	require.NotNil(t, mt.LdapIdmap)
	assert.Equal(t, filestorage.LdapIdmapSchemaTypeRfc2307, mt.LdapIdmap.SchemaType)
	assert.Equal(t, 300, *mt.LdapIdmap.CacheLifetimeSeconds)

	_, err := e.client.DeleteOutboundConnector(
		t.Context(),
		filestorage.DeleteOutboundConnectorRequest{OutboundConnectorId: c.Id},
	)
	assertStatus(t, err, http.StatusConflict, "Conflict")

	upd, err := e.client.UpdateMountTarget(t.Context(), filestorage.UpdateMountTargetRequest{
		MountTargetId: mt.Id,
		LdapIdmap: &filestorage.UpdateLdapIdmapDetails{
			UserSearchBase:       new("ou=users"),
			OutboundConnector1Id: c.Id,
			GroupSearchBase:      new("ou=g"),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "ou=users", *upd.LdapIdmap.UserSearchBase)

	none, err := e.client.UpdateMountTarget(t.Context(), filestorage.UpdateMountTargetRequest{
		MountTargetId: mt.Id, IdmapType: filestorage.MountTargetIdmapTypeNone,
	})
	require.NoError(t, err)
	assert.Nil(t, none.LdapIdmap)
}

// keytab builds an MIT keytab (file format version 2) holding one entry per principal.
func keytab(enctype uint16, kvno byte, principals ...[]string) []byte {
	out := []byte{0x05, 0x02}

	for _, p := range principals {
		realm, comps := p[0], p[1:]
		body := binary.BigEndian.AppendUint16(nil, uint16(len(comps)))
		body = binary.BigEndian.AppendUint16(body, uint16(len(realm)))
		body = append(body, realm...)

		for _, c := range comps {
			body = binary.BigEndian.AppendUint16(body, uint16(len(c)))
			body = append(body, c...)
		}

		body = binary.BigEndian.AppendUint32(body, 1)
		body = binary.BigEndian.AppendUint32(body, 0)
		body = append(body, kvno)
		body = binary.BigEndian.AppendUint16(body, enctype)
		body = binary.BigEndian.AppendUint16(body, 4)
		body = append(body, 1, 2, 3, 4)

		out = binary.BigEndian.AppendUint32(out, uint32(len(body)))
		out = append(out, body...)
	}

	return out
}

func TestKerberosAndKeytabs(t *testing.T) {
	t.Parallel()

	good := keytab(18, 3, []string{"EXAMPLE.COM", "nfs", "mt.example.com"})
	wrongRealm := keytab(18, 1, []string{"OTHER.ORG", "nfs", "mt.example.com"})

	e := newEnv(t, envOpts{configure: func(b *svc.InMemoryBackend) {
		b.SetSecretLookup(func(id string, _ *int) ([]byte, bool) {
			switch id {
			case "good":
				return good, true
			case "wrong":
				return wrongRealm, true
			case "junk":
				return []byte("not a keytab"), true
			}

			return nil, false
		})
	}})

	kerb := func(secret string) *filestorage.CreateKerberosDetails {
		return &filestorage.CreateKerberosDetails{
			KerberosRealm:     new("EXAMPLE.COM"),
			KeyTabSecretId:    new(secret),
			IsKerberosEnabled: new(true),
		}
	}
	create := func(k *filestorage.CreateKerberosDetails) error {
		_, err := e.client.CreateMountTarget(t.Context(), filestorage.CreateMountTargetRequest{
			AvailabilityDomain: new(
				testAD,
			),
			CompartmentId: new(testCompartment),
			SubnetId:      new(testSubnet),
			Kerberos:      k,
		})

		return err
	}

	assertStatus(t, create(kerb("missing")), http.StatusNotFound, "NotAuthorizedOrNotFound")
	assertStatus(t, create(kerb("junk")), http.StatusBadRequest, "InvalidParameter")
	assertStatus(t, create(kerb("wrong")), http.StatusBadRequest, "InvalidParameter")
	assertStatus(
		t,
		create(&filestorage.CreateKerberosDetails{KerberosRealm: new("example.com"), KeyTabSecretId: new("good")}),
		http.StatusBadRequest,
		"InvalidParameter",
	)
	require.NoError(t, create(kerb("good")))

	mt := e.createMT(t, func(d *filestorage.CreateMountTargetDetails) { d.Kerberos = kerb("good") })
	assert.True(t, *mt.Kerberos.IsKerberosEnabled)

	set := mt.ExportSetId
	fs := e.createFS(t, nil)
	x := e.createExport(t, set, fs.Id, "/k", []filestorage.ClientOptions{{
		Source: new("10.0.0.0/8"), AllowedAuth: []filestorage.ClientOptionsAllowedAuthEnum{"KRB5", "SYS"},
	}})
	assert.Len(t, x.ExportOptions[0].AllowedAuth, 2, "Kerberos auth is allowed once Kerberos is enabled")

	v, err := e.client.ValidateKeyTabs(t.Context(), filestorage.ValidateKeyTabsRequest{
		MountTargetId: mt.Id, KeyTabSecretDetails: &filestorage.KeyTabSecretDetails{KeyTabSecretId: new("good")},
	})
	require.NoError(t, err)
	require.Len(t, v.CurrentKerberosKeytabEntries, 1)
	assert.Equal(t, "nfs/mt.example.com@EXAMPLE.COM", *v.CurrentKerberosKeytabEntries[0].Principal)
	assert.Equal(
		t,
		filestorage.KerberosKeytabEntryEncryptionTypeAes256CtsHmacSha196,
		v.CurrentKerberosKeytabEntries[0].EncryptionType,
	)
	assert.Equal(t, int64(3), *v.CurrentKerberosKeytabEntries[0].KeyVersionNumber)

	_, err = e.client.ValidateKeyTabs(t.Context(), filestorage.ValidateKeyTabsRequest{
		MountTargetId: mt.Id, KeyTabSecretDetails: &filestorage.KeyTabSecretDetails{KeyTabSecretId: new("wrong")},
	})
	assertStatus(t, err, http.StatusBadRequest, "InvalidParameter")

	_, err = e.client.ValidateKeyTabs(t.Context(), filestorage.ValidateKeyTabsRequest{
		KeyTabSecretDetails: &filestorage.KeyTabSecretDetails{KeyTabSecretId: new("junk")},
	})
	assertStatus(t, err, http.StatusBadRequest, "InvalidParameter")
}
