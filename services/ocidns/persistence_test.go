package ocidns_test

import (
	"context"
	"testing"

	"github.com/miekg/dns"
	"github.com/oracle/oci-go-sdk/v65/common"
	ocidnssdk "github.com/oracle/oci-go-sdk/v65/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/services/ocidns"
)

var _ persistence.Persistable = (*ocidns.Handler)(nil)

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	src := newEnv(t, envOpts{register: true})
	zone := src.createZone(t, "persist.example", func(d *ocidnssdk.CreateZoneDetails) {
		d.FreeformTags = map[string]string{"k": "v"}
	})
	src.mustPatch(t, "persist.example",
		add("www.persist.example", "A", "192.0.2.1", 300), add("persist.example", "MX", "10 mx.persist.example.", 60))

	view := src.createView(t, "persisted-view")
	priv := src.createZone(t, "p.internal", func(d *ocidnssdk.CreateZoneDetails) { d.Scope, d.ViewId = ocidnssdk.ScopePrivate, view.Id })

	key, err := src.client.CreateTsigKey(t.Context(), ocidnssdk.CreateTsigKeyRequest{CreateTsigKeyDetails: ocidnssdk.CreateTsigKeyDetails{
		Name: common.String("k.example"), Algorithm: common.String("hmac-sha256"), Secret: common.String("c2VjcmV0"),
		CompartmentId: common.String(testCompartment),
	}})
	require.NoError(t, err)

	pol, err := src.client.CreateSteeringPolicy(t.Context(), ocidnssdk.CreateSteeringPolicyRequest{
		CreateSteeringPolicyDetails: ocidnssdk.CreateSteeringPolicyDetails{
			CompartmentId: common.String(testCompartment), DisplayName: common.String("pol"),
			Template: ocidnssdk.CreateSteeringPolicyDetailsTemplateCustom,
			Answers:  []ocidnssdk.SteeringPolicyAnswer{{Name: common.String("a"), Rtype: common.String("A"), Rdata: common.String("192.0.2.1")}},
		},
	})
	require.NoError(t, err)

	att, err := src.client.CreateSteeringPolicyAttachment(t.Context(), ocidnssdk.CreateSteeringPolicyAttachmentRequest{
		CreateSteeringPolicyAttachmentDetails: ocidnssdk.CreateSteeringPolicyAttachmentDetails{
			SteeringPolicyId: pol.Id, ZoneId: zone.Id, DomainName: common.String("www.persist.example"),
		},
	})
	require.NoError(t, err)

	rid := src.resolverID(t)
	_, err = src.client.CreateResolverEndpoint(t.Context(), ocidnssdk.CreateResolverEndpointRequest{
		ResolverId: rid, CreateResolverEndpointDetails: vnicEndpoint("ep", testSubnet, true, true),
	})
	require.NoError(t, err)

	before := src.getZone(t, "persist.example")
	snap := src.handler.Snapshot(t.Context())
	require.NotEmpty(t, snap)
	src.handler.Shutdown(context.Background())

	dst := newEnv(t, envOpts{})
	require.NoError(t, dst.handler.Restore(t.Context(), snap))
	dst.vcns.set(src.vcns.get()...)

	got := dst.getZone(t, "persist.example")
	assert.Equal(t, *zone.Id, *got.Id)
	assert.Equal(t, *before.ETag, *got.ETag, "etags survive a restore")
	assert.Equal(t, int64(2), *got.Serial)
	assert.Equal(t, map[string]string{"k": "v"}, got.FreeformTags)
	assert.ElementsMatch(t,
		[]string{"www.persist.example A 300 192.0.2.1", "persist.example MX 60 10 mx.persist.example."},
		userRecords(dst.records(t, "persist.example")))

	assert.Len(t, dst.query(t, "udp", "www.persist.example.", dns.TypeA).Answer, 1, "the data plane serves restored zones")

	_, err = dst.client.GetZone(t.Context(), ocidnssdk.GetZoneRequest{
		ZoneNameOrId: common.String("p.internal"), Scope: ocidnssdk.GetZoneScopePrivate, ViewId: view.Id,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, *priv.Id)

	tk, err := dst.client.GetTsigKey(t.Context(), ocidnssdk.GetTsigKeyRequest{TsigKeyId: key.Id})
	require.NoError(t, err)
	assert.Equal(t, "c2VjcmV0", *tk.Secret)

	a, err := dst.client.GetSteeringPolicyAttachment(t.Context(), ocidnssdk.GetSteeringPolicyAttachmentRequest{SteeringPolicyAttachmentId: att.Id})
	require.NoError(t, err)
	assert.Equal(t, *pol.Id, *a.SteeringPolicyId)

	r, err := dst.client.GetResolver(t.Context(), ocidnssdk.GetResolverRequest{ResolverId: rid})
	require.NoError(t, err)
	assert.Len(t, r.Endpoints, 1, "resolvers and endpoints are restored, not recreated")

	// Mutations after a restore keep counting from the restored versions.
	dst.mustPatch(t, "persist.example", add("new.persist.example", "A", "192.0.2.9", 60))
	assert.Equal(t, int64(3), *dst.getZone(t, "persist.example").Serial)
}

func TestRestoreRejectsBadSnapshots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    string
		wantErr bool
		empty   bool
	}{
		{name: "garbage", data: `not json`, wantErr: true},
		{name: "null zone", data: `{"version":1,"zones":{"z":null}}`, wantErr: true},
		{name: "null view", data: `{"version":1,"views":{"v":null}}`, wantErr: true},
		{name: "other version is dropped", data: `{"version":99,"zones":{"z":{"id":"z","name":"x.example"}}}`, empty: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			e.createZone(t, "keep.example", nil)

			err := e.handler.Restore(t.Context(), []byte(tt.data))
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			resp, err := e.client.ListZones(t.Context(), ocidnssdk.ListZonesRequest{CompartmentId: common.String(testCompartment)})
			require.NoError(t, err)
			assert.Empty(t, resp.Items)
		})
	}
}
