package gcpcompute_test

import (
	"net/http"
	"strconv"
	"testing"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/proto"
)

const (
	icLoc  = "global/interconnectLocations/iad-zone1-1"
	icType = "DEDICATED"
	icLink = "LINK_TYPE_ETHERNET_10G_LR"
)

func (e *env) interconnects(t *testing.T) *compute.InterconnectsClient {
	t.Helper()

	return client(t, e, compute.NewInterconnectsRESTClient)
}

func (e *env) attachments(t *testing.T) *compute.InterconnectAttachmentsClient {
	t.Helper()

	return client(t, e, compute.NewInterconnectAttachmentsRESTClient)
}

func (e *env) interconnect(t *testing.T, ic *computepb.Interconnect) error {
	t.Helper()

	op, err := e.interconnects(t).Insert(t.Context(),
		&computepb.InsertInterconnectRequest{Project: project, InterconnectResource: ic})

	return e.finish(t, op, err)
}

func dedicated(name string) *computepb.Interconnect {
	return &computepb.Interconnect{
		Name: s(name), Location: s(icLoc), InterconnectType: s(icType), LinkType: s(icLink),
		RequestedLinkCount: proto.Int32(2),
	}
}

func (e *env) attach(t *testing.T, a *computepb.InterconnectAttachment, validateOnly bool) error {
	t.Helper()

	op, err := e.attachments(t).Insert(t.Context(), &computepb.InsertInterconnectAttachmentRequest{
		Project: project, Region: region, InterconnectAttachmentResource: a, ValidateOnly: new(validateOnly),
	})

	return e.finish(t, op, err)
}

func (e *env) getAttachment(t *testing.T, name string) *computepb.InterconnectAttachment {
	t.Helper()

	a, err := e.attachments(t).Get(t.Context(), &computepb.GetInterconnectAttachmentRequest{
		Project: project, Region: region, InterconnectAttachment: name,
	})
	require.NoError(t, err)

	return a
}

func TestLicenseLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	lc := client(t, e, compute.NewLicensesRESTClient)
	codes := client(t, e, compute.NewLicenseCodesRESTClient)

	op, err := lc.Insert(t.Context(), &computepb.InsertLicenseRequest{
		Project: project, LicenseResource: &computepb.License{
			Name: s("lic"), Description: s("d"), Transferable: new(true),
			ResourceRequirements: &computepb.LicenseResourceRequirements{MinGuestCpuCount: proto.Int32(2)},
		},
	})
	e.done(t, op, err)

	got, err := lc.Get(t.Context(), &computepb.GetLicenseRequest{Project: project, License: "lic"})
	require.NoError(t, err)
	assert.NotZero(t, got.GetLicenseCode())
	assert.True(t, got.GetTransferable())
	assert.Contains(t, got.GetSelfLink(), "/global/licenses/lic")

	_, err = lc.Insert(t.Context(), &computepb.InsertLicenseRequest{
		Project: project, LicenseResource: &computepb.License{Name: s("lic")},
	})
	assertAPIError(t, err, http.StatusConflict, "alreadyExists")

	_, err = lc.Insert(t.Context(), &computepb.InsertLicenseRequest{
		Project: project, LicenseResource: &computepb.License{
			Name:                 s("neg"),
			ResourceRequirements: &computepb.LicenseResourceRequirements{MinMemoryMb: proto.Int32(-1)},
		},
	})
	assertAPIError(t, err, http.StatusBadRequest, "invalid")

	op, err = lc.Update(t.Context(), &computepb.UpdateLicenseRequest{
		Project: project, License: "lic", UpdateMask: s("description"),
		LicenseResource: &computepb.License{Description: s("new")},
	})
	e.done(t, op, err)

	after, err := lc.Get(t.Context(), &computepb.GetLicenseRequest{Project: project, License: "lic"})
	require.NoError(t, err)
	assert.Equal(t, "new", after.GetDescription())
	assert.Equal(t, got.GetLicenseCode(), after.GetLicenseCode())

	code, err := codes.Get(t.Context(), &computepb.GetLicenseCodeRequest{
		Project: project, LicenseCode: strconv.FormatUint(got.GetLicenseCode(), 10),
	})
	require.NoError(t, err)
	assert.True(t, code.GetTransferable())
	require.Len(t, code.GetLicenseAlias(), 1)
	assert.Equal(t, got.GetSelfLink(), code.GetLicenseAlias()[0].GetSelfLink())

	_, err = codes.Get(t.Context(), &computepb.GetLicenseCodeRequest{Project: project, LicenseCode: "42"})
	assertAPIError(t, err, http.StatusNotFound, "notFound")

	it := lc.List(t.Context(), &computepb.ListLicensesRequest{Project: project})
	l, err := it.Next()
	require.NoError(t, err)
	assert.Equal(t, "lic", l.GetName())

	_, err = it.Next()
	require.ErrorIs(t, err, iterator.Done)

	op, err = lc.Delete(t.Context(), &computepb.DeleteLicenseRequest{Project: project, License: "lic"})
	e.done(t, op, err)

	_, err = lc.Get(t.Context(), &computepb.GetLicenseRequest{Project: project, License: "lic"})
	assertAPIError(t, err, http.StatusNotFound, "notFound")
}

func TestInterconnectLocations(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	lc := client(t, e, compute.NewInterconnectLocationsRESTClient)

	loc, err := lc.Get(t.Context(), &computepb.GetInterconnectLocationRequest{
		Project: project, InterconnectLocation: "iad-zone1-1",
	})
	require.NoError(t, err)
	assert.Equal(t, "Ashburn", loc.GetCity())
	assert.Equal(t, "AVAILABLE", loc.GetStatus())
	require.Len(t, loc.GetRegionInfos(), 1)
	assert.Contains(t, loc.GetRegionInfos()[0].GetRegion(), "/regions/us-east4")

	_, err = lc.Get(t.Context(), &computepb.GetInterconnectLocationRequest{
		Project: project, InterconnectLocation: "xxx-zone1-1",
	})
	assertAPIError(t, err, http.StatusNotFound, "notFound")

	names := map[string]bool{}
	it := lc.List(t.Context(), &computepb.ListInterconnectLocationsRequest{Project: project})

	for {
		l, nerr := it.Next()
		if nerr != nil {
			require.ErrorIs(t, nerr, iterator.Done)

			break
		}

		names[l.GetName()] = true
	}

	assert.True(t, names["iad-zone1-1"])
	assert.True(t, names["iad-zone2-1"])
	assert.True(t, names["nrt-zone2-452"])
}

func TestInterconnectLifecycle(t *testing.T) {
	t.Parallel()

	invalid := []struct {
		mod  func(ic *computepb.Interconnect)
		name string
		code int
	}{
		{func(ic *computepb.Interconnect) { ic.Location = nil }, "no location", http.StatusBadRequest},
		{
			func(ic *computepb.Interconnect) { ic.Location = s("global/interconnectLocations/nope") },
			"unknown location",
			http.StatusNotFound,
		},
		{func(ic *computepb.Interconnect) { ic.LinkType = s("fast") }, "bad link type", http.StatusBadRequest},
		{
			func(ic *computepb.Interconnect) { ic.RequestedLinkCount = proto.Int32(0) },
			"no links",
			http.StatusBadRequest,
		},
		{
			func(ic *computepb.Interconnect) { ic.MacsecEnabled = new(true) },
			"macsec without keys",
			http.StatusBadRequest,
		},
		{
			func(ic *computepb.Interconnect) { ic.RequestedFeatures = []string{"IF_NOPE"} },
			"bad feature",
			http.StatusBadRequest,
		},
		{func(ic *computepb.Interconnect) { ic.InterconnectType = s("OTHER") }, "bad type", http.StatusBadRequest},
	}

	for _, tt := range invalid {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			ic := dedicated("ic")
			tt.mod(ic)
			assertAPIError(t, e.interconnect(t, ic), tt.code, "")
		})
	}

	t.Run("lifecycle", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		require.NoError(t, e.interconnect(t, dedicated("ic")))
		assertAPIError(t, e.interconnect(t, dedicated("ic")), http.StatusConflict, "alreadyExists")

		c := e.interconnects(t)
		get := func() *computepb.Interconnect {
			got, err := c.Get(t.Context(), &computepb.GetInterconnectRequest{Project: project, Interconnect: "ic"})
			require.NoError(t, err)

			return got
		}

		got := get()
		assert.True(t, got.GetAdminEnabled())
		assert.Equal(t, "ACTIVE", got.GetState())
		assert.Equal(t, "OS_ACTIVE", got.GetOperationalStatus())
		assert.Equal(t, int32(2), got.GetProvisionedLinkCount())
		assert.Contains(t, got.GetLocation(), "/global/interconnectLocations/iad-zone1-1")
		assert.Contains(t, got.GetAvailableFeatures(), "IF_MACSEC")

		patch := func(ic *computepb.Interconnect) error {
			op, err := c.Patch(t.Context(), &computepb.PatchInterconnectRequest{
				Project: project, Interconnect: "ic", InterconnectResource: ic,
			})

			return e.finish(t, op, err)
		}

		require.NoError(t, patch(&computepb.Interconnect{
			Description: s("x"), AdminEnabled: new(false), RequestedLinkCount: proto.Int32(4),
		}))

		got = get()
		assert.Equal(t, "x", got.GetDescription())
		assert.False(t, got.GetAdminEnabled())
		assert.Equal(t, int32(4), got.GetProvisionedLinkCount())

		assertAPIError(t, patch(&computepb.Interconnect{Location: s("global/interconnectLocations/iad-zone2-1")}),
			http.StatusBadRequest, "invalid")
		assertAPIError(t, patch(&computepb.Interconnect{LinkType: s("LINK_TYPE_ETHERNET_100G_LR")}),
			http.StatusBadRequest, "invalid")
		assertAPIError(t, patch(&computepb.Interconnect{RequestedFeatures: []string{"IF_MACSEC"}}),
			http.StatusBadRequest, "invalid")

		op, err := c.SetLabels(t.Context(), &computepb.SetLabelsInterconnectRequest{
			Project: project, Resource: "ic",
			GlobalSetLabelsRequestResource: &computepb.GlobalSetLabelsRequest{
				Labels: map[string]string{"a": "b"}, LabelFingerprint: s(got.GetLabelFingerprint()),
			},
		})
		require.NoError(t, e.finish(t, op, err))
		assert.Equal(t, map[string]string{"a": "b"}, get().GetLabels())

		_, err = c.SetLabels(t.Context(), &computepb.SetLabelsInterconnectRequest{
			Project: project, Resource: "ic",
			GlobalSetLabelsRequestResource: &computepb.GlobalSetLabelsRequest{
				Labels: map[string]string{"c": "d"}, LabelFingerprint: s("stale"),
			},
		})
		assertAPIError(t, err, http.StatusPreconditionFailed, "conditionNotMet")

		it := c.List(t.Context(), &computepb.ListInterconnectsRequest{Project: project})
		l, err := it.Next()
		require.NoError(t, err)
		assert.Equal(t, "ic", l.GetName())

		op, err = c.Delete(t.Context(), &computepb.DeleteInterconnectRequest{Project: project, Interconnect: "ic"})
		require.NoError(t, e.finish(t, op, err))

		_, err = c.Get(t.Context(), &computepb.GetInterconnectRequest{Project: project, Interconnect: "ic"})
		assertAPIError(t, err, http.StatusNotFound, "notFound")
	})
}

func TestInterconnectAttachmentLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.network(t, "vpc", false)

	op, err := e.routers(t).Insert(t.Context(), &computepb.InsertRouterRequest{
		Project: project, Region: region,
		RouterResource: &computepb.Router{Name: s("rtr"), Network: s("global/networks/vpc")},
	})
	e.done(t, op, err)
	require.NoError(t, e.interconnect(t, dedicated("ic")))

	dedicatedAtt := func(name string) *computepb.InterconnectAttachment {
		return &computepb.InterconnectAttachment{
			Name: s(name), Interconnect: s("global/interconnects/ic"), Router: s("regions/" + region + "/routers/rtr"),
		}
	}

	assertAPIError(t, e.attach(t, &computepb.InterconnectAttachment{Name: s("x"), Type: s("DEDICATED")}, false),
		http.StatusBadRequest, "required")
	assertAPIError(t, e.attach(t, &computepb.InterconnectAttachment{
		Name: s("x"), Interconnect: s("global/interconnects/ghost"),
	}, false), http.StatusNotFound, "notFound")
	assertAPIError(t, e.attach(t, &computepb.InterconnectAttachment{
		Name: s("x"), Interconnect: s("global/interconnects/ic"), Router: s("regions/" + region + "/routers/ghost"),
	}, false), http.StatusNotFound, "notFound")
	assertAPIError(t, e.attach(t, &computepb.InterconnectAttachment{
		Name: s("x"), Interconnect: s("global/interconnects/ic"), Mtu: proto.Int32(1234),
	}, false), http.StatusBadRequest, "invalid")

	require.NoError(t, e.attach(t, dedicatedAtt("validate"), true))

	_, err = e.attachments(t).Get(t.Context(), &computepb.GetInterconnectAttachmentRequest{
		Project: project, Region: region, InterconnectAttachment: "validate",
	})
	assertAPIError(t, err, http.StatusNotFound, "notFound")

	require.NoError(t, e.attach(t, dedicatedAtt("a1"), false))
	require.NoError(t, e.attach(t, dedicatedAtt("a2"), false))
	assertAPIError(t, e.attach(t, dedicatedAtt("a1"), false), http.StatusConflict, "alreadyExists")

	a1, a2 := e.getAttachment(t, "a1"), e.getAttachment(t, "a2")
	assert.Equal(t, "DEDICATED", a1.GetType())
	assert.Equal(t, "ACTIVE", a1.GetState())
	assert.Equal(t, "OS_ACTIVE", a1.GetOperationalStatus())
	assert.Equal(t, "169.254.0.1/29", a1.GetCloudRouterIpAddress())
	assert.Equal(t, "169.254.0.2/29", a1.GetCustomerRouterIpAddress())
	assert.Equal(t, "169.254.0.9/29", a2.GetCloudRouterIpAddress())
	assert.Equal(t, int32(2), a1.GetVlanTag8021Q())
	assert.Equal(t, int32(3), a2.GetVlanTag8021Q())
	assert.Equal(t, uint32(2), a1.GetPrivateInterconnectInfo().GetTag8021Q())
	assert.Equal(t, int32(1440), a1.GetMtu())
	assert.Equal(t, "IPV4_ONLY", a1.GetStackType())
	assert.Equal(t, "NONE", a1.GetEncryption())
	assert.Equal(t, "BPS_10G", a1.GetBandwidth())
	assert.True(t, a1.GetAdminEnabled())
	assert.Contains(t, a1.GetRouter(), "/regions/"+region+"/routers/rtr")

	ic, err := e.interconnects(t).
		Get(t.Context(), &computepb.GetInterconnectRequest{Project: project, Interconnect: "ic"})
	require.NoError(t, err)
	assert.Len(t, ic.GetInterconnectAttachments(), 2)

	assertAPIError(t, e.attach(t, &computepb.InterconnectAttachment{
		Name: s("dup-vlan"), Interconnect: s("global/interconnects/ic"), VlanTag8021Q: proto.Int32(2),
	}, false), http.StatusBadRequest, "invalid")

	_, err = e.interconnects(t).
		Delete(t.Context(), &computepb.DeleteInterconnectRequest{Project: project, Interconnect: "ic"})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")

	_, err = e.routers(t).
		Delete(t.Context(), &computepb.DeleteRouterRequest{Project: project, Region: region, Router: "rtr"})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")

	patch := func(a *computepb.InterconnectAttachment) error {
		pop, perr := e.attachments(t).Patch(t.Context(), &computepb.PatchInterconnectAttachmentRequest{
			Project: project, Region: region, InterconnectAttachment: "a1", InterconnectAttachmentResource: a,
		})

		return e.finish(t, pop, perr)
	}

	require.NoError(t, patch(&computepb.InterconnectAttachment{Bandwidth: s("BPS_1G"), Description: s("x")}))
	assert.Equal(t, "BPS_1G", e.getAttachment(t, "a1").GetBandwidth())
	assertAPIError(
		t,
		patch(&computepb.InterconnectAttachment{VlanTag8021Q: proto.Int32(9)}),
		http.StatusBadRequest,
		"invalid",
	)
	assertAPIError(
		t,
		patch(&computepb.InterconnectAttachment{Encryption: s("IPSEC")}),
		http.StatusBadRequest,
		"invalid",
	)
	assertAPIError(
		t,
		patch(&computepb.InterconnectAttachment{Bandwidth: s("BPS_7G")}),
		http.StatusBadRequest,
		"invalid",
	)
	assert.Equal(t, "169.254.0.1/29", e.getAttachment(t, "a1").GetCloudRouterIpAddress())

	agg := map[string]int{}
	it := e.attachments(t).
		AggregatedList(t.Context(), &computepb.AggregatedListInterconnectAttachmentsRequest{Project: project})

	for {
		kv, nerr := it.Next()
		if nerr != nil {
			require.ErrorIs(t, nerr, iterator.Done)

			break
		}

		agg[kv.Key] += len(kv.Value.GetInterconnectAttachments())
	}

	assert.Equal(t, 2, agg["regions/"+region])

	op, err = e.attachments(t).SetLabels(t.Context(), &computepb.SetLabelsInterconnectAttachmentRequest{
		Project: project, Region: region, Resource: "a1",
		RegionSetLabelsRequestResource: &computepb.RegionSetLabelsRequest{
			Labels: map[string]string{"k": "v"}, LabelFingerprint: s(a1.GetLabelFingerprint()),
		},
	})
	require.NoError(t, e.finish(t, op, err))
	assert.Equal(t, map[string]string{"k": "v"}, e.getAttachment(t, "a1").GetLabels())

	for _, n := range []string{"a1", "a2"} {
		op, err = e.attachments(t).Delete(t.Context(), &computepb.DeleteInterconnectAttachmentRequest{
			Project: project, Region: region, InterconnectAttachment: n,
		})
		require.NoError(t, e.finish(t, op, err))
	}

	op, err = e.routers(t).
		Delete(t.Context(), &computepb.DeleteRouterRequest{Project: project, Region: region, Router: "rtr"})
	require.NoError(t, e.finish(t, op, err))
}

func TestInterconnectAttachmentAddressing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		att  *computepb.InterconnectAttachment
		name string
		want string
		code int
	}{
		{
			name: "candidate subnet", want: "169.254.10.1/29",
			att: &computepb.InterconnectAttachment{CandidateSubnets: []string{"169.254.10.0/29"}},
		},
		{
			name: "subnet length 30", want: "169.254.0.1/30",
			att: &computepb.InterconnectAttachment{SubnetLength: proto.Int32(30)},
		},
		{
			name: "explicit pair", want: "169.254.50.9/30",
			att: &computepb.InterconnectAttachment{
				CandidateCloudRouterIpAddress:    s("169.254.50.9/30"),
				CandidateCustomerRouterIpAddress: s("169.254.50.10/30"),
			},
		},
		{
			name: "outside link local", code: http.StatusBadRequest,
			att: &computepb.InterconnectAttachment{CandidateSubnets: []string{"10.0.0.0/29"}},
		},
		{
			name: "too long prefix", code: http.StatusBadRequest,
			att: &computepb.InterconnectAttachment{CandidateSubnets: []string{"169.254.1.0/30"}},
		},
		{
			name: "half a pair", code: http.StatusBadRequest,
			att: &computepb.InterconnectAttachment{CandidateCloudRouterIpAddress: s("169.254.50.9/30")},
		},
		{
			name: "mixed lengths", code: http.StatusBadRequest,
			att: &computepb.InterconnectAttachment{
				CandidateCloudRouterIpAddress:    s("169.254.50.9/30"),
				CandidateCustomerRouterIpAddress: s("169.254.50.10/29"),
			},
		},
		{
			name: "bad subnet length", code: http.StatusBadRequest,
			att: &computepb.InterconnectAttachment{SubnetLength: proto.Int32(28)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			require.NoError(t, e.interconnect(t, dedicated("ic")))

			a := proto.Clone(tt.att).(*computepb.InterconnectAttachment)
			a.Name, a.Interconnect = s("att"), s("global/interconnects/ic")

			err := e.attach(t, a, false)
			if tt.code != 0 {
				assertAPIError(t, err, tt.code, "invalid")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, e.getAttachment(t, "att").GetCloudRouterIpAddress())
			assert.Empty(t, e.getAttachment(t, "att").GetCandidateSubnets())
		})
	}
}

func TestInterconnectAttachmentPartnerAndIPsec(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	require.NoError(t, e.attach(t, &computepb.InterconnectAttachment{
		Name: s("p"), Type: s("PARTNER"), EdgeAvailabilityDomain: s("AVAILABILITY_DOMAIN_1"), AdminEnabled: new(false),
	}, false))

	p := e.getAttachment(t, "p")
	assert.Equal(t, "PENDING_CUSTOMER", p.GetState())
	assert.Equal(t, "OS_UNPROVISIONED", p.GetOperationalStatus())
	assert.Regexp(t, `^[0-9a-f]{16}/`+region+`/1$`, p.GetPairingKey())
	assert.Zero(t, p.GetVlanTag8021Q())

	op, err := e.attachments(t).Patch(t.Context(), &computepb.PatchInterconnectAttachmentRequest{
		Project: project, Region: region, InterconnectAttachment: "p",
		InterconnectAttachmentResource: &computepb.InterconnectAttachment{AdminEnabled: new(true)},
	})
	require.NoError(t, e.finish(t, op, err))
	assert.Equal(t, "ACTIVE", e.getAttachment(t, "p").GetState())

	assertAPIError(t, e.attach(t, &computepb.InterconnectAttachment{
		Name: s("bad"), Type: s("PARTNER"), Interconnect: s("global/interconnects/ic"),
	}, false), http.StatusBadRequest, "invalid")
	assertAPIError(t, e.attach(t, &computepb.InterconnectAttachment{
		Name: s("bad"), Type: s("PARTNER"), EdgeAvailabilityDomain: s("AVAILABILITY_DOMAIN_9"),
	}, false), http.StatusBadRequest, "invalid")
	assertAPIError(
		t,
		e.attach(t, &computepb.InterconnectAttachment{Name: s("bad"), Type: s("PARTNER_PROVIDER")}, false),
		http.StatusBadRequest,
		"invalid",
	)

	e.network(t, "vpc", false)
	e.subnet(t, "sub", "10.0.0.0/24")

	op, err = e.addresses(t).Insert(t.Context(), &computepb.InsertAddressRequest{
		Project: project, Region: region, AddressResource: &computepb.Address{
			Name: s("ipsec"), AddressType: s("INTERNAL"), Subnetwork: s("regions/" + region + "/subnetworks/sub"),
		},
	})
	e.done(t, op, err)

	ipsec := "regions/" + region + "/addresses/ipsec"
	require.NoError(t, e.attach(t, &computepb.InterconnectAttachment{
		Name: s("ext"), Type: s("PARTNER"), Encryption: s("IPSEC"), IpsecInternalAddresses: []string{ipsec},
	}, false))
	assert.Contains(t, e.getAttachment(t, "ext").GetIpsecInternalAddresses()[0], "/addresses/ipsec")

	_, err = e.addresses(t).Delete(t.Context(), &computepb.DeleteAddressRequest{
		Project: project, Region: region, Address: "ipsec",
	})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")

	assertAPIError(t, e.attach(t, &computepb.InterconnectAttachment{
		Name: s("none"), Type: s("PARTNER"), IpsecInternalAddresses: []string{ipsec},
	}, false), http.StatusBadRequest, "invalid")
	assertAPIError(t, e.attach(t, &computepb.InterconnectAttachment{
		Name: s("ghost"), Type: s("PARTNER"), Encryption: s("IPSEC"),
		IpsecInternalAddresses: []string{"regions/" + region + "/addresses/ghost"},
	}, false), http.StatusNotFound, "notFound")
}

func TestInterconnectStateRestore(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	src.network(t, "vpc", false)
	require.NoError(t, src.interconnect(t, dedicated("ic")))
	require.NoError(t, src.attach(t, &computepb.InterconnectAttachment{
		Name: s("att"), Interconnect: s("global/interconnects/ic"),
	}, false))

	op, err := client(t, src, compute.NewLicensesRESTClient).Insert(t.Context(), &computepb.InsertLicenseRequest{
		Project: project, LicenseResource: &computepb.License{Name: s("lic")},
	})
	src.done(t, op, err)

	op, err = src.routers(t).Insert(t.Context(), &computepb.InsertRouterRequest{
		Project: project, Region: region, RouterResource: &computepb.Router{
			Name: s("rtr"), Network: s("global/networks/vpc"), Interfaces: []*computepb.RouterInterface{
				{Name: s("if0"), LinkedInterconnectAttachment: s("regions/" + region + "/interconnectAttachments/att")},
			},
		},
	})
	src.done(t, op, err)

	dst := newEnv(t)
	require.NoError(t, dst.backend.Restore(t.Context(), src.backend.Snapshot(t.Context())))

	a := dst.getAttachment(t, "att")
	assert.Equal(t, "169.254.0.1/29", a.GetCloudRouterIpAddress())
	assert.Equal(t, int32(2), a.GetVlanTag8021Q())

	ic, err := dst.interconnects(t).
		Get(t.Context(), &computepb.GetInterconnectRequest{Project: project, Interconnect: "ic"})
	require.NoError(t, err)
	assert.Len(t, ic.GetInterconnectAttachments(), 1)

	_, err = client(
		t,
		dst,
		compute.NewLicensesRESTClient,
	).Get(t.Context(), &computepb.GetLicenseRequest{Project: project, License: "lic"})
	require.NoError(t, err)

	_, err = dst.attachments(t).Delete(t.Context(), &computepb.DeleteInterconnectAttachmentRequest{
		Project: project, Region: region, InterconnectAttachment: "att",
	})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")
}

func TestInterconnectSetName(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	require.NoError(t, e.interconnect(t, dedicated("old")))
	require.NoError(t, e.interconnect(t, dedicated("taken")))
	require.NoError(t, e.attach(t, &computepb.InterconnectAttachment{
		Name: s("att"), Interconnect: s("global/interconnects/old"),
	}, false))

	rename := func(cur, next string) error {
		op, err := e.interconnects(t).SetName(t.Context(), &computepb.SetNameInterconnectRequest{
			Project:      project,
			Interconnect: cur,
			InterconnectsSetNameRequestResource: &computepb.InterconnectsSetNameRequest{
				CurrentName: s(cur),
				Name:        s(next),
			},
		})

		return e.finish(t, op, err)
	}

	assertAPIError(t, rename("ghost", "x"), http.StatusNotFound, "notFound")
	assertAPIError(t, rename("old", "taken"), http.StatusConflict, "alreadyExists")
	assertAPIError(t, rename("old", "Bad_Name"), http.StatusBadRequest, "invalid")
	require.NoError(t, rename("old", "renamed"))

	got, err := e.interconnects(t).
		Get(t.Context(), &computepb.GetInterconnectRequest{Project: project, Interconnect: "renamed"})
	require.NoError(t, err)
	assert.Contains(t, got.GetSelfLink(), "/global/interconnects/renamed")
	assert.Len(t, got.GetInterconnectAttachments(), 1)
	assert.Contains(t, e.getAttachment(t, "att").GetInterconnect(), "/interconnects/renamed")

	_, err = e.interconnects(t).
		Get(t.Context(), &computepb.GetInterconnectRequest{Project: project, Interconnect: "old"})
	assertAPIError(t, err, http.StatusNotFound, "notFound")
}

func TestHAVPNOverInterconnect(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.network(t, "vpc", false)
	e.subnet(t, "sub", "10.0.0.0/24")
	require.NoError(t, e.interconnect(t, dedicated("ic")))

	op, err := e.addresses(t).Insert(t.Context(), &computepb.InsertAddressRequest{
		Project: project, Region: region, AddressResource: &computepb.Address{
			Name: s("ipsec"), AddressType: s("INTERNAL"), Subnetwork: s("regions/" + region + "/subnetworks/sub"),
		},
	})
	e.done(t, op, err)

	addr, err := e.addresses(t).
		Get(t.Context(), &computepb.GetAddressRequest{Project: project, Region: region, Address: "ipsec"})
	require.NoError(t, err)

	require.NoError(t, e.attach(t, &computepb.InterconnectAttachment{
		Name: s("ipsec-att"), Interconnect: s("global/interconnects/ic"), Encryption: s("IPSEC"),
		IpsecInternalAddresses: []string{"regions/" + region + "/addresses/ipsec"},
	}, false))
	require.NoError(t, e.attach(t, &computepb.InterconnectAttachment{
		Name: s("plain-att"), Interconnect: s("global/interconnects/ic"),
	}, false))

	gateways := client(t, e, compute.NewVpnGatewaysRESTClient)
	insert := func(name, attachment string) error {
		iop, ierr := gateways.Insert(t.Context(), &computepb.InsertVpnGatewayRequest{
			Project: project, Region: region, VpnGatewayResource: &computepb.VpnGateway{
				Name: s(name), Network: s("global/networks/vpc"),
				VpnInterfaces: []*computepb.VpnGatewayVpnGatewayInterface{
					{InterconnectAttachment: s("regions/" + region + "/interconnectAttachments/" + attachment)},
				},
			},
		})

		return e.finish(t, iop, ierr)
	}

	assertAPIError(t, insert("bad", "plain-att"), http.StatusBadRequest, "invalid")
	assertAPIError(t, insert("bad", "ghost"), http.StatusNotFound, "notFound")
	require.NoError(t, insert("gw", "ipsec-att"))

	gw, err := gateways.Get(
		t.Context(),
		&computepb.GetVpnGatewayRequest{Project: project, Region: region, VpnGateway: "gw"},
	)
	require.NoError(t, err)
	require.Len(t, gw.GetVpnInterfaces(), 2)
	assert.Contains(t, gw.GetVpnInterfaces()[0].GetInterconnectAttachment(), "/interconnectAttachments/ipsec-att")
	assert.Equal(t, addr.GetAddress(), gw.GetVpnInterfaces()[0].GetIpAddress())
	assert.Empty(t, gw.GetVpnInterfaces()[1].GetInterconnectAttachment())

	_, err = e.attachments(t).Delete(t.Context(), &computepb.DeleteInterconnectAttachmentRequest{
		Project: project, Region: region, InterconnectAttachment: "ipsec-att",
	})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")
}
