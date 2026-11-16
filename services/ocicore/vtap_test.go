package ocicore_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ocicore"
)

func TestCaptureFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		details  core.CreateCaptureFilterDetails
		wantCode int
	}{
		{"vtap", core.CreateCaptureFilterDetails{
			FilterType: core.CreateCaptureFilterDetailsFilterTypeVtap,
			VtapCaptureFilterRules: []core.VtapCaptureFilterRuleDetails{{
				TrafficDirection: core.VtapCaptureFilterRuleDetailsTrafficDirectionIngress, Protocol: new("6"),
				SourceCidr: new("10.0.0.0/24"),
				TcpOptions: &core.TcpOptions{DestinationPortRange: &core.PortRange{Min: new(80), Max: new(80)}},
			}},
		}, 0},
		{"flow log", core.CreateCaptureFilterDetails{
			FilterType: core.CreateCaptureFilterDetailsFilterTypeFlowlog,
			FlowLogCaptureFilterRules: []core.FlowLogCaptureFilterRuleDetails{{
				SamplingRate: new(50), FlowLogType: core.FlowLogCaptureFilterRuleDetailsFlowLogTypeReject,
			}},
		}, 0},
		{"direction missing", core.CreateCaptureFilterDetails{
			FilterType:             core.CreateCaptureFilterDetailsFilterTypeVtap,
			VtapCaptureFilterRules: []core.VtapCaptureFilterRuleDetails{{Protocol: new("6")}},
		}, http.StatusBadRequest},
		{"bad cidr", core.CreateCaptureFilterDetails{
			FilterType: core.CreateCaptureFilterDetailsFilterTypeVtap,
			VtapCaptureFilterRules: []core.VtapCaptureFilterRuleDetails{{
				TrafficDirection: core.VtapCaptureFilterRuleDetailsTrafficDirectionEgress, SourceCidr: new("nope"),
			}},
		}, http.StatusBadRequest},
		{"wrong rule kind", core.CreateCaptureFilterDetails{
			FilterType: core.CreateCaptureFilterDetailsFilterTypeFlowlog,
			VtapCaptureFilterRules: []core.VtapCaptureFilterRuleDetails{{
				TrafficDirection: core.VtapCaptureFilterRuleDetailsTrafficDirectionEgress,
			}},
		}, http.StatusBadRequest},
		{
			"bad sampling",
			core.CreateCaptureFilterDetails{
				FilterType:                core.CreateCaptureFilterDetailsFilterTypeFlowlog,
				FlowLogCaptureFilterRules: []core.FlowLogCaptureFilterRuleDetails{{SamplingRate: new(101)}},
			},
			http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			tc.details.CompartmentId = new(testComp)

			f, err := e.client.CreateCaptureFilter(
				t.Context(),
				core.CreateCaptureFilterRequest{CreateCaptureFilterDetails: tc.details},
			)
			if tc.wantCode != 0 {
				assert.Equal(t, tc.wantCode, svcErr(t, err).GetHTTPStatusCode())

				return
			}

			require.NoError(t, err)
			assert.Equal(t, core.CaptureFilterLifecycleStateProvisioning, f.LifecycleState)

			e.settle()

			l, lerr := e.client.ListCaptureFilters(t.Context(), core.ListCaptureFiltersRequest{
				CompartmentId: new(testComp), FilterType: core.CaptureFilterFilterTypeEnum(tc.details.FilterType),
			})
			require.NoError(t, lerr)
			assert.Len(t, l.Items, 1)
		})
	}
}

func TestVtapLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	v := e.mkVcn(t, "10.0.0.0/16", "vtapvcn")
	s := e.mkSubnet(t, v, "10.0.1.0/24")

	src, err := e.backend.CreateServiceNic(oci.ServiceNicIn{CompartmentID: testComp, SubnetID: *s.Id})
	require.NoError(t, err)

	dst, err := e.backend.CreateServiceNic(oci.ServiceNicIn{CompartmentID: testComp, SubnetID: *s.Id})
	require.NoError(t, err)

	filter, err := e.client.CreateCaptureFilter(
		ctx,
		core.CreateCaptureFilterRequest{
			CompartmentId: new(testComp),
			FilterType:    core.CreateCaptureFilterDetailsFilterTypeVtap,
		},
	)
	require.NoError(t, err)

	flow, err := e.client.CreateCaptureFilter(
		ctx,
		core.CreateCaptureFilterRequest{
			CompartmentId: new(testComp),
			FilterType:    core.CreateCaptureFilterDetailsFilterTypeFlowlog,
		},
	)
	require.NoError(t, err)

	base := func() core.CreateVtapDetails {
		return core.CreateVtapDetails{
			CompartmentId: new(testComp), VcnId: v.Id, SourceId: &src.VnicID,
			CaptureFilterId: filter.Id, TargetId: &dst.VnicID,
		}
	}

	tests := []struct {
		mod      func(*core.CreateVtapDetails)
		name     string
		wantCode int
	}{
		{
			name: "flowlog filter", mod: func(d *core.CreateVtapDetails) { d.CaptureFilterId = flow.Id },
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "self target",
			mod:      func(d *core.CreateVtapDetails) { d.TargetId = d.SourceId },
			wantCode: http.StatusBadRequest,
		},
		{name: "ip outside vcn", mod: func(d *core.CreateVtapDetails) {
			d.TargetId, d.TargetIp, d.TargetType = nil, new("192.168.0.9"), core.CreateVtapDetailsTargetTypeIpAddress
		}, wantCode: http.StatusBadRequest},
		{
			name:     "unknown source",
			mod:      func(d *core.CreateVtapDetails) { d.SourceId = new("ocid1.vnic.oc1.iad.nope") },
			wantCode: http.StatusNotFound,
		},
		{
			name: "bad packet size", mod: func(d *core.CreateVtapDetails) { d.MaxPacketSize = new(9001) },
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		d := base()
		tc.mod(&d)

		_, cerr := e.client.CreateVtap(ctx, core.CreateVtapRequest{CreateVtapDetails: d})
		assert.Equal(t, tc.wantCode, svcErr(t, cerr).GetHTTPStatusCode(), tc.name)
	}

	d := base()
	d.IsVtapEnabled = new(true)
	d.VxlanNetworkIdentifier = new(int64(7))

	vt, err := e.client.CreateVtap(ctx, core.CreateVtapRequest{CreateVtapDetails: d})
	require.NoError(t, err)
	assert.Equal(t, core.VtapLifecycleStateDetailsRunning, vt.LifecycleStateDetails)
	assert.Equal(t, 9000, *vt.MaxPacketSize)
	assert.Equal(t, core.VtapTrafficModeDefault, vt.TrafficMode)

	_, err = e.client.DeleteCaptureFilter(ctx, core.DeleteCaptureFilterRequest{CaptureFilterId: filter.Id})
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode(), "a VTAP uses the filter")

	_, err = e.client.DeleteVcn(ctx, core.DeleteVcnRequest{VcnId: v.Id})
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode(), "the VCN holds a VTAP")

	up, err := e.client.UpdateVtap(
		ctx,
		core.UpdateVtapRequest{
			VtapId:        vt.Id,
			IsVtapEnabled: new(false),
			TrafficMode:   core.UpdateVtapDetailsTrafficModePriority,
		},
	)
	require.NoError(t, err)
	assert.False(t, *up.IsVtapEnabled)
	assert.Equal(t, core.VtapLifecycleStateDetailsStopped, up.LifecycleStateDetails)
	assert.Equal(t, core.VtapTrafficModePriority, up.TrafficMode)

	_, err = e.client.DeleteVtap(ctx, core.DeleteVtapRequest{VtapId: vt.Id})
	require.NoError(t, err)
	e.settle()

	_, err = e.client.DeleteCaptureFilter(ctx, core.DeleteCaptureFilterRequest{CaptureFilterId: filter.Id})
	require.NoError(t, err)
}
