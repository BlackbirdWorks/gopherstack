package gcpcompute_test

import (
	"net/http"
	"testing"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestTemplateFromSourceInstance(t *testing.T) {
	t.Parallel()

	src := "zones/" + zone + "/instances/vm1"

	tests := []struct {
		name    string
		tmpl    *computepb.InstanceTemplate
		status  int
		wantImg string
		wantLen int
	}{
		{name: "defaults", tmpl: &computepb.InstanceTemplate{Name: s("t"), SourceInstance: s(src)}, wantImg: "/images/debian-", wantLen: 2},
		{
			name: "family", wantLen: 2, wantImg: "/images/family/debian-12",
			tmpl: &computepb.InstanceTemplate{Name: s("t"), SourceInstance: s(src), SourceInstanceParams: &computepb.SourceInstanceParams{
				DiskConfigs: []*computepb.DiskInstantiationConfig{{DeviceName: s("persistent-disk-0"), InstantiateFrom: s("SOURCE_IMAGE_FAMILY")}},
			}},
		},
		{
			name: "custom image", wantLen: 2, wantImg: "global/images/mine",
			tmpl: &computepb.InstanceTemplate{Name: s("t"), SourceInstance: s(src), SourceInstanceParams: &computepb.SourceInstanceParams{
				DiskConfigs: []*computepb.DiskInstantiationConfig{{
					DeviceName: s("persistent-disk-0"), InstantiateFrom: s("CUSTOM_IMAGE"), CustomImage: s("global/images/mine"),
				}},
			}},
		},
		{
			name: "exclude data disk", wantLen: 1, wantImg: "/images/debian-",
			tmpl: &computepb.InstanceTemplate{Name: s("t"), SourceInstance: s(src), SourceInstanceParams: &computepb.SourceInstanceParams{
				DiskConfigs: []*computepb.DiskInstantiationConfig{{DeviceName: s("data"), InstantiateFrom: s("DO_NOT_INCLUDE")}},
			}},
		},
		{
			name: "exclude boot", status: http.StatusBadRequest,
			tmpl: &computepb.InstanceTemplate{Name: s("t"), SourceInstance: s(src), SourceInstanceParams: &computepb.SourceInstanceParams{
				DiskConfigs: []*computepb.DiskInstantiationConfig{{DeviceName: s("persistent-disk-0"), InstantiateFrom: s("DO_NOT_INCLUDE")}},
			}},
		},
		{
			name: "unknown device", status: http.StatusBadRequest,
			tmpl: &computepb.InstanceTemplate{Name: s("t"), SourceInstance: s(src), SourceInstanceParams: &computepb.SourceInstanceParams{
				DiskConfigs: []*computepb.DiskInstantiationConfig{{DeviceName: s("nope"), InstantiateFrom: s("BLANK")}},
			}},
		},
		{
			name: "read-only attach of a writable disk", status: http.StatusBadRequest,
			tmpl: &computepb.InstanceTemplate{Name: s("t"), SourceInstance: s(src), SourceInstanceParams: &computepb.SourceInstanceParams{
				DiskConfigs: []*computepb.DiskInstantiationConfig{{DeviceName: s("data"), InstantiateFrom: s("ATTACH_READ_ONLY")}},
			}},
		},
		{
			name: "properties conflict", status: http.StatusBadRequest,
			tmpl: &computepb.InstanceTemplate{Name: s("t"), SourceInstance: s(src), Properties: tmplSpec("x").GetProperties()},
		},
		{name: "missing instance", status: http.StatusNotFound, tmpl: &computepb.InstanceTemplate{Name: s("t"), SourceInstance: s("zones/" + zone + "/instances/none")}},
		{name: "params without source", status: http.StatusBadRequest, tmpl: &computepb.InstanceTemplate{
			Name: s("t"), Properties: tmplSpec("x").GetProperties(), SourceInstanceParams: &computepb.SourceInstanceParams{},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)

			op, err := e.disks(t).Insert(t.Context(), &computepb.InsertDiskRequest{
				Project: project, Zone: zone, DiskResource: &computepb.Disk{Name: s("data"), SizeGb: proto.Int64(15)},
			})
			e.done(t, op, err)

			spec := vmSpec("vm1")
			spec.Labels = map[string]string{"role": "src"}
			spec.Disks = append(spec.Disks, &computepb.AttachedDisk{
				Source: s("zones/" + zone + "/disks/data"), DeviceName: s("data"), AutoDelete: new(false),
			})
			e.vm(t, spec)

			op, err = e.templates(t).Insert(t.Context(), &computepb.InsertInstanceTemplateRequest{
				Project: project, InstanceTemplateResource: tt.tmpl,
			})
			if tt.status != 0 {
				require.Error(t, err)
				assertAPIError(t, err, tt.status, "")

				return
			}

			e.done(t, op, err)

			got, err := e.templates(t).Get(t.Context(), &computepb.GetInstanceTemplateRequest{Project: project, InstanceTemplate: "t"})
			require.NoError(t, err)
			assert.Contains(t, got.GetSourceInstance(), "/instances/vm1")

			props := got.GetProperties()
			require.Len(t, props.GetDisks(), tt.wantLen)
			assert.Contains(t, props.GetDisks()[0].GetInitializeParams().GetSourceImage(), tt.wantImg)
			assert.Equal(t, "src", props.GetLabels()["role"])
			assert.Equal(t, "e2-medium", props.GetMachineType())
			assert.Empty(t, props.GetNetworkInterfaces()[0].GetNetworkIP())

			op, err = e.instances(t).Insert(t.Context(), &computepb.InsertInstanceRequest{
				Project: project, Zone: zone, SourceInstanceTemplate: s("global/instanceTemplates/t"),
				InstanceResource: &computepb.Instance{Name: s("child")},
			})
			if tt.wantLen == 2 && tt.name != "custom image" {
				e.done(t, op, err)
				assert.Equal(t, "src", e.getVM(t, "child").GetLabels()["role"])
			}
		})
	}
}
