package gcpcompute_test

import (
	"errors"
	"strings"
	"testing"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/proto"
)

func customImages(t *testing.T, e *env, proj string) []*computepb.Image {
	t.Helper()

	it := client(t, e, compute.NewImagesRESTClient).List(t.Context(), &computepb.ListImagesRequest{Project: proj})

	var out []*computepb.Image

	for {
		img, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return out
		}

		require.NoError(t, err)

		out = append(out, img)
	}
}

func TestDefaultInstantiationCreatesCustomImages(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)

	op, err := e.disks(t).Insert(t.Context(), &computepb.InsertDiskRequest{
		Project: project, Zone: zone, DiskResource: &computepb.Disk{Name: s("data"), SizeGb: proto.Int64(15)},
	})
	e.done(t, op, err)

	spec := vmSpec("vm1")
	spec.Disks = append(spec.Disks, &computepb.AttachedDisk{
		Source: s("zones/" + zone + "/disks/data"), DeviceName: s("data"), AutoDelete: new(false),
	})
	e.vm(t, spec)

	op, err = e.templates(t).Insert(t.Context(), &computepb.InsertInstanceTemplateRequest{
		Project: project, InstanceTemplateResource: &computepb.InstanceTemplate{
			Name: s("t"), SourceInstance: s("zones/" + zone + "/instances/vm1"),
		},
	})
	e.done(t, op, err)

	imgs := customImages(t, e, project)
	require.Len(t, imgs, 2, "one custom image per read-write disk")

	got, err := e.templates(t).
		Get(t.Context(), &computepb.GetInstanceTemplateRequest{Project: project, InstanceTemplate: "t"})
	require.NoError(t, err)

	byLink := map[string]*computepb.Image{}
	for _, img := range imgs {
		byLink["projects/"+project+"/global/images/"+img.GetName()] = img
	}

	for _, d := range got.GetProperties().GetDisks() {
		img := byLink[d.GetInitializeParams().GetSourceImage()]
		require.NotNil(t, img, d.GetInitializeParams().GetSourceImage())
		assert.Equal(t, "READY", img.GetStatus())
		assert.Contains(t, img.GetSourceDisk(), "/disks/")
		assert.True(t, strings.HasPrefix(img.GetName(), "t-"), img.GetName())
	}

	op, err = e.instances(t).Insert(t.Context(), &computepb.InsertInstanceRequest{
		Project: project, Zone: zone, SourceInstanceTemplate: s("global/instanceTemplates/t"),
		InstanceResource: &computepb.Instance{Name: s("child")},
	})
	e.done(t, op, err)
	assert.Equal(t, "RUNNING", e.getVM(t, "child").GetStatus(), "the template boots from the images it created")
}

func TestFailedTemplateCreatesNoImages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tmpl *computepb.InstanceTemplate
		name string
	}{
		{name: "boot disk excluded", tmpl: &computepb.InstanceTemplate{
			Name: s("t"), SourceInstance: s("zones/" + zone + "/instances/vm1"),
			SourceInstanceParams: &computepb.SourceInstanceParams{DiskConfigs: []*computepb.DiskInstantiationConfig{
				{DeviceName: s("persistent-disk-0"), InstantiateFrom: s("DO_NOT_INCLUDE")},
			}},
		}},
		{name: "bad later disk", tmpl: &computepb.InstanceTemplate{
			Name: s("t"), SourceInstance: s("zones/" + zone + "/instances/vm1"),
			SourceInstanceParams: &computepb.SourceInstanceParams{DiskConfigs: []*computepb.DiskInstantiationConfig{
				{DeviceName: s("data"), InstantiateFrom: s("ATTACH_READ_ONLY")},
			}},
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
			spec.Disks = append(spec.Disks, &computepb.AttachedDisk{
				Source: s("zones/" + zone + "/disks/data"), DeviceName: s("data"),
			})
			e.vm(t, spec)

			_, err = e.templates(t).Insert(t.Context(), &computepb.InsertInstanceTemplateRequest{
				Project: project, InstanceTemplateResource: tt.tmpl,
			})
			assertAPIError(t, err, 400, "")
			assert.Empty(t, customImages(t, e, project), "a rejected template leaves no image behind")
		})
	}
}

func TestTemplateFromSourceInstanceInAnotherProject(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)
	e.vm(t, vmSpec("vm1"))

	op, err := e.templates(t).Insert(t.Context(), &computepb.InsertInstanceTemplateRequest{
		Project: "other-project", InstanceTemplateResource: &computepb.InstanceTemplate{
			Name: s("t"), SourceInstance: s("projects/" + project + "/zones/" + zone + "/instances/vm1"),
		},
	})
	e.done(t, op, err)

	got, err := e.templates(t).
		Get(t.Context(), &computepb.GetInstanceTemplateRequest{Project: "other-project", InstanceTemplate: "t"})
	require.NoError(t, err)
	assert.Equal(t, "e2-medium", got.GetProperties().GetMachineType())

	imgs := customImages(t, e, "other-project")
	require.Len(t, imgs, 1, "the images belong to the template's project")
	assert.Contains(t, imgs[0].GetSourceDisk(), "/projects/"+project+"/")
	assert.Empty(t, customImages(t, e, project))
}

func TestTemplateFromSourceInstanceWithLocalSSD(t *testing.T) {
	t.Parallel()

	for _, from := range []string{"", "DEFAULT", "BLANK", "DO_NOT_INCLUDE", "CUSTOM_IMAGE"} {
		t.Run("ssd "+from, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)

			spec := vmSpec("vm1")
			spec.MachineType = s("zones/" + zone + "/machineTypes/n2-standard-4")
			spec.Disks = append(spec.Disks, &computepb.AttachedDisk{
				Type: s("SCRATCH"), DeviceName: s("ssd0"), Interface: s("NVME"),
				InitializeParams: &computepb.AttachedDiskInitializeParams{DiskType: s("local-ssd")},
			})
			e.vm(t, spec)

			tmpl := &computepb.InstanceTemplate{Name: s("t"), SourceInstance: s("zones/" + zone + "/instances/vm1")}
			if from != "" {
				tmpl.SourceInstanceParams = &computepb.SourceInstanceParams{
					DiskConfigs: []*computepb.DiskInstantiationConfig{
						{DeviceName: s("ssd0"), InstantiateFrom: s(from)},
					},
				}
			}

			op, err := e.templates(t).Insert(t.Context(), &computepb.InsertInstanceTemplateRequest{
				Project: project, InstanceTemplateResource: tmpl,
			})

			if from == "CUSTOM_IMAGE" {
				assertAPIError(t, err, 400, "")

				return
			}

			e.done(t, op, err)

			got, err := e.templates(t).
				Get(t.Context(), &computepb.GetInstanceTemplateRequest{Project: project, InstanceTemplate: "t"})
			require.NoError(t, err)

			disks := got.GetProperties().GetDisks()
			if from == "DO_NOT_INCLUDE" {
				assert.Len(t, disks, 1)

				return
			}

			require.Len(t, disks, 2)
			assert.Equal(t, "SCRATCH", disks[1].GetType())
			assert.Equal(t, "local-ssd", disks[1].GetInitializeParams().GetDiskType())
		})
	}
}
