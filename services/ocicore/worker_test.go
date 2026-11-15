package ocicore_test

import (
	"testing"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ocicore"
)

func TestLaunchAndTerminateWorker(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	v := e.mkVcn(t, "10.0.0.0/16", "worker")
	s := e.mkSubnet(t, v, "10.0.1.0/24")

	nsg, err := e.client.CreateNetworkSecurityGroup(ctx, core.CreateNetworkSecurityGroupRequest{
		CompartmentId: new(testComp), VcnId: v.Id,
	})
	require.NoError(t, err)

	vcnOf, found := e.backend.LookupNSG(*nsg.Id)
	require.True(t, found)
	assert.Equal(t, *v.Id, vcnOf)

	_, found = e.backend.LookupNSG("ocid1.networksecuritygroup.oc1.iad.nope")
	assert.False(t, found)

	spec := oci.WorkerSpec{
		Compartment: testComp, Name: "worker-1", AD: testAD, SubnetID: *s.Id, Shape: "VM.Standard.E4.Flex",
		ImageID: *e.imageID(t, "Oracle Linux", "9", false), Ocpus: new(float64(2)), NsgIDs: []string{*nsg.Id},
	}

	w, err := e.backend.LaunchWorker(spec)
	require.NoError(t, err)
	assert.Regexp(t, `^10\.0\.1\.\d+$`, w.PrivateIP)
	assert.NotEmpty(t, w.FaultDomain)

	got, err := e.compute.GetInstance(ctx, core.GetInstanceRequest{InstanceId: &w.ID})
	require.NoError(t, err)
	assert.Equal(t, "worker-1", *got.DisplayName)

	spec.Shape = "No.Such.Shape"
	_, err = e.backend.LaunchWorker(spec)
	require.Error(t, err)

	require.NoError(t, e.backend.TerminateWorker(w.ID))
	require.NoError(t, e.backend.TerminateWorker(w.ID))
	require.NoError(t, e.backend.TerminateWorker("ocid1.instance.oc1.iad.nope"))
}

func TestLaunchWorkerSecondaryVnics(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	v := e.mkVcn(t, "10.0.0.0/16", "workervnic")
	s := e.mkSubnet(t, v, "10.0.1.0/24")
	s2 := e.mkSubnet(t, v, "10.0.2.0/24")

	spec := func(vnics ...oci.WorkerVnic) oci.WorkerSpec {
		return oci.WorkerSpec{
			Compartment: testComp, Name: "vnic-worker", AD: testAD, SubnetID: *s.Id, Shape: "VM.Standard.E4.Flex",
			ImageID: *e.imageID(t, "Oracle Linux", "9", false), Ocpus: new(float64(2)), SecondaryVnics: vnics,
		}
	}

	w, err := e.backend.LaunchWorker(spec(oci.WorkerVnic{
		Details: map[string]any{"subnetId": *s2.Id}, DisplayName: "second",
	}))
	require.NoError(t, err)

	atts, err := e.compute.ListVnicAttachments(ctx, core.ListVnicAttachmentsRequest{
		CompartmentId: new(testComp), InstanceId: &w.ID,
	})
	require.NoError(t, err)
	assert.Len(t, atts.Items, 2)

	_, err = e.backend.LaunchWorker(
		spec(oci.WorkerVnic{Details: map[string]any{"subnetId": "ocid1.subnet.oc1.iad.nope"}}),
	)
	require.Error(t, err)
}
