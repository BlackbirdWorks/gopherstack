package ocifilestorage_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/filestorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMountTargetLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	resp, err := e.client.CreateMountTarget(t.Context(), filestorage.CreateMountTargetRequest{
		AvailabilityDomain: new(testAD), CompartmentId: new(testCompartment), SubnetId: new(testSubnet),
		DisplayName: new("mt"), HostnameLabel: new("files-1"), IpAddress: new("10.0.1.77"),
	})
	require.NoError(t, err)
	assert.Equal(t, filestorage.MountTargetLifecycleStateCreating, resp.LifecycleState)
	require.Len(t, resp.PrivateIpIds, 1)
	assert.NotEmpty(t, *resp.ExportSetId)

	e.settle()

	mt := e.getMT(t, resp.Id)
	assert.Equal(t, filestorage.MountTargetLifecycleStateActive, mt.LifecycleState)
	assert.Equal(t, int64(1), *mt.RequestedThroughput)
	assert.Equal(t, filestorage.MountTargetIdmapTypeNone, mt.IdmapType)

	set, err := e.client.GetExportSet(t.Context(), filestorage.GetExportSetRequest{ExportSetId: mt.ExportSetId})
	require.NoError(t, err)
	assert.Equal(t, testVCN, *set.VcnId)
	assert.Equal(t, filestorage.ExportSetLifecycleStateActive, set.LifecycleState)
	assert.NotNil(t, set.MaxFsStatBytes)

	upd, err := e.client.UpdateMountTarget(t.Context(), filestorage.UpdateMountTargetRequest{
		MountTargetId: mt.Id, DisplayName: new("renamed"), NsgIds: []string{"n1", "n2"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"n1", "n2"}, upd.NsgIds)

	_, err = e.client.DeleteMountTarget(t.Context(), filestorage.DeleteMountTargetRequest{MountTargetId: mt.Id})
	require.NoError(t, err)
	assert.Equal(t, []string{"ocid1.vnic.oc1.iad.v1"}, e.net.deletedVnics(), "the VNIC is released")

	e.settle()

	gone, err := e.client.GetExportSet(t.Context(), filestorage.GetExportSetRequest{ExportSetId: mt.ExportSetId})
	require.NoError(t, err)
	assert.Equal(t, filestorage.ExportSetLifecycleStateDeleted, gone.LifecycleState)
}

func TestCreateMountTargetRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(*filestorage.CreateMountTargetDetails)
		name   string
		code   string
		status int
	}{
		{
			name:   "no-subnet",
			mutate: func(d *filestorage.CreateMountTargetDetails) { d.SubnetId = new("") },
			status: 400,
			code:   "MissingParameter",
		},
		{
			name:   "unknown-subnet",
			mutate: func(d *filestorage.CreateMountTargetDetails) { d.SubnetId = new("ocid1.subnet.oc1.iad.nope") },
			status: 404,
			code:   "NotAuthorizedOrNotFound",
		},
		{
			name:   "unknown-compartment",
			mutate: func(d *filestorage.CreateMountTargetDetails) { d.CompartmentId = new("ocid1.compartment.oc1..nope") },
			status: 404,
			code:   "NotAuthorizedOrNotFound",
		},
		{
			name:   "bad-hostname",
			mutate: func(d *filestorage.CreateMountTargetDetails) { d.HostnameLabel = new("-bad") },
			status: 400,
			code:   "InvalidParameter",
		},
		{
			name:   "bad-ip",
			mutate: func(d *filestorage.CreateMountTargetDetails) { d.IpAddress = new("nope") },
			status: 400,
			code:   "InvalidParameter",
		},
		{
			name:   "too-many-nsgs",
			mutate: func(d *filestorage.CreateMountTargetDetails) { d.NsgIds = []string{"1", "2", "3", "4", "5", "6"} },
			status: 400,
			code:   "InvalidParameter",
		},
		{
			name:   "throughput",
			mutate: func(d *filestorage.CreateMountTargetDetails) { d.RequestedThroughput = new(int64(5)) },
			status: 400,
			code:   "InvalidParameter",
		},
		{
			name:   "ldap-without-idmap",
			mutate: func(d *filestorage.CreateMountTargetDetails) { d.LdapIdmap = &filestorage.CreateLdapIdmapDetails{} },
			status: 400,
			code:   "InvalidParameter",
		},
		{
			name:   "ldap-missing",
			mutate: func(d *filestorage.CreateMountTargetDetails) { d.IdmapType = filestorage.MountTargetIdmapTypeLdap },
			status: 400,
			code:   "MissingParameter",
		},
		{name: "ldap-unknown-connector", mutate: func(d *filestorage.CreateMountTargetDetails) {
			d.IdmapType = filestorage.MountTargetIdmapTypeLdap
			d.LdapIdmap = &filestorage.CreateLdapIdmapDetails{
				OutboundConnector1Id: new(
					"ocid1.outboundconnector.oc1.iad.nope",
				),
				UserSearchBase:  new("ou=u"),
				GroupSearchBase: new("ou=g"),
			}
		}, status: 404, code: "NotAuthorizedOrNotFound"},
		{name: "kerberos-no-secret", mutate: func(d *filestorage.CreateMountTargetDetails) {
			d.Kerberos = &filestorage.CreateKerberosDetails{
				KerberosRealm:     new("EXAMPLE.COM"),
				IsKerberosEnabled: new(true),
			}
		}, status: 400, code: "MissingParameter"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			d := filestorage.CreateMountTargetDetails{
				AvailabilityDomain: new(testAD), CompartmentId: new(testCompartment), SubnetId: new(testSubnet),
			}
			tc.mutate(&d)

			_, err := e.client.CreateMountTarget(
				t.Context(),
				filestorage.CreateMountTargetRequest{CreateMountTargetDetails: d},
			)
			assertStatus(t, err, tc.status, tc.code)
		})
	}
}

func TestMountTargetShape(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	mt := e.createMT(t, nil)

	for _, bad := range []int64{1, 0, 4, 21, 81} {
		_, err := e.client.UpgradeShapeMountTarget(t.Context(), filestorage.UpgradeShapeMountTargetRequest{
			MountTargetId: mt.Id, RequestedThroughput: new(bad),
		})
		assertStatus(t, err, http.StatusBadRequest, "InvalidParameter")
	}

	up, err := e.client.UpgradeShapeMountTarget(t.Context(), filestorage.UpgradeShapeMountTargetRequest{
		MountTargetId: mt.Id, RequestedThroughput: new(int64(40)),
	})
	require.NoError(t, err)
	assert.Equal(t, filestorage.MountTargetLifecycleStateUpdating, up.LifecycleState)
	assert.NotNil(t, up.TimeBillingCycleEnd)

	e.settle()
	assert.Equal(t, int64(40), *e.getMT(t, mt.Id).RequestedThroughput)

	_, err = e.client.CancelDowngradeShapeMountTarget(
		t.Context(),
		filestorage.CancelDowngradeShapeMountTargetRequest{MountTargetId: mt.Id},
	)
	assertStatus(t, err, http.StatusConflict, "IncorrectState")

	_, err = e.client.ScheduleDowngradeShapeMountTarget(
		t.Context(),
		filestorage.ScheduleDowngradeShapeMountTargetRequest{
			MountTargetId: mt.Id, RequestedThroughput: new(int64(80)),
		},
	)
	assertStatus(t, err, http.StatusBadRequest, "InvalidParameter")

	_, err = e.client.ScheduleDowngradeShapeMountTarget(
		t.Context(),
		filestorage.ScheduleDowngradeShapeMountTargetRequest{
			MountTargetId: mt.Id, RequestedThroughput: new(int64(20)),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, int64(40), *e.getMT(t, mt.Id).RequestedThroughput, "downgrades wait for the billing cycle")

	e.clock.Advance(31 * 24 * time.Hour)
	e.backend.Sweep()
	assert.Equal(t, int64(20), *e.getMT(t, mt.Id).RequestedThroughput)

	_, err = e.client.ScheduleDowngradeShapeMountTarget(
		t.Context(),
		filestorage.ScheduleDowngradeShapeMountTargetRequest{
			MountTargetId: mt.Id, RequestedThroughput: new(int64(1)),
		},
	)
	require.NoError(t, err)

	_, err = e.client.CancelDowngradeShapeMountTarget(
		t.Context(),
		filestorage.CancelDowngradeShapeMountTargetRequest{MountTargetId: mt.Id},
	)
	require.NoError(t, err)
}

func TestListMountTargetsAndSets(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	a := e.createMT(t, func(d *filestorage.CreateMountTargetDetails) { d.DisplayName = new("a") })
	e.createMT(t, func(d *filestorage.CreateMountTargetDetails) { d.DisplayName = new("b") })

	mts, err := e.client.ListMountTargets(t.Context(), filestorage.ListMountTargetsRequest{
		CompartmentId: new(testCompartment), AvailabilityDomain: new(testAD), ExportSetId: a.ExportSetId,
	})
	require.NoError(t, err)
	require.Len(t, mts.Items, 1)
	assert.Equal(t, "a", *mts.Items[0].DisplayName)

	sets, err := e.client.ListExportSets(
		t.Context(),
		filestorage.ListExportSetsRequest{CompartmentId: new(testCompartment), AvailabilityDomain: new(testAD)},
	)
	require.NoError(t, err)
	assert.Len(t, sets.Items, 2)

	upd, err := e.client.UpdateExportSet(t.Context(), filestorage.UpdateExportSetRequest{
		ExportSetId: a.ExportSetId,
		DisplayName: new("es"), MaxFsStatBytes: new(int64(1000)), MaxFsStatFiles: new(int64(10)),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1000), *upd.MaxFsStatBytes)

	_, err = e.client.UpdateExportSet(t.Context(), filestorage.UpdateExportSetRequest{
		ExportSetId: a.ExportSetId, MaxFsStatBytes: new(int64(-1)),
	})
	assertStatus(t, err, http.StatusBadRequest, "InvalidParameter")
}

func TestChangeMountTargetCompartment(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	mt := e.createMT(t, nil)

	_, err := e.client.ChangeMountTargetCompartment(t.Context(), filestorage.ChangeMountTargetCompartmentRequest{
		MountTargetId: mt.Id, CompartmentId: new(otherCompart),
	})
	require.NoError(t, err)

	set, err := e.client.GetExportSet(t.Context(), filestorage.GetExportSetRequest{ExportSetId: mt.ExportSetId})
	require.NoError(t, err)
	assert.Equal(t, otherCompart, *set.CompartmentId, "the export set follows its mount target")
}

func TestMountTargetLock(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	mt := e.createMT(t, nil)

	_, err := e.client.AddMountTargetLock(t.Context(), filestorage.AddMountTargetLockRequest{
		MountTargetId:             mt.Id,
		AddMountTargetLockDetails: filestorage.ResourceLock{Type: filestorage.ResourceLockTypeDelete},
	})
	require.NoError(t, err)

	_, err = e.client.DeleteMountTarget(t.Context(), filestorage.DeleteMountTargetRequest{MountTargetId: mt.Id})
	assertStatus(t, err, http.StatusConflict, "Conflict")

	_, err = e.client.DeleteMountTarget(
		t.Context(),
		filestorage.DeleteMountTargetRequest{MountTargetId: mt.Id, IsLockOverride: new(true)},
	)
	require.NoError(t, err)
}
