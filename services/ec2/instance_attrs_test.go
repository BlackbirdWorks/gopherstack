package ec2_test

import (
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func newStoppedInstance(t *testing.T, b *ec2.InMemoryBackend) string {
	t.Helper()

	instances, err := b.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)
	instanceID := instances[0].ID

	_, err = b.StopInstances([]string{instanceID})
	require.NoError(t, err)
	b.TickLifecycleForTest()

	return instanceID
}

func TestBackend_ModifyAvailabilityZoneGroup(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	ok, err := b.ModifyAvailabilityZoneGroup("us-east-1-wl1-bos-wlz-1", "opted-in")
	require.NoError(t, err)
	assert.True(t, ok)

	_, err = b.ModifyAvailabilityZoneGroup("", "opted-in")
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)

	_, err = b.ModifyAvailabilityZoneGroup("us-east-1-wl1-bos-wlz-1", "bogus")
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)
}

func TestBackend_ModifyHosts_SuccessAndUnsuccessful(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	hosts, err := b.AllocateHosts("us-east-1a", "c5.metal", 1)
	require.NoError(t, err)
	hostID := hosts[0].HostID

	successful, unsuccessful, err := b.ModifyHosts(
		[]string{hostID, "h-doesnotexist"}, "on", "", "", "", "",
	)
	require.NoError(t, err)
	assert.Equal(t, []string{hostID}, successful)
	require.Len(t, unsuccessful, 1)
	assert.Equal(t, "h-doesnotexist", unsuccessful[0].HostID)
	assert.Equal(t, "InvalidHostID.NotFound", unsuccessful[0].Code)

	got := b.DescribeHosts([]string{hostID})
	require.Len(t, got, 1)
	assert.Equal(t, "on", got[0].AutoPlacement)
}

func TestBackend_ModifyHosts_RequiresHostIDs(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, _, err := b.ModifyHosts(nil, "on", "", "", "", "")
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)
}

func TestBackend_ModifyInstanceCapacityReservationAttributes(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	instances, err := b.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)
	instanceID := instances[0].ID

	b.AddCapacityReservationInternal(&ec2.CapacityReservation{CapacityReservationID: "cr-1234567890abcdef0"})

	inst, err := b.ModifyInstanceCapacityReservationAttributes(instanceID, "open", "cr-1234567890abcdef0", "")
	require.NoError(t, err)
	assert.Equal(t, "open", inst.CapacityReservationSpec.CapacityReservationPreference)
	assert.Equal(t, "cr-1234567890abcdef0", inst.CapacityReservationSpec.CapacityReservationID)

	_, err = b.ModifyInstanceCapacityReservationAttributes(instanceID, "none", "cr-doesnotexist", "")
	require.ErrorIs(t, err, ec2.ErrCapacityReservationNotFound)
}

func TestBackend_ModifyInstanceCPUOptions_RequiresStopped(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	instances, err := b.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)
	instanceID := instances[0].ID

	coreCount := int32(2)

	_, err = b.ModifyInstanceCPUOptions(instanceID, &coreCount, nil, "")
	require.ErrorIs(t, err, ec2.ErrInvalidInstanceState)

	stoppedID := newStoppedInstance(t, b)
	threads := int32(1)

	inst, err := b.ModifyInstanceCPUOptions(stoppedID, &coreCount, &threads, "enabled")
	require.NoError(t, err)
	assert.Equal(t, int32(2), inst.CPUOptions.CoreCount)
	assert.Equal(t, int32(1), inst.CPUOptions.ThreadsPerCore)
	assert.Equal(t, "enabled", inst.CPUOptions.NestedVirtualization)
}

func TestBackend_ModifyInstanceEventStartTime(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	instances, err := b.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)
	instanceID := instances[0].ID

	notBefore := time.Now().Add(24 * time.Hour).UTC().Truncate(time.Second)

	event, err := b.ModifyInstanceEventStartTime(instanceID, "instance-event-abc123", notBefore)
	require.NoError(t, err)
	assert.Equal(t, "instance-event-abc123", event.InstanceEventID)
	assert.True(t, event.NotBefore.Equal(notBefore))

	_, err = b.ModifyInstanceEventStartTime("i-doesnotexist", "instance-event-abc123", notBefore)
	require.ErrorIs(t, err, ec2.ErrInstanceNotFound)
}

func TestBackend_ModifyInstanceMaintenanceOptions(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	instances, err := b.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)
	instanceID := instances[0].ID

	inst, err := b.ModifyInstanceMaintenanceOptions(instanceID, "disabled", "disabled")
	require.NoError(t, err)
	assert.Equal(t, "disabled", inst.MaintenanceOptions.AutoRecovery)
	assert.Equal(t, "disabled", inst.MaintenanceOptions.RebootMigration)
}

func TestBackend_ModifyInstanceNetworkPerformanceOptions(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	instances, err := b.RunInstances("ami-test", "efa.48xlarge", "", 1)
	require.NoError(t, err)
	instanceID := instances[0].ID

	inst, err := b.ModifyInstanceNetworkPerformanceOptions(instanceID, "ebs-1")
	require.NoError(t, err)
	assert.Equal(t, "ebs-1", inst.NetworkPerformanceOptions.BandwidthWeighting)

	_, err = b.ModifyInstanceNetworkPerformanceOptions(instanceID, "")
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)
}

func TestBackend_ModifyInstancePlacement_RequiresStopped(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	instances, err := b.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)
	instanceID := instances[0].ID

	_, err = b.ModifyInstancePlacement(ec2.ModifyInstancePlacementInput{InstanceID: instanceID, Tenancy: "dedicated"})
	require.ErrorIs(t, err, ec2.ErrInvalidInstanceState)

	stoppedID := newStoppedInstance(t, b)

	ok, err := b.ModifyInstancePlacement(ec2.ModifyInstancePlacementInput{
		InstanceID: stoppedID, Tenancy: "dedicated", GroupName: "my-pg",
	})
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestBackend_ModifyInstancePlacement_UnknownHostFails(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	stoppedID := newStoppedInstance(t, b)

	_, err := b.ModifyInstancePlacement(ec2.ModifyInstancePlacementInput{
		InstanceID: stoppedID, HostID: "h-doesnotexist",
	})
	require.ErrorIs(t, err, ec2.ErrHostNotFound)
}

func TestBackend_ModifyPrivateDNSNameOptions(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	instances, err := b.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)
	instanceID := instances[0].ID

	enableA := true

	ok, err := b.ModifyPrivateDNSNameOptions(instanceID, "resource-name", &enableA, nil)
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestBackend_ModifyPublicIPDNSNameOptions(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.ModifyPublicIPDNSNameOptions("eni-doesnotexist", "public-ipv4-dns-name")
	require.ErrorIs(t, err, ec2.ErrNetworkInterfaceNotFound)
}

func TestBackend_InstanceEventWindow_AssociateDisassociate(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	ew, err := b.CreateInstanceEventWindow("test-window", "0 4 * * 6,7")
	require.NoError(t, err)

	instances, err := b.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)
	instanceID := instances[0].ID

	updated, err := b.AssociateInstanceEventWindow(ew.InstanceEventWindowID, []string{instanceID}, nil)
	require.NoError(t, err)
	assert.Contains(t, updated.InstanceIDs, instanceID)

	updated, err = b.DisassociateInstanceEventWindow(ew.InstanceEventWindowID, []string{instanceID}, nil)
	require.NoError(t, err)
	assert.NotContains(t, updated.InstanceIDs, instanceID)

	_, err = b.AssociateInstanceEventWindow("instance-event-window-doesnotexist", []string{instanceID}, nil)
	require.ErrorIs(t, err, ec2.ErrInstanceEventWindowNotFound)
}

func TestBackend_GetInstanceTpmEkPub_Deterministic(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	instances, err := b.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)
	instanceID := instances[0].ID

	v1, err := b.GetInstanceTpmEkPub(instanceID, "der", "rsa-2048")
	require.NoError(t, err)
	assert.NotEmpty(t, v1)

	v2, err := b.GetInstanceTpmEkPub(instanceID, "der", "rsa-2048")
	require.NoError(t, err)
	assert.Equal(t, v1, v2)

	v3, err := b.GetInstanceTpmEkPub(instanceID, "tpmt", "rsa-2048")
	require.NoError(t, err)
	assert.NotEqual(t, v1, v3)

	_, err = b.GetInstanceTpmEkPub("i-doesnotexist", "der", "rsa-2048")
	require.ErrorIs(t, err, ec2.ErrInstanceNotFound)
}

func TestBackend_GetInstanceUefiData_Deterministic(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	instances, err := b.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)
	instanceID := instances[0].ID

	v1, err := b.GetInstanceUefiData(instanceID)
	require.NoError(t, err)
	assert.NotEmpty(t, v1)

	v2, err := b.GetInstanceUefiData(instanceID)
	require.NoError(t, err)
	assert.Equal(t, v1, v2)

	_, err = b.GetInstanceUefiData("i-doesnotexist")
	require.ErrorIs(t, err, ec2.ErrInstanceNotFound)
}

func TestSetInstanceAttribute_UserData(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	instances, err := b.RunInstances("ami-123", "t3.micro", "", 1)
	require.NoError(t, err)

	id := instances[0].ID
	// userData can be set on running instances (via RunInstances initial setup path).
	err = b.SetInstanceAttribute(id, "userData", "dXNlci1kYXRh")
	require.NoError(t, err)

	insts := b.DescribeInstances([]string{id}, "")
	require.Len(t, insts, 1)
	assert.Equal(t, "dXNlci1kYXRh", insts[0].UserData)
}

func TestSetInstanceAttribute_InstanceType_RequiresStopped(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	instances, err := b.RunInstances("ami-123", "t3.micro", "", 1)
	require.NoError(t, err)
	b.TickLifecycleForTest() // pending → running

	id := instances[0].ID

	// Should fail on running instance.
	err = b.SetInstanceAttribute(id, "instanceType", "t3.large")
	require.Error(t, err)
	require.ErrorIs(t, err, ec2.ErrInvalidInstanceState)

	// Stop then modify — should succeed.
	_, stopErr := b.StopInstances([]string{id})
	require.NoError(t, stopErr)
	b.TickLifecycleForTest() // stopping → stopped

	err = b.SetInstanceAttribute(id, "instanceType", "t3.large")
	require.NoError(t, err)

	insts := b.DescribeInstances([]string{id}, "")
	require.Len(t, insts, 1)
	assert.Equal(t, "t3.large", insts[0].InstanceType)
}

func TestSetInstanceAttribute_NotFound(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	err := b.SetInstanceAttribute("i-nonexistent", "userData", "abc")
	require.Error(t, err)
	assert.ErrorIs(t, err, ec2.ErrInstanceNotFound)
}

// ---- Gap 15: Spot price history ----

func TestEnaSupport_DefaultTrue(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	instances, err := b.RunInstances("ami-123", "t3.micro", "", 1)
	require.NoError(t, err)
	assert.True(t, instances[0].EnaSupport, "EnaSupport should default to true for new instances")
}

func TestDescribeInstanceAttribute_EnaSupport(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	instances, err := h.Backend.RunInstances("ami-123", "t3.micro", "", 1)
	require.NoError(t, err)

	vals := url.Values{
		"Action":     {"DescribeInstanceAttribute"},
		"Version":    {"2016-11-15"},
		"InstanceId": {instances[0].ID},
		"Attribute":  {"enaSupport"},
	}

	resp, err := dispatchHandler(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, "DescribeInstanceAttributeResponse")
	assert.Contains(t, resp, "true")
}

// ---- Gap 6: SG rule references ----

// TestModifyInstanceAttribute_Validation covers the handler-level attribute
// selection and stopped-state guard rules for ModifyInstanceAttribute.
func TestModifyInstanceAttribute_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		params  url.Values
		wantErr error
		name    string
	}{
		{
			name: "empty_attribute_is_rejected",
			params: url.Values{
				"Action":     {"ModifyInstanceAttribute"},
				"Version":    {"2016-11-15"},
				"InstanceId": {"__PLACEHOLDER__"},
			},
			wantErr: ec2.ErrMissingParameter,
		},
		{
			name: "unknown_attribute_is_rejected",
			params: url.Values{
				"Action":     {"ModifyInstanceAttribute"},
				"Version":    {"2016-11-15"},
				"InstanceId": {"__PLACEHOLDER__"},
				"Attribute":  {"bogusAttribute"},
				"Value":      {"whatever"},
			},
			wantErr: ec2.ErrInvalidParameter,
		},
		{
			name: "generic_form_respects_stopped_guard_on_running_instance",
			params: url.Values{
				"Action":     {"ModifyInstanceAttribute"},
				"Version":    {"2016-11-15"},
				"InstanceId": {"__PLACEHOLDER__"},
				"Attribute":  {"instanceType"},
				"Value":      {"t3.large"},
			},
			wantErr: ec2.ErrInvalidInstanceState,
		},
		{
			name: "generic_form_non_guarded_attr_succeeds_on_running_instance",
			params: url.Values{
				"Action":     {"ModifyInstanceAttribute"},
				"Version":    {"2016-11-15"},
				"InstanceId": {"__PLACEHOLDER__"},
				"Attribute":  {"sourceDestCheck"},
				"Value":      {"false"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
			h := newTestHandlerWithBackend(b)

			instances, err := b.RunInstances("ami-123", "t3.micro", "", 1)
			require.NoError(t, err)
			b.TickLifecycleForTest() // pending -> running

			id := instances[0].ID
			params := cloneValues(tt.params)
			params.Set("InstanceId", id)

			_, dispErr := dispatchHandler(h, params)

			if tt.wantErr != nil {
				require.Error(t, dispErr)
				assert.ErrorIs(t, dispErr, tt.wantErr)

				return
			}

			require.NoError(t, dispErr)
		})
	}
}

// TestRunInstances_UserDataValidation covers base64 and 16 KiB validation of the
// UserData parameter on RunInstances.

// cloneValues returns a shallow copy of the url.Values so parallel subtests can
// mutate InstanceId without racing on the shared table entry.
func cloneValues(in url.Values) url.Values {
	out := make(url.Values, len(in))
	for k, v := range in {
		cp := make([]string, len(v))
		copy(cp, v)
		out[k] = cp
	}

	return out
}
