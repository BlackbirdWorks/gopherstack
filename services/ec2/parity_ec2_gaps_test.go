package ec2_test

import (
	"encoding/base64"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ec2 "github.com/blackbirdworks/gopherstack/services/ec2"
)

// base64Encode is a test helper mirroring how the AWS SDK base64-encodes user
// data before it reaches the RunInstances API.
func base64Encode(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}

// vpcResources bundles the identifiers of the resources created by
// buildVPCWithResources for use in assertions.
type vpcResources struct {
	vpcID       string
	subnetID    string
	eniID       string
	instanceIDs []string
}

// buildVPCWithResources creates a VPC with a subnet, `numInstances` running
// instances (each with an auto-attached ENI), one standalone ENI, and one NAT
// gateway, returning the created resource identifiers for later assertions.
func buildVPCWithResources(
	t *testing.T,
	b *ec2.InMemoryBackend,
	cidr, az string,
	numInstances int,
) vpcResources {
	t.Helper()

	vpc, err := b.CreateVpc(cidr)
	require.NoError(t, err)

	subnet, err := b.CreateSubnet(vpc.ID, cidr, az)
	require.NoError(t, err)

	instances, err := b.RunInstances("ami-123", "t3.micro", subnet.ID, numInstances)
	require.NoError(t, err)

	ids := make([]string, 0, len(instances))
	for _, inst := range instances {
		ids = append(ids, inst.ID)
	}

	eni, err := b.CreateNetworkInterface(subnet.ID, "standalone-eni")
	require.NoError(t, err)

	addr, err := b.AllocateAddress()
	require.NoError(t, err)

	_, err = b.CreateNatGateway(subnet.ID, addr.AllocationID)
	require.NoError(t, err)

	return vpcResources{
		vpcID:       vpc.ID,
		subnetID:    subnet.ID,
		eniID:       eni.ID,
		instanceIDs: ids,
	}
}

// TestPersistence_SecondaryIndexRebuild is the guard test for the Restore
// secondary-index rebuild. It creates a VPC with instances and ENIs, snapshots,
// restores into a fresh backend, and asserts that (a) DeleteVpc still cascades
// (proving instanceIDsByVPC / subnetIDsByVPC / natGatewayIDsByVPC / eniIDsByVPC
// were rebuilt) and (b) ENI-by-instance termination cleanup still works
// (proving eniIDsByInstance was rebuilt).
func TestPersistence_SecondaryIndexRebuild(t *testing.T) {
	t.Parallel()

	tests := []struct {
		verify func(t *testing.T, fresh *ec2.InMemoryBackend, res vpcResources)
		name   string
	}{
		{
			name: "delete_vpc_cascade_survives_restore",
			verify: func(t *testing.T, fresh *ec2.InMemoryBackend, res vpcResources) {
				t.Helper()

				require.NoError(t, fresh.DeleteVpc(res.vpcID))

				// VPC gone.
				assert.Empty(t, fresh.DescribeVpcs([]string{res.vpcID}))
				// Subnet cascaded (index-driven).
				assert.Empty(t, fresh.DescribeSubnets([]string{res.subnetID}))
				// NAT gateways cascaded (natGatewayIDsByVPC-driven).
				assert.Empty(t, fresh.DescribeNatGateways(nil))
				// Standalone ENI cascaded (eniIDsByVPC-driven).
				assert.Empty(t, fresh.DescribeNetworkInterfaces([]string{res.eniID}))
				// Instances terminated (instanceIDsByVPC-driven).
				insts := fresh.DescribeInstances(res.instanceIDs, "")
				require.Len(t, insts, len(res.instanceIDs))
				for _, inst := range insts {
					assert.Equal(t, ec2.StateTerminated, inst.State)
				}
			},
		},
		{
			name: "eni_by_instance_cleanup_survives_restore",
			verify: func(t *testing.T, fresh *ec2.InMemoryBackend, res vpcResources) {
				t.Helper()

				// Each instance has exactly one auto-attached ENI; before
				// termination they must exist.
				before := fresh.DescribeNetworkInterfaces(nil)
				require.NotEmpty(t, before)

				// Terminating uses eniIDsByInstance to delete attached ENIs. If
				// the index was not rebuilt on restore this is a no-op and the
				// ENIs leak.
				_, err := fresh.TerminateInstances(res.instanceIDs)
				require.NoError(t, err)

				remaining := fresh.DescribeNetworkInterfaces(nil)
				for _, eni := range remaining {
					for _, instID := range res.instanceIDs {
						assert.NotEqual(t, instID, eni.InstanceID,
							"instance %s still has attached ENI %s after termination", instID, eni.ID)
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			res := buildVPCWithResources(t, original, "10.20.0.0/16", "us-east-1a", 2)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, res)
		})
	}
}

// TestDeleteVpc_PerVPCIndexCascade exercises the natGatewayIDsByVPC and
// eniIDsByVPC index maintenance across DeleteNatGateway, DeleteSubnet, and
// DeleteVpc so the index never drifts from the underlying maps.
func TestDeleteVpc_PerVPCIndexCascade(t *testing.T) {
	t.Parallel()

	t.Run("explicit_nat_delete_then_delete_vpc", func(t *testing.T) {
		t.Parallel()

		b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
		res := buildVPCWithResources(t, b, "10.30.0.0/16", "us-east-1a", 1)

		nats := b.DescribeNatGateways(nil)
		require.Len(t, nats, 1)

		// Explicitly delete the NAT — deindex must remove it from the VPC index.
		require.NoError(t, b.DeleteNatGateway(nats[0].ID))
		assert.Empty(t, b.DescribeNatGateways(nil))

		// DeleteVpc must not error or attempt to re-delete the missing NAT.
		require.NoError(t, b.DeleteVpc(res.vpcID))
		assert.Empty(t, b.DescribeVpcs([]string{res.vpcID}))
	})

	t.Run("delete_subnet_scopes_nat_and_eni_removal", func(t *testing.T) {
		t.Parallel()

		b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

		vpc, err := b.CreateVpc("10.40.0.0/16")
		require.NoError(t, err)

		subnetA, err := b.CreateSubnet(vpc.ID, "10.40.1.0/24", "us-east-1a")
		require.NoError(t, err)
		subnetB, err := b.CreateSubnet(vpc.ID, "10.40.2.0/24", "us-east-1b")
		require.NoError(t, err)

		eniA, err := b.CreateNetworkInterface(subnetA.ID, "eni-a")
		require.NoError(t, err)
		eniB, err := b.CreateNetworkInterface(subnetB.ID, "eni-b")
		require.NoError(t, err)

		// Delete subnet A — only eni-a removed; eni-b (same VPC) survives.
		require.NoError(t, b.DeleteSubnet(subnetA.ID))
		assert.Empty(t, b.DescribeNetworkInterfaces([]string{eniA.ID}))
		require.Len(t, b.DescribeNetworkInterfaces([]string{eniB.ID}), 1)

		// DeleteVpc removes the remaining subnet-B ENI via the VPC index.
		require.NoError(t, b.DeleteVpc(vpc.ID))
		assert.Empty(t, b.DescribeNetworkInterfaces([]string{eniB.ID}))
	})
}

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
func TestRunInstances_UserDataValidation(t *testing.T) {
	t.Parallel()

	// 16 KiB of 'a' encodes to valid base64; decoded length == 16384 (allowed).
	okDecoded := make([]byte, 16384)
	for i := range okDecoded {
		okDecoded[i] = 'a'
	}
	tooBig := make([]byte, 16385)
	for i := range tooBig {
		tooBig[i] = 'a'
	}

	tests := []struct {
		wantErr  error
		name     string
		userData string
	}{
		{
			name:     "valid_base64_within_limit",
			userData: base64Encode([]byte("#!/bin/bash\necho hello")),
		},
		{
			name:     "empty_user_data_allowed",
			userData: "",
		},
		{
			name:     "exactly_16kib_allowed",
			userData: base64Encode(okDecoded),
		},
		{
			name:     "malformed_base64_rejected",
			userData: "!!!not-base64!!!",
			wantErr:  ec2.ErrInvalidUserData,
		},
		{
			name:     "over_16kib_rejected",
			userData: base64Encode(tooBig),
			wantErr:  ec2.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			params := url.Values{
				"Action":       {"RunInstances"},
				"Version":      {"2016-11-15"},
				"ImageId":      {"ami-12345678"},
				"InstanceType": {"t3.micro"},
				"MinCount":     {"1"},
				"MaxCount":     {"1"},
			}
			if tt.userData != "" {
				params.Set("UserData", tt.userData)
			}

			resp, err := dispatchHandler(h, params)

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Contains(t, resp, "RunInstancesResponse")
		})
	}
}

// TestCreateVolume_GP3Coupling covers the AWS gp3 iops/throughput coupling
// validation on CreateVolume.
func TestCreateVolume_GP3Coupling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		body    string
		wantErr bool
	}{
		{
			name: "gp3_defaults_ok",
			body: "Action=CreateVolume&Version=2016-11-15&AvailabilityZone=us-east-1a&Size=20&VolumeType=gp3",
		},
		{
			name: "gp3_custom_within_bounds_ok",
			body: "Action=CreateVolume&Version=2016-11-15&AvailabilityZone=us-east-1a" +
				"&Size=100&VolumeType=gp3&Iops=6000&Throughput=500",
		},
		{
			name: "gp3_iops_above_max_rejected",
			body: "Action=CreateVolume&Version=2016-11-15&AvailabilityZone=us-east-1a" +
				"&Size=100&VolumeType=gp3&Iops=20000&Throughput=125",
			wantErr: true,
		},
		{
			name: "gp3_iops_below_min_rejected",
			body: "Action=CreateVolume&Version=2016-11-15&AvailabilityZone=us-east-1a" +
				"&Size=100&VolumeType=gp3&Iops=1000&Throughput=125",
			wantErr: true,
		},
		{
			name: "gp3_throughput_above_max_rejected",
			body: "Action=CreateVolume&Version=2016-11-15&AvailabilityZone=us-east-1a" +
				"&Size=100&VolumeType=gp3&Iops=16000&Throughput=2000",
			wantErr: true,
		},
		{
			name: "gp3_throughput_to_iops_ratio_violation_rejected",
			// throughput 1000 * 4 = 4000 > iops 3000 -> violates 0.25 MiB/s per IOPS.
			body: "Action=CreateVolume&Version=2016-11-15&AvailabilityZone=us-east-1a" +
				"&Size=100&VolumeType=gp3&Iops=3000&Throughput=1000",
			wantErr: true,
		},
		{
			name: "gp3_iops_to_size_ratio_violation_rejected",
			// size 10 GiB, iops 8000 > 10*500=5000 and above baseline -> violation.
			body: "Action=CreateVolume&Version=2016-11-15&AvailabilityZone=us-east-1a" +
				"&Size=10&VolumeType=gp3&Iops=8000&Throughput=125",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vals, err := url.ParseQuery(tt.body)
			require.NoError(t, err)

			resp, dispErr := dispatchHandler(h, vals)

			if tt.wantErr {
				require.Error(t, dispErr)
				assert.ErrorIs(t, dispErr, ec2.ErrInvalidParameter)

				return
			}

			require.NoError(t, dispErr)
			assert.Contains(t, resp, "CreateVolumeResponse")
		})
	}
}

// TestPagination_ForgedTokenRejected asserts that a forged/tampered NextToken is
// rejected with InvalidPaginationToken across the three opaque-token describe
// operations, rather than silently re-paging from offset 0.
func TestPagination_ForgedTokenRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
	}{
		{name: "describe_instances", action: "DescribeInstances"},
		{name: "describe_images", action: "DescribeImages"},
		{name: "describe_instance_types", action: "DescribeInstanceTypes"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			params := url.Values{
				"Action":     {tt.action},
				"Version":    {"2016-11-15"},
				"MaxResults": {"5"},
				"NextToken":  {"this-is-a-forged-token"},
			}

			_, err := dispatchHandler(h, params)
			require.Error(t, err)
			assert.ErrorIs(t, err, ec2.ErrInvalidPaginationToken)
		})
	}
}

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
