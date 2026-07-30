package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestDescribeNetworkInterfaces(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantENIs bool
	}{
		{
			name:     "describe_after_run_instance",
			wantENIs: true,
		},
		{
			name:     "describe_empty",
			wantENIs: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if tt.wantENIs {
				_, err := b.RunInstances("ami-123", "t2.micro", "", 1)
				require.NoError(t, err)
			}

			enis := b.DescribeNetworkInterfaces(nil)

			if tt.wantENIs {
				assert.NotEmpty(t, enis)
				assert.NotEmpty(t, enis[0].PrivateIP)
			} else {
				assert.Empty(t, enis)
			}
		})
	}
}

func TestNetworkInterfaceCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*ec2.InMemoryBackend) string
		name    string
		op      string
		wantErr bool
	}{
		{
			name: "create_eni",
			op:   "create",
		},
		{
			name:    "create_eni_bad_subnet",
			op:      "create_bad_subnet",
			wantErr: true,
		},
		{
			name: "delete_eni",
			op:   "delete",
		},
		{
			name:    "delete_eni_not_found",
			op:      "delete_not_found",
			wantErr: true,
		},
		{
			name: "attach_detach_eni",
			op:   "attach_detach",
		},
		{
			name:    "detach_not_found",
			op:      "detach_not_found",
			wantErr: true,
		},
		{
			name:    "delete_attached_eni",
			op:      "delete_attached",
			wantErr: true,
		},
		{
			name: "assign_private_ips_by_count",
			op:   "assign_count",
		},
		{
			name: "assign_private_ips_explicit",
			op:   "assign_explicit",
		},
		{
			name: "unassign_private_ips",
			op:   "unassign",
		},
		{
			name: "modify_description",
			op:   "modify_description",
		},
		{
			name: "modify_source_dest_check",
			op:   "modify_source_dest",
		},
		{
			name:    "modify_not_found",
			op:      "modify_not_found",
			wantErr: true,
		},
		{
			name:    "attach_already_attached",
			op:      "attach_already_attached",
			wantErr: true,
		},
		{
			name:    "modify_unknown_attr",
			op:      "modify_unknown_attr",
			wantErr: true,
		},
		{
			name: "modify_clear_description",
			op:   "modify_clear_description",
		},
		{
			name: "attached_eni_defaults_delete_on_termination_false",
			op:   "attach_delete_on_termination_default",
		},
		{
			name: "set_delete_on_termination_true",
			op:   "set_delete_on_termination_true",
		},
		{
			name:    "set_delete_on_termination_unknown_attachment",
			op:      "set_delete_on_termination_not_found",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			var err error

			switch tt.op {
			case "create":
				eni, cerr := b.CreateNetworkInterface("subnet-default", "test-eni")
				require.NoError(t, cerr)
				assert.NotEmpty(t, eni.ID)
				assert.Equal(t, "available", eni.Status)
				assert.Equal(t, "test-eni", eni.Description)
				assert.True(t, eni.SourceDestCheck)

			case "create_bad_subnet":
				_, err = b.CreateNetworkInterface("subnet-nonexistent", "")
				require.Error(t, err)

			case "delete":
				eni, cerr := b.CreateNetworkInterface("subnet-default", "")
				require.NoError(t, cerr)
				err = b.DeleteNetworkInterface(eni.ID)
				require.NoError(t, err)
				enis := b.DescribeNetworkInterfaces([]string{eni.ID})
				assert.Empty(t, enis)

			case "delete_not_found":
				err = b.DeleteNetworkInterface("eni-nonexistent")
				require.Error(t, err)

			case "attach_detach":
				instances, cerr := b.RunInstances("ami-123", "t2.micro", "", 1)
				require.NoError(t, cerr)
				eni, cerr := b.CreateNetworkInterface("subnet-default", "")
				require.NoError(t, cerr)

				attachID, aerr := b.AttachNetworkInterface(eni.ID, instances[0].ID, 1)
				require.NoError(t, aerr)
				assert.NotEmpty(t, attachID)

				enis := b.DescribeNetworkInterfaces([]string{eni.ID})
				require.Len(t, enis, 1)
				assert.Equal(t, "in-use", enis[0].Status)
				assert.Equal(t, attachID, enis[0].AttachmentID)

				derr := b.DetachNetworkInterface(attachID, false)
				require.NoError(t, derr)

				enis = b.DescribeNetworkInterfaces([]string{eni.ID})
				require.Len(t, enis, 1)
				assert.Equal(t, "available", enis[0].Status)
				assert.Empty(t, enis[0].AttachmentID)

			case "detach_not_found":
				err = b.DetachNetworkInterface("eni-attach-nonexistent", false)
				require.Error(t, err)

			case "delete_attached":
				instances, cerr := b.RunInstances("ami-123", "t2.micro", "", 1)
				require.NoError(t, cerr)
				eni, cerr := b.CreateNetworkInterface("subnet-default", "")
				require.NoError(t, cerr)
				_, aerr := b.AttachNetworkInterface(eni.ID, instances[0].ID, 1)
				require.NoError(t, aerr)
				err = b.DeleteNetworkInterface(eni.ID)
				require.Error(t, err)

			case "assign_count":
				eni, cerr := b.CreateNetworkInterface("subnet-default", "")
				require.NoError(t, cerr)
				aerr := b.AssignPrivateIPAddresses(eni.ID, 2, nil)
				require.NoError(t, aerr)
				enis := b.DescribeNetworkInterfaces([]string{eni.ID})
				require.Len(t, enis, 1)
				assert.Len(t, enis[0].SecondaryPrivateIPs, 2)

			case "assign_explicit":
				eni, cerr := b.CreateNetworkInterface("subnet-default", "")
				require.NoError(t, cerr)
				aerr := b.AssignPrivateIPAddresses(eni.ID, 0, []string{"10.0.1.100", "10.0.1.101"})
				require.NoError(t, aerr)
				enis := b.DescribeNetworkInterfaces([]string{eni.ID})
				require.Len(t, enis, 1)
				assert.Contains(t, enis[0].SecondaryPrivateIPs, "10.0.1.100")
				assert.Contains(t, enis[0].SecondaryPrivateIPs, "10.0.1.101")

			case "unassign":
				eni, cerr := b.CreateNetworkInterface("subnet-default", "")
				require.NoError(t, cerr)
				aerr := b.AssignPrivateIPAddresses(eni.ID, 0, []string{"10.0.1.100"})
				require.NoError(t, aerr)
				uerr := b.UnassignPrivateIPAddresses(eni.ID, []string{"10.0.1.100"})
				require.NoError(t, uerr)
				enis := b.DescribeNetworkInterfaces([]string{eni.ID})
				require.Len(t, enis, 1)
				assert.Empty(t, enis[0].SecondaryPrivateIPs)

			case "modify_description":
				eni, cerr := b.CreateNetworkInterface("subnet-default", "original")
				require.NoError(t, cerr)
				merr := b.ModifyNetworkInterfaceAttribute(eni.ID, "description", "updated")
				require.NoError(t, merr)
				enis := b.DescribeNetworkInterfaces([]string{eni.ID})
				require.Len(t, enis, 1)
				assert.Equal(t, "updated", enis[0].Description)

			case "modify_source_dest":
				eni, cerr := b.CreateNetworkInterface("subnet-default", "")
				require.NoError(t, cerr)
				merr := b.ModifyNetworkInterfaceAttribute(eni.ID, "sourceDestCheck", "false")
				require.NoError(t, merr)
				enis := b.DescribeNetworkInterfaces([]string{eni.ID})
				require.Len(t, enis, 1)
				assert.False(t, enis[0].SourceDestCheck)

			case "modify_not_found":
				err = b.ModifyNetworkInterfaceAttribute("eni-nonexistent", "description", "x")
				require.Error(t, err)

			case "attach_already_attached":
				instances, cerr := b.RunInstances("ami-123", "t2.micro", "", 1)
				require.NoError(t, cerr)
				eni, cerr := b.CreateNetworkInterface("subnet-default", "")
				require.NoError(t, cerr)
				_, aerr := b.AttachNetworkInterface(eni.ID, instances[0].ID, 1)
				require.NoError(t, aerr)
				_, err = b.AttachNetworkInterface(eni.ID, instances[0].ID, 2)
				require.Error(t, err)

			case "modify_unknown_attr":
				eni, cerr := b.CreateNetworkInterface("subnet-default", "")
				require.NoError(t, cerr)
				err = b.ModifyNetworkInterfaceAttribute(eni.ID, "unknownAttr", "value")
				require.Error(t, err)

			case "modify_clear_description":
				eni, cerr := b.CreateNetworkInterface("subnet-default", "original")
				require.NoError(t, cerr)
				merr := b.ModifyNetworkInterfaceAttribute(eni.ID, "description", "")
				require.NoError(t, merr)
				enis := b.DescribeNetworkInterfaces([]string{eni.ID})
				require.Len(t, enis, 1)
				assert.Empty(t, enis[0].Description)

			case "attach_delete_on_termination_default":
				// Real AWS default: an interface created separately via
				// CreateNetworkInterface and attached later has
				// DeleteOnTermination=false, unlike the primary interface
				// auto-created at instance launch (DeleteOnTermination=true).
				instances, cerr := b.RunInstances("ami-123", "t2.micro", "", 1)
				require.NoError(t, cerr)
				eni, cerr := b.CreateNetworkInterface("subnet-default", "")
				require.NoError(t, cerr)
				_, aerr := b.AttachNetworkInterface(eni.ID, instances[0].ID, 1)
				require.NoError(t, aerr)
				enis := b.DescribeNetworkInterfaces([]string{eni.ID})
				require.Len(t, enis, 1)
				assert.False(t, enis[0].DeleteOnTermination)

				launchENIs := b.DescribeNetworkInterfaces(nil)
				foundLaunchENI := false

				for _, e := range launchENIs {
					if e.InstanceID == instances[0].ID && e.DeviceIndex == 0 {
						assert.True(t, e.DeleteOnTermination,
							"launch-time primary ENI must default DeleteOnTermination=true")

						foundLaunchENI = true
					}
				}

				assert.True(t, foundLaunchENI, "RunInstances must create a primary ENI")

			case "set_delete_on_termination_true":
				instances, cerr := b.RunInstances("ami-123", "t2.micro", "", 1)
				require.NoError(t, cerr)
				eni, cerr := b.CreateNetworkInterface("subnet-default", "")
				require.NoError(t, cerr)
				attachID, aerr := b.AttachNetworkInterface(eni.ID, instances[0].ID, 1)
				require.NoError(t, aerr)

				serr := b.SetNetworkInterfaceDeleteOnTermination(attachID, true)
				require.NoError(t, serr)

				enis := b.DescribeNetworkInterfaces([]string{eni.ID})
				require.Len(t, enis, 1)
				assert.True(t, enis[0].DeleteOnTermination)

			case "set_delete_on_termination_not_found":
				err = b.SetNetworkInterfaceDeleteOnTermination("eni-attach-nonexistent", true)
				require.Error(t, err)
			}

			if tt.wantErr {
				require.Error(t, err)
			}
		})
	}
}
