package ec2_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/ec2"
)

// newTestBackend creates a fresh backend for testing.
func newTestBackend() *ec2.InMemoryBackend {
	return ec2.NewInMemoryBackend("000000000000", "us-east-1")
}

// postFormExt sends a form-encoded POST to the EC2 handler (copy of helper from handler_test.go).
func postFormExt(t *testing.T, h *ec2.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// newExtHandler creates a new EC2 handler with a fresh backend.
func newExtHandler() *ec2.Handler {
	bk := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(bk)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	return h
}

// ---- Backend tests ----

func TestStartStopInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*ec2.InMemoryBackend) string
		name       string
		op         string
		instanceID string
		wantState  string
		wantErr    bool
	}{
		{
			name: "stop_running_instance",
			setup: func(b *ec2.InMemoryBackend) string {
				instances, err := b.RunInstances("ami-123", "t2.micro", "", 1)
				if err != nil {
					return ""
				}

				return instances[0].ID
			},
			op:        "stop",
			wantErr:   false,
			wantState: "stopped",
		},
		{
			name: "start_stopped_instance",
			setup: func(b *ec2.InMemoryBackend) string {
				instances, err := b.RunInstances("ami-123", "t2.micro", "", 1)
				if err != nil {
					return ""
				}

				id := instances[0].ID
				_, _ = b.StopInstances([]string{id})

				return id
			},
			op:        "start",
			wantErr:   false,
			wantState: "running",
		},
		{
			name:       "stop_nonexistent",
			op:         "stop",
			instanceID: "i-doesnotexist",
			wantErr:    true,
		},
		{
			name:       "start_nonexistent",
			op:         "start",
			instanceID: "i-doesnotexist",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			id := tt.instanceID

			if tt.setup != nil {
				id = tt.setup(b)
			}

			if tt.op == "stop" {
				instances, err := b.StopInstances([]string{id})
				if tt.wantErr {
					require.Error(t, err)

					return
				}

				require.NoError(t, err)
				require.Len(t, instances, 1)
				assert.Equal(t, tt.wantState, instances[0].State.Name)
			} else {
				instances, err := b.StartInstances([]string{id})
				if tt.wantErr {
					require.Error(t, err)

					return
				}

				require.NoError(t, err)
				require.Len(t, instances, 1)
				assert.Equal(t, tt.wantState, instances[0].State.Name)
			}
		})
	}
}

func TestRebootInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		instanceID string
		wantErr    bool
	}{
		{
			name:    "reboot_existing",
			wantErr: false,
		},
		{
			name:       "reboot_nonexistent",
			instanceID: "i-doesnotexist",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			id := tt.instanceID

			if id == "" {
				instances, err := b.RunInstances("ami-123", "t2.micro", "", 1)
				require.NoError(t, err)
				id = instances[0].ID
			}

			err := b.RebootInstances([]string{id})

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestKeyPairOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		op      string
		keyName string
		wantErr bool
	}{
		{
			name:    "create_keypair",
			op:      "create",
			keyName: "my-key",
			wantErr: false,
		},
		{
			name:    "create_keypair_empty_name",
			op:      "create",
			keyName: "",
			wantErr: true,
		},
		{
			name:    "create_duplicate",
			op:      "create_duplicate",
			keyName: "dup-key",
			wantErr: true,
		},
		{
			name:    "describe_all",
			op:      "describe_all",
			keyName: "desc-key",
			wantErr: false,
		},
		{
			name:    "describe_by_name",
			op:      "describe_by_name",
			keyName: "desc-by-name-key",
			wantErr: false,
		},
		{
			name:    "delete_keypair",
			op:      "delete",
			keyName: "del-key",
			wantErr: false,
		},
		{
			name:    "delete_nonexistent",
			op:      "delete_nonexistent",
			keyName: "nonexistent-key",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			switch tt.op {
			case "create":
				kp, err := b.CreateKeyPair(tt.keyName)
				if tt.wantErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					assert.Equal(t, tt.keyName, kp.Name)
					assert.NotEmpty(t, kp.Fingerprint)
					assert.NotEmpty(t, kp.Material)
				}

			case "create_duplicate":
				_, err := b.CreateKeyPair(tt.keyName)
				require.NoError(t, err)
				_, err = b.CreateKeyPair(tt.keyName)
				require.Error(t, err)

			case "describe_all":
				_, err := b.CreateKeyPair(tt.keyName)
				require.NoError(t, err)
				kps := b.DescribeKeyPairs(nil)
				assert.NotEmpty(t, kps)
				assert.Empty(t, kps[0].Material, "material should be stripped on describe")

			case "describe_by_name":
				_, err := b.CreateKeyPair(tt.keyName)
				require.NoError(t, err)
				kps := b.DescribeKeyPairs([]string{tt.keyName})
				require.Len(t, kps, 1)
				assert.Equal(t, tt.keyName, kps[0].Name)

			case "delete":
				_, err := b.CreateKeyPair(tt.keyName)
				require.NoError(t, err)
				err = b.DeleteKeyPair(tt.keyName)
				require.NoError(t, err)
				kps := b.DescribeKeyPairs([]string{tt.keyName})
				assert.Empty(t, kps)

			case "delete_nonexistent":
				err := b.DeleteKeyPair(tt.keyName)
				require.Error(t, err)
			}
		})
	}
}

func TestVolumeOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		op      string
		az      string
		volType string
		size    int
		wantErr bool
	}{
		{
			name:    "create_volume_defaults",
			op:      "create",
			wantErr: false,
		},
		{
			name:    "create_volume_custom",
			op:      "create",
			az:      "us-east-1b",
			volType: "gp3",
			size:    100,
			wantErr: false,
		},
		{
			name:    "describe_all",
			op:      "describe_all",
			wantErr: false,
		},
		{
			name:    "delete_volume",
			op:      "delete",
			wantErr: false,
		},
		{
			name:    "delete_nonexistent",
			op:      "delete_nonexistent",
			wantErr: true,
		},
		{
			name:    "attach_detach",
			op:      "attach_detach",
			wantErr: false,
		},
		{
			name:    "delete_attached_volume",
			op:      "delete_attached",
			wantErr: true,
		},
		{
			name:    "attach_nonexistent_volume",
			op:      "attach_nonexistent_vol",
			wantErr: true,
		},
		{
			name:    "attach_nonexistent_instance",
			op:      "attach_nonexistent_inst",
			wantErr: true,
		},
		{
			name:    "detach_not_attached",
			op:      "detach_not_attached",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			switch tt.op {
			case "create":
				vol, err := b.CreateVolume(tt.az, tt.volType, tt.size)
				require.NoError(t, err)
				assert.NotEmpty(t, vol.ID)
				assert.Equal(t, "available", vol.State)
				if tt.az != "" {
					assert.Equal(t, tt.az, vol.AZ)
				}
				if tt.volType != "" {
					assert.Equal(t, tt.volType, vol.VolumeType)
				}
				if tt.size > 0 {
					assert.Equal(t, tt.size, vol.Size)
				}

			case "describe_all":
				_, err := b.CreateVolume("", "", 0)
				require.NoError(t, err)
				vols := b.DescribeVolumes(nil)
				assert.NotEmpty(t, vols)

			case "delete":
				vol, err := b.CreateVolume("", "", 0)
				require.NoError(t, err)
				err = b.DeleteVolume(vol.ID)
				require.NoError(t, err)
				vols := b.DescribeVolumes([]string{vol.ID})
				assert.Empty(t, vols)

			case "delete_nonexistent":
				err := b.DeleteVolume("vol-nonexistent")
				require.Error(t, err)

			case "attach_detach":
				vol, err := b.CreateVolume("", "", 0)
				require.NoError(t, err)
				instances, err := b.RunInstances("ami-123", "t2.micro", "", 1)
				require.NoError(t, err)
				att, err := b.AttachVolume(vol.ID, instances[0].ID, "/dev/sdf")
				require.NoError(t, err)
				assert.Equal(t, "attached", att.State)
				vols := b.DescribeVolumes([]string{vol.ID})
				require.Len(t, vols, 1)
				assert.Equal(t, "in-use", vols[0].State)
				detatt, err := b.DetachVolume(vol.ID, false)
				require.NoError(t, err)
				assert.Equal(t, "detached", detatt.State)

			case "delete_attached":
				vol, err := b.CreateVolume("", "", 0)
				require.NoError(t, err)
				instances, err := b.RunInstances("ami-123", "t2.micro", "", 1)
				require.NoError(t, err)
				_, err = b.AttachVolume(vol.ID, instances[0].ID, "/dev/sdf")
				require.NoError(t, err)
				err = b.DeleteVolume(vol.ID)
				require.Error(t, err)

			case "attach_nonexistent_vol":
				instances, err := b.RunInstances("ami-123", "t2.micro", "", 1)
				require.NoError(t, err)
				_, err = b.AttachVolume("vol-nonexistent", instances[0].ID, "/dev/sdf")
				require.Error(t, err)

			case "attach_nonexistent_inst":
				vol, err := b.CreateVolume("", "", 0)
				require.NoError(t, err)
				_, err = b.AttachVolume(vol.ID, "i-nonexistent", "/dev/sdf")
				require.Error(t, err)

			case "detach_not_attached":
				vol, err := b.CreateVolume("", "", 0)
				require.NoError(t, err)
				_, err = b.DetachVolume(vol.ID, false)
				require.Error(t, err)
			}
		})
	}
}

func TestElasticIPOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		op      string
		wantErr bool
	}{
		{name: "allocate", op: "allocate", wantErr: false},
		{name: "describe_all", op: "describe_all", wantErr: false},
		{name: "associate_disassociate", op: "associate_disassociate", wantErr: false},
		{name: "release", op: "release", wantErr: false},
		{name: "release_nonexistent", op: "release_nonexistent", wantErr: true},
		{name: "associate_bad_alloc", op: "associate_bad_alloc", wantErr: true},
		{name: "associate_bad_instance", op: "associate_bad_instance", wantErr: true},
		{name: "disassociate_nonexistent", op: "disassociate_nonexistent", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			switch tt.op {
			case "allocate":
				addr, err := b.AllocateAddress()
				require.NoError(t, err)
				assert.NotEmpty(t, addr.AllocationID)
				assert.NotEmpty(t, addr.PublicIP)

			case "describe_all":
				_, err := b.AllocateAddress()
				require.NoError(t, err)
				addrs := b.DescribeAddresses(nil)
				assert.NotEmpty(t, addrs)

			case "associate_disassociate":
				addr, err := b.AllocateAddress()
				require.NoError(t, err)
				instances, err := b.RunInstances("ami-123", "t2.micro", "", 1)
				require.NoError(t, err)
				assocID, err := b.AssociateAddress(addr.AllocationID, instances[0].ID)
				require.NoError(t, err)
				assert.NotEmpty(t, assocID)
				err = b.DisassociateAddress(assocID)
				require.NoError(t, err)

			case "release":
				addr, err := b.AllocateAddress()
				require.NoError(t, err)
				err = b.ReleaseAddress(addr.AllocationID)
				require.NoError(t, err)
				addrs := b.DescribeAddresses([]string{addr.AllocationID})
				assert.Empty(t, addrs)

			case "release_nonexistent":
				err := b.ReleaseAddress("eipalloc-nonexistent")
				require.Error(t, err)

			case "associate_bad_alloc":
				instances, err := b.RunInstances("ami-123", "t2.micro", "", 1)
				require.NoError(t, err)
				_, err = b.AssociateAddress("eipalloc-nonexistent", instances[0].ID)
				require.Error(t, err)

			case "associate_bad_instance":
				addr, err := b.AllocateAddress()
				require.NoError(t, err)
				_, err = b.AssociateAddress(addr.AllocationID, "i-nonexistent")
				require.Error(t, err)

			case "disassociate_nonexistent":
				err := b.DisassociateAddress("eipassoc-nonexistent")
				require.Error(t, err)
			}
		})
	}
}

func TestInternetGatewayOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		op      string
		wantErr bool
	}{
		{name: "create", op: "create", wantErr: false},
		{name: "describe_all", op: "describe_all", wantErr: false},
		{name: "delete", op: "delete", wantErr: false},
		{name: "delete_nonexistent", op: "delete_nonexistent", wantErr: true},
		{name: "attach_detach", op: "attach_detach", wantErr: false},
		{name: "attach_bad_igw", op: "attach_bad_igw", wantErr: true},
		{name: "attach_bad_vpc", op: "attach_bad_vpc", wantErr: true},
		{name: "detach_not_attached", op: "detach_not_attached", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			switch tt.op {
			case "create":
				igw, err := b.CreateInternetGateway()
				require.NoError(t, err)
				assert.NotEmpty(t, igw.ID)
				assert.Empty(t, igw.Attachments)

			case "describe_all":
				_, err := b.CreateInternetGateway()
				require.NoError(t, err)
				igws := b.DescribeInternetGateways(nil)
				assert.NotEmpty(t, igws)

			case "delete":
				igw, err := b.CreateInternetGateway()
				require.NoError(t, err)
				err = b.DeleteInternetGateway(igw.ID)
				require.NoError(t, err)
				igws := b.DescribeInternetGateways([]string{igw.ID})
				assert.Empty(t, igws)

			case "delete_nonexistent":
				err := b.DeleteInternetGateway("igw-nonexistent")
				require.Error(t, err)

			case "attach_detach":
				igw, err := b.CreateInternetGateway()
				require.NoError(t, err)
				vpc, err := b.CreateVpc("10.0.0.0/16")
				require.NoError(t, err)
				err = b.AttachInternetGateway(igw.ID, vpc.ID)
				require.NoError(t, err)
				igws := b.DescribeInternetGateways([]string{igw.ID})
				require.Len(t, igws, 1)
				assert.Len(t, igws[0].Attachments, 1)
				err = b.DetachInternetGateway(igw.ID, vpc.ID)
				require.NoError(t, err)
				igws = b.DescribeInternetGateways([]string{igw.ID})
				require.Len(t, igws, 1)
				assert.Empty(t, igws[0].Attachments)

			case "attach_bad_igw":
				vpc, err := b.CreateVpc("10.0.0.0/16")
				require.NoError(t, err)
				err = b.AttachInternetGateway("igw-nonexistent", vpc.ID)
				require.Error(t, err)

			case "attach_bad_vpc":
				igw, err := b.CreateInternetGateway()
				require.NoError(t, err)
				err = b.AttachInternetGateway(igw.ID, "vpc-nonexistent")
				require.Error(t, err)

			case "detach_not_attached":
				igw, err := b.CreateInternetGateway()
				require.NoError(t, err)
				err = b.DetachInternetGateway(igw.ID, "vpc-default")
				require.Error(t, err)
			}
		})
	}
}

func TestRouteTableOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		op      string
		wantErr bool
	}{
		{name: "create", op: "create", wantErr: false},
		{name: "create_bad_vpc", op: "create_bad_vpc", wantErr: true},
		{name: "describe_all", op: "describe_all", wantErr: false},
		{name: "delete", op: "delete", wantErr: false},
		{name: "delete_nonexistent", op: "delete_nonexistent", wantErr: true},
		{name: "create_route", op: "create_route", wantErr: false},
		{name: "delete_route", op: "delete_route", wantErr: false},
		{name: "delete_route_not_found", op: "delete_route_not_found", wantErr: true},
		{name: "associate_disassociate", op: "associate_disassociate", wantErr: false},
		{name: "associate_bad_rt", op: "associate_bad_rt", wantErr: true},
		{name: "associate_bad_subnet", op: "associate_bad_subnet", wantErr: true},
		{name: "disassociate_nonexistent", op: "disassociate_nonexistent", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			switch tt.op {
			case "create":
				rt, err := b.CreateRouteTable("vpc-default")
				require.NoError(t, err)
				assert.NotEmpty(t, rt.ID)
				assert.Equal(t, "vpc-default", rt.VPCID)

			case "create_bad_vpc":
				_, err := b.CreateRouteTable("vpc-nonexistent")
				require.Error(t, err)

			case "describe_all":
				_, err := b.CreateRouteTable("vpc-default")
				require.NoError(t, err)
				rts := b.DescribeRouteTables(nil)
				assert.NotEmpty(t, rts)

			case "delete":
				rt, err := b.CreateRouteTable("vpc-default")
				require.NoError(t, err)
				err = b.DeleteRouteTable(rt.ID)
				require.NoError(t, err)

			case "delete_nonexistent":
				err := b.DeleteRouteTable("rtb-nonexistent")
				require.Error(t, err)

			case "create_route":
				rt, err := b.CreateRouteTable("vpc-default")
				require.NoError(t, err)
				err = b.CreateRoute(rt.ID, "0.0.0.0/0", "igw-123", "")
				require.NoError(t, err)
				rts := b.DescribeRouteTables([]string{rt.ID})
				require.Len(t, rts, 1)
				assert.Len(t, rts[0].Routes, 1)

			case "delete_route":
				rt, err := b.CreateRouteTable("vpc-default")
				require.NoError(t, err)
				err = b.CreateRoute(rt.ID, "0.0.0.0/0", "igw-123", "")
				require.NoError(t, err)
				err = b.DeleteRoute(rt.ID, "0.0.0.0/0")
				require.NoError(t, err)
				rts := b.DescribeRouteTables([]string{rt.ID})
				require.Len(t, rts, 1)
				assert.Empty(t, rts[0].Routes)

			case "delete_route_not_found":
				rt, err := b.CreateRouteTable("vpc-default")
				require.NoError(t, err)
				err = b.DeleteRoute(rt.ID, "10.0.0.0/8")
				require.Error(t, err)

			case "associate_disassociate":
				rt, err := b.CreateRouteTable("vpc-default")
				require.NoError(t, err)
				assocID, err := b.AssociateRouteTable(rt.ID, "subnet-default")
				require.NoError(t, err)
				assert.NotEmpty(t, assocID)
				err = b.DisassociateRouteTable(assocID)
				require.NoError(t, err)

			case "associate_bad_rt":
				_, err := b.AssociateRouteTable("rtb-nonexistent", "subnet-default")
				require.Error(t, err)

			case "associate_bad_subnet":
				rt, err := b.CreateRouteTable("vpc-default")
				require.NoError(t, err)
				_, err = b.AssociateRouteTable(rt.ID, "subnet-nonexistent")
				require.Error(t, err)

			case "disassociate_nonexistent":
				err := b.DisassociateRouteTable("rtbassoc-nonexistent")
				require.Error(t, err)
			}
		})
	}
}

func TestNatGatewayOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		op      string
		wantErr bool
	}{
		{name: "create", op: "create", wantErr: false},
		{name: "create_bad_subnet", op: "create_bad_subnet", wantErr: true},
		{name: "create_bad_alloc", op: "create_bad_alloc", wantErr: true},
		{name: "describe_all", op: "describe_all", wantErr: false},
		{name: "delete", op: "delete", wantErr: false},
		{name: "delete_nonexistent", op: "delete_nonexistent", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			switch tt.op {
			case "create":
				addr, err := b.AllocateAddress()
				require.NoError(t, err)
				ngw, err := b.CreateNatGateway("subnet-default", addr.AllocationID)
				require.NoError(t, err)
				assert.NotEmpty(t, ngw.ID)
				assert.Equal(t, "available", ngw.State)
				assert.NotEmpty(t, ngw.PublicIP)
				assert.NotEmpty(t, ngw.PrivateIP)

			case "create_bad_subnet":
				addr, err := b.AllocateAddress()
				require.NoError(t, err)
				_, err = b.CreateNatGateway("subnet-nonexistent", addr.AllocationID)
				require.Error(t, err)

			case "create_bad_alloc":
				_, err := b.CreateNatGateway("subnet-default", "eipalloc-nonexistent")
				require.Error(t, err)

			case "describe_all":
				addr, err := b.AllocateAddress()
				require.NoError(t, err)
				_, err = b.CreateNatGateway("subnet-default", addr.AllocationID)
				require.NoError(t, err)
				ngws := b.DescribeNatGateways(nil)
				assert.NotEmpty(t, ngws)

			case "delete":
				addr, err := b.AllocateAddress()
				require.NoError(t, err)
				ngw, err := b.CreateNatGateway("subnet-default", addr.AllocationID)
				require.NoError(t, err)
				err = b.DeleteNatGateway(ngw.ID)
				require.NoError(t, err)
				ngws := b.DescribeNatGateways([]string{ngw.ID})
				assert.Empty(t, ngws)

			case "delete_nonexistent":
				err := b.DeleteNatGateway("nat-nonexistent")
				require.Error(t, err)
			}
		})
	}
}

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

func TestSecurityGroupRuleOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		op      string
		wantErr bool
	}{
		{name: "authorize_ingress", op: "auth_ingress", wantErr: false},
		{name: "authorize_egress", op: "auth_egress", wantErr: false},
		{name: "revoke_ingress", op: "revoke_ingress", wantErr: false},
		{name: "authorize_ingress_bad_sg", op: "auth_ingress_bad_sg", wantErr: true},
		{name: "authorize_egress_bad_sg", op: "auth_egress_bad_sg", wantErr: true},
		{name: "revoke_ingress_bad_sg", op: "revoke_ingress_bad_sg", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			rule := ec2.SecurityGroupRule{Protocol: "tcp", FromPort: 80, ToPort: 80, IPRange: "0.0.0.0/0"}

			switch tt.op {
			case "auth_ingress":
				sg, err := b.CreateSecurityGroup("test-sg", "test", "vpc-default")
				require.NoError(t, err)
				err = b.AuthorizeSecurityGroupIngress(sg.ID, []ec2.SecurityGroupRule{rule})
				require.NoError(t, err)
				sgs := b.DescribeSecurityGroups([]string{sg.ID})
				require.Len(t, sgs, 1)
				assert.Len(t, sgs[0].IngressRules, 1)

			case "auth_egress":
				sg, err := b.CreateSecurityGroup("test-sg-egress", "test", "vpc-default")
				require.NoError(t, err)
				err = b.AuthorizeSecurityGroupEgress(sg.ID, []ec2.SecurityGroupRule{rule})
				require.NoError(t, err)
				sgs := b.DescribeSecurityGroups([]string{sg.ID})
				require.Len(t, sgs, 1)
				assert.Len(t, sgs[0].EgressRules, 1)

			case "revoke_ingress":
				sg, err := b.CreateSecurityGroup("test-sg-revoke", "test", "vpc-default")
				require.NoError(t, err)
				err = b.AuthorizeSecurityGroupIngress(sg.ID, []ec2.SecurityGroupRule{rule})
				require.NoError(t, err)
				err = b.RevokeSecurityGroupIngress(sg.ID, []ec2.SecurityGroupRule{rule})
				require.NoError(t, err)
				sgs := b.DescribeSecurityGroups([]string{sg.ID})
				require.Len(t, sgs, 1)
				assert.Empty(t, sgs[0].IngressRules)

			case "auth_ingress_bad_sg":
				err := b.AuthorizeSecurityGroupIngress("sg-nonexistent", []ec2.SecurityGroupRule{rule})
				require.Error(t, err)

			case "auth_egress_bad_sg":
				err := b.AuthorizeSecurityGroupEgress("sg-nonexistent", []ec2.SecurityGroupRule{rule})
				require.Error(t, err)

			case "revoke_ingress_bad_sg":
				err := b.RevokeSecurityGroupIngress("sg-nonexistent", []ec2.SecurityGroupRule{rule})
				require.Error(t, err)
			}
		})
	}
}

func TestDescribeImages(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	amis := b.DescribeImages()
	assert.NotEmpty(t, amis)
	assert.Equal(t, "ami-0c55b159cbfafe1f0", amis[0].ImageID)
}

func TestDescribeRegions(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	regions := b.DescribeRegions()
	assert.NotEmpty(t, regions)
	assert.Contains(t, regions, "us-east-1")
}

func TestDescribeAvailabilityZones(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		region string
		wantAZ string
	}{
		{name: "default_region", region: "", wantAZ: "us-east-1a"},
		{name: "explicit_region", region: "eu-west-1", wantAZ: "eu-west-1a"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			azs := b.DescribeAvailabilityZones(tt.region)
			assert.Len(t, azs, 3)
			assert.Contains(t, azs, tt.wantAZ)
		})
	}
}

func TestDescribeInstanceStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		runCount  int
		filterIDs bool
		wantCount int
	}{
		{name: "all_instances", runCount: 2, filterIDs: false, wantCount: 2},
		{name: "filtered_by_id", runCount: 2, filterIDs: true, wantCount: 1},
		{name: "empty", runCount: 0, filterIDs: false, wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			var firstID string

			for range tt.runCount {
				instances, err := b.RunInstances("ami-123", "t2.micro", "", 1)
				require.NoError(t, err)
				if firstID == "" {
					firstID = instances[0].ID
				}
			}

			var ids []string
			if tt.filterIDs && firstID != "" {
				ids = []string{firstID}
			}

			statuses := b.DescribeInstanceStatus(ids)
			assert.Len(t, statuses, tt.wantCount)
		})
	}
}

// ---- Handler tests ----

func TestHandlerExtOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn      func(*ec2.Handler) string
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "StartInstances_missing_id",
			body:         "Action=StartInstances&Version=2016-11-15",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "StartInstances_success",
			setupFn: func(h *ec2.Handler) string {
				instances, _ := h.Backend.RunInstances("ami-123", "t2.micro", "", 1)
				_, _ = h.Backend.StopInstances([]string{instances[0].ID})

				return fmt.Sprintf(
					"Action=StartInstances&Version=2016-11-15&InstanceId.1=%s",
					url.QueryEscape(instances[0].ID),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"StartInstancesResponse"},
		},
		{
			name: "StopInstances_success",
			setupFn: func(h *ec2.Handler) string {
				instances, _ := h.Backend.RunInstances("ami-123", "t2.micro", "", 1)

				return fmt.Sprintf(
					"Action=StopInstances&Version=2016-11-15&InstanceId.1=%s",
					url.QueryEscape(instances[0].ID),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"StopInstancesResponse"},
		},
		{
			name: "RebootInstances_success",
			setupFn: func(h *ec2.Handler) string {
				instances, _ := h.Backend.RunInstances("ami-123", "t2.micro", "", 1)

				return fmt.Sprintf(
					"Action=RebootInstances&Version=2016-11-15&InstanceId.1=%s",
					url.QueryEscape(instances[0].ID),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"RebootInstancesResponse"},
		},
		{
			name:         "DescribeInstanceStatus_all",
			body:         "Action=DescribeInstanceStatus&Version=2016-11-15",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeInstanceStatusResponse"},
		},
		{
			name:         "DescribeImages",
			body:         "Action=DescribeImages&Version=2016-11-15",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeImagesResponse", "ami-"},
		},
		{
			name:         "DescribeRegions",
			body:         "Action=DescribeRegions&Version=2016-11-15",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeRegionsResponse", "us-east-1"},
		},
		{
			name:         "DescribeAvailabilityZones",
			body:         "Action=DescribeAvailabilityZones&Version=2016-11-15",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeAvailabilityZonesResponse", "us-east-1a"},
		},
		{
			name:         "CreateKeyPair_success",
			body:         "Action=CreateKeyPair&Version=2016-11-15&KeyName=test-key",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateKeyPairResponse", "test-key", "keyMaterial"},
		},
		{
			name:         "CreateKeyPair_missing_name",
			body:         "Action=CreateKeyPair&Version=2016-11-15",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "DescribeKeyPairs",
			setupFn: func(h *ec2.Handler) string {
				_, _ = h.Backend.CreateKeyPair("list-key")

				return "Action=DescribeKeyPairs&Version=2016-11-15"
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeKeyPairsResponse", "list-key"},
		},
		{
			name: "DeleteKeyPair_success",
			setupFn: func(h *ec2.Handler) string {
				_, _ = h.Backend.CreateKeyPair("del-key")

				return "Action=DeleteKeyPair&Version=2016-11-15&KeyName=del-key"
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteKeyPairResponse"},
		},
		{
			name:         "DeleteKeyPair_not_found",
			body:         "Action=DeleteKeyPair&Version=2016-11-15&KeyName=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidKeyPair.NotFound"},
		},
		{
			name:         "ImportKeyPair_success",
			body:         "Action=ImportKeyPair&Version=2016-11-15&KeyName=imported-key&PublicKeyMaterial=dGVzdA==",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateKeyPairResponse", "imported-key"},
		},
		{
			name:         "CreateVolume_success",
			body:         "Action=CreateVolume&Version=2016-11-15&AvailabilityZone=us-east-1a&Size=20&VolumeType=gp2",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateVolumeResponse", "vol-", "available"},
		},
		{
			name: "DescribeVolumes",
			setupFn: func(h *ec2.Handler) string {
				_, _ = h.Backend.CreateVolume("us-east-1a", "gp2", 20)

				return "Action=DescribeVolumes&Version=2016-11-15"
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeVolumesResponse"},
		},
		{
			name:         "DeleteVolume_missing_id",
			body:         "Action=DeleteVolume&Version=2016-11-15",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "DeleteVolume_not_found",
			body:         "Action=DeleteVolume&Version=2016-11-15&VolumeId=vol-nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidVolume.NotFound"},
		},
		{
			name:         "AllocateAddress",
			body:         "Action=AllocateAddress&Version=2016-11-15",
			wantCode:     http.StatusOK,
			wantContains: []string{"AllocateAddressResponse", "eipalloc-"},
		},
		{
			name: "DescribeAddresses",
			setupFn: func(h *ec2.Handler) string {
				_, _ = h.Backend.AllocateAddress()

				return "Action=DescribeAddresses&Version=2016-11-15"
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeAddressesResponse"},
		},
		{
			name:         "ReleaseAddress_not_found",
			body:         "Action=ReleaseAddress&Version=2016-11-15&AllocationId=eipalloc-nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidAllocationID.NotFound"},
		},
		{
			name:         "CreateInternetGateway",
			body:         "Action=CreateInternetGateway&Version=2016-11-15",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateInternetGatewayResponse", "igw-"},
		},
		{
			name: "DescribeInternetGateways",
			setupFn: func(h *ec2.Handler) string {
				_, _ = h.Backend.CreateInternetGateway()

				return "Action=DescribeInternetGateways&Version=2016-11-15"
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeInternetGatewaysResponse"},
		},
		{
			name:         "DeleteInternetGateway_not_found",
			body:         "Action=DeleteInternetGateway&Version=2016-11-15&InternetGatewayId=igw-nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidInternetGatewayID.NotFound"},
		},
		{
			name:         "CreateRouteTable_success",
			body:         "Action=CreateRouteTable&Version=2016-11-15&VpcId=vpc-default",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateRouteTableResponse", "rtb-"},
		},
		{
			name:         "CreateRouteTable_missing_vpc",
			body:         "Action=CreateRouteTable&Version=2016-11-15",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "DescribeRouteTables",
			setupFn: func(h *ec2.Handler) string {
				_, _ = h.Backend.CreateRouteTable("vpc-default")

				return "Action=DescribeRouteTables&Version=2016-11-15"
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeRouteTablesResponse"},
		},
		{
			name:         "DeleteRouteTable_not_found",
			body:         "Action=DeleteRouteTable&Version=2016-11-15&RouteTableId=rtb-nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidRouteTableID.NotFound"},
		},
		{
			name: "CreateRoute_success",
			setupFn: func(h *ec2.Handler) string {
				rt, _ := h.Backend.CreateRouteTable("vpc-default")

				return fmt.Sprintf(
					"Action=CreateRoute&Version=2016-11-15&RouteTableId=%s&DestinationCidrBlock=0.0.0.0/0&GatewayId=igw-123",
					rt.ID,
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateRouteResponse"},
		},
		{
			name: "DeleteRoute_success",
			setupFn: func(h *ec2.Handler) string {
				rt, _ := h.Backend.CreateRouteTable("vpc-default")
				_ = h.Backend.CreateRoute(rt.ID, "0.0.0.0/0", "igw-123", "")

				return fmt.Sprintf(
					"Action=DeleteRoute&Version=2016-11-15&RouteTableId=%s&DestinationCidrBlock=0.0.0.0/0",
					rt.ID,
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteRouteResponse"},
		},
		{
			name: "AssociateRouteTable_success",
			setupFn: func(h *ec2.Handler) string {
				rt, _ := h.Backend.CreateRouteTable("vpc-default")

				return fmt.Sprintf(
					"Action=AssociateRouteTable&Version=2016-11-15&RouteTableId=%s&SubnetId=subnet-default",
					rt.ID,
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"AssociateRouteTableResponse", "rtbassoc-"},
		},
		{
			name:         "DescribeNatGateways_empty",
			body:         "Action=DescribeNatGateways&Version=2016-11-15",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeNatGatewaysResponse"},
		},
		{
			name: "CreateNatGateway_success",
			setupFn: func(h *ec2.Handler) string {
				addr, _ := h.Backend.AllocateAddress()

				return "Action=CreateNatGateway&Version=2016-11-15" +
					"&SubnetId=subnet-default&AllocationId=" + addr.AllocationID
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateNatGatewayResponse", "nat-"},
		},
		{
			name:         "DeleteNatGateway_not_found",
			body:         "Action=DeleteNatGateway&Version=2016-11-15&NatGatewayId=nat-nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidNatGatewayID.NotFound"},
		},
		{
			name:         "DescribeNetworkInterfaces",
			body:         "Action=DescribeNetworkInterfaces&Version=2016-11-15",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeNetworkInterfacesResponse"},
		},
		{
			name:         "AuthorizeSecurityGroupIngress_missing_group",
			body:         "Action=AuthorizeSecurityGroupIngress&Version=2016-11-15",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "AuthorizeSecurityGroupIngress_success",
			setupFn: func(h *ec2.Handler) string {
				sg, _ := h.Backend.CreateSecurityGroup("test-sg-auth", "test", "vpc-default")

				return "Action=AuthorizeSecurityGroupIngress&Version=2016-11-15" +
					"&GroupId=" + sg.ID +
					"&IpPermissions.1.IpProtocol=tcp&IpPermissions.1.FromPort=80" +
					"&IpPermissions.1.ToPort=80&IpPermissions.1.IpRanges.1.CidrIp=0.0.0.0/0"
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"AuthorizeSecurityGroupIngressResponse"},
		},
		{
			name: "AuthorizeSecurityGroupEgress_success",
			setupFn: func(h *ec2.Handler) string {
				sg, _ := h.Backend.CreateSecurityGroup("test-sg-egr", "test", "vpc-default")

				return "Action=AuthorizeSecurityGroupEgress&Version=2016-11-15" +
					"&GroupId=" + sg.ID +
					"&IpPermissions.1.IpProtocol=tcp&IpPermissions.1.FromPort=443" +
					"&IpPermissions.1.ToPort=443&IpPermissions.1.IpRanges.1.CidrIp=0.0.0.0/0"
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"AuthorizeSecurityGroupEgressResponse"},
		},
		{
			name: "RevokeSecurityGroupIngress_success",
			setupFn: func(h *ec2.Handler) string {
				sg, _ := h.Backend.CreateSecurityGroup("test-sg-revoke-h", "test", "vpc-default")
				_ = h.Backend.AuthorizeSecurityGroupIngress(sg.ID, []ec2.SecurityGroupRule{
					{Protocol: "tcp", FromPort: 80, ToPort: 80, IPRange: "0.0.0.0/0"},
				})

				return "Action=RevokeSecurityGroupIngress&Version=2016-11-15" +
					"&GroupId=" + sg.ID +
					"&IpPermissions.1.IpProtocol=tcp&IpPermissions.1.FromPort=80" +
					"&IpPermissions.1.ToPort=80&IpPermissions.1.IpRanges.1.CidrIp=0.0.0.0/0"
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"RevokeSecurityGroupIngressResponse"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newExtHandler()
			body := tt.body

			if tt.setupFn != nil {
				body = tt.setupFn(h)
			}

			rec := postFormExt(t, h, body)

			assert.Equal(t, tt.wantCode, rec.Code)

			respBody := rec.Body.String()
			for _, want := range tt.wantContains {
				assert.Contains(t, respBody, want, "response should contain %q", want)
			}
		})
	}
}

func TestRunInstancesPrivateIP(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	instances, err := b.RunInstances("ami-123", "t2.micro", "", 1)
	require.NoError(t, err)
	require.Len(t, instances, 1)
	assert.NotEmpty(t, instances[0].PrivateIP, "instance should have a private IP assigned")

	enis := b.DescribeNetworkInterfaces(nil)
	assert.NotEmpty(t, enis, "ENI should be created with the instance")
	assert.Equal(t, instances[0].PrivateIP, enis[0].PrivateIP)
	assert.Equal(t, instances[0].ID, enis[0].InstanceID)
}

func TestHandlerRunInstancesIncludesPrivateIP(t *testing.T) {
	t.Parallel()

	h := newExtHandler()
	rec := postFormExt(
		t,
		h,
		"Action=RunInstances&Version=2016-11-15&ImageId=ami-123&InstanceType=t2.micro&MinCount=1&MaxCount=1",
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName      xml.Name `xml:"RunInstancesResponse"`
		InstancesSet struct {
			Items []struct {
				PrivateIPAddress string `xml:"privateIpAddress"`
			} `xml:"item"`
		} `xml:"instancesSet"`
	}

	require.NoError(t, xml.Unmarshal([]byte(strings.TrimPrefix(rec.Body.String(), xml.Header)), &resp))
	require.Len(t, resp.InstancesSet.Items, 1)
	assert.NotEmpty(t, resp.InstancesSet.Items[0].PrivateIPAddress)
}

func TestPersistenceWithExtendedResources(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	// Populate various resources
	_, err := b.CreateKeyPair("persist-key")
	require.NoError(t, err)
	vol, err := b.CreateVolume("us-east-1a", "gp2", 20)
	require.NoError(t, err)
	addr, err := b.AllocateAddress()
	require.NoError(t, err)
	_, err = b.CreateInternetGateway()
	require.NoError(t, err)
	rt, err := b.CreateRouteTable("vpc-default")
	require.NoError(t, err)
	_, err = b.CreateNatGateway("subnet-default", addr.AllocationID)
	require.NoError(t, err)
	_, err = b.RunInstances("ami-123", "t2.micro", "", 1)
	require.NoError(t, err)

	snap := b.Snapshot()
	require.NotEmpty(t, snap)

	b2 := newTestBackend()
	require.NoError(t, b2.Restore(snap))

	kps := b2.DescribeKeyPairs([]string{"persist-key"})
	assert.Len(t, kps, 1)

	vols := b2.DescribeVolumes([]string{vol.ID})
	assert.Len(t, vols, 1)

	addrs := b2.DescribeAddresses(nil)
	assert.NotEmpty(t, addrs)

	igws := b2.DescribeInternetGateways(nil)
	assert.NotEmpty(t, igws)

	rts := b2.DescribeRouteTables([]string{rt.ID})
	assert.Len(t, rts, 1)

	ngws := b2.DescribeNatGateways(nil)
	assert.NotEmpty(t, ngws)

	enis := b2.DescribeNetworkInterfaces(nil)
	assert.NotEmpty(t, enis)
}
