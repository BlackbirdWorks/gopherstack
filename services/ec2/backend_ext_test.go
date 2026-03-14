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

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// Compile-time assertion: InMemoryBackend must satisfy the Backend interface.
// Any future backend implementation must satisfy this same interface.
var _ ec2.Backend = (*ec2.InMemoryBackend)(nil)

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
		{
			// start a running instance must fail
			name: "start_running_instance",
			setup: func(b *ec2.InMemoryBackend) string {
				instances, err := b.RunInstances("ami-123", "t2.micro", "", 1)
				if err != nil {
					return ""
				}

				return instances[0].ID
			},
			op:      "start",
			wantErr: true,
		},
		{
			// stop an already-stopped instance must fail
			name: "stop_stopped_instance",
			setup: func(b *ec2.InMemoryBackend) string {
				instances, err := b.RunInstances("ami-123", "t2.micro", "", 1)
				if err != nil {
					return ""
				}

				id := instances[0].ID
				_, _ = b.StopInstances([]string{id})

				return id
			},
			op:      "stop",
			wantErr: true,
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
				changes, err := b.StopInstances([]string{id})
				if tt.wantErr {
					require.Error(t, err)

					return
				}

				require.NoError(t, err)
				require.Len(t, changes, 1)
				assert.Equal(t, tt.wantState, changes[0].CurrentState.Name)
			} else {
				changes, err := b.StartInstances([]string{id})
				if tt.wantErr {
					require.Error(t, err)

					return
				}

				require.NoError(t, err)
				require.Len(t, changes, 1)
				assert.Equal(t, tt.wantState, changes[0].CurrentState.Name)
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
		{
			name:    "import_keypair",
			op:      "import",
			keyName: "imported-key",
			wantErr: false,
		},
		{
			name:    "import_keypair_empty_name",
			op:      "import",
			keyName: "",
			wantErr: true,
		},
		{
			name:    "import_keypair_duplicate",
			op:      "import_duplicate",
			keyName: "dup-import-key",
			wantErr: true,
		},
		{
			name:    "import_keypair_retrievable",
			op:      "import_retrievable",
			keyName: "retrievable-key",
			wantErr: false,
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

			case "import":
				kp, err := b.ImportKeyPair(tt.keyName)
				if tt.wantErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					assert.Equal(t, tt.keyName, kp.Name)
					assert.NotEmpty(t, kp.Fingerprint)
					assert.Empty(t, kp.Material, "import should not set private key material")
				}

			case "import_duplicate":
				_, err := b.ImportKeyPair(tt.keyName)
				require.NoError(t, err)
				_, err = b.ImportKeyPair(tt.keyName)
				require.ErrorIs(t, err, ec2.ErrDuplicateKeyPairName)

			case "import_retrievable":
				_, err := b.ImportKeyPair(tt.keyName)
				require.NoError(t, err)
				kps := b.DescribeKeyPairs([]string{tt.keyName})
				require.Len(t, kps, 1)
				assert.Equal(t, tt.keyName, kps[0].Name)
				assert.NotEmpty(t, kps[0].Fingerprint)
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
		{
			// StartInstances on a running instance must fail with IncorrectInstanceState
			name: "StartInstances_invalid_state",
			setupFn: func(h *ec2.Handler) string {
				instances, _ := h.Backend.RunInstances("ami-123", "t2.micro", "", 1)

				return fmt.Sprintf(
					"Action=StartInstances&Version=2016-11-15&InstanceId.1=%s",
					url.QueryEscape(instances[0].ID),
				)
			},
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"IncorrectInstanceState"},
		},
		{
			// StopInstances on a stopped instance must fail with IncorrectInstanceState
			name: "StopInstances_invalid_state",
			setupFn: func(h *ec2.Handler) string {
				instances, _ := h.Backend.RunInstances("ami-123", "t2.micro", "", 1)
				_, _ = h.Backend.StopInstances([]string{instances[0].ID})

				return fmt.Sprintf(
					"Action=StopInstances&Version=2016-11-15&InstanceId.1=%s",
					url.QueryEscape(instances[0].ID),
				)
			},
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"IncorrectInstanceState"},
		},
		{
			name: "DescribeImageAttribute_success",
			body: "Action=DescribeImageAttribute&Version=2016-11-15" +
				"&ImageId=ami-0c55b159cbfafe1f0&Attribute=launchPermission",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeImageAttributeResponse", "launchPermission", "all"},
		},
		{
			name:         "DescribeImageAttribute_missing_image_id",
			body:         "Action=DescribeImageAttribute&Version=2016-11-15&Attribute=launchPermission",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "DescribeImageAttribute_missing_attribute",
			body:         "Action=DescribeImageAttribute&Version=2016-11-15&ImageId=ami-0c55b159cbfafe1f0",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			// ImportKeyPair without PublicKeyMaterial must fail
			name:         "ImportKeyPair_missing_material",
			body:         "Action=ImportKeyPair&Version=2016-11-15&KeyName=no-material-key",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
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
			}

			if tt.wantErr {
				require.Error(t, err)
			}
		})
	}
}

func TestSpotInstanceOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		op      string
		wantErr bool
	}{
		{name: "request_spot", op: "request"},
		{name: "request_bad_image", op: "request_bad_image", wantErr: true},
		{name: "describe_all", op: "describe_all"},
		{name: "describe_by_id", op: "describe_by_id"},
		{name: "cancel", op: "cancel"},
		{name: "cancel_not_found", op: "cancel_not_found", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			switch tt.op {
			case "request":
				req, err := b.RequestSpotInstances("ami-123", "t2.micro", "", "0.05")
				require.NoError(t, err)
				assert.NotEmpty(t, req.ID)
				assert.Equal(t, "active", req.State)
				assert.NotEmpty(t, req.InstanceID)

			case "request_bad_image":
				_, err := b.RequestSpotInstances("", "t2.micro", "", "0.05")
				require.Error(t, err)

			case "describe_all":
				_, err := b.RequestSpotInstances("ami-123", "t2.micro", "", "0.01")
				require.NoError(t, err)
				reqs := b.DescribeSpotInstanceRequests(nil)
				assert.NotEmpty(t, reqs)

			case "describe_by_id":
				req, err := b.RequestSpotInstances("ami-123", "t2.micro", "", "0.01")
				require.NoError(t, err)
				reqs := b.DescribeSpotInstanceRequests([]string{req.ID})
				require.Len(t, reqs, 1)
				assert.Equal(t, req.ID, reqs[0].ID)

			case "cancel":
				req, err := b.RequestSpotInstances("ami-123", "t2.micro", "", "0.01")
				require.NoError(t, err)
				err = b.CancelSpotInstanceRequests([]string{req.ID})
				require.NoError(t, err)
				reqs := b.DescribeSpotInstanceRequests([]string{req.ID})
				require.Len(t, reqs, 1)
				assert.Equal(t, "cancelled", reqs[0].State)

			case "cancel_not_found":
				err := b.CancelSpotInstanceRequests([]string{"sir-nonexistent"})
				require.Error(t, err)
			}
		})
	}
}

func TestPlacementGroupOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		op      string
		wantErr bool
	}{
		{name: "create", op: "create"},
		{name: "create_bad_name", op: "create_bad_name", wantErr: true},
		{name: "create_duplicate", op: "create_duplicate", wantErr: true},
		{name: "describe_all", op: "describe_all"},
		{name: "describe_by_name", op: "describe_by_name"},
		{name: "delete", op: "delete"},
		{name: "delete_not_found", op: "delete_not_found", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			switch tt.op {
			case "create":
				pg, err := b.CreatePlacementGroup("test-pg", "cluster")
				require.NoError(t, err)
				assert.Equal(t, "test-pg", pg.Name)
				assert.Equal(t, "cluster", pg.Strategy)
				assert.Equal(t, "available", pg.State)

			case "create_bad_name":
				_, err := b.CreatePlacementGroup("", "cluster")
				require.Error(t, err)

			case "create_duplicate":
				_, err := b.CreatePlacementGroup("dup-pg", "cluster")
				require.NoError(t, err)
				_, err = b.CreatePlacementGroup("dup-pg", "spread")
				require.Error(t, err)

			case "describe_all":
				_, err := b.CreatePlacementGroup("pg1", "cluster")
				require.NoError(t, err)
				pgs := b.DescribePlacementGroups(nil)
				assert.NotEmpty(t, pgs)

			case "describe_by_name":
				_, err := b.CreatePlacementGroup("pg-named", "spread")
				require.NoError(t, err)
				pgs := b.DescribePlacementGroups([]string{"pg-named"})
				require.Len(t, pgs, 1)
				assert.Equal(t, "pg-named", pgs[0].Name)

			case "delete":
				_, err := b.CreatePlacementGroup("del-pg", "cluster")
				require.NoError(t, err)
				err = b.DeletePlacementGroup("del-pg")
				require.NoError(t, err)
				pgs := b.DescribePlacementGroups([]string{"del-pg"})
				assert.Empty(t, pgs)

			case "delete_not_found":
				err := b.DeletePlacementGroup("nonexistent-pg")
				require.Error(t, err)
			}
		})
	}
}

func TestHandlerNewOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn      func(*ec2.Handler) string
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		// ---- NetworkInterface ----
		{
			name:         "CreateNetworkInterface_success",
			body:         "Action=CreateNetworkInterface&Version=2016-11-15&SubnetId=subnet-default&Description=test",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateNetworkInterfaceResponse", "eni-"},
		},
		{
			name:         "CreateNetworkInterface_missing_subnet",
			body:         "Action=CreateNetworkInterface&Version=2016-11-15",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "DeleteNetworkInterface_success",
			setupFn: func(h *ec2.Handler) string {
				eni, _ := h.Backend.CreateNetworkInterface("subnet-default", "")

				return fmt.Sprintf(
					"Action=DeleteNetworkInterface&Version=2016-11-15&NetworkInterfaceId=%s",
					url.QueryEscape(eni.ID),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteNetworkInterfaceResponse"},
		},
		{
			name:         "DeleteNetworkInterface_not_found",
			body:         "Action=DeleteNetworkInterface&Version=2016-11-15&NetworkInterfaceId=eni-nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidNetworkInterfaceID.NotFound"},
		},
		{
			name: "AttachNetworkInterface_success",
			setupFn: func(h *ec2.Handler) string {
				instances, _ := h.Backend.RunInstances("ami-123", "t2.micro", "", 1)
				eni, _ := h.Backend.CreateNetworkInterface("subnet-default", "")

				return fmt.Sprintf(
					"Action=AttachNetworkInterface&Version=2016-11-15&NetworkInterfaceId=%s&InstanceId=%s&DeviceIndex=1",
					url.QueryEscape(eni.ID),
					url.QueryEscape(instances[0].ID),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"AttachNetworkInterfaceResponse", "eni-attach-"},
		},
		{
			name: "DetachNetworkInterface_success",
			setupFn: func(h *ec2.Handler) string {
				instances, _ := h.Backend.RunInstances("ami-123", "t2.micro", "", 1)
				eni, _ := h.Backend.CreateNetworkInterface("subnet-default", "")
				attachID, _ := h.Backend.AttachNetworkInterface(eni.ID, instances[0].ID, 1)

				return fmt.Sprintf(
					"Action=DetachNetworkInterface&Version=2016-11-15&AttachmentId=%s",
					url.QueryEscape(attachID),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DetachNetworkInterfaceResponse"},
		},
		{
			name: "AssignPrivateIPAddresses_success",
			setupFn: func(h *ec2.Handler) string {
				eni, _ := h.Backend.CreateNetworkInterface("subnet-default", "")

				return fmt.Sprintf(
					"Action=AssignPrivateIPAddresses&Version=2016-11-15&NetworkInterfaceId=%s&SecondaryPrivateIpAddressCount=1",
					url.QueryEscape(eni.ID),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"AssignPrivateIPAddressesResponse"},
		},
		{
			name: "UnassignPrivateIPAddresses_success",
			setupFn: func(h *ec2.Handler) string {
				eni, _ := h.Backend.CreateNetworkInterface("subnet-default", "")
				_ = h.Backend.AssignPrivateIPAddresses(eni.ID, 0, []string{"10.0.1.50"})

				return fmt.Sprintf(
					"Action=UnassignPrivateIPAddresses&Version=2016-11-15&NetworkInterfaceId=%s&PrivateIpAddress.1=10.0.1.50",
					url.QueryEscape(eni.ID),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"UnassignPrivateIPAddressesResponse"},
		},
		{
			name: "ModifyNetworkInterfaceAttribute_description",
			setupFn: func(h *ec2.Handler) string {
				eni, _ := h.Backend.CreateNetworkInterface("subnet-default", "orig")

				return fmt.Sprintf(
					"Action=ModifyNetworkInterfaceAttribute&Version=2016-11-15&NetworkInterfaceId=%s&Description.Value=new-desc",
					url.QueryEscape(eni.ID),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"ModifyNetworkInterfaceAttributeResponse"},
		},
		// ---- Instance Attribute stubs ----
		{
			name: "ModifyInstanceAttribute_success",
			setupFn: func(h *ec2.Handler) string {
				instances, _ := h.Backend.RunInstances("ami-123", "t2.micro", "", 1)

				return fmt.Sprintf(
					"Action=ModifyInstanceAttribute&Version=2016-11-15&InstanceId=%s&InstanceType.Value=t3.micro",
					url.QueryEscape(instances[0].ID),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"ModifyInstanceAttributeResponse"},
		},
		{
			name:         "ModifyInstanceAttribute_missing_id",
			body:         "Action=ModifyInstanceAttribute&Version=2016-11-15",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "ResetInstanceAttribute_success",
			setupFn: func(h *ec2.Handler) string {
				instances, _ := h.Backend.RunInstances("ami-123", "t2.micro", "", 1)

				return fmt.Sprintf(
					"Action=ResetInstanceAttribute&Version=2016-11-15&InstanceId=%s&Attribute=sourceDestCheck",
					url.QueryEscape(instances[0].ID),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"ResetInstanceAttributeResponse"},
		},
		// ---- Spot Instances ----
		{
			name: "RequestSpotInstances_success",
			body: "Action=RequestSpotInstances&Version=2016-11-15" +
				"&LaunchSpecification.ImageId=ami-123" +
				"&LaunchSpecification.InstanceType=t2.micro" +
				"&SpotPrice=0.05",
			wantCode:     http.StatusOK,
			wantContains: []string{"RequestSpotInstancesResponse", "sir-"},
		},
		{
			name:         "RequestSpotInstances_missing_image",
			body:         "Action=RequestSpotInstances&Version=2016-11-15&SpotPrice=0.05",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "DescribeSpotInstanceRequests_empty",
			body:         "Action=DescribeSpotInstanceRequests&Version=2016-11-15",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeSpotInstanceRequestsResponse"},
		},
		{
			name: "DescribeSpotInstanceRequests_after_request",
			setupFn: func(h *ec2.Handler) string {
				_, _ = h.Backend.RequestSpotInstances("ami-123", "t2.micro", "", "0.01")

				return "Action=DescribeSpotInstanceRequests&Version=2016-11-15"
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeSpotInstanceRequestsResponse", "sir-"},
		},
		{
			name: "CancelSpotInstanceRequests_success",
			setupFn: func(h *ec2.Handler) string {
				req, _ := h.Backend.RequestSpotInstances("ami-123", "t2.micro", "", "0.01")

				return fmt.Sprintf(
					"Action=CancelSpotInstanceRequests&Version=2016-11-15&SpotInstanceRequestId.1=%s",
					url.QueryEscape(req.ID),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"CancelSpotInstanceRequestsResponse", "cancelled"},
		},
		{
			name:         "CancelSpotInstanceRequests_missing_ids",
			body:         "Action=CancelSpotInstanceRequests&Version=2016-11-15",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "DescribeSpotPriceHistory",
			body:         "Action=DescribeSpotPriceHistory&Version=2016-11-15",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeSpotPriceHistoryResponse"},
		},
		// ---- Placement Groups ----
		{
			name:         "CreatePlacementGroup_success",
			body:         "Action=CreatePlacementGroup&Version=2016-11-15&GroupName=test-pg&Strategy=cluster",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreatePlacementGroupResponse"},
		},
		{
			name:         "CreatePlacementGroup_missing_name",
			body:         "Action=CreatePlacementGroup&Version=2016-11-15&Strategy=cluster",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "DescribePlacementGroups_empty",
			body:         "Action=DescribePlacementGroups&Version=2016-11-15",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribePlacementGroupsResponse"},
		},
		{
			name: "DescribePlacementGroups_after_create",
			setupFn: func(h *ec2.Handler) string {
				_, _ = h.Backend.CreatePlacementGroup("list-pg", "spread")

				return "Action=DescribePlacementGroups&Version=2016-11-15"
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribePlacementGroupsResponse", "list-pg"},
		},
		{
			name: "DeletePlacementGroup_success",
			setupFn: func(h *ec2.Handler) string {
				_, _ = h.Backend.CreatePlacementGroup("del-pg", "cluster")

				return "Action=DeletePlacementGroup&Version=2016-11-15&GroupName=del-pg"
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DeletePlacementGroupResponse"},
		},
		{
			name:         "DeletePlacementGroup_not_found",
			body:         "Action=DeletePlacementGroup&Version=2016-11-15&GroupName=nonexistent-pg",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidPlacementGroup.NotFound"},
		},
		// ---- Volume / Snapshot Attributes ----
		{
			name: "DescribeVolumeAttribute_success",
			setupFn: func(h *ec2.Handler) string {
				vol, _ := h.Backend.CreateVolume("us-east-1a", "gp2", 20)

				return fmt.Sprintf(
					"Action=DescribeVolumeAttribute&Version=2016-11-15&VolumeId=%s&Attribute=autoEnableIO",
					url.QueryEscape(vol.ID),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeVolumeAttributeResponse"},
		},
		{
			name:         "DescribeVolumeAttribute_missing_volume",
			body:         "Action=DescribeVolumeAttribute&Version=2016-11-15&Attribute=autoEnableIO",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "ModifyVolumeAttribute_success",
			setupFn: func(h *ec2.Handler) string {
				vol, _ := h.Backend.CreateVolume("us-east-1a", "gp2", 20)

				return fmt.Sprintf(
					"Action=ModifyVolumeAttribute&Version=2016-11-15&VolumeId=%s&AutoEnableIO.Value=true",
					url.QueryEscape(vol.ID),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"ModifyVolumeAttributeResponse"},
		},
		{
			name:         "ModifyVolumeAttribute_missing_volume",
			body:         "Action=ModifyVolumeAttribute&Version=2016-11-15",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "DescribeSnapshotAttribute_success",
			body: "Action=DescribeSnapshotAttribute&Version=2016-11-15" +
				"&SnapshotId=snap-12345678&Attribute=createVolumePermission",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeSnapshotAttributeResponse", "snap-12345678"},
		},
		{
			name:         "DescribeSnapshotAttribute_missing_snapshot",
			body:         "Action=DescribeSnapshotAttribute&Version=2016-11-15&Attribute=createVolumePermission",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "ModifySnapshotAttribute_success",
			body: "Action=ModifySnapshotAttribute&Version=2016-11-15" +
				"&SnapshotId=snap-12345678&Attribute=createVolumePermission",
			wantCode:     http.StatusOK,
			wantContains: []string{"ModifySnapshotAttributeResponse"},
		},
		{
			name:         "ModifySnapshotAttribute_missing_snapshot",
			body:         "Action=ModifySnapshotAttribute&Version=2016-11-15",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
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
			for _, want := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), want, "expected %q in response", want)
			}
		})
	}
}

// TestHandlerPreviouslyUncoveredOps covers handler functions that had 0% coverage
// to ensure the overall package coverage meets the 85% threshold.
func TestHandlerPreviouslyUncoveredOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn      func(*ec2.Handler) string
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "AttachVolume_success",
			setupFn: func(h *ec2.Handler) string {
				instances, _ := h.Backend.RunInstances("ami-123", "t2.micro", "", 1)
				vol, _ := h.Backend.CreateVolume("us-east-1a", "gp2", 20)

				return fmt.Sprintf(
					"Action=AttachVolume&Version=2016-11-15&VolumeId=%s&InstanceId=%s&Device=/dev/sdf",
					url.QueryEscape(vol.ID), url.QueryEscape(instances[0].ID),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"AttachVolumeResponse"},
		},
		{
			name: "DetachVolume_success",
			setupFn: func(h *ec2.Handler) string {
				instances, _ := h.Backend.RunInstances("ami-123", "t2.micro", "", 1)
				vol, _ := h.Backend.CreateVolume("us-east-1a", "gp2", 20)
				_, _ = h.Backend.AttachVolume(vol.ID, instances[0].ID, "/dev/sdf")

				return fmt.Sprintf(
					"Action=DetachVolume&Version=2016-11-15&VolumeId=%s",
					url.QueryEscape(vol.ID),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DetachVolumeResponse"},
		},
		{
			name: "AssociateAddress_success",
			setupFn: func(h *ec2.Handler) string {
				instances, _ := h.Backend.RunInstances("ami-123", "t2.micro", "", 1)
				addr, _ := h.Backend.AllocateAddress()

				return fmt.Sprintf(
					"Action=AssociateAddress&Version=2016-11-15&AllocationId=%s&InstanceId=%s",
					url.QueryEscape(addr.AllocationID), url.QueryEscape(instances[0].ID),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"AssociateAddressResponse"},
		},
		{
			name: "DisassociateAddress_success",
			setupFn: func(h *ec2.Handler) string {
				instances, _ := h.Backend.RunInstances("ami-123", "t2.micro", "", 1)
				addr, _ := h.Backend.AllocateAddress()
				assocID, _ := h.Backend.AssociateAddress(addr.AllocationID, instances[0].ID)

				return fmt.Sprintf(
					"Action=DisassociateAddress&Version=2016-11-15&AssociationId=%s",
					url.QueryEscape(assocID),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DisassociateAddressResponse"},
		},
		{
			name: "AttachInternetGateway_success",
			setupFn: func(h *ec2.Handler) string {
				igw, _ := h.Backend.CreateInternetGateway()

				return fmt.Sprintf(
					"Action=AttachInternetGateway&Version=2016-11-15&InternetGatewayId=%s&VpcId=vpc-default",
					url.QueryEscape(igw.ID),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"AttachInternetGatewayResponse"},
		},
		{
			name: "DetachInternetGateway_success",
			setupFn: func(h *ec2.Handler) string {
				igw, _ := h.Backend.CreateInternetGateway()
				_ = h.Backend.AttachInternetGateway(igw.ID, "vpc-default")

				return fmt.Sprintf(
					"Action=DetachInternetGateway&Version=2016-11-15&InternetGatewayId=%s&VpcId=vpc-default",
					url.QueryEscape(igw.ID),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DetachInternetGatewayResponse"},
		},
		{
			name: "DisassociateRouteTable_success",
			setupFn: func(h *ec2.Handler) string {
				rt, _ := h.Backend.CreateRouteTable("vpc-default")
				assocID, _ := h.Backend.AssociateRouteTable(rt.ID, "subnet-default")

				return fmt.Sprintf(
					"Action=DisassociateRouteTable&Version=2016-11-15&AssociationId=%s",
					url.QueryEscape(assocID),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DisassociateRouteTableResponse"},
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
			for _, want := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}
