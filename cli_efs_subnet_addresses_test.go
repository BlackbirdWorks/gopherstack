package main

import (
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	efssdk "github.com/aws/aws-sdk-go-v2/service/efs"
	efstypes "github.com/aws/aws-sdk-go-v2/service/efs/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	efsbackend "github.com/blackbirdworks/gopherstack/services/efs"
)

func TestInitializeServices_EFSNoFreeAddresses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cidr        string
		explicitIP  string
		enis        int
		wantNoFree  bool
		wantSuccess bool
	}{
		{name: "exhausted-subnet-rejected", cidr: "10.0.0.0/28", enis: 11, wantNoFree: true},
		{name: "one-address-left-succeeds", cidr: "10.0.0.0/28", enis: 10, wantSuccess: true},
		{name: "explicit-ip-skips-check", cidr: "10.0.0.0/28", enis: 11, explicitIP: "10.0.0.9", wantSuccess: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			services, err := initializeServices(newTestAppContext(t, 19800, 19900))
			require.NoError(t, err)

			byName := serviceByName(services)

			ec2H, ok := byName["EC2"].(*ec2backend.Handler)
			require.True(t, ok)

			efsH, ok := byName["EFS"].(*efsbackend.Handler)
			require.True(t, ok)

			vpc, err := ec2H.Backend.CreateVpc("10.0.0.0/16", "")
			require.NoError(t, err)

			subnet, err := ec2H.Backend.CreateSubnet(vpc.ID, tt.cidr, "us-east-1a")
			require.NoError(t, err)

			for range tt.enis {
				_, err = ec2H.Backend.CreateNetworkInterface(subnet.ID, "fill")
				require.NoError(t, err)
			}

			e := echo.New()
			registry := service.NewRegistry()
			require.NoError(t, registry.Register(efsH))
			e.Use(service.NewServiceRouter(registry).RouteHandler())

			srv := httptest.NewServer(e)
			t.Cleanup(srv.Close)

			cfg, err := awscfg.LoadDefaultConfig(
				t.Context(),
				awscfg.WithRegion("us-east-1"),
				awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
			)
			require.NoError(t, err)

			client := efssdk.NewFromConfig(cfg, func(o *efssdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })

			fs, err := client.CreateFileSystem(t.Context(), &efssdk.CreateFileSystemInput{
				CreationToken: aws.String("free-" + tt.name),
			})
			require.NoError(t, err)

			in := &efssdk.CreateMountTargetInput{FileSystemId: fs.FileSystemId, SubnetId: aws.String(subnet.ID)}
			if tt.explicitIP != "" {
				in.IpAddress = aws.String(tt.explicitIP)
			}

			_, err = client.CreateMountTarget(t.Context(), in)

			var noFree *efstypes.NoFreeAddressesInSubnet

			assert.Equal(t, tt.wantNoFree, errors.As(err, &noFree), "error: %v", err)

			if tt.wantSuccess {
				assert.NoError(t, err)
			}
		})
	}
}
