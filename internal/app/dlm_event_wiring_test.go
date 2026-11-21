package app

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	dlmbackend "github.com/blackbirdworks/gopherstack/services/dlm"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestDLMEventPolicyRunsOnSnapshotShare(t *testing.T) {
	t.Parallel()

	const owner = "111111111111"

	tests := []struct {
		name     string
		regex    string
		wantCopy bool
	}{
		{"matching", "^Share.*$", true},
		{"regex_miss", "^Other$", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ec2Bk := ec2backend.NewInMemoryBackend("000000000000", "us-east-1")
			ec2H := ec2backend.NewHandler(ec2Bk)
			ec2H.EnableRegions(t.Context())
			dlmBk := dlmbackend.NewInMemoryBackend("000000000000", "us-east-1")
			dlmH := dlmbackend.NewHandler(dlmBk)

			wireDLMCompute(map[string]service.Registerable{"EC2": ec2H, "DLM": dlmH})

			_, err := dlmBk.CreateLifecyclePolicy("e", "arn:aws:iam::000000000000:role/r", "ENABLED", nil,
				map[string]any{
					"PolicyType": "EVENT_BASED_POLICY",
					"EventSource": map[string]any{"Type": "MANAGED_CWE", "Parameters": map[string]any{
						"EventType": "shareSnapshot", "SnapshotOwner": []any{owner}, "DescriptionRegex": tc.regex,
					}},
					"Actions": []any{map[string]any{"Name": "copy", "CrossRegionCopy": []any{map[string]any{
						"Target":                  "eu-west-1",
						"EncryptionConfiguration": map[string]any{"Encrypted": false},
					}}}},
				})
			require.NoError(t, err)

			e := echo.New()
			registry := service.NewRegistry()
			require.NoError(t, registry.Register(ec2H))
			e.Use(service.NewServiceRouter(registry).RouteHandler())

			srv := httptest.NewServer(e)
			t.Cleanup(srv.Close)

			cfg, err := awscfg.LoadDefaultConfig(t.Context(), awscfg.WithRegion("us-east-1"),
				awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
			require.NoError(t, err)

			client := ec2sdk.NewFromConfig(cfg, func(o *ec2sdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })

			vol, err := ec2Bk.CreateVolume("us-east-1a", "gp3", 8, "")
			require.NoError(t, err)

			snap, err := ec2Bk.CreateSnapshot(vol.ID, "Share me")
			require.NoError(t, err)
			ec2Bk.ClaimSnapshotOwner(owner, snap.SnapshotID)

			_, err = client.ModifySnapshotAttribute(t.Context(), &ec2sdk.ModifySnapshotAttributeInput{
				SnapshotId: aws.String(snap.SnapshotID),
				CreateVolumePermission: &ec2types.CreateVolumePermissionModifications{
					Add: []ec2types.CreateVolumePermission{{UserId: aws.String("000000000000")}},
				},
			})
			require.NoError(t, err)

			copies, err := ec2H.BackendFor("eu-west-1").DescribeSnapshots(nil)
			require.NoError(t, err)
			assert.Equal(t, tc.wantCopy, len(copies) == 1)
		})
	}
}
