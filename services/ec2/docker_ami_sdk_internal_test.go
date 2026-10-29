package ec2

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func newInternalSDKClient(t *testing.T, h *Handler) *ec2sdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return ec2sdk.NewFromConfig(cfg, func(o *ec2sdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })
}

func TestSDKDockerUnknownAMI(t *testing.T) {
	t.Parallel()

	h, _, api := newDockerHandler(t, DockerComputeConfig{})
	client := newInternalSDKClient(t, h)

	_, err := client.RunInstances(t.Context(), &ec2sdk.RunInstancesInput{
		ImageId: aws.String("ami-00000000deadbeef0"), MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
	})

	var apiErr smithy.APIError

	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "InvalidAMIID.NotFound", apiErr.ErrorCode())
	assert.Empty(t, api.created)
}

func TestSDKDockerLocalImageIsAMI(t *testing.T) {
	t.Parallel()

	h, _, api := newDockerHandler(t, DockerComputeConfig{Image: "alpine:3.20"})
	client := newInternalSDKClient(t, h)
	ctx := t.Context()

	api.images["gopherstack-ec2/web:"+amiDiscover] = "x"

	out, err := client.DescribeImages(ctx, &ec2sdk.DescribeImagesInput{Owners: []string{"self"}})
	require.NoError(t, err)
	require.Len(t, out.Images, 1)
	assert.Equal(t, amiDiscover, aws.ToString(out.Images[0].ImageId))
	assert.Equal(t, "web", aws.ToString(out.Images[0].Name))

	run, err := client.RunInstances(ctx, &ec2sdk.RunInstancesInput{
		ImageId: aws.String(amiDiscover), MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, api.created, 1)
	assert.Equal(t, "gopherstack-ec2/web:"+amiDiscover, api.created[0].Config.Image)

	img, err := client.CreateImage(ctx, &ec2sdk.CreateImageInput{
		InstanceId: run.Instances[0].InstanceId, Name: aws.String("snap one"),
	})
	require.NoError(t, err)
	require.Len(t, api.commits, 1)
	assert.Equal(t, "gopherstack-ec2/snap-one:"+aws.ToString(img.ImageId), api.commits[0].Reference)

	_, err = client.RunInstances(ctx, &ec2sdk.RunInstancesInput{
		ImageId: img.ImageId, MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
	})
	require.NoError(t, err)
	assert.Equal(t, api.commits[0].Reference, api.created[1].Config.Image)
}

func TestSDKDockerEBSVolumes(t *testing.T) {
	t.Parallel()

	h, _, api := newDockerHandler(t, DockerComputeConfig{EBSVolumes: true})
	client := newInternalSDKClient(t, h)
	ctx := t.Context()

	run, err := client.RunInstances(ctx, &ec2sdk.RunInstancesInput{
		ImageId: aws.String(amiAL2), MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
		BlockDeviceMappings: []types.BlockDeviceMapping{{
			DeviceName: aws.String("/dev/sdf"),
			Ebs:        &types.EbsBlockDevice{VolumeSize: aws.Int32(2), DeleteOnTermination: aws.Bool(false)},
		}},
	})
	require.NoError(t, err)
	require.Len(t, api.created[0].HostConfig.Mounts, 1)
	assert.Equal(t, "/mnt/ebs/sdf", api.created[0].HostConfig.Mounts[0].Target)

	vol, err := client.CreateVolume(ctx, &ec2sdk.CreateVolumeInput{
		AvailabilityZone: aws.String("us-east-1a"), Size: aws.Int32(1),
	})
	require.NoError(t, err)

	_, err = client.AttachVolume(ctx, &ec2sdk.AttachVolumeInput{
		VolumeId: vol.VolumeId, InstanceId: run.Instances[0].InstanceId, Device: aws.String("/dev/sdg"),
	})
	require.NoError(t, err)
	api.writeFile(richProvider, "/mnt/ebs/sdg/f", []byte("v"))

	_, err = client.DetachVolume(ctx, &ec2sdk.DetachVolumeInput{VolumeId: vol.VolumeId})
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), api.files[fakeVolPrefix+aws.ToString(vol.VolumeId)]["/f"])

	_, err = client.DeleteVolume(ctx, &ec2sdk.DeleteVolumeInput{VolumeId: vol.VolumeId})
	require.NoError(t, err)
	assert.NotContains(t, api.volumes, "gopherstack-ebs-"+aws.ToString(vol.VolumeId))
}
