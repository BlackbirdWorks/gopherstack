package app

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	ssmbackend "github.com/blackbirdworks/gopherstack/services/ssm"
)

const ssmRunCommandImage = "alpine:3.21"

// containerCompute backs each instance with a plain sleeping container.
type containerCompute struct{ t *testing.T }

func (c containerCompute) Launch(ctx context.Context, req ec2backend.LaunchRequest) (ec2backend.LaunchResult, error) {
	out, err := exec.CommandContext(ctx, "docker", "run", "-d", "--name", "gs-ssm-"+req.InstanceID,
		ssmRunCommandImage, "sleep", "600").Output()
	if err != nil {
		return ec2backend.LaunchResult{}, err
	}

	id := strings.TrimSpace(string(out))
	c.t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", id).Run() })

	return ec2backend.LaunchResult{ProviderID: id}, nil
}

func (containerCompute) Terminate(ctx context.Context, _, id string) error {
	return exec.CommandContext(ctx, "docker", "rm", "-f", id).Run()
}
func (containerCompute) Start(context.Context, string, string) error { return nil }
func (containerCompute) Stop(context.Context, string, string) error  { return nil }

func TestSSMRunCommandInDockerInstance(t *testing.T) {
	t.Parallel()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available")
	}

	if err := exec.Command("docker", "image", "inspect", ssmRunCommandImage).Run(); err != nil {
		t.Skipf("image %s not present", ssmRunCommandImage)
	}

	fx := newSFNFixture(t)
	byName := serviceByName(fx.services)

	h := ec2HandlerOf(byName)
	require.NotNil(t, h)

	bk, ok := h.Backend.(*ec2backend.InMemoryBackend)
	require.True(t, ok)
	bk.WithCompute(containerCompute{t: t})

	executor, err := ssmbackend.NewDockerScriptExecutor()
	require.NoError(t, err)
	installSSMRunCommand(byName, executor)

	ctx := t.Context()
	ec2c, ssmc, s3c := ec2.NewFromConfig(fx.cfg), ssm.NewFromConfig(fx.cfg), s3.NewFromConfig(fx.cfg)

	run, err := ec2c.RunInstances(ctx, &ec2.RunInstancesInput{
		ImageId: aws.String("ami-0abc"), MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
		InstanceType:       ec2types.InstanceTypeT3Micro,
		IamInstanceProfile: &ec2types.IamInstanceProfileSpecification{Name: aws.String("web")},
	})
	require.NoError(t, err)

	id := aws.ToString(run.Instances[0].InstanceId)

	info, err := ssmc.DescribeInstanceInformation(ctx, &ssm.DescribeInstanceInformationInput{})
	require.NoError(t, err)
	require.Len(t, info.InstanceInformationList, 1)
	assert.Equal(t, id, aws.ToString(info.InstanceInformationList[0].InstanceId))

	_, err = s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("ssm-out")})
	require.NoError(t, err)

	sent, err := ssmc.SendCommand(ctx, &ssm.SendCommandInput{
		DocumentName:       aws.String("AWS-RunShellScript"),
		InstanceIds:        []string{id},
		Parameters:         map[string][]string{"commands": {"echo hi; exit 3"}},
		OutputS3BucketName: aws.String("ssm-out"),
	})
	require.NoError(t, err)

	cmdID := aws.ToString(sent.Command.CommandId)

	var inv *ssm.GetCommandInvocationOutput

	require.Eventually(t, func() bool {
		inv, err = ssmc.GetCommandInvocation(ctx, &ssm.GetCommandInvocationInput{
			CommandId: aws.String(cmdID), InstanceId: aws.String(id),
		})

		return err == nil && inv.Status != "InProgress" && inv.Status != "Pending"
	}, 60*time.Second, 200*time.Millisecond)

	assert.Equal(t, "Failed", string(inv.Status))
	assert.Equal(t, int32(3), inv.ResponseCode)
	assert.Equal(t, "hi\n", aws.ToString(inv.StandardOutputContent))

	obj, err := s3c.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("ssm-out"),
		Key:    aws.String(cmdID + "/" + id + "/awsrunShellScript/0.awsrunShellScript/stdout"),
	})
	require.NoError(t, err)

	body := make([]byte, 16)
	n, _ := obj.Body.Read(body)
	assert.Equal(t, "hi\n", string(body[:n]))

	slow, err := ssmc.SendCommand(ctx, &ssm.SendCommandInput{
		DocumentName: aws.String("AWS-RunShellScript"),
		InstanceIds:  []string{id},
		Parameters:   map[string][]string{"commands": {"sleep 777"}},
	})
	require.NoError(t, err)

	slowID := aws.ToString(slow.Command.CommandId)

	require.Eventually(t, func() bool {
		out, _ := exec.Command("docker", "exec", "gs-ssm-"+id, "ps").Output()

		return strings.Contains(string(out), "sleep 777")
	}, 30*time.Second, 200*time.Millisecond)

	_, err = ssmc.CancelCommand(ctx, &ssm.CancelCommandInput{CommandId: aws.String(slowID)})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		out, _ := exec.Command("docker", "exec", "gs-ssm-"+id, "ps").Output()

		return !strings.Contains(string(out), "sleep 777")
	}, 30*time.Second, 200*time.Millisecond)

	got, err := ssmc.GetCommandInvocation(ctx, &ssm.GetCommandInvocationInput{
		CommandId: aws.String(slowID), InstanceId: aws.String(id),
	})
	require.NoError(t, err)
	assert.Equal(t, "Cancelled", string(got.Status))
}
