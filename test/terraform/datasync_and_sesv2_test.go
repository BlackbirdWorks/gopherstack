package terraform_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	datasyncsvc "github.com/aws/aws-sdk-go-v2/service/datasync"
	datasynctypes "github.com/aws/aws-sdk-go-v2/service/datasync/types"
	sesv2svc "github.com/aws/aws-sdk-go-v2/service/sesv2"
	sesv2types "github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_DatasyncAndSesv2 provisions DataSync (agent, EFS/NFS/SMB/HDFS/
// object-storage/Azure Blob locations, task -- the FSx-backed locations are
// split into datasync-fsx-locations.tf since each backing FSx file system's create/
// delete waiter takes tens of seconds to minutes) and SESv2 (email identity
// feedback/mail-from/policy
// attributes, configuration set + event destination, contact list,
// dedicated IP pool + assignment, account suppression + VDM attributes)
// resources via Terraform and verifies each through its own SDK client's
// Get/Describe path.
func TestTerraform_DatasyncAndSesv2(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "datasync-and-sesv2",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				verifyDatasyncAndSesv2DataSync(ctx, t)
				verifyDatasyncAndSesv2SESv2(ctx, t)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

func verifyDatasyncAndSesv2DataSync(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := datasyncsvc.NewFromConfig(cfg, func(o *datasyncsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	agentsOut, err := client.ListAgents(ctx, &datasyncsvc.ListAgentsInput{})
	require.NoError(t, err, "ListAgents should succeed")

	var agentARN string

	for _, a := range agentsOut.Agents {
		if aws.ToString(a.Name) == "dssv-agent" {
			agentARN = aws.ToString(a.AgentArn)
		}
	}

	require.NotEmpty(t, agentARN, "agent should be listed")

	agentOut, err := client.DescribeAgent(ctx, &datasyncsvc.DescribeAgentInput{AgentArn: aws.String(agentARN)})
	require.NoError(t, err, "DescribeAgent should succeed")
	assert.Equal(t, "dssv-agent", aws.ToString(agentOut.Name))

	locsOut, err := client.ListLocations(ctx, &datasyncsvc.ListLocationsInput{})
	require.NoError(t, err, "ListLocations should succeed")
	require.GreaterOrEqual(t, len(locsOut.Locations), 6, "should have created 6 datasync locations")

	arns := make([]string, len(locsOut.Locations))
	for i, l := range locsOut.Locations {
		arns[i] = aws.ToString(l.LocationArn)
	}

	var efsOut *datasyncsvc.DescribeLocationEfsOutput

	for _, a := range arns {
		in := &datasyncsvc.DescribeLocationEfsInput{LocationArn: aws.String(a)}
		if out, derr := client.DescribeLocationEfs(ctx, in); derr == nil {
			efsOut = out

			break
		}
	}

	require.NotNil(t, efsOut, "EFS location should be describable")
	require.NotNil(t, efsOut.Ec2Config)

	var nfsOut *datasyncsvc.DescribeLocationNfsOutput

	for _, a := range arns {
		out, derr := client.DescribeLocationNfs(ctx, &datasyncsvc.DescribeLocationNfsInput{LocationArn: aws.String(a)})
		if derr == nil && out.OnPremConfig != nil {
			nfsOut = out

			break
		}
	}

	require.NotNil(t, nfsOut, "NFS location should be describable")
	assert.Contains(t, nfsOut.OnPremConfig.AgentArns, agentARN)

	var smbOut *datasyncsvc.DescribeLocationSmbOutput

	for _, a := range arns {
		in := &datasyncsvc.DescribeLocationSmbInput{LocationArn: aws.String(a)}
		if out, derr := client.DescribeLocationSmb(ctx, in); derr == nil {
			smbOut = out

			break
		}
	}

	require.NotNil(t, smbOut, "SMB location should be describable")
	assert.Equal(t, "Guest", aws.ToString(smbOut.User))

	var hdfsOut *datasyncsvc.DescribeLocationHdfsOutput

	for _, a := range arns {
		in := &datasyncsvc.DescribeLocationHdfsInput{LocationArn: aws.String(a)}
		if out, derr := client.DescribeLocationHdfs(ctx, in); derr == nil {
			hdfsOut = out

			break
		}
	}

	require.NotNil(t, hdfsOut, "HDFS location should be describable")
	assert.Equal(t, "dssv-user", aws.ToString(hdfsOut.SimpleUser))
	require.Len(t, hdfsOut.NameNodes, 1)
	assert.Equal(t, "namenode.dssv.example.com", aws.ToString(hdfsOut.NameNodes[0].Hostname))

	var osOut *datasyncsvc.DescribeLocationObjectStorageOutput

	for _, a := range arns {
		in := &datasyncsvc.DescribeLocationObjectStorageInput{LocationArn: aws.String(a)}
		if out, derr := client.DescribeLocationObjectStorage(ctx, in); derr == nil {
			osOut = out

			break
		}
	}

	require.NotNil(t, osOut, "object storage location should be describable")
	assert.Equal(t, "dssv-bucket", bucketNameFromURI(aws.ToString(osOut.LocationUri)))

	var azOut *datasyncsvc.DescribeLocationAzureBlobOutput

	for _, a := range arns {
		in := &datasyncsvc.DescribeLocationAzureBlobInput{LocationArn: aws.String(a)}
		if out, derr := client.DescribeLocationAzureBlob(ctx, in); derr == nil {
			azOut = out

			break
		}
	}

	require.NotNil(t, azOut, "Azure Blob location should be describable")
	assert.Equal(t, datasynctypes.AzureBlobAuthenticationTypeSas, azOut.AuthenticationType)

	taskOut, err := client.ListTasks(ctx, &datasyncsvc.ListTasksInput{})
	require.NoError(t, err, "ListTasks should succeed")

	var taskARN string

	for _, tsk := range taskOut.Tasks {
		if aws.ToString(tsk.Name) == "dssv-task" {
			taskARN = aws.ToString(tsk.TaskArn)
		}
	}

	require.NotEmpty(t, taskARN, "task should be listed")

	describeTaskOut, err := client.DescribeTask(ctx, &datasyncsvc.DescribeTaskInput{TaskArn: aws.String(taskARN)})
	require.NoError(t, err, "DescribeTask should succeed")
	assert.Equal(t, "dssv-task", aws.ToString(describeTaskOut.Name))
}

// bucketNameFromURI extracts the bucket name from an
// object-storage://host/bucket/subdirectory URI.
func bucketNameFromURI(uri string) string {
	rest, ok := strings.CutPrefix(uri, "object-storage://")
	if !ok {
		return ""
	}

	_, rest, ok = strings.Cut(rest, "/")
	if !ok {
		return ""
	}

	bucket, _, _ := strings.Cut(rest, "/")

	return bucket
}

func verifyDatasyncAndSesv2SESv2(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := sesv2svc.NewFromConfig(cfg, func(o *sesv2svc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	identity := "dssv.example.com"

	idOut, err := client.GetEmailIdentity(ctx, &sesv2svc.GetEmailIdentityInput{EmailIdentity: aws.String(identity)})
	require.NoError(t, err, "GetEmailIdentity should succeed")
	assert.True(t, idOut.FeedbackForwardingStatus)
	require.NotNil(t, idOut.MailFromAttributes)
	assert.Equal(t, "bounce.dssv.example.com", aws.ToString(idOut.MailFromAttributes.MailFromDomain))
	assert.Contains(t, idOut.Policies, "dssv-identity-policy")

	configOut, err := client.GetConfigurationSetEventDestinations(
		ctx,
		&sesv2svc.GetConfigurationSetEventDestinationsInput{
			ConfigurationSetName: aws.String("dssv-config-set"),
		},
	)
	require.NoError(t, err, "GetConfigurationSetEventDestinations should succeed")
	require.Len(t, configOut.EventDestinations, 1)
	assert.Equal(t, "dssv-event-dest", aws.ToString(configOut.EventDestinations[0].Name))
	require.NotNil(t, configOut.EventDestinations[0].SnsDestination)

	contactListOut, err := client.GetContactList(
		ctx,
		&sesv2svc.GetContactListInput{ContactListName: aws.String("dssv-contacts")},
	)
	require.NoError(t, err, "GetContactList should succeed")
	require.Len(t, contactListOut.Topics, 1)
	assert.Equal(t, "dssv-topic", aws.ToString(contactListOut.Topics[0].TopicName))

	poolOut, err := client.GetDedicatedIpPool(
		ctx,
		&sesv2svc.GetDedicatedIpPoolInput{PoolName: aws.String("dssv-pool")},
	)
	require.NoError(t, err, "GetDedicatedIpPool should succeed")
	require.NotNil(t, poolOut.DedicatedIpPool)
	assert.Equal(t, "dssv-pool", aws.ToString(poolOut.DedicatedIpPool.PoolName))

	ipsOut, err := client.GetDedicatedIps(
		ctx,
		&sesv2svc.GetDedicatedIpsInput{PoolName: aws.String("dssv-pool")},
	)
	require.NoError(t, err, "GetDedicatedIps should succeed")

	var foundIP bool

	for _, ip := range ipsOut.DedicatedIps {
		if aws.ToString(ip.Ip) == "10.20.30.40" {
			foundIP = true
		}
	}

	assert.True(t, foundIP, "dedicated IP should be assigned to the pool")

	acctOut, err := client.GetAccount(ctx, &sesv2svc.GetAccountInput{})
	require.NoError(t, err, "GetAccount should succeed")
	require.NotNil(t, acctOut.SuppressionAttributes)
	assert.ElementsMatch(
		t,
		[]sesv2types.SuppressionListReason{
			sesv2types.SuppressionListReasonBounce,
			sesv2types.SuppressionListReasonComplaint,
		},
		acctOut.SuppressionAttributes.SuppressedReasons,
	)
	require.NotNil(t, acctOut.VdmAttributes)
	assert.Equal(t, sesv2types.FeatureStatusEnabled, acctOut.VdmAttributes.VdmEnabled)
}
