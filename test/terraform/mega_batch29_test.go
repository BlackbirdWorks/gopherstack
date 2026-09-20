package terraform_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kafkasvc "github.com/aws/aws-sdk-go-v2/service/kafka"
	kafkatypes "github.com/aws/aws-sdk-go-v2/service/kafka/types"
	transfersvc "github.com/aws/aws-sdk-go-v2/service/transfer"
	transfertypes "github.com/aws/aws-sdk-go-v2/service/transfer/types"
	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"
)

// TestTerraform_MegaBatch29 provisions Transfer Family (server, user, SSH key,
// access, certificate, profile x2, agreement, connector, workflow, tag) and
// Kafka/MSK (two provisioned clusters, cluster policy, VPC connection,
// serverless cluster, SCRAM secret association x2, replicator) resources via
// Terraform and verifies each through its own SDK client's Describe path.
func TestTerraform_MegaBatch29(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "mega-batch-29",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				certPEM, _, err := devtls.GenerateSelfSignedCertPEM("mega-batch-29.example.com")
				require.NoError(t, err, "generate self-signed cert")

				pub, _, err := ed25519.GenerateKey(rand.Reader)
				require.NoError(t, err, "generate ed25519 key")

				sshPub, err := ssh.NewPublicKey(pub)
				require.NoError(t, err, "wrap ed25519 public key")

				return map[string]any{
					"CertPEM":      strings.ReplaceAll(strings.TrimSpace(string(certPEM)), "\n", `\n`),
					"SSHPublicKey": strings.TrimSpace(string(ssh.MarshalAuthorizedKey(sshPub))),
				}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				verifyMegaBatch29Transfer(ctx, t)
				verifyMegaBatch29Kafka(ctx, t)
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

func verifyMegaBatch29Transfer(ctx context.Context, t *testing.T) {
	t.Helper()

	client := createTransferClient(t)

	listOut, err := client.ListServers(ctx, &transfersvc.ListServersInput{})
	require.NoError(t, err, "ListServers should succeed")

	serverID := ""

	for _, s := range listOut.Servers {
		descOut, descErr := client.DescribeServer(ctx, &transfersvc.DescribeServerInput{ServerId: s.ServerId})
		require.NoError(t, descErr, "DescribeServer should succeed")

		for _, tag := range descOut.Server.Tags {
			if aws.ToString(tag.Key) == "Name" && aws.ToString(tag.Value) == "mega-batch-29-server" {
				serverID = aws.ToString(s.ServerId)
			}
		}
	}

	require.NotEmpty(t, serverID, "mega-batch-29-server not found")

	userOut, err := client.DescribeUser(ctx, &transfersvc.DescribeUserInput{
		ServerId: aws.String(serverID),
		UserName: aws.String("mega-batch-29-user"),
	})
	require.NoError(t, err, "DescribeUser should succeed")
	require.Len(t, userOut.User.SshPublicKeys, 1)
	assert.Contains(t, aws.ToString(userOut.User.HomeDirectory), "mega-batch-29-transfer-bucket")

	accessOut, err := client.DescribeAccess(ctx, &transfersvc.DescribeAccessInput{
		ServerId:   aws.String(serverID),
		ExternalId: aws.String("S-1-1-12-1234567890-123456789-1234567890-1234"),
	})
	require.NoError(t, err, "DescribeAccess should succeed")
	assert.Equal(t, "S-1-1-12-1234567890-123456789-1234567890-1234", aws.ToString(accessOut.Access.ExternalId))

	certsOut, err := client.ListCertificates(ctx, &transfersvc.ListCertificatesInput{})
	require.NoError(t, err, "ListCertificates should succeed")
	require.NotEmpty(t, certsOut.Certificates)

	certOut, err := client.DescribeCertificate(ctx, &transfersvc.DescribeCertificateInput{
		CertificateId: certsOut.Certificates[0].CertificateId,
	})
	require.NoError(t, err, "DescribeCertificate should succeed")
	assert.Equal(t, transfertypes.CertificateUsageTypeSigning, certOut.Certificate.Usage)

	profilesOut, err := client.ListProfiles(ctx, &transfersvc.ListProfilesInput{})
	require.NoError(t, err, "ListProfiles should succeed")

	var localProfileID, partnerProfileID string

	for _, p := range profilesOut.Profiles {
		profOut, profErr := client.DescribeProfile(ctx, &transfersvc.DescribeProfileInput{ProfileId: p.ProfileId})
		require.NoError(t, profErr, "DescribeProfile should succeed")

		switch aws.ToString(profOut.Profile.As2Id) {
		case "MEGABATCH29LOCAL":
			localProfileID = aws.ToString(p.ProfileId)
		case "MEGABATCH29PARTNER":
			partnerProfileID = aws.ToString(p.ProfileId)
		}
	}

	require.NotEmpty(t, localProfileID, "mega-batch-29 local profile not found")
	require.NotEmpty(t, partnerProfileID, "mega-batch-29 partner profile not found")

	agreementsOut, err := client.ListAgreements(ctx, &transfersvc.ListAgreementsInput{ServerId: aws.String(serverID)})
	require.NoError(t, err, "ListAgreements should succeed")
	require.NotEmpty(t, agreementsOut.Agreements)

	agreementOut, err := client.DescribeAgreement(ctx, &transfersvc.DescribeAgreementInput{
		ServerId:    aws.String(serverID),
		AgreementId: agreementsOut.Agreements[0].AgreementId,
	})
	require.NoError(t, err, "DescribeAgreement should succeed")
	assert.Equal(t, localProfileID, aws.ToString(agreementOut.Agreement.LocalProfileId))
	assert.Equal(t, partnerProfileID, aws.ToString(agreementOut.Agreement.PartnerProfileId))

	connectorsOut, err := client.ListConnectors(ctx, &transfersvc.ListConnectorsInput{})
	require.NoError(t, err, "ListConnectors should succeed")
	require.NotEmpty(t, connectorsOut.Connectors)

	connOut, err := client.DescribeConnector(ctx, &transfersvc.DescribeConnectorInput{
		ConnectorId: connectorsOut.Connectors[0].ConnectorId,
	})
	require.NoError(t, err, "DescribeConnector should succeed")
	assert.Equal(t, "https://mega-batch-29.example.com/as2", aws.ToString(connOut.Connector.Url))

	workflowsOut, err := client.ListWorkflows(ctx, &transfersvc.ListWorkflowsInput{})
	require.NoError(t, err, "ListWorkflows should succeed")
	require.NotEmpty(t, workflowsOut.Workflows)

	wfOut, err := client.DescribeWorkflow(ctx, &transfersvc.DescribeWorkflowInput{
		WorkflowId: workflowsOut.Workflows[0].WorkflowId,
	})
	require.NoError(t, err, "DescribeWorkflow should succeed")
	require.Len(t, wfOut.Workflow.Steps, 1)
	assert.Equal(t, transfertypes.WorkflowStepTypeCopy, wfOut.Workflow.Steps[0].Type)

	serverDescOut, err := client.DescribeServer(ctx, &transfersvc.DescribeServerInput{ServerId: aws.String(serverID)})
	require.NoError(t, err, "DescribeServer should succeed")

	tagsOut, err := client.ListTagsForResource(ctx, &transfersvc.ListTagsForResourceInput{
		Arn: serverDescOut.Server.Arn,
	})
	require.NoError(t, err, "ListTagsForResource should succeed")

	foundTag := false

	for _, tag := range tagsOut.Tags {
		if aws.ToString(tag.Key) == "Environment" && aws.ToString(tag.Value) == "mega-batch-29" {
			foundTag = true
		}
	}

	assert.True(t, foundTag, "Environment=mega-batch-29 tag not found on transfer server")
}

func verifyMegaBatch29Kafka(ctx context.Context, t *testing.T) {
	t.Helper()

	client := createKafkaClient(t)

	listOut, err := client.ListClusters(ctx, &kafkasvc.ListClustersInput{
		ClusterNameFilter: aws.String("mega-batch-29-"),
	})
	require.NoError(t, err, "ListClusters should succeed")

	var sourceArn, targetArn string

	for _, c := range listOut.ClusterInfoList {
		switch aws.ToString(c.ClusterName) {
		case "mega-batch-29-source":
			sourceArn = aws.ToString(c.ClusterArn)
		case "mega-batch-29-target":
			targetArn = aws.ToString(c.ClusterArn)
		}
	}

	require.NotEmpty(t, sourceArn, "mega-batch-29-source cluster not found")
	require.NotEmpty(t, targetArn, "mega-batch-29-target cluster not found")

	policyOut, err := client.GetClusterPolicy(ctx, &kafkasvc.GetClusterPolicyInput{ClusterArn: aws.String(targetArn)})
	require.NoError(t, err, "GetClusterPolicy should succeed")
	assert.Contains(t, aws.ToString(policyOut.Policy), "MegaBatch29ClusterPolicy")

	vpcConnsOut, err := client.ListVpcConnections(ctx, &kafkasvc.ListVpcConnectionsInput{})
	require.NoError(t, err, "ListVpcConnections should succeed")

	foundVpcConn := false

	for _, v := range vpcConnsOut.VpcConnections {
		if aws.ToString(v.TargetClusterArn) == targetArn {
			foundVpcConn = true
		}
	}

	assert.True(t, foundVpcConn, "mega-batch-29 VPC connection not found")

	serverlessOut, err := client.ListClustersV2(ctx, &kafkasvc.ListClustersV2Input{
		ClusterNameFilter: aws.String("mega-batch-29-serverless"),
		ClusterTypeFilter: aws.String("SERVERLESS"),
	})
	require.NoError(t, err, "ListClustersV2 should succeed")
	require.NotEmpty(t, serverlessOut.ClusterInfoList)
	assert.Equal(t, kafkatypes.ClusterTypeServerless, serverlessOut.ClusterInfoList[0].ClusterType)

	scramOut, err := client.ListScramSecrets(ctx, &kafkasvc.ListScramSecretsInput{ClusterArn: aws.String(targetArn)})
	require.NoError(t, err, "ListScramSecrets should succeed")
	assert.NotEmpty(t, scramOut.SecretArnList)

	singleScramOut, err := client.ListScramSecrets(
		ctx,
		&kafkasvc.ListScramSecretsInput{ClusterArn: aws.String(sourceArn)},
	)
	require.NoError(t, err, "ListScramSecrets (source) should succeed")
	assert.NotEmpty(t, singleScramOut.SecretArnList)

	replicatorsOut, err := client.ListReplicators(ctx, &kafkasvc.ListReplicatorsInput{})
	require.NoError(t, err, "ListReplicators should succeed")

	replicatorArn := ""

	for _, r := range replicatorsOut.Replicators {
		if aws.ToString(r.ReplicatorName) == "mega-batch-29-replicator" {
			replicatorArn = aws.ToString(r.ReplicatorArn)
		}
	}

	require.NotEmpty(t, replicatorArn, "mega-batch-29-replicator not found")

	replicatorOut, err := client.DescribeReplicator(ctx,
		&kafkasvc.DescribeReplicatorInput{ReplicatorArn: aws.String(replicatorArn)})
	require.NoError(t, err, "DescribeReplicator should succeed")
	require.Len(t, replicatorOut.ReplicationInfoList, 1)
	require.Len(t, replicatorOut.KafkaClusters, 2)

	gotArns := make([]string, 0, len(replicatorOut.KafkaClusters))

	for _, kc := range replicatorOut.KafkaClusters {
		require.NotNil(t, kc.AmazonMskCluster)
		gotArns = append(gotArns, aws.ToString(kc.AmazonMskCluster.MskClusterArn))
	}

	assert.ElementsMatch(t, []string{sourceArn, targetArn}, gotArns)
}
