package terraform_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	batchsvc53 "github.com/aws/aws-sdk-go-v2/service/batch"
	codeconnectionssvc53 "github.com/aws/aws-sdk-go-v2/service/codeconnections"
	codeconnectionstypes53 "github.com/aws/aws-sdk-go-v2/service/codeconnections/types"
	codedeploysvc53 "github.com/aws/aws-sdk-go-v2/service/codedeploy"
	codestarconnectionssvc53 "github.com/aws/aws-sdk-go-v2/service/codestarconnections"
	codestarconnectionstypes53 "github.com/aws/aws-sdk-go-v2/service/codestarconnections/types"
	ec2svc53 "github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types53 "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	eksvc53 "github.com/aws/aws-sdk-go-v2/service/eks"
	ekstypes53 "github.com/aws/aws-sdk-go-v2/service/eks/types"
	elasticbeanstalksvc53 "github.com/aws/aws-sdk-go-v2/service/elasticbeanstalk"
	networkmanagersvc53 "github.com/aws/aws-sdk-go-v2/service/networkmanager"
	networkmanagertypes53 "github.com/aws/aws-sdk-go-v2/service/networkmanager/types"
	networkmonitorsvc53 "github.com/aws/aws-sdk-go-v2/service/networkmonitor"
	rdssvc53 "github.com/aws/aws-sdk-go-v2/service/rds"
	servicediscoverysvc53 "github.com/aws/aws-sdk-go-v2/service/servicediscovery"
	servicediscoverytypes53 "github.com/aws/aws-sdk-go-v2/service/servicediscovery/types"
	sfnsvc53 "github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes53 "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	transcribesvc53 "github.com/aws/aws-sdk-go-v2/service/transcribe"
	workspacessvc53 "github.com/aws/aws-sdk-go-v2/service/workspaces"
	workspacestypes53 "github.com/aws/aws-sdk-go-v2/service/workspaces/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_MegaBatch53 provisions 31 previously-uncovered class-A
// terraform resource types (EC2 spot datafeed subscription, network
// interface permission, EBS fast snapshot restore, EBS snapshot import, AMI
// from instance, the VPC Route Server family, Transit Gateway VPC attachment
// accepter, multicast domain association/group member/source, policy table
// association, prefix list reference, Batch job definition, CodeDeploy
// deployment group, CodeConnections/CodeStarConnections hosts, Service
// Discovery instance, Step Functions alias, Transcribe language model,
// WorkSpaces connection alias, Network Manager connection, CloudWatch
// Network Monitor probe, Elastic Beanstalk application version/configuration
// template, RDS cluster snapshot copy, and EKS identity provider
// config/pod identity association) via Terraform, verifying each through
// its own SDK client.
func TestTerraform_MegaBatch53(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "mega-batch-53",
			setup:   setupEndpoint,
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				verifyMegaBatch53EC2Misc(ctx, t)
				verifyMegaBatch53RouteServer(ctx, t)
				verifyMegaBatch53TransitGateway(ctx, t)
				verifyMegaBatch53Batch(ctx, t)
				verifyMegaBatch53CodeDeploy(ctx, t)
				verifyMegaBatch53Hosts(ctx, t)
				verifyMegaBatch53ServiceDiscovery(ctx, t)
				verifyMegaBatch53SFN(ctx, t)
				verifyMegaBatch53Transcribe(ctx, t)
				verifyMegaBatch53Workspaces(ctx, t)
				verifyMegaBatch53NetworkManager(ctx, t)
				verifyMegaBatch53NetworkMonitor(ctx, t)
				verifyMegaBatch53ElasticBeanstalk(ctx, t)
				verifyMegaBatch53RDS(ctx, t)
				verifyMegaBatch53EKS(ctx, t)
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

func verifyMegaBatch53EC2Misc(ctx context.Context, t *testing.T) {
	t.Helper()
	client := ec2svc53.NewFromConfig(megaConfig(t), func(o *ec2svc53.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	subOut, err := client.DescribeSpotDatafeedSubscription(ctx, &ec2svc53.DescribeSpotDatafeedSubscriptionInput{})
	require.NoError(t, err, "DescribeSpotDatafeedSubscription should succeed")
	require.NotNil(t, subOut.SpotDatafeedSubscription)
	assert.Equal(t, "mega-batch-53-datafeed", aws.ToString(subOut.SpotDatafeedSubscription.Bucket))

	permOut, err := client.DescribeNetworkInterfacePermissions(
		ctx,
		&ec2svc53.DescribeNetworkInterfacePermissionsInput{},
	)
	require.NoError(t, err, "DescribeNetworkInterfacePermissions should succeed")
	findBy(t, permOut.NetworkInterfacePermissions, func(p ec2types53.NetworkInterfacePermission) bool {
		return aws.ToString(p.AwsAccountId) == "000000000000" &&
			p.Permission == ec2types53.InterfacePermissionTypeInstanceAttach
	}, "an INSTANCE-ATTACH network interface permission for account 000000000000")

	amiOut, err := client.DescribeImages(ctx, &ec2svc53.DescribeImagesInput{
		Filters: []ec2types53.Filter{{Name: aws.String("name"), Values: []string{"mega-batch-53-ami"}}},
	})
	require.NoError(t, err, "DescribeImages should succeed")
	require.Len(t, amiOut.Images, 1)

	fsrOut, err := client.DescribeFastSnapshotRestores(ctx, &ec2svc53.DescribeFastSnapshotRestoresInput{})
	require.NoError(t, err, "DescribeFastSnapshotRestores should succeed")
	findBy(t, fsrOut.FastSnapshotRestores, func(r ec2types53.DescribeFastSnapshotRestoreSuccessItem) bool {
		return aws.ToString(r.AvailabilityZone) == "us-east-1a" &&
			r.State == ec2types53.FastSnapshotRestoreStateCodeEnabled
	}, "an enabled fast snapshot restore in us-east-1a")

	importOut, err := client.DescribeImportSnapshotTasks(ctx, &ec2svc53.DescribeImportSnapshotTasksInput{})
	require.NoError(t, err, "DescribeImportSnapshotTasks should succeed")
	task := findBy(t, importOut.ImportSnapshotTasks, func(task ec2types53.ImportSnapshotTask) bool {
		return aws.ToString(task.Description) == "mega-batch-53 imported snapshot"
	}, "the mega-batch-53 import snapshot task")
	require.NotNil(t, task.SnapshotTaskDetail)
	assert.NotEmpty(t, aws.ToString(task.SnapshotTaskDetail.SnapshotId), "import must produce a backing snapshot")
	assert.Equal(t, "completed", aws.ToString(task.SnapshotTaskDetail.Status))
}

func verifyMegaBatch53RouteServer(ctx context.Context, t *testing.T) {
	t.Helper()
	client := ec2svc53.NewFromConfig(megaConfig(t), func(o *ec2svc53.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	rsOut, err := client.DescribeRouteServers(ctx, &ec2svc53.DescribeRouteServersInput{})
	require.NoError(t, err, "DescribeRouteServers should succeed")
	rs := findBy(t, rsOut.RouteServers, func(r ec2types53.RouteServer) bool {
		return aws.ToInt64(r.AmazonSideAsn) == 4210000001
	}, "route server with ASN 4210000001")

	epOut, err := client.DescribeRouteServerEndpoints(ctx, &ec2svc53.DescribeRouteServerEndpointsInput{})
	require.NoError(t, err, "DescribeRouteServerEndpoints should succeed")
	findBy(t, epOut.RouteServerEndpoints, func(e ec2types53.RouteServerEndpoint) bool {
		return aws.ToString(e.RouteServerId) == aws.ToString(rs.RouteServerId)
	}, "a route server endpoint on the mega-batch-53 route server")

	peerOut, err := client.DescribeRouteServerPeers(ctx, &ec2svc53.DescribeRouteServerPeersInput{})
	require.NoError(t, err, "DescribeRouteServerPeers should succeed")
	findBy(t, peerOut.RouteServerPeers, func(p ec2types53.RouteServerPeer) bool {
		return aws.ToString(p.PeerAddress) == "10.213.1.100"
	}, "route server peer 10.213.1.100")
}

func verifyMegaBatch53TransitGateway(ctx context.Context, t *testing.T) {
	t.Helper()
	client := ec2svc53.NewFromConfig(megaConfig(t), func(o *ec2svc53.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	attOut, err := client.DescribeTransitGatewayVpcAttachments(ctx, &ec2svc53.DescribeTransitGatewayVpcAttachmentsInput{
		Filters: []ec2types53.Filter{{Name: aws.String("state"), Values: []string{"available"}}},
	})
	require.NoError(t, err, "DescribeTransitGatewayVpcAttachments should succeed")
	// The mega-batch-53 TGW attachment has exactly 1 subnet (the EKS cluster
	// uses two subnets directly, not via a TGW attachment, so this is unambiguous).
	att := findBy(t, attOut.TransitGatewayVpcAttachments, func(a ec2types53.TransitGatewayVpcAttachment) bool {
		return len(a.SubnetIds) == 1
	}, "an available TGW VPC attachment with one subnet")
	attachmentID := aws.ToString(att.TransitGatewayAttachmentId)
	tgwID := aws.ToString(att.TransitGatewayId)

	domOut, err := client.DescribeTransitGatewayMulticastDomains(
		ctx,
		&ec2svc53.DescribeTransitGatewayMulticastDomainsInput{
			Filters: []ec2types53.Filter{{Name: aws.String("transit-gateway-id"), Values: []string{tgwID}}},
		},
	)
	require.NoError(t, err, "DescribeTransitGatewayMulticastDomains should succeed")
	require.NotEmpty(t, domOut.TransitGatewayMulticastDomains)
	domainID := aws.ToString(domOut.TransitGatewayMulticastDomains[0].TransitGatewayMulticastDomainId)

	mcastAssocOut, err := client.GetTransitGatewayMulticastDomainAssociations(
		ctx, &ec2svc53.GetTransitGatewayMulticastDomainAssociationsInput{
			TransitGatewayMulticastDomainId: aws.String(domainID),
		},
	)
	require.NoError(t, err, "GetTransitGatewayMulticastDomainAssociations should succeed")
	findBy(
		t,
		mcastAssocOut.MulticastDomainAssociations,
		func(a ec2types53.TransitGatewayMulticastDomainAssociation) bool {
			return aws.ToString(a.TransitGatewayAttachmentId) == attachmentID
		},
		"a multicast domain association for the mega-batch-53 TGW attachment",
	)

	groupsOut, err := client.SearchTransitGatewayMulticastGroups(
		ctx,
		&ec2svc53.SearchTransitGatewayMulticastGroupsInput{
			TransitGatewayMulticastDomainId: aws.String(domainID),
		},
	)
	require.NoError(t, err, "SearchTransitGatewayMulticastGroups should succeed")
	findBy(t, groupsOut.MulticastGroups, func(g ec2types53.TransitGatewayMulticastGroup) bool {
		return aws.ToString(g.GroupIpAddress) == "224.0.1.1" && aws.ToBool(g.GroupMember)
	}, "multicast group member 224.0.1.1")
	findBy(t, groupsOut.MulticastGroups, func(g ec2types53.TransitGatewayMulticastGroup) bool {
		return aws.ToString(g.GroupIpAddress) == "224.0.1.1" && aws.ToBool(g.GroupSource)
	}, "multicast group source 224.0.1.1")

	polOut, err := client.DescribeTransitGatewayPolicyTables(ctx, &ec2svc53.DescribeTransitGatewayPolicyTablesInput{})
	require.NoError(t, err, "DescribeTransitGatewayPolicyTables should succeed")
	polTable := findBy(t, polOut.TransitGatewayPolicyTables, func(p ec2types53.TransitGatewayPolicyTable) bool {
		return aws.ToString(p.TransitGatewayId) == tgwID
	}, "a policy table on the mega-batch-53 transit gateway")

	polAssocOut, err := client.GetTransitGatewayPolicyTableAssociations(
		ctx, &ec2svc53.GetTransitGatewayPolicyTableAssociationsInput{
			TransitGatewayPolicyTableId: polTable.TransitGatewayPolicyTableId,
		},
	)
	require.NoError(t, err, "GetTransitGatewayPolicyTableAssociations should succeed")
	findBy(t, polAssocOut.Associations, func(a ec2types53.TransitGatewayPolicyTableAssociation) bool {
		return aws.ToString(a.TransitGatewayAttachmentId) == attachmentID
	}, "a policy table association for the mega-batch-53 TGW attachment")

	rtOut, err := client.DescribeTransitGatewayRouteTables(ctx, &ec2svc53.DescribeTransitGatewayRouteTablesInput{})
	require.NoError(t, err, "DescribeTransitGatewayRouteTables should succeed")
	rt := findBy(t, rtOut.TransitGatewayRouteTables, func(r ec2types53.TransitGatewayRouteTable) bool {
		return aws.ToString(r.TransitGatewayId) == tgwID
	}, "a route table on the mega-batch-53 transit gateway")

	refOut, err := client.GetTransitGatewayPrefixListReferences(
		ctx,
		&ec2svc53.GetTransitGatewayPrefixListReferencesInput{
			TransitGatewayRouteTableId: rt.TransitGatewayRouteTableId,
		},
	)
	require.NoError(t, err, "GetTransitGatewayPrefixListReferences should succeed")
	findBy(t, refOut.TransitGatewayPrefixListReferences, func(r ec2types53.TransitGatewayPrefixListReference) bool {
		return r.TransitGatewayAttachment != nil &&
			aws.ToString(r.TransitGatewayAttachment.TransitGatewayAttachmentId) == attachmentID
	}, "a prefix list reference for the mega-batch-53 TGW attachment")
}

func verifyMegaBatch53Batch(ctx context.Context, t *testing.T) {
	t.Helper()
	client := batchsvc53.NewFromConfig(megaConfig(t), func(o *batchsvc53.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.DescribeJobDefinitions(ctx, &batchsvc53.DescribeJobDefinitionsInput{
		JobDefinitionName: aws.String("mega-batch-53-job-def"),
	})
	require.NoError(t, err, "DescribeJobDefinitions should succeed")
	require.NotEmpty(t, out.JobDefinitions)
	assert.Equal(t, "container", aws.ToString(out.JobDefinitions[0].Type))
}

func verifyMegaBatch53CodeDeploy(ctx context.Context, t *testing.T) {
	t.Helper()
	client := codedeploysvc53.NewFromConfig(megaConfig(t), func(o *codedeploysvc53.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.GetDeploymentGroup(ctx, &codedeploysvc53.GetDeploymentGroupInput{
		ApplicationName:     aws.String("mega-batch-53-app"),
		DeploymentGroupName: aws.String("mega-batch-53-dg"),
	})
	require.NoError(t, err, "GetDeploymentGroup should succeed")
	require.NotNil(t, out.DeploymentGroupInfo)
	require.Len(t, out.DeploymentGroupInfo.Ec2TagFilters, 1)
	assert.Equal(t, "mega-batch-53-instance", aws.ToString(out.DeploymentGroupInfo.Ec2TagFilters[0].Value))
}

func verifyMegaBatch53Hosts(ctx context.Context, t *testing.T) {
	t.Helper()

	ccClient := codeconnectionssvc53.NewFromConfig(megaConfig(t), func(o *codeconnectionssvc53.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	ccOut, err := ccClient.ListHosts(ctx, &codeconnectionssvc53.ListHostsInput{})
	require.NoError(t, err, "codeconnections ListHosts should succeed")
	findBy(t, ccOut.Hosts, func(h codeconnectionstypes53.Host) bool {
		return aws.ToString(h.Name) == "mega-batch-53-cc-host"
	}, "codeconnections host mega-batch-53-cc-host")

	cscClient := codestarconnectionssvc53.NewFromConfig(megaConfig(t), func(o *codestarconnectionssvc53.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	cscOut, err := cscClient.ListHosts(ctx, &codestarconnectionssvc53.ListHostsInput{})
	require.NoError(t, err, "codestarconnections ListHosts should succeed")
	findBy(t, cscOut.Hosts, func(h codestarconnectionstypes53.Host) bool {
		return aws.ToString(h.Name) == "mega-batch-53-csc-host"
	}, "codestarconnections host mega-batch-53-csc-host")
}

func verifyMegaBatch53ServiceDiscovery(ctx context.Context, t *testing.T) {
	t.Helper()
	client := servicediscoverysvc53.NewFromConfig(megaConfig(t), func(o *servicediscoverysvc53.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	svcOut, err := client.ListServices(ctx, &servicediscoverysvc53.ListServicesInput{})
	require.NoError(t, err, "ListServices should succeed")
	svc := findBy(t, svcOut.Services, func(s servicediscoverytypes53.ServiceSummary) bool {
		return aws.ToString(s.Name) == "mega-batch-53-svc"
	}, "service mega-batch-53-svc")

	instOut, err := client.GetInstance(ctx, &servicediscoverysvc53.GetInstanceInput{
		ServiceId:  svc.Id,
		InstanceId: aws.String("mega-batch-53-svc-instance"),
	})
	require.NoError(t, err, "GetInstance should succeed")
	require.NotNil(t, instOut.Instance)
	assert.Equal(t, "10.213.9.9", instOut.Instance.Attributes["AWS_INSTANCE_IPV4"])
}

func verifyMegaBatch53SFN(ctx context.Context, t *testing.T) {
	t.Helper()
	client := sfnsvc53.NewFromConfig(megaConfig(t), func(o *sfnsvc53.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	smArn := "arn:aws:states:us-east-1:000000000000:stateMachine:mega-batch-53-sm"

	out, err := client.ListStateMachineAliases(ctx, &sfnsvc53.ListStateMachineAliasesInput{
		StateMachineArn: aws.String(smArn),
	})
	require.NoError(t, err, "ListStateMachineAliases should succeed")
	findBy(t, out.StateMachineAliases, func(a sfntypes53.StateMachineAliasListItem) bool {
		return strings.HasSuffix(aws.ToString(a.StateMachineAliasArn), ":mega-batch-53-alias")
	}, "state machine alias mega-batch-53-alias")
}

func verifyMegaBatch53Transcribe(ctx context.Context, t *testing.T) {
	t.Helper()
	client := transcribesvc53.NewFromConfig(megaConfig(t), func(o *transcribesvc53.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.DescribeLanguageModel(ctx, &transcribesvc53.DescribeLanguageModelInput{
		ModelName: aws.String("mega-batch-53-lm"),
	})
	require.NoError(t, err, "DescribeLanguageModel should succeed")
	require.NotNil(t, out.LanguageModel)
	assert.Equal(t, "en-US", string(out.LanguageModel.LanguageCode))
}

func verifyMegaBatch53Workspaces(ctx context.Context, t *testing.T) {
	t.Helper()
	client := workspacessvc53.NewFromConfig(megaConfig(t), func(o *workspacessvc53.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.DescribeConnectionAliases(ctx, &workspacessvc53.DescribeConnectionAliasesInput{})
	require.NoError(t, err, "DescribeConnectionAliases should succeed")
	findBy(t, out.ConnectionAliases, func(a workspacestypes53.ConnectionAlias) bool {
		return aws.ToString(a.ConnectionString) == "mega-batch-53.workspaces.example.com"
	}, "connection alias mega-batch-53.workspaces.example.com")
}

func verifyMegaBatch53NetworkManager(ctx context.Context, t *testing.T) {
	t.Helper()
	client := networkmanagersvc53.NewFromConfig(megaConfig(t), func(o *networkmanagersvc53.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	gnOut, err := client.DescribeGlobalNetworks(ctx, &networkmanagersvc53.DescribeGlobalNetworksInput{})
	require.NoError(t, err, "DescribeGlobalNetworks should succeed")
	gn := findBy(t, gnOut.GlobalNetworks, func(n networkmanagertypes53.GlobalNetwork) bool {
		return aws.ToString(n.Description) == "mega-batch-53-global-network"
	}, "global network mega-batch-53-global-network")

	connOut, err := client.GetConnections(ctx, &networkmanagersvc53.GetConnectionsInput{
		GlobalNetworkId: gn.GlobalNetworkId,
	})
	require.NoError(t, err, "GetConnections should succeed")
	require.Len(t, connOut.Connections, 1)
}

func verifyMegaBatch53NetworkMonitor(ctx context.Context, t *testing.T) {
	t.Helper()
	client := networkmonitorsvc53.NewFromConfig(megaConfig(t), func(o *networkmonitorsvc53.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	monOut, err := client.GetMonitor(ctx, &networkmonitorsvc53.GetMonitorInput{
		MonitorName: aws.String("mega-batch-53-monitor"),
	})
	require.NoError(t, err, "GetMonitor should succeed")
	require.Len(t, monOut.Probes, 1)

	probeOut, err := client.GetProbe(ctx, &networkmonitorsvc53.GetProbeInput{
		MonitorName: aws.String("mega-batch-53-monitor"),
		ProbeId:     monOut.Probes[0].ProbeId,
	})
	require.NoError(t, err, "GetProbe should succeed")
	assert.Equal(t, "10.213.1.50", aws.ToString(probeOut.Destination))
	assert.EqualValues(t, 443, aws.ToInt32(probeOut.DestinationPort))
}

func verifyMegaBatch53ElasticBeanstalk(ctx context.Context, t *testing.T) {
	t.Helper()
	client := elasticbeanstalksvc53.NewFromConfig(megaConfig(t), func(o *elasticbeanstalksvc53.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	versOut, err := client.DescribeApplicationVersions(ctx, &elasticbeanstalksvc53.DescribeApplicationVersionsInput{
		ApplicationName: aws.String("mega-batch-53-eb-app"),
	})
	require.NoError(t, err, "DescribeApplicationVersions should succeed")
	require.Len(t, versOut.ApplicationVersions, 1)
	require.NotNil(t, versOut.ApplicationVersions[0].SourceBundle)
	assert.Equal(t, "mega-batch-53-datafeed", aws.ToString(versOut.ApplicationVersions[0].SourceBundle.S3Bucket))

	cfgOut, err := client.DescribeConfigurationSettings(ctx, &elasticbeanstalksvc53.DescribeConfigurationSettingsInput{
		ApplicationName: aws.String("mega-batch-53-eb-app"),
		TemplateName:    aws.String("mega-batch-53-eb-template"),
	})
	require.NoError(t, err, "DescribeConfigurationSettings should succeed")
	require.Len(t, cfgOut.ConfigurationSettings, 1)
	assert.Equal(t,
		"64bit Amazon Linux 2023 v4.1.1 running Python 3.11",
		aws.ToString(cfgOut.ConfigurationSettings[0].SolutionStackName),
	)
}

func verifyMegaBatch53RDS(ctx context.Context, t *testing.T) {
	t.Helper()
	client := rdssvc53.NewFromConfig(megaConfig(t), func(o *rdssvc53.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.DescribeDBClusterSnapshots(ctx, &rdssvc53.DescribeDBClusterSnapshotsInput{
		DBClusterSnapshotIdentifier: aws.String("mega-batch-53-rds-snap-copy"),
	})
	require.NoError(t, err, "DescribeDBClusterSnapshots should succeed")
	require.Len(t, out.DBClusterSnapshots, 1)
	assert.Contains(t, aws.ToString(out.DBClusterSnapshots[0].SourceDBClusterSnapshotArn), "mega-batch-53-rds-snap")
}

func verifyMegaBatch53EKS(ctx context.Context, t *testing.T) {
	t.Helper()
	client := eksvc53.NewFromConfig(megaConfig(t), func(o *eksvc53.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	idpOut, err := client.DescribeIdentityProviderConfig(ctx, &eksvc53.DescribeIdentityProviderConfigInput{
		ClusterName: aws.String("mega-batch-53-eks"),
		IdentityProviderConfig: &ekstypes53.IdentityProviderConfig{
			Name: aws.String("mega-batch-53-idp"),
			Type: aws.String("oidc"),
		},
	})
	require.NoError(t, err, "DescribeIdentityProviderConfig should succeed")
	require.NotNil(t, idpOut.IdentityProviderConfig)
	require.NotNil(t, idpOut.IdentityProviderConfig.Oidc)
	assert.Equal(t, "mega-batch-53-client", aws.ToString(idpOut.IdentityProviderConfig.Oidc.ClientId))

	assocOut, err := client.ListPodIdentityAssociations(ctx, &eksvc53.ListPodIdentityAssociationsInput{
		ClusterName: aws.String("mega-batch-53-eks"),
	})
	require.NoError(t, err, "ListPodIdentityAssociations should succeed")
	findBy(t, assocOut.Associations, func(a ekstypes53.PodIdentityAssociationSummary) bool {
		return aws.ToString(a.ServiceAccount) == "mega-batch-53-sa"
	}, "pod identity association for service account mega-batch-53-sa")
}
