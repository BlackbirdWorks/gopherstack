package integration_test

import (
	"context"
	"encoding/base64"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	aasdk "github.com/aws/aws-sdk-go-v2/service/accessanalyzer"
	aatypes "github.com/aws/aws-sdk-go-v2/service/accessanalyzer/types"
	amplifysdk "github.com/aws/aws-sdk-go-v2/service/amplify"
	bedrockagentsdk "github.com/aws/aws-sdk-go-v2/service/bedrockagent"
	cleanroomssdk "github.com/aws/aws-sdk-go-v2/service/cleanrooms"
	cleanroomstypes "github.com/aws/aws-sdk-go-v2/service/cleanrooms/types"
	detectivesdk "github.com/aws/aws-sdk-go-v2/service/detective"
	dlmsdk "github.com/aws/aws-sdk-go-v2/service/dlm"
	dlmtypes "github.com/aws/aws-sdk-go-v2/service/dlm/types"
	fissdk "github.com/aws/aws-sdk-go-v2/service/fis"
	fistypes "github.com/aws/aws-sdk-go-v2/service/fis/types"
	grafanasdk "github.com/aws/aws-sdk-go-v2/service/grafana"
	grafanatypes "github.com/aws/aws-sdk-go-v2/service/grafana/types"
	iotdataplanesdk "github.com/aws/aws-sdk-go-v2/service/iotdataplane"
	managedblockchainsdk "github.com/aws/aws-sdk-go-v2/service/managedblockchain"
	managedblockchaintypes "github.com/aws/aws-sdk-go-v2/service/managedblockchain/types"
	networkmanagersdk "github.com/aws/aws-sdk-go-v2/service/networkmanager"
	nmtypes "github.com/aws/aws-sdk-go-v2/service/networkmanager/types"
	networkmonitorsdk "github.com/aws/aws-sdk-go-v2/service/networkmonitor"
	outpostssdk "github.com/aws/aws-sdk-go-v2/service/outposts"
	securityhubsdk "github.com/aws/aws-sdk-go-v2/service/securityhub"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tagRoutingCleanupCtx returns a context for use inside t.Cleanup callbacks.
// t.Context() must not be used there: Go 1.24+ cancels it before cleanups run.
func tagRoutingCleanupCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 30*time.Second)
}

// tagProbe drives TagResource/List.../UntagResource for one already-created
// resource in one service, using that service's own distinct tag value so
// TestIntegration_TagRouting_CrossServiceIsolation can assert no probe ever
// observes another probe's value.
type tagProbe struct {
	tag     func(ctx context.Context) error
	list    func(ctx context.Context) (string, error)
	untag   func(ctx context.Context) error
	service string
	want    string
}

func newGrafanaTagProbe(ctx context.Context, t *testing.T) tagProbe {
	t.Helper()

	client := createGrafanaClient(t)

	wsOut, createErr := client.CreateWorkspace(ctx, &grafanasdk.CreateWorkspaceInput{
		AccountAccessType: grafanatypes.AccountAccessTypeCurrentAccount,
		AuthenticationProviders: []grafanatypes.AuthenticationProviderTypes{
			grafanatypes.AuthenticationProviderTypesAwsSso,
		},
		PermissionType: grafanatypes.PermissionTypeCustomerManaged,
		WorkspaceName:  aws.String("integ-tagrouting-" + uuid.NewString()[:8]),
	})
	require.NoError(t, createErr, "grafana CreateWorkspace should succeed")
	workspaceID := aws.ToString(wsOut.Workspace.Id)
	resourceARN := "arn:aws:grafana:us-east-1:000000000000:/workspaces/" + workspaceID

	t.Cleanup(func() {
		cctx, cancel := tagRoutingCleanupCtx()
		defer cancel()
		_, _ = client.DeleteWorkspace(
			cctx,
			&grafanasdk.DeleteWorkspaceInput{WorkspaceId: aws.String(workspaceID)},
		)
	})

	return tagProbe{
		service: "grafana",
		want:    "grafana-value",
		tag: func(ctx context.Context) error {
			_, tagErr := client.TagResource(ctx, &grafanasdk.TagResourceInput{
				ResourceArn: aws.String(
					resourceARN,
				), Tags: map[string]string{"env": "grafana-value"},
			})

			return tagErr
		},
		list: func(ctx context.Context) (string, error) {
			out, listErr := client.ListTagsForResource(
				ctx,
				&grafanasdk.ListTagsForResourceInput{ResourceArn: aws.String(resourceARN)},
			)
			if listErr != nil {
				return "", listErr
			}

			return out.Tags["env"], nil
		},
		untag: func(ctx context.Context) error {
			_, untagErr := client.UntagResource(ctx, &grafanasdk.UntagResourceInput{
				ResourceArn: aws.String(resourceARN), TagKeys: []string{"env"},
			})

			return untagErr
		},
	}
}

func newNetworkManagerTagProbe(ctx context.Context, t *testing.T) tagProbe {
	t.Helper()

	client := createNetworkManagerClient(t)

	gnOut, createErr := client.CreateGlobalNetwork(
		ctx,
		&networkmanagersdk.CreateGlobalNetworkInput{},
	)
	require.NoError(t, createErr, "networkmanager CreateGlobalNetwork should succeed")
	gnID := gnOut.GlobalNetwork.GlobalNetworkId
	resourceARN := aws.ToString(gnOut.GlobalNetwork.GlobalNetworkArn)

	t.Cleanup(func() {
		cctx, cancel := tagRoutingCleanupCtx()
		defer cancel()
		_, _ = client.DeleteGlobalNetwork(
			cctx,
			&networkmanagersdk.DeleteGlobalNetworkInput{GlobalNetworkId: gnID},
		)
	})

	return tagProbe{
		service: "networkmanager",
		want:    "networkmanager-value",
		tag: func(ctx context.Context) error {
			_, tagErr := client.TagResource(ctx, &networkmanagersdk.TagResourceInput{
				ResourceArn: aws.String(resourceARN),
				Tags: []nmtypes.Tag{
					{Key: aws.String("env"), Value: aws.String("networkmanager-value")},
				},
			})

			return tagErr
		},
		list: func(ctx context.Context) (string, error) {
			out, listErr := client.ListTagsForResource(
				ctx,
				&networkmanagersdk.ListTagsForResourceInput{ResourceArn: aws.String(resourceARN)},
			)
			if listErr != nil {
				return "", listErr
			}

			for _, tg := range out.TagList {
				if aws.ToString(tg.Key) == "env" {
					return aws.ToString(tg.Value), nil
				}
			}

			return "", nil
		},
		untag: func(ctx context.Context) error {
			_, untagErr := client.UntagResource(ctx, &networkmanagersdk.UntagResourceInput{
				ResourceArn: aws.String(resourceARN), TagKeys: []string{"env"},
			})

			return untagErr
		},
	}
}

func newBedrockAgentTagProbe(ctx context.Context, t *testing.T) tagProbe {
	t.Helper()

	client := createBedrockAgentClient(t)

	agentOut, createErr := client.CreateAgent(ctx, &bedrockagentsdk.CreateAgentInput{
		AgentName:            aws.String("integ-tagrouting-" + uuid.NewString()[:8]),
		FoundationModel:      aws.String("anthropic.claude-v2"),
		AgentResourceRoleArn: aws.String("arn:aws:iam::000000000000:role/AmazonBedrockRole"),
	})
	require.NoError(t, createErr, "bedrockagent CreateAgent should succeed")
	agentID := aws.ToString(agentOut.Agent.AgentId)
	resourceARN := aws.ToString(agentOut.Agent.AgentArn)

	t.Cleanup(func() {
		cctx, cancel := tagRoutingCleanupCtx()
		defer cancel()
		_, _ = client.DeleteAgent(
			cctx,
			&bedrockagentsdk.DeleteAgentInput{AgentId: aws.String(agentID)},
		)
	})

	return tagProbe{
		service: "bedrockagent",
		want:    "bedrockagent-value",
		tag: func(ctx context.Context) error {
			_, tagErr := client.TagResource(ctx, &bedrockagentsdk.TagResourceInput{
				ResourceArn: aws.String(
					resourceARN,
				), Tags: map[string]string{"env": "bedrockagent-value"},
			})

			return tagErr
		},
		list: func(ctx context.Context) (string, error) {
			out, listErr := client.ListTagsForResource(
				ctx,
				&bedrockagentsdk.ListTagsForResourceInput{ResourceArn: aws.String(resourceARN)},
			)
			if listErr != nil {
				return "", listErr
			}

			return out.Tags["env"], nil
		},
		untag: func(ctx context.Context) error {
			_, untagErr := client.UntagResource(ctx, &bedrockagentsdk.UntagResourceInput{
				ResourceArn: aws.String(resourceARN), TagKeys: []string{"env"},
			})

			return untagErr
		},
	}
}

// createCleanroomsClient returns a Clean Rooms client pointed at the shared test container.
func createCleanroomsClient(t *testing.T) *cleanroomssdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return cleanroomssdk.NewFromConfig(
		cfg,
		func(o *cleanroomssdk.Options) { o.BaseEndpoint = aws.String(endpoint) },
	)
}

func newCleanroomsTagProbe(ctx context.Context, t *testing.T) tagProbe {
	t.Helper()

	client := createCleanroomsClient(t)

	out, createErr := client.CreateCollaboration(ctx, &cleanroomssdk.CreateCollaborationInput{
		Name:               aws.String("integ-tagrouting-" + uuid.NewString()[:8]),
		Description:        aws.String("tag routing regression"),
		CreatorDisplayName: aws.String("integ-creator"),
		CreatorMemberAbilities: []cleanroomstypes.MemberAbility{
			cleanroomstypes.MemberAbilityCanQuery,
		},
		Members:        []cleanroomstypes.MemberSpecification{},
		QueryLogStatus: cleanroomstypes.CollaborationQueryLogStatusDisabled,
	})
	require.NoError(t, createErr, "cleanrooms CreateCollaboration should succeed")
	resourceARN := aws.ToString(out.Collaboration.Arn)

	return tagProbe{
		service: "cleanrooms",
		want:    "cleanrooms-value",
		tag: func(ctx context.Context) error {
			_, tagErr := client.TagResource(ctx, &cleanroomssdk.TagResourceInput{
				ResourceArn: aws.String(
					resourceARN,
				), Tags: map[string]string{"env": "cleanrooms-value"},
			})

			return tagErr
		},
		list: func(ctx context.Context) (string, error) {
			out, listErr := client.ListTagsForResource(
				ctx,
				&cleanroomssdk.ListTagsForResourceInput{ResourceArn: aws.String(resourceARN)},
			)
			if listErr != nil {
				return "", listErr
			}

			return out.Tags["env"], nil
		},
		untag: func(ctx context.Context) error {
			_, untagErr := client.UntagResource(ctx, &cleanroomssdk.UntagResourceInput{
				ResourceArn: aws.String(resourceARN), TagKeys: []string{"env"},
			})

			return untagErr
		},
	}
}

func newAmplifyTagProbe(ctx context.Context, t *testing.T) tagProbe {
	t.Helper()

	client := createAmplifyClient(t)

	out, createErr := client.CreateApp(ctx, &amplifysdk.CreateAppInput{
		Name: aws.String("integ-tagrouting-" + uuid.NewString()[:8]),
	})
	require.NoError(t, createErr, "amplify CreateApp should succeed")
	appID := aws.ToString(out.App.AppId)
	resourceARN := aws.ToString(out.App.AppArn)

	t.Cleanup(func() {
		cctx, cancel := tagRoutingCleanupCtx()
		defer cancel()
		_, _ = client.DeleteApp(cctx, &amplifysdk.DeleteAppInput{AppId: aws.String(appID)})
	})

	return tagProbe{
		service: "amplify",
		want:    "amplify-value",
		tag: func(ctx context.Context) error {
			_, tagErr := client.TagResource(ctx, &amplifysdk.TagResourceInput{
				ResourceArn: aws.String(
					resourceARN,
				), Tags: map[string]string{"env": "amplify-value"},
			})

			return tagErr
		},
		list: func(ctx context.Context) (string, error) {
			out, listErr := client.ListTagsForResource(
				ctx,
				&amplifysdk.ListTagsForResourceInput{ResourceArn: aws.String(resourceARN)},
			)
			if listErr != nil {
				return "", listErr
			}

			return out.Tags["env"], nil
		},
		untag: func(ctx context.Context) error {
			_, untagErr := client.UntagResource(ctx, &amplifysdk.UntagResourceInput{
				ResourceArn: aws.String(resourceARN), TagKeys: []string{"env"},
			})

			return untagErr
		},
	}
}

func newDetectiveTagProbe(ctx context.Context, t *testing.T) tagProbe {
	t.Helper()

	client := createDetectiveClient(t)

	out, createErr := client.CreateGraph(ctx, &detectivesdk.CreateGraphInput{})
	require.NoError(t, createErr, "detective CreateGraph should succeed")
	resourceARN := aws.ToString(out.GraphArn)

	t.Cleanup(func() {
		cctx, cancel := tagRoutingCleanupCtx()
		defer cancel()
		_, _ = client.DeleteGraph(
			cctx,
			&detectivesdk.DeleteGraphInput{GraphArn: aws.String(resourceARN)},
		)
	})

	return tagProbe{
		service: "detective",
		want:    "detective-value",
		tag: func(ctx context.Context) error {
			_, tagErr := client.TagResource(ctx, &detectivesdk.TagResourceInput{
				ResourceArn: aws.String(
					resourceARN,
				), Tags: map[string]string{"env": "detective-value"},
			})

			return tagErr
		},
		list: func(ctx context.Context) (string, error) {
			out, listErr := client.ListTagsForResource(
				ctx,
				&detectivesdk.ListTagsForResourceInput{ResourceArn: aws.String(resourceARN)},
			)
			if listErr != nil {
				return "", listErr
			}

			return out.Tags["env"], nil
		},
		untag: func(ctx context.Context) error {
			_, untagErr := client.UntagResource(ctx, &detectivesdk.UntagResourceInput{
				ResourceArn: aws.String(resourceARN), TagKeys: []string{"env"},
			})

			return untagErr
		},
	}
}

// createDLMClient returns a DLM client pointed at the shared test container.
func createDLMClient(t *testing.T) *dlmsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return dlmsdk.NewFromConfig(
		cfg,
		func(o *dlmsdk.Options) { o.BaseEndpoint = aws.String(endpoint) },
	)
}

func newDLMTagProbe(ctx context.Context, t *testing.T) tagProbe {
	t.Helper()

	client := createDLMClient(t)

	out, createErr := client.CreateLifecyclePolicy(ctx, &dlmsdk.CreateLifecyclePolicyInput{
		Description:      aws.String("integ tag routing policy"),
		ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/AWSDataLifecycleManagerRole"),
		State:            dlmtypes.SettablePolicyStateValuesEnabled,
	})
	require.NoError(t, createErr, "dlm CreateLifecyclePolicy should succeed")
	policyID := aws.ToString(out.PolicyId)
	resourceARN := "arn:aws:dlm:us-east-1:000000000000:policy/" + policyID

	t.Cleanup(func() {
		cctx, cancel := tagRoutingCleanupCtx()
		defer cancel()
		_, _ = client.DeleteLifecyclePolicy(
			cctx,
			&dlmsdk.DeleteLifecyclePolicyInput{PolicyId: aws.String(policyID)},
		)
	})

	return tagProbe{
		service: "dlm",
		want:    "dlm-value",
		tag: func(ctx context.Context) error {
			_, tagErr := client.TagResource(ctx, &dlmsdk.TagResourceInput{
				ResourceArn: aws.String(resourceARN), Tags: map[string]string{"env": "dlm-value"},
			})

			return tagErr
		},
		list: func(ctx context.Context) (string, error) {
			out, listErr := client.ListTagsForResource(
				ctx,
				&dlmsdk.ListTagsForResourceInput{ResourceArn: aws.String(resourceARN)},
			)
			if listErr != nil {
				return "", listErr
			}

			return out.Tags["env"], nil
		},
		untag: func(ctx context.Context) error {
			_, untagErr := client.UntagResource(ctx, &dlmsdk.UntagResourceInput{
				ResourceArn: aws.String(resourceARN), TagKeys: []string{"env"},
			})

			return untagErr
		},
	}
}

// createFISClient returns a FIS client pointed at the shared test container.
func createFISClient(t *testing.T) *fissdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return fissdk.NewFromConfig(
		cfg,
		func(o *fissdk.Options) { o.BaseEndpoint = aws.String(endpoint) },
	)
}

func newFISTagProbe(ctx context.Context, t *testing.T) tagProbe {
	t.Helper()

	client := createFISClient(t)

	out, createErr := client.CreateExperimentTemplate(ctx, &fissdk.CreateExperimentTemplateInput{
		ClientToken: aws.String(uuid.NewString()),
		Description: aws.String("integ tag routing template"),
		RoleArn:     aws.String("arn:aws:iam::000000000000:role/FisRole"),
		StopConditions: []fistypes.CreateExperimentTemplateStopConditionInput{
			{Source: aws.String("none")},
		},
		Actions: map[string]fistypes.CreateExperimentTemplateActionInput{},
		Targets: map[string]fistypes.CreateExperimentTemplateTargetInput{},
	})
	require.NoError(t, createErr, "fis CreateExperimentTemplate should succeed")
	templateID := aws.ToString(out.ExperimentTemplate.Id)
	resourceARN := aws.ToString(out.ExperimentTemplate.Arn)

	t.Cleanup(func() {
		cctx, cancel := tagRoutingCleanupCtx()
		defer cancel()
		_, _ = client.DeleteExperimentTemplate(
			cctx,
			&fissdk.DeleteExperimentTemplateInput{Id: aws.String(templateID)},
		)
	})

	return tagProbe{
		service: "fis",
		want:    "fis-value",
		tag: func(ctx context.Context) error {
			_, tagErr := client.TagResource(ctx, &fissdk.TagResourceInput{
				ResourceArn: aws.String(resourceARN), Tags: map[string]string{"env": "fis-value"},
			})

			return tagErr
		},
		list: func(ctx context.Context) (string, error) {
			out, listErr := client.ListTagsForResource(
				ctx,
				&fissdk.ListTagsForResourceInput{ResourceArn: aws.String(resourceARN)},
			)
			if listErr != nil {
				return "", listErr
			}

			return out.Tags["env"], nil
		},
		untag: func(ctx context.Context) error {
			_, untagErr := client.UntagResource(ctx, &fissdk.UntagResourceInput{
				ResourceArn: aws.String(resourceARN), TagKeys: []string{"env"},
			})

			return untagErr
		},
	}
}

func newAccessAnalyzerTagProbe(ctx context.Context, t *testing.T) tagProbe {
	t.Helper()

	client := createAccessAnalyzerClient(t)

	out, createErr := client.CreateAnalyzer(ctx, &aasdk.CreateAnalyzerInput{
		AnalyzerName: aws.String("integ-tagrouting-" + uuid.NewString()[:8]),
		Type:         aatypes.TypeAccount,
	})
	require.NoError(t, createErr, "accessanalyzer CreateAnalyzer should succeed")
	resourceARN := aws.ToString(out.Arn)

	t.Cleanup(func() {
		cctx, cancel := tagRoutingCleanupCtx()
		defer cancel()
		_, _ = client.DeleteAnalyzer(
			cctx,
			&aasdk.DeleteAnalyzerInput{AnalyzerName: aws.String(aws.ToString(out.Arn))},
		)
	})

	return tagProbe{
		service: "accessanalyzer",
		want:    "accessanalyzer-value",
		tag: func(ctx context.Context) error {
			_, tagErr := client.TagResource(ctx, &aasdk.TagResourceInput{
				ResourceArn: aws.String(
					resourceARN,
				), Tags: map[string]string{"env": "accessanalyzer-value"},
			})

			return tagErr
		},
		list: func(ctx context.Context) (string, error) {
			out, listErr := client.ListTagsForResource(
				ctx,
				&aasdk.ListTagsForResourceInput{ResourceArn: aws.String(resourceARN)},
			)
			if listErr != nil {
				return "", listErr
			}

			return out.Tags["env"], nil
		},
		untag: func(ctx context.Context) error {
			_, untagErr := client.UntagResource(ctx, &aasdk.UntagResourceInput{
				ResourceArn: aws.String(resourceARN), TagKeys: []string{"env"},
			})

			return untagErr
		},
	}
}

// createManagedBlockchainTagClient returns a Managed Blockchain client
// pointed at the shared test container. managedblockchain_test.go's own
// client helper lives behind a "//go:build integration" tag and is not
// visible to this file's default (untagged) build.
func createManagedBlockchainTagClient(t *testing.T) *managedblockchainsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return managedblockchainsdk.NewFromConfig(cfg, func(o *managedblockchainsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

func newManagedBlockchainTagProbe(ctx context.Context, t *testing.T) tagProbe {
	t.Helper()

	client := createManagedBlockchainTagClient(t)

	out, createErr := client.CreateNetwork(ctx, &managedblockchainsdk.CreateNetworkInput{
		ClientRequestToken: aws.String(uuid.NewString()),
		Framework:          managedblockchaintypes.FrameworkHyperledgerFabric,
		FrameworkVersion:   aws.String("1.4"),
		Name:               aws.String("integ-tagrouting-" + uuid.NewString()[:8]),
		VotingPolicy: &managedblockchaintypes.VotingPolicy{
			ApprovalThresholdPolicy: &managedblockchaintypes.ApprovalThresholdPolicy{
				ThresholdPercentage:     aws.Int32(50),
				ProposalDurationInHours: aws.Int32(24),
				ThresholdComparator:     managedblockchaintypes.ThresholdComparatorGreaterThan,
			},
		},
		MemberConfiguration: &managedblockchaintypes.MemberConfiguration{
			Name: aws.String("integ-tagrouting-member"),
			FrameworkConfiguration: &managedblockchaintypes.MemberFrameworkConfiguration{
				Fabric: &managedblockchaintypes.MemberFabricConfiguration{
					AdminUsername: aws.String("admin"),
					AdminPassword: aws.String("Password123!"),
				},
			},
		},
	})
	require.NoError(t, createErr, "managedblockchain CreateNetwork should succeed")

	getOut, createErr := client.GetNetwork(
		ctx,
		&managedblockchainsdk.GetNetworkInput{NetworkId: out.NetworkId},
	)
	require.NoError(t, createErr, "managedblockchain GetNetwork should succeed")
	resourceARN := aws.ToString(getOut.Network.Arn)

	return tagProbe{
		service: "managedblockchain",
		want:    "managedblockchain-value",
		tag: func(ctx context.Context) error {
			_, tagErr := client.TagResource(ctx, &managedblockchainsdk.TagResourceInput{
				ResourceArn: aws.String(
					resourceARN,
				), Tags: map[string]string{"env": "managedblockchain-value"},
			})

			return tagErr
		},
		list: func(ctx context.Context) (string, error) {
			out, listErr := client.ListTagsForResource(
				ctx,
				&managedblockchainsdk.ListTagsForResourceInput{
					ResourceArn: aws.String(resourceARN),
				},
			)
			if listErr != nil {
				return "", listErr
			}

			return out.Tags["env"], nil
		},
		untag: func(ctx context.Context) error {
			_, untagErr := client.UntagResource(ctx, &managedblockchainsdk.UntagResourceInput{
				ResourceArn: aws.String(resourceARN), TagKeys: []string{"env"},
			})

			return untagErr
		},
	}
}

// createNetworkMonitorClient returns a Network Monitor client pointed at the shared test container.
func createNetworkMonitorClient(t *testing.T) *networkmonitorsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return networkmonitorsdk.NewFromConfig(
		cfg,
		func(o *networkmonitorsdk.Options) { o.BaseEndpoint = aws.String(endpoint) },
	)
}

func newNetworkMonitorTagProbe(ctx context.Context, t *testing.T) tagProbe {
	t.Helper()

	client := createNetworkMonitorClient(t)

	out, createErr := client.CreateMonitor(ctx, &networkmonitorsdk.CreateMonitorInput{
		MonitorName: aws.String("integ-tagrouting-" + uuid.NewString()[:8]),
	})
	require.NoError(t, createErr, "networkmonitor CreateMonitor should succeed")
	monitorName := aws.ToString(out.MonitorName)
	resourceARN := aws.ToString(out.MonitorArn)

	t.Cleanup(func() {
		cctx, cancel := tagRoutingCleanupCtx()
		defer cancel()
		_, _ = client.DeleteMonitor(
			cctx,
			&networkmonitorsdk.DeleteMonitorInput{MonitorName: aws.String(monitorName)},
		)
	})

	return tagProbe{
		service: "networkmonitor",
		want:    "networkmonitor-value",
		tag: func(ctx context.Context) error {
			_, tagErr := client.TagResource(ctx, &networkmonitorsdk.TagResourceInput{
				ResourceArn: aws.String(
					resourceARN,
				), Tags: map[string]string{"env": "networkmonitor-value"},
			})

			return tagErr
		},
		list: func(ctx context.Context) (string, error) {
			out, listErr := client.ListTagsForResource(
				ctx,
				&networkmonitorsdk.ListTagsForResourceInput{ResourceArn: aws.String(resourceARN)},
			)
			if listErr != nil {
				return "", listErr
			}

			return out.Tags["env"], nil
		},
		untag: func(ctx context.Context) error {
			_, untagErr := client.UntagResource(ctx, &networkmonitorsdk.UntagResourceInput{
				ResourceArn: aws.String(resourceARN), TagKeys: []string{"env"},
			})

			return untagErr
		},
	}
}

// createOutpostsClient returns an Outposts client pointed at the shared test container.
func createOutpostsClient(t *testing.T) *outpostssdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return outpostssdk.NewFromConfig(
		cfg,
		func(o *outpostssdk.Options) { o.BaseEndpoint = aws.String(endpoint) },
	)
}

func newOutpostsTagProbe(ctx context.Context, t *testing.T) tagProbe {
	t.Helper()

	client := createOutpostsClient(t)

	out, createErr := client.CreateSite(ctx, &outpostssdk.CreateSiteInput{
		Name: aws.String("integ-tagrouting-" + uuid.NewString()[:8]),
	})
	require.NoError(t, createErr, "outposts CreateSite should succeed")
	siteID := aws.ToString(out.Site.SiteId)
	resourceARN := aws.ToString(out.Site.SiteArn)

	t.Cleanup(func() {
		cctx, cancel := tagRoutingCleanupCtx()
		defer cancel()
		_, _ = client.DeleteSite(cctx, &outpostssdk.DeleteSiteInput{SiteId: aws.String(siteID)})
	})

	return tagProbe{
		service: "outposts",
		want:    "outposts-value",
		tag: func(ctx context.Context) error {
			_, tagErr := client.TagResource(ctx, &outpostssdk.TagResourceInput{
				ResourceArn: aws.String(
					resourceARN,
				), Tags: map[string]string{"env": "outposts-value"},
			})

			return tagErr
		},
		list: func(ctx context.Context) (string, error) {
			out, listErr := client.ListTagsForResource(
				ctx,
				&outpostssdk.ListTagsForResourceInput{ResourceArn: aws.String(resourceARN)},
			)
			if listErr != nil {
				return "", listErr
			}

			return out.Tags["env"], nil
		},
		untag: func(ctx context.Context) error {
			_, untagErr := client.UntagResource(ctx, &outpostssdk.UntagResourceInput{
				ResourceArn: aws.String(resourceARN), TagKeys: []string{"env"},
			})

			return untagErr
		},
	}
}

func newSecurityHubTagProbe(ctx context.Context, t *testing.T) tagProbe {
	t.Helper()

	client := createSecurityHubClient(t)

	_, createErr := client.EnableSecurityHub(ctx, &securityhubsdk.EnableSecurityHubInput{})
	if createErr != nil {
		require.ErrorContains(
			t,
			createErr,
			"already",
			"EnableSecurityHub should succeed or already be enabled",
		)
	}

	descOut, createErr := client.DescribeHub(ctx, &securityhubsdk.DescribeHubInput{})
	require.NoError(t, createErr, "securityhub DescribeHub should succeed")
	resourceARN := aws.ToString(descOut.HubArn)

	return tagProbe{
		service: "securityhub",
		want:    "securityhub-value",
		tag: func(ctx context.Context) error {
			_, tagErr := client.TagResource(ctx, &securityhubsdk.TagResourceInput{
				ResourceArn: aws.String(
					resourceARN,
				), Tags: map[string]string{"env": "securityhub-value"},
			})

			return tagErr
		},
		list: func(ctx context.Context) (string, error) {
			out, listErr := client.ListTagsForResource(
				ctx, &securityhubsdk.ListTagsForResourceInput{ResourceArn: aws.String(resourceARN)},
			)
			if listErr != nil {
				return "", listErr
			}

			return out.Tags["env"], nil
		},
		untag: func(ctx context.Context) error {
			_, untagErr := client.UntagResource(ctx, &securityhubsdk.UntagResourceInput{
				ResourceArn: aws.String(resourceARN), TagKeys: []string{"env"},
			})

			return untagErr
		},
	}
}

// createBedrockAgentClient returns a Bedrock Agent client pointed at the shared test container.
func createBedrockAgentClient(t *testing.T) *bedrockagentsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return bedrockagentsdk.NewFromConfig(cfg, func(o *bedrockagentsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_TagRouting_CrossServiceIsolation exercises TagResource/
// ListTagsForResource(or GetTags)/UntagResource for every service whose
// RouteMatcher claims the shared "/tags/{resourceArn}" path, in one test
// binary run against the shared multi-service router. Each service's own
// tagging test previously ran and passed in isolation while a routing
// regression silently misrouted another service's tag requests -- this test
// tags every probed resource with a distinct value first, THEN lists every
// one, so a future MatchPriority change or a missing ARN/scope guard that
// steals another service's prefix shows up immediately as a cross-
// contaminated tag value or an outright routing failure. See
// bedrockagent/cleanrooms/grafana/outposts/resiliencehub/mgn/networkmanager's
// Handler.RouteMatcher and pkgs/httputils.MatchesTaggedResourceARN.
//
// apigateway and pipes are deliberately NOT probed here: both have their own
// pre-existing, unrelated tag-storage/wire-decoding bugs (apigateway's
// UntagResource rejects the real tagKeys query-param shape; pipes' TagResource
// mutates a copy that ListTagsForResource never sees) uncovered while writing
// this test. Neither is a routing collision, so fixing them is out of scope
// here -- tracked as follow-up.
func TestIntegration_TagRouting_CrossServiceIsolation(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()

	probeFactories := []func(ctx context.Context, t *testing.T) tagProbe{
		newGrafanaTagProbe,
		newNetworkManagerTagProbe,
		newBedrockAgentTagProbe,
		newCleanroomsTagProbe,
		newAmplifyTagProbe,
		newDetectiveTagProbe,
		newDLMTagProbe,
		newFISTagProbe,
		newAccessAnalyzerTagProbe,
		newManagedBlockchainTagProbe,
		newNetworkMonitorTagProbe,
		newOutpostsTagProbe,
		newSecurityHubTagProbe,
	}

	probes := make([]tagProbe, len(probeFactories))
	for i, newProbe := range probeFactories {
		probes[i] = newProbe(ctx, t)
	}

	for _, p := range probes {
		require.NoErrorf(t, p.tag(ctx), "%s TagResource should succeed", p.service)
	}

	for _, p := range probes {
		got, err := p.list(ctx)
		require.NoErrorf(t, err, "%s ListTagsForResource should succeed", p.service)
		assert.Equalf(t, p.want, got, "%s must see only its own tag value", p.service)
	}

	for _, p := range probes {
		require.NoErrorf(t, p.untag(ctx), "%s UntagResource should succeed", p.service)
	}
}

// registerIoTDataPlaneConnection seeds a connection via the gopherstack-only
// admin path (POST /_admin/connections/{clientId}). RegisterConnection has
// no real AWS wire equivalent, so it isn't reachable through the SDK client
// -- this raw HTTP call is the only way to seed one for the real GetConnection
// wire path exercised below.
func registerIoTDataPlaneConnection(ctx context.Context, t *testing.T, clientID string) {
	t.Helper()

	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost, endpoint+"/_admin/connections/"+clientID, nil,
	)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "registering iotdataplane connection should succeed")
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)
}

// TestIntegration_ConnectionsRouting_CrossServiceIsolation exercises the real
// AWS "GET /connections/{id}" wire path for both IoT Data Plane and Outposts
// in one test binary run against the shared multi-service router --
// iotdataplane's own GetConnection collided with Outposts' GetConnection on
// this exact path (gopherstack-vpoh), and each service's own test suite
// passing in isolation is exactly what hid it: neither ran the other's
// requests through the shared router in the same process. See
// TestIntegration_TagRouting_CrossServiceIsolation for the equivalent
// "/tags/" coverage, and services/iotdataplane's Handler.RouteMatcher /
// pkgs/httputils.ScopedPrefixMatch for the fix.
func TestIntegration_ConnectionsRouting_CrossServiceIsolation(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()

	clientID := "integ-conn-" + uuid.NewString()[:8]
	registerIoTDataPlaneConnection(ctx, t, clientID)

	outpostsClient := createOutpostsClient(t)
	site := createTestSite(ctx, t, outpostsClient)
	outpost := createTestOutpost(ctx, t, outpostsClient, aws.ToString(site.SiteId))
	assetID := seededAssetID(ctx, t, outpostsClient, aws.ToString(outpost.OutpostId))
	clientKey := base64.StdEncoding.EncodeToString(make([]byte, 32))

	startOut, startErr := outpostsClient.StartConnection(ctx, &outpostssdk.StartConnectionInput{
		AssetId:                     aws.String(assetID),
		ClientPublicKey:             aws.String(clientKey),
		NetworkInterfaceDeviceIndex: 0,
	})
	require.NoError(t, startErr, "outposts StartConnection should succeed")
	connectionID := aws.ToString(startOut.ConnectionId)

	iotDataClient := createIoTDataPlaneClient(t)

	iotOut, iotErr := iotDataClient.GetConnection(ctx, &iotdataplanesdk.GetConnectionInput{
		ClientId: aws.String(clientID),
	})
	require.NoError(t, iotErr, "iotdataplane GetConnection should succeed")
	assert.Equal(t, clientID, aws.ToString(iotOut.ClientId))
	assert.True(t, iotOut.Connected)

	outOut, outErr := outpostsClient.GetConnection(ctx, &outpostssdk.GetConnectionInput{
		ConnectionId: aws.String(connectionID),
	})
	require.NoError(t, outErr, "outposts GetConnection should succeed")
	assert.Equal(t, connectionID, aws.ToString(outOut.ConnectionId))
	require.NotNil(t, outOut.ConnectionDetails)
	assert.Equal(t, clientKey, aws.ToString(outOut.ConnectionDetails.ClientPublicKey))
}
